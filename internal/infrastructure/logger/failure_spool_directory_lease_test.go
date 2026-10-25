package logger

import (
	"context"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	infraspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestNewLoggerRejectsEnabledSpoolWithoutRuntimeConfig(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(basePath, "opensearch")
	created, err := NewLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, created)
	require.ErrorIs(t, err, ErrFailureSpoolRuntimeConfigRequired)
	spoolRoot, resolveErr := canonicalSpoolRoot(
		loggerdurable.NewSpoolConfigAdapter(cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig),
	)
	require.NoError(t, resolveErr)
	lease, acquireErr := directorylease.Acquire("test.nil_sync_runtime_config", spoolRoot)
	require.NoError(t, acquireErr)
	lease.Release()
}

func TestNewAsyncLoggerRejectsEnabledSpoolWithoutRuntimeConfig(t *testing.T) {
	t.Parallel()

	base := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
	cfg := testAsyncFailureSpoolLoggerConfig(base)
	created, err := NewAsyncLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		nil,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, created)
	require.ErrorIs(t, err, ErrFailureSpoolRuntimeConfigRequired)
	spoolRoot, resolveErr := canonicalSpoolRoot(
		loggerdurable.NewSpoolConfigAdapter(base.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig),
	)
	require.NoError(t, resolveErr)
	lease, acquireErr := directorylease.Acquire("test.nil_async_runtime_config", spoolRoot)
	require.NoError(t, acquireErr)
	lease.Release()
}

func TestNewLoggerOwnsRootUntilClose(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(root, "opensearch")
	runtimeCfg := testFailureSpoolRuntimeConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first, err := NewLogger(
		ctx,
		cfg,
		streamrouting.None,
		runtimeCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	owned := first.(*topLevelLoggerRuntime)
	require.Len(t, owned.failureSpoolPipelines.pipelines, 1)
	pipeline := owned.failureSpoolPipelines.pipelines[0]
	require.Equal(t, pkglogger.LoggerContractSync, pipeline.id.contractKind)
	require.Equal(t, pkglogger.LogOutputOpenSearch, pipeline.id.backend)
	require.Len(t, pipeline.writers, 1)
	require.NotNil(t, pipeline.writers[0].handler)
	require.NotNil(t, pipeline.runner)
	require.NotNil(t, pipeline.runner.supervisor)
	require.NotNil(t, pipeline.directoryLease)
	require.Equal(t, pipeline.root, filepath.Clean(pipeline.writers[0].handler.Store.RootDir.String()))
	spoolWriter := pipeline.writers[0].handler
	spoolRunner := pipeline.runner.supervisor

	second, err := NewLogger(
		ctx,
		cfg,
		streamrouting.None,
		runtimeCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	var conflict *directorylease.ConflictError
	require.ErrorAs(t, err, &conflict)
	wantScope := failureSpoolDirectoryScopeLabel(
		pkglogger.LoggerContractSync,
		pkglogger.LogOutputOpenSearch,
	)
	require.Equal(t, wantScope, conflict.RequestedScopeLabel)
	require.Equal(t, wantScope, conflict.ExistingScopeLabel)

	require.NoError(t, first.(logging.CloseableLogger).Close())
	require.ErrorIs(
		t,
		spoolWriter.HandleFailure(context.Background(), loggerfailure.SendResult{}),
		spoolstorehandler.ErrSpoolStoreHandlerClosing,
	)
	require.False(t, spoolRunner.IsRunning())
	replacement, err := NewLogger(
		ctx,
		cfg,
		streamrouting.None,
		runtimeCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.NoError(t, replacement.(logging.CloseableLogger).Close())
}

// 構築に渡したcontextは構築処理だけに適用し、構築後に終了してもfailure spoolの再送runnerは
// Closeまで動き続けます。synctestのbubble内で、runnerの起動とcontextの終了後の状態を、
// 全goroutineが停止してから確認します（実時間の待機に依存しません）。
func TestNewLoggerKeepsFailureSpoolRunnerActiveAfterCallerContextCancellation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		created, err := NewLogger(
			ctx,
			cfg,
			streamrouting.None,
			testFailureSpoolRuntimeConfig(),
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
		)
		require.NoError(t, err)
		owned := created.(*topLevelLoggerRuntime)
		// 途中の確認が失敗しても、通常のCloseで再送runnerと終了処理のgoroutineを全て終えてから
		// bubbleを終えます。先にForceCloseすると、後片付けの再試行が仮想時刻を待ったまま残ります。
		t.Cleanup(func() { _ = boundedwait.CloseInBubble(t, "logger Close", owned.Close, owned.ForceClose) })
		supervisor := owned.failureSpoolPipelines.pipelines[0].runner.supervisor

		synctest.Wait()
		require.True(t, supervisor.IsRunning(), "the failure spool runner did not start")
		cancel()
		synctest.Wait()
		require.True(t, supervisor.IsRunning(), "ending the constructor context stopped the failure spool runner")
		require.NoError(t, boundedwait.CloseInBubble(t, "logger Close", owned.Close, owned.ForceClose))
	})
}

func TestNewLoggerRejectsAlreadyCanceledContextBeforeAcquiringFailureSpoolRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(root, "opensearch")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	created, err := NewLogger(
		ctx,
		cfg,
		streamrouting.None,
		testFailureSpoolRuntimeConfig(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, created)
	require.ErrorIs(t, err, context.Canceled)

	spoolRoot, resolveErr := canonicalSpoolRoot(
		loggerdurable.NewSpoolConfigAdapter(cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig),
	)
	require.NoError(t, resolveErr)
	lease, acquireErr := directorylease.Acquire("test.failure_spool.canceled_constructor", spoolRoot)
	require.NoError(t, acquireErr)
	lease.Release()
}

func TestNewLoggerRejectsInvalidFailureSpoolRunnerConfigBeforeReturning(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(root, "opensearch")
	runtimeCfg := testFailureSpoolRuntimeConfig()
	runtimeCfg.RunnerConfig.OpensearchConfig.Runner.Interval = -time.Second

	created, err := NewLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		runtimeCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, created)
	require.ErrorIs(t, err, infraspool.ErrInvalidRunnerInterval)

	spoolRoot, resolveErr := canonicalSpoolRoot(
		loggerdurable.NewSpoolConfigAdapter(cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig),
	)
	require.NoError(t, resolveErr)
	lease, acquireErr := directorylease.Acquire("test.failure_spool.invalid_runner", spoolRoot)
	require.NoError(t, acquireErr)
	lease.Release()
}

func TestNewAsyncLoggerRejectsInvalidFailureSpoolRunnerConfigBeforeReturning(t *testing.T) {
	t.Parallel()

	base := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
	cfg := testAsyncFailureSpoolLoggerConfig(base)
	runtimeCfg := testFailureSpoolRuntimeConfig()
	runtimeCfg.RunnerConfig.OpensearchConfig.Runner.Interval = -time.Second

	created, err := NewAsyncLogger(
		context.Background(),
		cfg,
		streamrouting.None,
		runtimeCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, created)
	require.ErrorIs(t, err, infraspool.ErrInvalidRunnerInterval)

	spoolRoot, resolveErr := canonicalSpoolRoot(
		loggerdurable.NewSpoolConfigAdapter(base.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig),
	)
	require.NoError(t, resolveErr)
	lease, acquireErr := directorylease.Acquire("test.failure_spool.invalid_async_runner", spoolRoot)
	require.NoError(t, acquireErr)
	lease.Release()
}

func TestNewAsyncLoggerOwnsRootUntilClose(t *testing.T) {
	t.Parallel()

	base := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
	cfg := testAsyncFailureSpoolLoggerConfig(base)
	runtimeCfg := testFailureSpoolRuntimeConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first, err := NewAsyncLogger(
		ctx,
		cfg,
		streamrouting.None,
		runtimeCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	owned := first.(*topLevelAsyncLoggerRuntime)
	require.Len(t, owned.failureSpoolPipelines.pipelines, 1)
	pipeline := owned.failureSpoolPipelines.pipelines[0]
	require.Equal(t, pkglogger.LoggerContractAsyncBestEffort, pipeline.id.contractKind)
	require.Equal(t, pkglogger.LogOutputOpenSearch, pipeline.id.backend)
	require.Len(t, pipeline.writers, 1)
	require.NotNil(t, pipeline.writers[0].handler)
	require.NotNil(t, pipeline.runner)
	require.NotNil(t, pipeline.runner.supervisor)
	require.NotNil(t, pipeline.directoryLease)
	require.Equal(t, pipeline.root, filepath.Clean(pipeline.writers[0].handler.Store.RootDir.String()))
	spoolWriter := pipeline.writers[0].handler
	spoolRunner := pipeline.runner.supervisor

	second, err := NewAsyncLogger(
		ctx,
		cfg,
		streamrouting.None,
		runtimeCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	require.NoError(t, first.(logging.AsyncCloseableLogger).Close())
	require.ErrorIs(
		t,
		spoolWriter.HandleFailure(context.Background(), loggerfailure.SendResult{}),
		spoolstorehandler.ErrSpoolStoreHandlerClosing,
	)
	require.False(t, spoolRunner.IsRunning())
	replacement, err := NewAsyncLogger(
		ctx,
		cfg,
		streamrouting.None,
		runtimeCfg,
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.NoError(t, replacement.(logging.AsyncCloseableLogger).Close())
}

// 非同期loggerでも、構築に渡したcontextの終了はfailure spoolの再送runnerを止めません。
// 確認の方法はTestNewLoggerKeepsFailureSpoolRunnerActiveAfterCallerContextCancellationと同じです。
func TestNewAsyncLoggerKeepsFailureSpoolRunnerActiveAfterCallerContextCancellation(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		base := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		created, err := NewAsyncLogger(
			ctx,
			testAsyncFailureSpoolLoggerConfig(base),
			streamrouting.None,
			testFailureSpoolRuntimeConfig(),
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
		)
		require.NoError(t, err)
		owned := created.(*topLevelAsyncLoggerRuntime)
		t.Cleanup(func() { _ = boundedwait.CloseInBubble(t, "async logger Close", owned.Close, owned.ForceClose) })
		supervisor := owned.failureSpoolPipelines.pipelines[0].runner.supervisor

		synctest.Wait()
		require.True(t, supervisor.IsRunning(), "the failure spool runner did not start")
		cancel()
		synctest.Wait()
		require.True(t, supervisor.IsRunning(), "ending the constructor context stopped the failure spool runner")
		require.NoError(t, boundedwait.CloseInBubble(t, "async logger Close", owned.Close, owned.ForceClose))
	})
}

func TestSyncAndAsyncPairedRuntimesCannotShareFailureSpoolRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	syncCfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(root, "opensearch")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	syncLogger, err := NewLogger(
		ctx,
		syncCfg,
		streamrouting.None,
		testFailureSpoolRuntimeConfig(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, syncLogger.(logging.CloseableLogger).Close()) }()

	asyncLogger, err := NewAsyncLogger(
		ctx,
		testAsyncFailureSpoolLoggerConfig(syncCfg),
		streamrouting.None,
		testFailureSpoolRuntimeConfig(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, asyncLogger)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	var conflict *directorylease.ConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(
		t,
		failureSpoolDirectoryScopeLabel(pkglogger.LoggerContractAsyncBestEffort, pkglogger.LogOutputOpenSearch),
		conflict.RequestedScopeLabel,
	)
	require.Equal(
		t,
		failureSpoolDirectoryScopeLabel(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch),
		conflict.ExistingScopeLabel,
	)
}

func TestFailureSpoolRecoveryRuntimeOwnsRootUntilClose(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	asyncConfig := testConstructibleOpenSearchFailureSpoolLoggerConfig(root, "async-opensearch")
	configs := FailureSpoolRecoveryConfigs{
		Sync:  testConstructibleOpenSearchFailureSpoolLoggerConfig(root, "opensearch"),
		Async: testAsyncFailureSpoolLoggerConfig(asyncConfig),
	}
	runtimeCfg := testFailureSpoolRuntimeConfig()

	first, err := NewFailureSpoolRecoveryRuntime(
		configs,
		runtimeCfg,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, first.Close()) })
	require.Len(t, first.failureSpoolDirectoryLeases, 2)
	for index, contractKind := range []pkglogger.LoggerContractKind{pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort} {
		set := first.failureSpoolDirectoryLeases[index]
		require.Equal(t, contractKind, set.contractKind)
		lease, found := set.leaseFor(pkglogger.LogOutputOpenSearch)
		require.True(t, found)
		require.NotNil(t, lease)
		// 同じbackendでも、各契約の保存先がCloseまでそれぞれ予約されます。
		conflicting, acquireErr := directorylease.Acquire("test.recovery.contract_root", set.entries[0].root)
		if conflicting != nil {
			conflicting.Release()
		}
		require.ErrorIs(t, acquireErr, directorylease.ErrDirectoryLeaseConflict)
	}

	second, err := NewFailureSpoolRecoveryRuntime(
		configs,
		runtimeCfg,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Nil(t, second)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	require.NoError(t, first.Close())
	replacement, err := NewFailureSpoolRecoveryRuntime(
		configs,
		runtimeCfg,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	require.NoError(t, replacement.Close())
}

func TestFailureSpoolDirectoryLeaseRejectsRootSharedByDistinctPipelines(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	plan := []failureSpoolPipelinePlan{
		testFailureSpoolPipelinePlan(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch, root),
		testFailureSpoolPipelinePlan(pkglogger.LoggerContractSync, pkglogger.LogOutputLoki, root),
	}
	_, err := acquireFailureSpoolDirectoryLeases(plan)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	// 二つ目のpipelineで失敗した場合も、一つ目のLeaseを残しません。
	replacement, acquireErr := directorylease.Acquire("test.failure_spool.rollback", root)
	require.NoError(t, acquireErr)
	replacement.Release()
}

func TestFailureSpoolDirectoryLeaseSetRejectsMixedContractsBeforeAcquiringRoots(t *testing.T) {
	t.Parallel()

	syncRoot := filepath.Join(t.TempDir(), "sync")
	asyncRoot := filepath.Join(t.TempDir(), "async")
	plan := []failureSpoolPipelinePlan{
		testFailureSpoolPipelinePlan(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch, syncRoot),
		testFailureSpoolPipelinePlan(pkglogger.LoggerContractAsyncBestEffort, pkglogger.LogOutputOpenSearch, asyncRoot),
	}
	set, err := acquireFailureSpoolDirectoryLeases(plan)
	require.Nil(t, set)
	require.ErrorIs(t, err, ErrMixedFailureSpoolContracts)
	var mixedErr *MixedFailureSpoolContractError
	require.ErrorAs(t, err, &mixedErr)
	require.Equal(t, pkglogger.LoggerContractSync, mixedErr.ContractKind)
	require.Equal(t, pkglogger.LoggerContractAsyncBestEffort, mixedErr.PipelineContractKind)
	lease, acquireErr := directorylease.Acquire("test.mixed_contracts.rollback", syncRoot, asyncRoot)
	require.NoError(t, acquireErr)
	lease.Release()
}

func TestFailureSpoolRecoveryRuntimeReleasesAllContractsOnConstructionFailure(t *testing.T) {
	for _, failurePoint := range []string{"later lease", "later runner"} {
		t.Run(failurePoint, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			syncConfig := testConstructibleOpenSearchFailureSpoolLoggerConfig(root, "sync-opensearch")
			asyncConfig := testConstructibleOpenSearchFailureSpoolLoggerConfig(root, "a-async-opensearch")
			clickHouseConfig := testClickHouseLoggerConfig(root, "z-async-clickhouse", "http://127.0.0.1:8123")
			asyncLoggerConfig := testAsyncFailureSpoolLoggerConfig(asyncConfig)
			asyncLoggerConfig.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch, pkglogger.LogOutputClickHouse}
			asyncLoggerConfig.ClickHouse = clickHouseConfig.ClickHouse
			asyncLoggerConfig.ClickHouseClient = clickHouseConfig.ClickHouseClient
			configs := FailureSpoolRecoveryConfigs{Sync: syncConfig, Async: asyncLoggerConfig}
			plan, err := buildFailureSpoolPipelinePlan(failureSpoolRecoveryPipelineSources(configs))
			require.NoError(t, err)
			require.Len(t, plan, 3)
			var blockedRoot string
			for _, pipeline := range plan {
				if pipeline.contractKind == pkglogger.LoggerContractAsyncBestEffort && pipeline.backend == pkglogger.LogOutputClickHouse {
					blockedRoot = pipeline.root
				}
			}
			require.NotEmpty(t, blockedRoot)
			runtimeConfig := testFailureSpoolRuntimeConfig()
			var blocker *directorylease.Lease
			if failurePoint == "later lease" {
				// async側の二件目で競合させ、syncとasyncの先行取得の両方を確認します。
				blocker, err = directorylease.Acquire("test.recovery.blocked", blockedRoot)
				require.NoError(t, err)
				t.Cleanup(blocker.Release)
			} else {
				// 全契約の使用権を取得した後、後続runnerの構築を失敗させます。
				runtimeConfig.RunnerConfig.ClickHouseConfig.Runner.Interval = -time.Second
			}
			runtime, err := NewFailureSpoolRecoveryRuntime(configs, runtimeConfig, observer.NopTracer{}, observer.NopMeter{})
			require.Nil(t, runtime)
			if failurePoint == "later lease" {
				require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
			} else {
				require.ErrorIs(t, err, infraspool.ErrInvalidRunnerInterval)
			}
			for _, pipeline := range plan {
				if blocker != nil && pipeline.root == blockedRoot {
					continue
				}
				lease, acquireErr := directorylease.Acquire("test.recovery.all_contracts.rollback", pipeline.root)
				require.NoError(t, acquireErr, "construction failure retained root %q", pipeline.root)
				lease.Release()
			}
			if blocker != nil {
				blocker.Release()
			}
			replacement, err := NewFailureSpoolRecoveryRuntime(configs, testFailureSpoolRuntimeConfig(), observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			require.NoError(t, replacement.Close())
		})
	}
}

func TestFailureSpoolDirectoryLeaseRejectsParentChildRootsAndRollsBack(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	child := filepath.Join(parent, "child")
	plan := []failureSpoolPipelinePlan{
		testFailureSpoolPipelinePlan(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch, parent),
		testFailureSpoolPipelinePlan(pkglogger.LoggerContractSync, pkglogger.LogOutputLoki, child),
	}
	_, err := acquireFailureSpoolDirectoryLeases(plan)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)

	replacement, acquireErr := directorylease.Acquire("test.failure_spool.parent.rollback", parent)
	require.NoError(t, acquireErr)
	replacement.Release()
}

func TestFailureSpoolDirectoryLeaseReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	plan := []failureSpoolPipelinePlan{
		testFailureSpoolPipelinePlan(pkglogger.LoggerContractSync, pkglogger.LogOutputCloudWatchLogs, root),
	}
	leases, err := acquireFailureSpoolDirectoryLeases(plan)
	require.NoError(t, err)

	leases.Release()
	leases.Release()

	replacement, err := acquireFailureSpoolDirectoryLeases(plan)
	require.NoError(t, err)
	replacement.Release()
}

func testFailureSpoolPipelinePlan(
	contractKind pkglogger.LoggerContractKind,
	backend pkglogger.LogOutputDestination,
	root string,
) failureSpoolPipelinePlan {
	return failureSpoolPipelinePlan{
		contractKind: contractKind,
		backend:      backend,
		root:         filepath.Clean(root),
	}
}

func testConstructibleOpenSearchFailureSpoolLoggerConfig(
	basePath string,
	dir string,
) pkglogger.LoggerConfig {
	cfg := testOpenSearchLoggerConfig(basePath, dir, "http://127.0.0.1:9200")
	cfg.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	cfg.OpenSearch.OpenSearchFlushManagerConfig.Index = "logs"
	cfg.OpenSearch.OpenSearchFlushManagerConfig.Resource = activationResource()
	return cfg
}

func testAsyncFailureSpoolLoggerConfig(
	base pkglogger.LoggerConfig,
) pkglogger.AsyncBestEffortMultiLoggerConfig {
	return pkglogger.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations: base.LogOutputDestinations,
		Timeout:               time.Second,
		MaxParallel:           1,
		MaxLogEntryBytes:      base.MaxLogEntryBytes,
		OpenSearch:            base.OpenSearch,
		OpenSearchClient:      base.OpenSearchClient,
	}
}

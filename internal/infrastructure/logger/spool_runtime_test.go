package logger

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	appconfig "github.com/go-jappe/heterolog/internal/config"
	configspool "github.com/go-jappe/heterolog/internal/config/spool"
	configspoolrunner "github.com/go-jappe/heterolog/internal/config/spool/spoolrunner"
	configspoolsupervisor "github.com/go-jappe/heterolog/internal/config/spool/spoolsupervisor"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/directorylease"
	infraspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"

	"github.com/stretchr/testify/require"
)

const (
	testLokiBufferFlushBytes            = 1 * 1024 * 1024
	testLokiFallbackQueueMaxEntries     = 1
	testLokiMaxLineBytes                = 256 * 1024
	testLokiMaxPushRequestBytes         = 4 * 1024 * 1024
	testClickHouseBufferFlushBytes      = 1 * 1024 * 1024
	testClickHouseMaxInsertRequestBytes = 16 * 1024 * 1024
)

func TestFailureSpoolRecoveryConfigsExcludesDeliveryScope(t *testing.T) {
	t.Parallel()

	if _, ok := reflect.TypeOf(FailureSpoolRecoveryConfigs{}).FieldByName("Delivery"); ok {
		t.Fatal("application spool runtime must not accept Delivery logger config")
	}
}

func TestHasSelectedSpoolUsesDurableTypeAsSoleActivationSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		types []pkgdurable.DurableType
		want  bool
	}{
		{name: "none", types: []pkgdurable.DurableType{pkgdurable.DurableTypeNone}},
		{name: "file archive", types: []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive}},
		{name: "spool", types: []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}, want: true},
		{
			name: "spool and file archive",
			types: []pkgdurable.DurableType{
				pkgdurable.DurableTypeSpool,
				pkgdurable.DurableTypeFileArchive,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := hasSelectedSpool(pkgdurable.FailureStoreConfig{DurableType: tt.types})
			if got != tt.want {
				t.Fatalf("hasSelectedSpool(%v) = %t, want %t", tt.types, got, tt.want)
			}
		})
	}
}

func TestValidateSelectedSpoolConfigReportsNotSelected(t *testing.T) {
	t.Parallel()

	err := validateSelectedSpoolConfig("opensearch", pkgdurable.FailureStoreConfig{
		DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
	})
	if !errors.Is(err, ErrDurableSpoolNotSelected) {
		t.Fatalf("validateSelectedSpoolConfig() error = %v, want %v", err, ErrDurableSpoolNotSelected)
	}
}

func TestBuildFailureSpoolPipelinePlanRejectsInvalidDurableTypeSelection(t *testing.T) {
	t.Parallel()

	const scope = "failure store"
	tests := []struct {
		name  string
		types []pkgdurable.DurableType
		// wantは、FailureStoreDurableTypesError.Errとして保持される検証errorです。
		want pkgdurable.DurableTypeError
	}{
		{name: "empty", want: pkgdurable.DurableTypeError{Scope: scope, Err: pkgdurable.ErrDurableTypesRequired}},
		{
			name: "none mixed with spool",
			types: []pkgdurable.DurableType{
				pkgdurable.DurableTypeNone,
				pkgdurable.DurableTypeSpool,
			},
			want: pkgdurable.DurableTypeError{
				Scope: scope, Type: pkgdurable.DurableTypeNone, Err: pkgdurable.ErrDurableTypeNoneNotAlone,
			},
		},
		{
			name: "duplicate",
			types: []pkgdurable.DurableType{
				pkgdurable.DurableTypeSpool,
				pkgdurable.DurableTypeSpool,
			},
			want: pkgdurable.DurableTypeError{
				Scope: scope, Type: pkgdurable.DurableTypeSpool, Err: pkgdurable.ErrDuplicateDurableType,
			},
		},
		{
			name:  "unknown",
			types: []pkgdurable.DurableType{"unknown"},
			want:  pkgdurable.DurableTypeError{Scope: scope, Type: "unknown", Err: pkgdurable.ErrUnsupportedDurableType},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := buildFailureSpoolPipelinePlan([]failureSpoolPipelineSource{{
				contractKind: pkglogger.LoggerContractSync,
				config: pkglogger.LoggerConfig{
					LogOutputDestinations: pkglogger.LogOutputDestinations{
						pkglogger.LogOutputOpenSearch,
					},
					OpenSearch: &pkglogger.OpenSearchConfig{
						OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
							FailureStoreConfig: pkgdurable.FailureStoreConfig{
								DurableType: tt.types,
							},
						},
					},
				},
			}})
			if err == nil {
				t.Fatal("buildFailureSpoolPipelinePlan() error = nil, want invalid durable type selection")
			}
			var durableTypesErr *FailureStoreDurableTypesError
			if !errors.As(err, &durableTypesErr) {
				t.Fatalf("buildFailureSpoolPipelinePlan() error = %v, want *FailureStoreDurableTypesError", err)
			}
			if durableTypesErr.ContractKind != pkglogger.LoggerContractSync ||
				durableTypesErr.Backend != pkglogger.LogOutputOpenSearch {
				t.Fatalf(
					"durable type error target = %s/%s, want %s/%s",
					durableTypesErr.ContractKind,
					durableTypesErr.Backend,
					pkglogger.LoggerContractSync,
					pkglogger.LogOutputOpenSearch,
				)
			}
			var typeErr *pkgdurable.DurableTypeError
			if !errors.As(durableTypesErr.Err, &typeErr) || *typeErr != tt.want || !errors.Is(err, tt.want.Err) {
				t.Fatalf("durable type validation cause = %#v, want %#v", durableTypesErr.Err, tt.want)
			}
		})
	}
}

func TestNewFailureSpoolRecoveryRuntimeBuildsPipelinesSelectedByConfigFixture(t *testing.T) {
	repoRoot := configFixtureRootForLoggerTest(t)
	envPath := filepath.Join(repoRoot, ".env")
	if _, err := os.Stat(envPath); err != nil {
		t.Fatalf("config fixture .env is unavailable: %v", err)
	}
	// 設定loaderが読む環境変数だけを外し、実行環境の変数は残します。
	testutil.UnsetConfigEnvironment(t)
	if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
		t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
	}
	// Keep the test on the .env file loading path while redirecting durable
	// files away from the production filesystem.
	if err := os.Setenv("HETEROLOG_SYSTEM_LIB_DIR", t.TempDir()); err != nil {
		t.Fatalf("set temporary HETEROLOG_SYSTEM_LIB_DIR: %v", err)
	}

	// This scenario exercises replay for both backends in both contracts.
	if err := os.Setenv("HETEROLOG_ASYNC_LOG_TYPE", "file,stdout,slack,opensearch,loki"); err != nil {
		t.Fatal(err)
	}

	loaded, err := appconfig.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() from config fixture error = %v", err)
	}
	spoolConfig := appconfig.NewSpoolConfig(&loaded)
	loggerConfigs := FailureSpoolRecoveryConfigs{
		Sync:  appconfig.ToLoggerConfig(loaded),
		Async: appconfig.ToAsyncBestEffortMultiLoggerConfig(loaded),
	}
	runtime, err := NewFailureSpoolRecoveryRuntime(
		loggerConfigs,
		spoolConfig,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewFailureSpoolRecoveryRuntime() from config fixture error = %v", err)
	}

	plan, err := buildFailureSpoolPipelinePlan(
		failureSpoolRecoveryPipelineSources(loggerConfigs),
	)
	if err != nil {
		t.Fatalf("buildFailureSpoolPipelinePlan() error = %v", err)
	}
	expectedByBackend := make(map[string]int)
	for _, pipeline := range plan {
		expectedByBackend[pipeline.backend.String()]++
	}
	for backend, expected := range expectedByBackend {
		if got := countSupervisorsForBackend(runtime, backend); got != expected {
			t.Fatalf("%s supervisors = %d, want %d enabled pipelines", backend, got, expected)
		}
	}
	wantRunnerIdentities := []failureSpoolRunnerIdentity{
		{contractKind: pkglogger.LoggerContractSync, backend: pkglogger.LogOutputOpenSearch},
		{contractKind: pkglogger.LoggerContractSync, backend: pkglogger.LogOutputLoki},
		{contractKind: pkglogger.LoggerContractAsyncBestEffort, backend: pkglogger.LogOutputOpenSearch},
		{contractKind: pkglogger.LoggerContractAsyncBestEffort, backend: pkglogger.LogOutputLoki},
	}
	if got := failureSpoolRunnerIdentities(runtime); !reflect.DeepEqual(got, wantRunnerIdentities) {
		t.Fatalf("config fixture failure spool runners = %v, want %v", got, wantRunnerIdentities)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	if err := runtime.Start(runCtx); err != nil {
		cancel()
		t.Fatalf("config fixture runtime Start() error = %v", err)
	}
	cancel()
	if err := runtime.Close(); err != nil {
		t.Fatalf("config fixture runtime Close() error = %v", err)
	}
}

func TestNewFailureSpoolRecoveryRuntimeBuildsSyncAndAsyncSpoolsOnly(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	configs := FailureSpoolRecoveryConfigs{
		Sync: testClickHouseLoggerConfig(basePath, "sync-clickhouse", "http://127.0.0.1:8123"),
		Async: pkglogger.AsyncBestEffortMultiLoggerConfig{
			OpenSearch: testOpenSearchFailureSpoolConfig(
				testSpoolFailureStore(basePath, "async-opensearch"),
			),
			OpenSearchClient: testOpenSearchClient("http://127.0.0.1:9200"),
		},
	}

	runtime, err := NewFailureSpoolRecoveryRuntime(
		configs,
		testFailureSpoolRuntimeConfig(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewFailureSpoolRecoveryRuntime() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	supervisors := failureSpoolRuntimeSupervisors(runtime)
	if len(supervisors) != 2 {
		t.Fatalf("supervisors = %d, want 2", len(supervisors))
	}
	if !hasSupervisorForBackend(runtime, "opensearch") {
		t.Fatal("async OpenSearch supervisor was not built")
	}
	if !hasSupervisorForBackend(runtime, "clickhouse") {
		t.Fatal("sync ClickHouse supervisor was not built")
	}
}

func TestNewFailureSpoolRecoveryRuntimeRejectsSharedRootForSameDestination(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	configs := FailureSpoolRecoveryConfigs{
		Sync: testOpenSearchLoggerConfig(basePath, "opensearch", "http://127.0.0.1:9200"),
		Async: pkglogger.AsyncBestEffortMultiLoggerConfig{
			OpenSearch: testOpenSearchFailureSpoolConfig(
				testSpoolFailureStore(basePath, "opensearch"),
			),
			OpenSearchClient: testOpenSearchClient("http://127.0.0.1:9200"),
		},
	}

	_, err := NewFailureSpoolRecoveryRuntime(
		configs,
		testFailureSpoolRuntimeConfig(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
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

func TestNewFailureSpoolRecoveryRuntimeBuildsSeparateSupervisorsForDistinctPipelineRoots(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	configs := FailureSpoolRecoveryConfigs{
		Sync: testOpenSearchLoggerConfig(basePath, "opensearch", "http://127.0.0.1:9200"),
		Async: pkglogger.AsyncBestEffortMultiLoggerConfig{
			OpenSearch: testOpenSearchFailureSpoolConfig(
				testSpoolFailureStore(basePath, "async-opensearch"),
			),
			OpenSearchClient: testOpenSearchClient("http://127.0.0.1:9200"),
		},
	}

	runtime, err := NewFailureSpoolRecoveryRuntime(
		configs,
		testFailureSpoolRuntimeConfig(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewFailureSpoolRecoveryRuntime() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	if got := countSupervisorsForBackend(
		runtime,
		"opensearch",
	); got != 2 {
		t.Fatalf("OpenSearch supervisors = %d, want 2 pipeline-specific supervisors", got)
	}
	wantRunnerIdentities := []failureSpoolRunnerIdentity{
		{contractKind: pkglogger.LoggerContractSync, backend: pkglogger.LogOutputOpenSearch},
		{contractKind: pkglogger.LoggerContractAsyncBestEffort, backend: pkglogger.LogOutputOpenSearch},
	}
	if got := failureSpoolRunnerIdentities(runtime); !reflect.DeepEqual(got, wantRunnerIdentities) {
		t.Fatalf("failure spool runner identities = %v, want %v", got, wantRunnerIdentities)
	}
}

func TestNewFailureSpoolRecoveryRuntimeRejectsSharedCloudWatchRootForSameDestination(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	newConfig := func() pkglogger.LoggerConfig {
		return pkglogger.LoggerConfig{
			CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
				CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
					LogGroupName:       "test-group",
					LogStreamName:      "test-stream",
					FailureStoreConfig: testSpoolFailureStore(basePath, "cloudwatchlogs"),
				},
			},
			CloudWatchLogsClient: &pkgcloudwatchlogs.Config{
				Region:          "ap-northeast-1",
				Endpoint:        "http://127.0.0.1:4566",
				LogGroupName:    "test-group",
				AccessKeyID:     "access-key",
				SecretAccessKey: "secret-key",
			},
		}
	}
	asyncConfig := newConfig()
	configs := FailureSpoolRecoveryConfigs{
		Sync: newConfig(),
		Async: pkglogger.AsyncBestEffortMultiLoggerConfig{
			CloudWatchLogs:       asyncConfig.CloudWatchLogs,
			CloudWatchLogsClient: asyncConfig.CloudWatchLogsClient,
		},
	}

	_, err := NewFailureSpoolRecoveryRuntime(
		configs,
		testFailureSpoolRuntimeConfig(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	var conflict *directorylease.ConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(
		t,
		failureSpoolDirectoryScopeLabel(pkglogger.LoggerContractAsyncBestEffort, pkglogger.LogOutputCloudWatchLogs),
		conflict.RequestedScopeLabel,
	)
	require.Equal(
		t,
		failureSpoolDirectoryScopeLabel(pkglogger.LoggerContractSync, pkglogger.LogOutputCloudWatchLogs),
		conflict.ExistingScopeLabel,
	)
}

func TestNewFailureSpoolRecoveryRuntimeRejectsRootSharedAcrossBackends(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	sharedFailureStore := testSpoolFailureStore(basePath, "shared")
	config := pkglogger.LoggerConfig{
		OpenSearch:       testOpenSearchFailureSpoolConfig(sharedFailureStore),
		OpenSearchClient: testOpenSearchClient("http://127.0.0.1:9200"),
		Loki: &pkglogger.LokiConfig{
			LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{
				FailureStoreConfig: sharedFailureStore,
			},
		},
		LokiClient: &pkgloki.Config{Endpoint: "http://127.0.0.1:3100"},
	}

	_, err := NewFailureSpoolRecoveryRuntime(
		FailureSpoolRecoveryConfigs{Sync: config},
		testFailureSpoolRuntimeConfig(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.ErrorIs(t, err, directorylease.ErrDirectoryLeaseConflict)
	var conflict *directorylease.ConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(
		t,
		failureSpoolDirectoryScopeLabel(pkglogger.LoggerContractSync, pkglogger.LogOutputLoki),
		conflict.RequestedScopeLabel,
	)
	require.Equal(
		t,
		failureSpoolDirectoryScopeLabel(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch),
		conflict.ExistingScopeLabel,
	)
}

func TestFailureSpoolRuntimeStartRejectsNilContext(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	runtime, err := NewFailureSpoolRecoveryRuntime(
		FailureSpoolRecoveryConfigs{Sync: testOpenSearchLoggerConfig(basePath, "opensearch", "http://127.0.0.1:9200")},
		testFailureSpoolRuntimeConfig(),
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewFailureSpoolRecoveryRuntime() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	//lint:ignore SA1012 nil contextを拒否する契約自体を検証する。
	if err := runtime.Start(nil); !errors.Is(err, ErrFailureSpoolRuntimeContextNil) {
		t.Fatalf("Start(nil) error = %v, want %v", err, ErrFailureSpoolRuntimeContextNil)
	}
}

func TestFailureSpoolRuntimeStartRejectsAlreadyCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runtime := &FailureSpoolRuntime{
		runners: []failureSpoolRunnerRuntime{{
			contractKind: pkglogger.LoggerContractSync,
			backend:      pkglogger.LogOutputOpenSearch,
		}},
	}

	if err := runtime.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start(canceled context) error = %v, want %v", err, context.Canceled)
	}
	if runtime.startState != failureSpoolRuntimeStartStateReady {
		t.Fatal("runtime started with an already canceled context")
	}
}

func TestFailureSpoolRuntimeSecondStartIsRejected(t *testing.T) {
	t.Parallel()

	runtime := &FailureSpoolRuntime{
		runners: []failureSpoolRunnerRuntime{{
			contractKind: pkglogger.LoggerContractSync,
			backend:      pkglogger.LogOutputOpenSearch,
		}},
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	if err := runtime.Start(context.Background()); !errors.Is(err, ErrFailureSpoolRuntimeAlreadyStarted) {
		t.Fatalf("second Start() error = %v, want %v", err, ErrFailureSpoolRuntimeAlreadyStarted)
	}
	if err := runtime.Close(); !errors.Is(err, infraspool.ErrNilRunnerSupervisor) {
		t.Fatalf("Close() error = %v, want %v", err, infraspool.ErrNilRunnerSupervisor)
	}
}

func TestFailureSpoolRuntimeErrorIdentifiesEachLoggerContractAndBackendRunner(t *testing.T) {
	t.Parallel()

	runtime := &FailureSpoolRuntime{
		runners: []failureSpoolRunnerRuntime{
			{contractKind: pkglogger.LoggerContractSync, backend: pkglogger.LogOutputOpenSearch},
			{contractKind: pkglogger.LoggerContractAsyncBestEffort, backend: pkglogger.LogOutputOpenSearch},
		},
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	closeErr := runtime.Close()
	if !errors.Is(closeErr, infraspool.ErrNilRunnerSupervisor) {
		t.Fatalf(
			"Close() error = %v, want %v",
			closeErr,
			infraspool.ErrNilRunnerSupervisor,
		)
	}
	// runnerごとのFailureSpoolRunnerErrorを集め、契約種別とbackendの組で発生元を比較します。
	// runnerは並行に終了するため、順序は比較しません。
	var got []failureSpoolRunnerIdentity
	for _, runnerErr := range failureSpoolRunnerErrors(closeErr) {
		if !errors.Is(runnerErr, infraspool.ErrNilRunnerSupervisor) {
			t.Fatalf("runner error = %v, want cause %v", runnerErr, infraspool.ErrNilRunnerSupervisor)
		}
		got = append(got, failureSpoolRunnerIdentity{contractKind: runnerErr.ContractKind, backend: runnerErr.Backend})
	}
	sort.Slice(got, func(i, j int) bool { return got[i].contractKind < got[j].contractKind })
	want := []failureSpoolRunnerIdentity{
		{contractKind: pkglogger.LoggerContractSync, backend: pkglogger.LogOutputOpenSearch},
		{contractKind: pkglogger.LoggerContractAsyncBestEffort, backend: pkglogger.LogOutputOpenSearch},
	}
	sort.Slice(want, func(i, j int) bool { return want[i].contractKind < want[j].contractKind })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Close() runner errors = %v, want %v", got, want)
	}
}

// failureSpoolRunnerErrorsはJoinされたerrorを辿り、runnerごとのerrorをすべて返します。
// errors.Asは最初の一件だけを返すため、複数runnerの識別には使えません。
func failureSpoolRunnerErrors(err error) []*FailureSpoolRunnerError {
	switch wrapped := err.(type) {
	case *FailureSpoolRunnerError:
		return []*FailureSpoolRunnerError{wrapped}
	case interface{ Unwrap() []error }:
		var found []*FailureSpoolRunnerError
		for _, cause := range wrapped.Unwrap() {
			found = append(found, failureSpoolRunnerErrors(cause)...)
		}
		return found
	case interface{ Unwrap() error }:
		return failureSpoolRunnerErrors(wrapped.Unwrap())
	default:
		return nil
	}
}

func TestFailureSpoolRuntimeStartAfterCloseIsRejected(t *testing.T) {
	t.Parallel()

	// supervisorを実行する前にclosed判定されることを確認するため、
	// 要素自体は不要なdummyでよい。
	runtime := &FailureSpoolRuntime{
		runners: []failureSpoolRunnerRuntime{{supervisor: nil}},
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := runtime.Start(context.Background()); !errors.Is(err, ErrFailureSpoolRuntimeClosed) {
		t.Fatalf("Start() error = %v, want %v", err, ErrFailureSpoolRuntimeClosed)
	}
}

func TestFailureSpoolRuntimeConcurrentStartAndClose(t *testing.T) {
	t.Parallel()

	// StartとCloseが同時にlifecycle境界へ入る状況を繰り返し、
	// CloseのWait開始後にsupervisorが追加されたり、cancelの読み書きが競合したり
	// しないことをrace detectorと終了状態の両方で確認する。
	basePath := t.TempDir()
	for i := 0; i < 50; i++ {
		runtime, err := NewFailureSpoolRecoveryRuntime(
			FailureSpoolRecoveryConfigs{
				Sync: testOpenSearchLoggerConfig(
					basePath,
					"concurrent-opensearch",
					"http://127.0.0.1:9200",
				),
			},
			testFailureSpoolRuntimeConfig(),
			observer.NopTracer{},
			observer.NopMeter{},
		)
		if err != nil {
			t.Fatalf("iteration %d: NewFailureSpoolRecoveryRuntime() error = %v", i, err)
		}

		startGate := make(chan struct{})
		startErr := make(chan error, 1)
		closeErr := make(chan error, 1)
		go func() {
			<-startGate
			startErr <- runtime.Start(context.Background())
		}()
		go func() {
			<-startGate
			closeErr <- runtime.Close()
		}()
		close(startGate)

		gotStartErr := <-startErr
		if gotStartErr != nil && !errors.Is(gotStartErr, ErrFailureSpoolRuntimeClosed) {
			t.Fatalf(
				"iteration %d: Start() error = %v, want nil or %v",
				i,
				gotStartErr,
				ErrFailureSpoolRuntimeClosed,
			)
		}
		if err := <-closeErr; err != nil {
			t.Fatalf("iteration %d: Close() error = %v", i, err)
		}
		for supervisorIndex, runner := range runtime.runners {
			if runner.supervisor.IsRunning() {
				t.Fatalf(
					"iteration %d: supervisor[%d] is still running after Close",
					i,
					supervisorIndex,
				)
			}
		}
	}
}

func TestLoggerWithOwnedResourcesClosesProducerBeforeFailureSpoolPipeline(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	leases, err := acquireFailureSpoolDirectoryLeases([]failureSpoolPipelinePlan{
		testFailureSpoolPipelinePlan(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch, root),
	})
	if err != nil {
		t.Fatalf("acquireFailureSpoolDirectoryLeases() error = %v", err)
	}
	t.Cleanup(leases.Release)

	runtime := &FailureSpoolRuntime{}
	pipelines := &failureSpoolPipelineSet{
		runtime:       runtime,
		leases:        leases,
		writersClosed: true,
	}
	closeErr := errors.New("backend final flush failed")
	logger := &spoolRuntimeCloseLogger{
		runtime:   runtime,
		closeErrs: []error{closeErr, nil},
	}
	owned := &topLevelLoggerRuntime{
		topLevelLoggerDelegate: logger,
		failureSpoolPipelines:  pipelines,
	}

	if err := owned.closeAttempt(); !errors.Is(err, closeErr) {
		t.Fatalf("first Close() error = %v, want %v", err, closeErr)
	}
	if runtimeClosed(runtime) {
		t.Fatal("runtime closed before backend final flush succeeded")
	}
	conflicting, acquireErr := directorylease.Acquire("test.close_retry", root)
	if conflicting != nil {
		conflicting.Release()
		t.Fatal("failure spool Lease was released while producer Close remained retryable")
	}
	if !errors.Is(acquireErr, directorylease.ErrDirectoryLeaseConflict) {
		t.Fatalf("Acquire() after failed producer Close error = %v, want Lease conflict", acquireErr)
	}

	if err := owned.closeAttempt(); err != nil {
		t.Fatalf("retry Close() error = %v", err)
	}
	if !runtimeClosed(runtime) {
		t.Fatal("runtime is still open after backend Close succeeded")
	}
	if got := logger.closeCallCount(); got != 2 {
		t.Fatalf("backend Close calls = %d, want 2", got)
	}
	replacement, acquireErr := directorylease.Acquire("test.close_retry.replacement", root)
	if acquireErr != nil {
		t.Fatalf("Acquire() after successful Close error = %v", acquireErr)
	}
	replacement.Release()

	if err := owned.closeAttempt(); err != nil {
		t.Fatalf("idempotent Close() error = %v", err)
	}
	if got := logger.closeCallCount(); got != 2 {
		t.Fatalf("backend Close calls after completed Close = %d, want 2", got)
	}
}

func TestNewFailureSpoolRecoveryRuntimeRequiresTuningConfigForEnabledPipeline(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	_, err := NewFailureSpoolRecoveryRuntime(
		FailureSpoolRecoveryConfigs{
			Sync: testOpenSearchLoggerConfig(
				basePath,
				"opensearch",
				"http://127.0.0.1:9200",
			),
		},
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if !errors.Is(err, ErrFailureSpoolRuntimeConfigRequired) {
		t.Fatalf("NewFailureSpoolRecoveryRuntime() error = %v, want %v", err, ErrFailureSpoolRuntimeConfigRequired)
	}
}

func TestNewFailureSpoolRecoveryRuntimeIsNoOpWithoutEnabledPipeline(t *testing.T) {
	t.Parallel()

	runtime, err := NewFailureSpoolRecoveryRuntime(
		FailureSpoolRecoveryConfigs{},
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewFailureSpoolRecoveryRuntime() error = %v, want disabled no-op", err)
	}
	if got := len(runtime.runners); got != 0 {
		t.Fatalf("supervisors = %d, want 0", got)
	}
}

func TestNewFailureSpoolRecoveryRuntimeBuildsAllFourBackendSupervisors(t *testing.T) {
	t.Parallel()

	basePath := t.TempDir()
	loggerConfig := pkglogger.LoggerConfig{
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
				TimeConfig: pkglogger.TimeConfig{
					IsLocalTimeEnabled: true,
					TimeZone:           "Asia/Tokyo",
				},
				LogBodyConfig: pkglogger.LogBodyConfig{
					LogLineDateFormat: "opensearch:2006-01-02 15:04:05 MST",
				},
				FlushInterval:       time.Hour,
				MaxBulkDocument:     1,
				BufferFlushBytes:    testOpenSearchBufferFlushBytes,
				MaxBulkRequestBytes: testOpenSearchMaxBulkRequestBytes,
			},
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				FailureStoreConfig: testSpoolFailureStore(basePath, "opensearch"),
			},
		},
		OpenSearchClient: &pkgopensearch.Config{
			Endpoint: "http://127.0.0.1:9200",
			AuthConfig: pkgopensearch.AuthConfig{
				Type: pkgopensearch.AuthTypeNone,
			},
		},
		Loki: &pkglogger.LokiConfig{
			LokiLoggerBaseConfig: pkglogger.LokiLoggerBaseConfig{
				TimeConfig: pkglogger.TimeConfig{
					IsLocalTimeEnabled: true,
					TimeZone:           "America/New_York",
				},
				LogBodyConfig: pkglogger.LogBodyConfig{
					LogLineDateFormat: "loki:2006-01-02 15:04:05 MST",
				},
				FlushInterval:           time.Hour,
				MaxEntries:              1,
				BufferFlushBytes:        testLokiBufferFlushBytes,
				FallbackQueueMaxEntries: testLokiFallbackQueueMaxEntries,
				MaxLineBytes:            testLokiMaxLineBytes,
				MaxPushRequestBytes:     testLokiMaxPushRequestBytes,
			},
			LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{
				FailureStoreConfig: testSpoolFailureStore(basePath, "loki"),
			},
		},
		LokiClient: &pkgloki.Config{Endpoint: "http://127.0.0.1:3100"},
		CloudWatchLogs: &pkglogger.CloudWatchLogsConfig{
			CloudWatchLogsLoggerBaseConfig: pkglogger.CloudWatchLogsLoggerBaseConfig{
				TimeConfig: pkglogger.TimeConfig{
					IsLocalTimeEnabled: true,
					TimeZone:           "Europe/London",
				},
				LogBodyConfig: pkglogger.LogBodyConfig{
					LogLineDateFormat: "cloudwatchlogs:2006-01-02 15:04:05 MST",
				},
				FlushInterval:          time.Hour,
				BufferFlushMaxEvents:   1,
				BufferFlushBytes:       1,
				FallbackQueueMaxEvents: 1,
			},
			CloudWatchLogsFlushManagerConfig: pkglogger.CloudWatchLogsFlushManagerConfig{
				LogGroupName:       "test-group",
				LogStreamName:      "test-stream",
				FailureStoreConfig: testSpoolFailureStore(basePath, "cloudwatchlogs"),
			},
		},
		CloudWatchLogsClient: &pkgcloudwatchlogs.Config{
			Endpoint:        "http://127.0.0.1:4566",
			Region:          "ap-northeast-1",
			LogGroupName:    "test-group",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		},
		ClickHouse: &pkglogger.ClickHouseConfig{
			ClickHouseLoggerBaseConfig: pkglogger.ClickHouseLoggerBaseConfig{
				TimeConfig: pkglogger.TimeConfig{
					IsLocalTimeEnabled: true,
					TimeZone:           "Australia/Sydney",
				},
				LogBodyConfig: pkglogger.LogBodyConfig{
					LogLineDateFormat: "clickhouse:2006-01-02 15:04:05 MST",
				},
				FlushInterval:           time.Hour,
				MaxEntries:              1,
				BufferFlushBytes:        testClickHouseBufferFlushBytes,
				FallbackQueueMaxEntries: 1,
				MaxInsertRequestBytes:   testClickHouseMaxInsertRequestBytes,
			},
			ClickHouseFlushManagerConfig: pkglogger.ClickHouseFlushManagerConfig{
				FailureStoreConfig: testSpoolFailureStore(basePath, "clickhouse"),
			},
		},
		ClickHouseClient: &pkgclickhouse.Config{
			Endpoint: "http://127.0.0.1:8123",
			Database: "default",
			Table:    "logs",
		},
	}
	spoolConfig := testFailureSpoolRuntimeConfig()

	runtime, err := NewFailureSpoolRecoveryRuntime(
		FailureSpoolRecoveryConfigs{Sync: loggerConfig},
		spoolConfig,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if err != nil {
		t.Fatalf("NewFailureSpoolRecoveryRuntime() error = %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	supervisors := failureSpoolRuntimeSupervisors(runtime)
	if len(supervisors) != 4 {
		t.Fatalf("supervisors = %d, want 4", len(supervisors))
	}
	wantBackends := []string{"opensearch", "loki", "cloudwatchlogs", "clickhouse"}
	for i, want := range wantBackends {
		require.NotNil(t, runtime.runners[i].supervisor)
		require.NotNil(t, runtime.runners[i].supervisor.Runner)
		if got := runtime.runners[i].backend.String(); got != want {
			t.Fatalf("supervisor[%d] backend = %q, want %q", i, got, want)
		}
	}

	// recoveryでも全backendのruntimeが所有する時刻源はUTCの瞬間だけを返します。spool directoryの
	// calendar timezoneは、各backend設定から別の依存として構築時に渡されます。
	for index, runner := range runtime.runners {
		require.NotNil(t, runner.clock)
		got := runner.clock.Now()
		if got.Location() != time.UTC {
			t.Errorf("%s recovery spool clock location = %s, want UTC", wantBackends[index], got.Location())
		}
	}
}

func testSpoolFailureStore(basePath string, dir string) pkgdurable.FailureStoreConfig {
	return pkgdurable.FailureStoreConfig{
		MaxConcurrent: 1,
		DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
		DurableStoreConfig: pkgdurable.DurableStoreConfig{
			SpoolConfig: pkgdurable.SpoolConfig{
				SpoolPath: basePath,
				Target:    "runtime-test",
				Dir:       dir,
			},
		},
	}
}

func testOpenSearchLoggerConfig(basePath, dir, endpoint string) pkglogger.LoggerConfig {
	return pkglogger.LoggerConfig{
		Timeout:          time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		OpenSearch:       testOpenSearchFailureSpoolConfig(testSpoolFailureStore(basePath, dir)),
		OpenSearchClient: testOpenSearchClient(endpoint),
	}
}

func testOpenSearchFailureSpoolConfig(
	failureStore pkgdurable.FailureStoreConfig,
) *pkglogger.OpenSearchConfig {
	return &pkglogger.OpenSearchConfig{
		OpenSearchLoggerBaseConfig: pkglogger.OpenSearchLoggerBaseConfig{
			FlushInterval:       time.Hour,
			MaxBulkDocument:     1,
			BufferFlushBytes:    testOpenSearchBufferFlushBytes,
			MaxBulkRequestBytes: testOpenSearchMaxBulkRequestBytes,
		},
		OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
			FailureStoreConfig: failureStore,
			MaxRetryAttempts:   1,
		},
	}
}

func testOpenSearchClient(endpoint string) *pkgopensearch.Config {
	return &pkgopensearch.Config{
		Endpoint: endpoint,
		AuthConfig: pkgopensearch.AuthConfig{
			Type: pkgopensearch.AuthTypeNone,
		},
	}
}

func testClickHouseLoggerConfig(basePath, dir, endpoint string) pkglogger.LoggerConfig {
	return pkglogger.LoggerConfig{
		Timeout:          time.Second,
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
		ClickHouse: &pkglogger.ClickHouseConfig{
			ClickHouseLoggerBaseConfig: pkglogger.ClickHouseLoggerBaseConfig{
				FlushInterval:           time.Hour,
				MaxEntries:              1,
				BufferFlushBytes:        testClickHouseBufferFlushBytes,
				FallbackQueueMaxEntries: 1,
				MaxInsertRequestBytes:   testClickHouseMaxInsertRequestBytes,
			},
			ClickHouseFlushManagerConfig: pkglogger.ClickHouseFlushManagerConfig{
				FailureStoreConfig: testSpoolFailureStore(basePath, dir),
			},
		},
		ClickHouseClient: testClickHouseClient(endpoint),
	}
}

func testClickHouseClient(endpoint string) *pkgclickhouse.Config {
	return &pkgclickhouse.Config{
		Endpoint: endpoint,
		Database: "default",
		Table:    "logs",
	}
}

func testFailureSpoolRuntimeConfig() *configspool.SpoolConfig {
	cfg := &configspool.SpoolConfig{}
	cfg.SupervisorConfig.RestartDelay = configspoolsupervisor.DefaultRestartDelay
	for _, backend := range []*configspoolrunner.BackendSpoolRunnerConfig{
		&cfg.RunnerConfig.OpensearchConfig,
		&cfg.RunnerConfig.LokiConfig,
		&cfg.RunnerConfig.CloudWatchLogsConfig,
		&cfg.RunnerConfig.ClickHouseConfig,
	} {
		backend.Runner.Interval = configspoolrunner.DefaultRunnerInterval
		backend.LifeCycle.SizePurgeInterval = time.Hour
		backend.Worker.Finalize.RetryDelay = time.Millisecond
		backend.Worker.Finalize.MaxEntries = 100
	}
	return cfg
}

func hasSupervisorForBackend(runtime *FailureSpoolRuntime, backend string) bool {
	return countSupervisorsForBackend(runtime, backend) > 0
}

func countSupervisorsForBackend(runtime *FailureSpoolRuntime, backend string) int {
	if runtime == nil {
		return 0
	}
	count := 0
	for _, runner := range runtime.runners {
		if runner.backend.String() == backend {
			count++
		}
	}
	return count
}

func failureSpoolRuntimeSupervisors(runtime *FailureSpoolRuntime) []*infraspool.RunnerSupervisor {
	if runtime == nil {
		return nil
	}
	supervisors := make([]*infraspool.RunnerSupervisor, len(runtime.runners))
	for index, runner := range runtime.runners {
		supervisors[index] = runner.supervisor
	}
	return supervisors
}

type failureSpoolRunnerIdentity struct {
	contractKind pkglogger.LoggerContractKind
	backend      pkglogger.LogOutputDestination
}

func failureSpoolRunnerIdentities(runtime *FailureSpoolRuntime) []failureSpoolRunnerIdentity {
	if runtime == nil {
		return nil
	}
	identities := make([]failureSpoolRunnerIdentity, len(runtime.runners))
	for index, runner := range runtime.runners {
		identities[index] = failureSpoolRunnerIdentity{
			contractKind: runner.contractKind,
			backend:      runner.backend,
		}
	}
	return identities
}

type spoolRuntimeCloseLogger struct {
	mu        sync.Mutex
	runtime   *FailureSpoolRuntime
	closeErrs []error
	calls     int
}

func (l *spoolRuntimeCloseLogger) StopAccepting() {}

func (l *spoolRuntimeCloseLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if runtimeClosed(l.runtime) {
		return errors.New("runtime closed before backend logger")
	}
	index := l.calls
	l.calls++
	if index < len(l.closeErrs) {
		return l.closeErrs[index]
	}
	return nil
}

func (l *spoolRuntimeCloseLogger) closeCallCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func (*spoolRuntimeCloseLogger) Debug(context.Context, string, ...applogging.Field) error {
	return nil
}

func (*spoolRuntimeCloseLogger) Info(context.Context, string, ...applogging.Field) error {
	return nil
}

func (*spoolRuntimeCloseLogger) Warn(context.Context, string, ...applogging.Field) error {
	return nil
}

func (*spoolRuntimeCloseLogger) Error(context.Context, string, ...applogging.Field) error {
	return nil
}

func (*spoolRuntimeCloseLogger) ErrorWithStack(
	context.Context,
	string,
	error,
	...applogging.Field,
) error {
	return nil
}

func runtimeClosed(runtime *FailureSpoolRuntime) bool {
	runtime.lifecycleMu.Lock()
	defer runtime.lifecycleMu.Unlock()
	return runtime.startState == failureSpoolRuntimeStartStateClosed
}

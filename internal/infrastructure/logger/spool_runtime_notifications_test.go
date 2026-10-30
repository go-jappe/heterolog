package logger

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	logspool "github.com/go-jappe/heterolog/internal/infrastructure/spool/log"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"

	"github.com/stretchr/testify/require"
)

func TestFailureSpoolRunnerNotificationsUseRegisteredWriterHandlers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
		plan, err := buildFailureSpoolPipelinePlan([]failureSpoolPipelineSource{{
			contractKind: pkglogger.LoggerContractSync, config: cfg,
		}})
		require.NoError(t, err)
		pipelines, err := newFailureSpoolPipelineSet(context.Background(), plan, testFailureSpoolRuntimeConfig(), observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = pipelines.close() })
		now := time.Date(2026, 9, 27, 9, 30, 0, 0, time.FixedZone("test", 9*60*60))
		pipelines.runtime.runners[0].clock = spoolRoutingFixedClock{now: now}
		spoolCfg := loggerdurable.NewSpoolConfigAdapter(cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig)
		store, err := fsstorestore.NewSpoolStore(clock.NewUTCClock(), spoolCfg, observer.NopTracer{}, observer.NopMeter{})
		require.NoError(t, err)
		var errorsSeen []error
		var warnings []*logging.LogEntry
		writer := &spoolstorehandler.SpoolStoreHandler{
			Store: store,
			OnError: func(_ context.Context, err error, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				require.True(t, entry.Internal)
				require.Equal(t, "failure_spool_runner", entry.Source)
				require.Equal(t, now.UTC(), entry.OccurredAt)
				errorsSeen = append(errorsSeen, err)
			},
			OnWarn: func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				warnings = append(warnings, entry)
			},
		}
		require.NoError(t, pipelines.registerWriter(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch, writer))
		supervisor := pipelines.runtime.runners[0].supervisor
		require.NotNil(t, supervisor.Reporter)
		runErr := errors.New("purge failed")
		corruptErr := errors.New("spool decode failed")
		report := logspool.RunReport{ProcessReport: worker.ProcessReport{
			Status: worker.ProcessStatusQuarantined, ProcessingPath: "corrupt.hlogspool", Err: corruptErr,
		}}
		// 処理失敗と隔離成功の警告をそれぞれ通知し、処理結果はそのままSnapshotへ残します。
		supervisor.ReportSpoolRunnerRun(context.Background(), report, runErr)
		synctest.Wait()
		require.Len(t, errorsSeen, 1)
		require.ErrorIs(t, errorsSeen[0], runErr)
		require.Len(t, warnings, 1)
		require.ErrorIs(t, warnings[0].Err, corruptErr)
		require.Equal(t, report, supervisor.Snapshot().LastRunReport)

		// 同じエラーが戻り値にあるときはErrorだけ。正常結果と停止要求だけなら通知しません。
		supervisor.ReportSpoolRunnerRun(context.Background(), report, corruptErr)
		supervisor.ReportSpoolRunnerRun(context.Background(), logspool.RunReport{}, nil)
		stoppedCtx, cancel := context.WithCancel(context.Background())
		cancel()
		supervisor.ReportSpoolRunnerRun(stoppedCtx, logspool.RunReport{}, fmt.Errorf("stop: %w", context.Canceled))
		synctest.Wait()
		require.Len(t, errorsSeen, 2)
		require.Len(t, warnings, 1)
		// 運転中の送信timeoutは停止要求ではなく、通常の障害としてOnErrorへ送ります。
		supervisor.ReportSpoolRunnerRun(context.Background(), logspool.RunReport{}, context.DeadlineExceeded)
		synctest.Wait()
		require.Len(t, errorsSeen, 3)
		require.ErrorIs(t, errorsSeen[2], context.DeadlineExceeded)
		require.NoError(t, pipelines.runtime.Close())
		synctest.Wait()
	})
}

func TestFailureSpoolRecoveryOnErrorCanRequestClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
		failure := errors.New("backend rejected spool resend")
		var runtime *FailureSpoolRuntime
		var reported error
		closed := false
		var err error
		runtime, err = NewFailureSpoolRecoveryRuntime(
			FailureSpoolRecoveryConfigs{Sync: cfg}, testFailureSpoolRuntimeConfig(), observer.NopTracer{}, observer.NopMeter{},
			WithFailureSpoolRecoveryOnError(func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				reported = err
				runtime.RequestClose()
				closed = true
			}),
			withFailureSpoolRecoveryTestSender(failingFailureSpoolNotificationSender{err: failure}),
		)
		require.NoError(t, err)
		// 途中の確認が失敗しても、runnerのgoroutineを止めてからbubbleを終えます。
		t.Cleanup(func() { _ = runtime.Close() })
		store := newFailureSpoolNotificationTestStore(t, cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig)
		storeFailureSpoolNotificationTestRecord(t, store, `{"message":"retry me"}`)
		require.NoError(t, runtime.Start(context.Background()))
		synctest.Wait()
		require.True(t, closed, "OnError must not wait for its own runner goroutine")
		require.ErrorIs(t, reported, failure)
		require.False(t, runtime.runners[0].supervisor.IsRunning())
		require.NoError(t, runtime.Close())
	})
}

func TestFailureSpoolCloseDrainsAcceptedNotificationUnlessForced(t *testing.T) {
	for _, forced := range []bool{false, true} {
		t.Run(fmt.Sprintf("force=%v", forced), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
				started := make(chan context.Context, 1)
				release := make(chan struct{})
				releaseNotification := sync.OnceFunc(func() { close(release) })
				runtime, err := NewFailureSpoolRecoveryRuntime(
					FailureSpoolRecoveryConfigs{Sync: cfg}, testFailureSpoolRuntimeConfig(), observer.NopTracer{}, observer.NopMeter{},
					WithFailureSpoolRecoveryOnError(func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
						started <- ctx
						<-release
					}),
					withFailureSpoolRecoveryTestSender(failingFailureSpoolNotificationSender{err: errors.New("send failed")}),
				)
				require.NoError(t, err)
				// Cleanupは後に登録したものから実行されます。Closeは受理済みの通知を待つため、
				// 途中の確認が失敗しても先に通知を解放するよう、解放をCloseより後に登録します。
				t.Cleanup(func() { _ = runtime.Close() })
				t.Cleanup(releaseNotification)
				store := newFailureSpoolNotificationTestStore(t, cfg.OpenSearch.OpenSearchFlushManagerConfig.FailureStoreConfig.DurableStoreConfig.SpoolConfig)
				storeFailureSpoolNotificationTestRecord(t, store, `{"message":"retry me"}`)
				require.NoError(t, runtime.Start(context.Background()))
				notificationContext := boundedwait.ReceiveInBubble(t, started, "the OnError notification")
				closed := make(chan error, 1)
				go func() { closed <- runtime.Close() }()
				synctest.Wait()
				require.NoError(t, notificationContext.Err())
				select {
				case err := <-closed:
					t.Fatalf("Close returned before notification: %v", err)
				default:
				}
				if forced {
					require.ErrorIs(t, runtime.RequestForceClose(), pkgshutdown.ErrForcedClose)
					require.ErrorIs(t, boundedwait.ReceiveInBubble(t, closed, "the forced Close"), pkgshutdown.ErrForcedClose)
					require.ErrorIs(t, notificationContext.Err(), context.Canceled)
					select {
					case <-runtime.ShutdownDone():
						t.Fatal("cleanup ended while notification was still running")
					default:
					}
				}
				releaseNotification()
				if !forced {
					require.NoError(t, boundedwait.ReceiveInBubble(t, closed, "Close after the notification"))
					require.NoError(t, notificationContext.Err())
				}
				boundedwait.ReceiveInBubble(t, runtime.ShutdownDone(), "the runtime shutdown")
			})
		})
	}
}

func TestFailureSpoolPreservationWarningReceivesForceCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConstructibleOpenSearchFailureSpoolLoggerConfig(t.TempDir(), "opensearch")
		started := make(chan context.Context, 1)
		notified := make(chan struct{})
		runtime, err := NewFailureSpoolRecoveryRuntime(
			FailureSpoolRecoveryConfigs{Sync: cfg}, testFailureSpoolRuntimeConfig(), observer.NopTracer{}, observer.NopMeter{},
			WithFailureSpoolRecoveryOnWarn(func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				started <- ctx
				<-ctx.Done()
				close(notified)
			}),
		)
		require.NoError(t, err)
		// 通知はforceの中止まで戻らないため、途中の確認が失敗した場合もforceで通知を解放してから
		// Closeします。
		t.Cleanup(func() {
			_ = runtime.RequestForceClose()
			_ = runtime.Close()
		})
		require.NoError(t, runtime.Start(context.Background()))
		// Storeは通常のキャンセルを外して通知するため、runtime側でforceだけを接続します。
		runtime.runners[0].storeOnWarn(context.WithoutCancel(context.Background()), &logging.LogEntry{}, observer.NopTracer{}, observer.NopMeter{})
		notificationContext := boundedwait.ReceiveInBubble(t, started, "the OnWarn notification")
		require.NoError(t, notificationContext.Err())
		require.ErrorIs(t, runtime.RequestForceClose(), pkgshutdown.ErrForcedClose)
		boundedwait.ReceiveInBubble(t, notified, "the canceled notification")
		require.ErrorIs(t, notificationContext.Err(), context.Canceled)
		_ = runtime.Close()
		boundedwait.ReceiveInBubble(t, runtime.ShutdownDone(), "the runtime shutdown")
	})
}

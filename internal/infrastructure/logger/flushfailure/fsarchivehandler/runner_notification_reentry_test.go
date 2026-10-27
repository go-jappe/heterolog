package fsarchivehandler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestFailureArchiveRunnerPeriodicNotificationCanRequestShutdown(t *testing.T) {
	// runnerの周期は仮想時間で進め、実時間の周期に依存せずに通知を発生させます。
	synctest.Test(t, func(t *testing.T) {
		ready := make(chan struct{})
		shutdownResult := make(chan error, 1)
		var (
			pipeline *FailureArchivePipeline
			once     sync.Once
		)
		pipeline = newTestArchiveHandler(
			t,
			time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC),
			func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
				cfg.lifecycleRunnerConfig.Interval = 10 * time.Millisecond
			},
			InternalLogHandlers{OnDebug: func(
				context.Context,
				*logging.LogEntry,
				pkgobserver.Tracer,
				pkgobserver.Meter,
			) {
				<-ready
				once.Do(func() {
					pipeline.RequestShutdown()
					shutdownResult <- nil
				})
			}},
		)
		close(ready)

		select {
		case err := <-shutdownResult:
			if err != nil {
				t.Fatalf("RequestShutdown() from periodic runner notification error = %v", err)
			}
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("periodic runner notification could not request shutdown")
		}
		if err := pipeline.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitFailureArchiveRunnerNotificationsDone(t, pipeline)
	})
}

func TestFailureArchiveRunnerAcceptedNotificationCanRequestShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ready := make(chan struct{})
		innerShutdownResult := make(chan error, 1)
		var (
			pipeline *FailureArchivePipeline
			once     sync.Once
		)
		pipeline = newTestArchiveHandler(
			t,
			time.Date(2026, 8, 31, 12, 30, 0, 0, time.UTC),
			func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
				cfg.lifecycleRunnerConfig.Interval = time.Hour
			},
			InternalLogHandlers{OnDebug: func(
				context.Context,
				*logging.LogEntry,
				pkgobserver.Tracer,
				pkgobserver.Meter,
			) {
				<-ready
				once.Do(func() {
					pipeline.RequestShutdown()
					innerShutdownResult <- nil
				})
			}},
		)
		pipeline.runnerRuntime.runners[0].Reporter.ReportRun(context.Background(), logfilelifecycle.RunReport{}, nil)
		close(ready)

		outerShutdownResult := make(chan error, 1)
		go func() {
			outerShutdownResult <- pipeline.Shutdown(context.Background())
		}()

		select {
		case err := <-outerShutdownResult:
			if err != nil {
				t.Fatalf("outer Shutdown() error = %v", err)
			}
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("outer Shutdown deadlocked while dispatching accepted runner notification")
		}
		select {
		case err := <-innerShutdownResult:
			if err != nil {
				t.Fatalf("RequestShutdown() from accepted runner notification error = %v", err)
			}
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("accepted runner notification could not request shutdown")
		}
		waitFailureArchiveRunnerNotificationsDone(t, pipeline)
	})
}

func TestFailureArchiveRunnerPeriodicNotificationPanicIsIsolated(t *testing.T) {
	// runnerの周期は仮想時間で進め、実時間の周期に依存せずに通知を発生させます。
	synctest.Test(t, func(t *testing.T) {
		afterPanic := make(chan struct{}, 1)
		var calls atomic.Int32
		pipeline := newTestArchiveHandler(
			t,
			time.Date(2026, 8, 31, 13, 0, 0, 0, time.UTC),
			func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
				cfg.lifecycleRunnerConfig.Interval = 10 * time.Millisecond
			},
			InternalLogHandlers{OnDebug: func(
				context.Context,
				*logging.LogEntry,
				pkgobserver.Tracer,
				pkgobserver.Meter,
			) {
				if calls.Add(1) == 1 {
					panic("periodic runner notification panic")
				}
				select {
				case afterPanic <- struct{}{}:
				default:
				}
			}},
		)

		select {
		case <-afterPanic:
		case <-time.After(failureArchiveHangTimeout):
			t.Fatal("runner notification dispatcher stopped after callback panic")
		}
		if err := pipeline.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() after periodic callback panic error = %v", err)
		}
		waitFailureArchiveRunnerNotificationsDone(t, pipeline)
	})
}

func TestFailureArchiveRunnerAcceptedNotificationPanicDoesNotInterruptShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		pipeline := newTestArchiveHandler(
			t,
			time.Date(2026, 8, 31, 13, 30, 0, 0, time.UTC),
			func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeRunner
				cfg.lifecycleRunnerConfig.Interval = time.Hour
			},
			InternalLogHandlers{OnDebug: func(
				context.Context,
				*logging.LogEntry,
				pkgobserver.Tracer,
				pkgobserver.Meter,
			) {
				calls.Add(1)
				panic("accepted runner notification panic")
			}},
		)

		pipeline.runnerRuntime.runners[0].Reporter.ReportRun(context.Background(), logfilelifecycle.RunReport{}, nil)
		if err := pipeline.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() was interrupted by accepted notification panic: %v", err)
		}
		waitFailureArchiveRunnerNotificationsDone(t, pipeline)
		if calls.Load() == 0 {
			t.Fatal("accepted runner notification was not dispatched")
		}
	})
}

func waitFailureArchiveRunnerNotificationsDone(
	t *testing.T,
	pipeline *FailureArchivePipeline,
) {
	t.Helper()
	if pipeline == nil || pipeline.runnerNotifications == nil {
		t.Fatal("failure archive runner notification dispatcher is nil")
	}
	select {
	case <-pipeline.runnerNotifications.Done():
	default:
		t.Fatal("Shutdown returned before the runner notification dispatcher drained")
	}
}

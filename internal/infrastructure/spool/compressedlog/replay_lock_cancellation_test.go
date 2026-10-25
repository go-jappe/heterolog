package compressedlog

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRunnerCancellationWhileWaitingForAnotherCycle(t *testing.T) {
	for _, operation := range []string{"run", "run once"} {
		t.Run(operation, func(t *testing.T) {
			store := replaytest.NewStore(t)
			worker := newReplayTestWorker(t, store, nil)
			runner, err := NewRunner(worker, NewRunnerConfigAdapter(RunnerConfig{Interval: time.Millisecond}), nil)
			if err != nil {
				t.Fatal(err)
			}
			synctest.Test(t, func(t *testing.T) {
				runner.runMu.Lock()
				defer runner.runMu.Unlock()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var err error
				if operation == "run" {
					err = runner.Run(ctx)
				} else {
					_, err = runner.RunOnce(ctx)
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("waiting runner = %v", err)
				}
			})
			if runner.IsRunning() {
				t.Fatal("canceled call left the periodic loop running")
			}
			if _, err := runner.RunOnce(context.Background()); err != nil {
				t.Fatalf("retry cycle: %v", err)
			}
		})
	}
}

func TestCoordinatorForcedCancellationWhileHandoffIsStillProtected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := managedUploaderTestConfig(t.TempDir())
		cfg.RunnerInterval = time.Hour
		cfg.LifecycleInterval = time.Second
		coordinator := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
		reported := make(chan error, 1)
		route, err := coordinator.RegisterRoute(ManagedRouteConfig{
			FileName: replaytest.FileNamePattern("app_"),
			RunObserver: func(observedContext context.Context, _ *RunReport, purge *objectstoragespool.TerminalPurgeReport, err error) {
				if purge != nil {
					if !errors.Is(observedContext.Err(), context.Canceled) {
						t.Error("observer did not receive the canceled purger context")
					}
					reported <- err
				}
			},
		}, &replaytest.Uploader{})
		if err != nil {
			t.Fatal(err)
		}
		defer route.Close()
		if err := coordinator.storeOrPurgeMu.RLockContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer coordinator.storeOrPurgeMu.RUnlock()
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case err := <-reported:
			t.Fatalf("purge bypassed handoff protection: %v", err)
		default:
		}
		// 通常の周期停止は受理済み処理を待ち、強制終了だけがlock待ちを中止します。
		_ = coordinator.RequestForceClose()
		<-coordinator.runLoopStopped
		if err := <-reported; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled purger report = %v", err)
		}
	})
}

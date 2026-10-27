package logrecovery

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

type retryLifecycleStub struct {
	close                func() error
	mu                   sync.Mutex
	runs, closes, forces int
	run                  func(context.Context, int) (logfilelifecycle.RunReport, error)
	force                func()
	notifications        <-chan struct{}
}

func (lifecycle *retryLifecycleStub) RecoverOnce(ctx context.Context) (logfilelifecycle.RunReport, error) {
	lifecycle.mu.Lock()
	lifecycle.runs++
	attempt := lifecycle.runs
	lifecycle.mu.Unlock()
	return lifecycle.run(ctx, attempt)
}

func (lifecycle *retryLifecycleStub) ForceClose() error {
	lifecycle.mu.Lock()
	lifecycle.forces++
	lifecycle.mu.Unlock()
	if lifecycle.force != nil {
		lifecycle.force()
	}
	return pkgshutdown.ErrForcedClose
}

func (lifecycle *retryLifecycleStub) CloseObjectStorageCompressedLogFileUploadFailureRoutes() error {
	lifecycle.mu.Lock()
	lifecycle.closes++
	lifecycle.mu.Unlock()
	if lifecycle.close != nil {
		return lifecycle.close()
	}
	return nil
}

func (lifecycle *retryLifecycleStub) NotificationsDone() <-chan struct{} {
	if lifecycle.notifications != nil {
		return lifecycle.notifications
	}
	done := make(chan struct{})
	close(done)
	return done
}

func retryStubSpec(lifecycle *retryLifecycleStub, builds *atomic.Int64) retryRecoverySpec {
	return retryRecoverySpec{name: "file-logger", build: func(logfilelifecycle.RunReporter) ([]recoveryLifecycle, error) {
		builds.Add(1)
		return []recoveryLifecycle{lifecycle}, nil
	}}
}

func TestRetryRecoveryKeepsOwnerAndClearsResolvedErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("temporary copy failure")
		lifecycle := &retryLifecycleStub{run: func(_ context.Context, attempt int) (logfilelifecycle.RunReport, error) {
			report := logfilelifecycle.RunReport{DirectoryPath: "/logs"}
			if attempt == 1 {
				report.Compressed = []logfilelifecycle.CompressionReport{{CompressedPath: "/logs/first.log.gz"}}
				report.RecoveryErrors = []logfilelifecycle.FileRecoveryError{{Path: "/logs/second.log", Err: failure}}
				return report, failure
			}
			report.Compressed = []logfilelifecycle.CompressionReport{{CompressedPath: "/logs/second.log.gz"}}
			return report, nil
		}}
		var builds atomic.Int64
		var sawRetry atomic.Bool
		report, err := runRetryRecovery(context.Background(), []retryRecoverySpec{retryStubSpec(lifecycle, &builds)}, Options{
			Retry: true, OnProgress: func(progress Progress) {
				if progress.Phase == ProgressRetryWaiting {
					sawRetry.Store(true)
				}
			},
		})
		require.NoError(t, err)
		require.Empty(t, report.Errors)
		require.Empty(t, report.Directories[0].FailedFiles)
		require.Empty(t, report.Directories[0].Error)
		require.ElementsMatch(t, []string{"/logs/first.log.gz", "/logs/second.log.gz"}, report.Directories[0].CompressedFiles)
		require.EqualValues(t, 1, builds.Load())
		require.Equal(t, 2, lifecycle.runs)
		require.Equal(t, 1, lifecycle.closes)
		require.True(t, sawRetry.Load())
	})
}

func TestRetryRecoveryGracefulStopKeepsAcceptedRetryContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, stop := context.WithCancel(context.Background())
		defer stop()
		lifecycle := &retryLifecycleStub{run: func(workCtx context.Context, attempt int) (logfilelifecycle.RunReport, error) {
			// runは再試行workerのgoroutineで動くため、t.Fatalではなく失敗を記録して続けます。
			if err := workCtx.Err(); err != nil {
				t.Errorf("通常終了では受理済み処理を中止しない: %v", err)
			}
			if attempt == 1 {
				stop()
				return logfilelifecycle.RunReport{}, errors.New("retry accepted operation")
			}
			return logfilelifecycle.RunReport{DirectoryPath: "/logs"}, nil
		}}
		var builds atomic.Int64
		report, err := runRetryRecovery(ctx, []retryRecoverySpec{retryStubSpec(lifecycle, &builds)}, Options{Retry: true})
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, report.Directories, 1)
		require.Equal(t, 2, lifecycle.runs)
		require.Zero(t, lifecycle.forces)
		require.Equal(t, 1, lifecycle.closes)
	})
}

func TestRetryRecoveryForceWaitsForAcceptedIO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var forcedSeen, retryAfterForce atomic.Bool
		force := make(chan struct{})
		entered := make(chan struct{})
		canceled := make(chan struct{})
		release := make(chan struct{})
		requestForce := sync.OnceFunc(func() { close(force) })
		releaseIO := sync.OnceFunc(func() { close(release) })
		// 途中の確認が失敗しても、強制停止を要求してから受理済みのI/Oを解放し、復旧処理の
		// goroutineを残さずにbubbleを終えます。
		t.Cleanup(func() {
			requestForce()
			releaseIO()
		})
		lifecycle := &retryLifecycleStub{run: func(ctx context.Context, _ int) (logfilelifecycle.RunReport, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return logfilelifecycle.RunReport{}, ctx.Err()
		}}
		var builds atomic.Int64
		returned := make(chan error, 1)
		go func() {
			_, err := runRetryRecovery(context.Background(), []retryRecoverySpec{retryStubSpec(lifecycle, &builds)}, Options{
				Retry: true, ForceStop: force,
				OnProgress: func(progress Progress) {
					if progress.Phase == ProgressForced {
						forcedSeen.Store(true)
					}
					if progress.Phase == ProgressRetryWaiting && forcedSeen.Load() {
						retryAfterForce.Store(true)
					}
				},
			})
			returned <- err
		}()
		boundedwait.ReceiveInBubble(t, entered, "the accepted recovery I/O")
		requestForce()
		boundedwait.ReceiveInBubble(t, canceled, "the cancellation of the accepted I/O")
		synctest.Wait()
		select {
		case <-returned:
			t.Fatal("OS I/Oの終了前には戻らない")
		default:
		}
		releaseIO()
		err := boundedwait.ReceiveInBubble(t, returned, "the forced recovery")
		require.ErrorIs(t, err, pkgshutdown.ErrForcedClose)
		require.Equal(t, 1, lifecycle.runs)
		require.Equal(t, 1, lifecycle.closes)
		require.GreaterOrEqual(t, lifecycle.forces, 1)
		require.True(t, forcedSeen.Load())
		require.False(t, retryAfterForce.Load(), "強制停止後に次回再試行を案内しない")
	})
}

func TestRetryRecoveryForcesOwnerPublishedAfterForceRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		force := make(chan struct{})
		building, release := make(chan struct{}), make(chan struct{})
		lifecycle := &retryLifecycleStub{run: func(context.Context, int) (logfilelifecycle.RunReport, error) {
			t.Error("強制停止後は新しい処理を開始しない")
			return logfilelifecycle.RunReport{}, nil
		}}
		returned := make(chan error, 1)
		go func() {
			_, err := runRetryRecovery(context.Background(), []retryRecoverySpec{{name: "file-logger", build: func(logfilelifecycle.RunReporter) ([]recoveryLifecycle, error) {
				close(building)
				<-release
				return []recoveryLifecycle{lifecycle}, nil
			}}}, Options{Retry: true, ForceStop: force})
			returned <- err
		}()
		<-building
		close(force)
		synctest.Wait()
		close(release)
		require.ErrorIs(t, <-returned, pkgshutdown.ErrForcedClose)
		// ForceCloseは繰り返し呼べる契約なので、回数ではなく強制停止が届いたことを確認します。
		require.GreaterOrEqual(t, lifecycle.forces, 1)
		require.Equal(t, 1, lifecycle.closes)
	})
}

func TestRetryRecoveryRetriesPanicAndWaitsForNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		notifications := make(chan struct{})
		lifecycle := &retryLifecycleStub{notifications: notifications, run: func(_ context.Context, attempt int) (logfilelifecycle.RunReport, error) {
			if attempt == 1 {
				panic("interrupted file operation")
			}
			return logfilelifecycle.RunReport{DirectoryPath: "/logs"}, nil
		}}
		var builds atomic.Int64
		returned := make(chan error, 1)
		go func() {
			_, err := runRetryRecovery(context.Background(), []retryRecoverySpec{retryStubSpec(lifecycle, &builds)}, Options{Retry: true})
			returned <- err
		}()
		time.Sleep(DefaultRetryInterval)
		synctest.Wait()
		select {
		case <-returned:
			t.Fatal("通知の完了を待つ")
		default:
		}
		close(notifications)
		require.NoError(t, <-returned)
		require.Equal(t, 2, lifecycle.runs)
		require.EqualValues(t, 1, builds.Load())
	})
}

func TestRetryRecoveryDoesNotStartAfterForceRequest(t *testing.T) {
	force := make(chan struct{})
	close(force)
	_, err := runRetryRecovery(context.Background(), []retryRecoverySpec{{name: "file-logger", build: func(logfilelifecycle.RunReporter) ([]recoveryLifecycle, error) {
		// buildは再試行workerのgoroutineで動くため、t.Fatalではなく失敗を記録します。
		t.Error("強制停止済みの処理を開始しない")
		return nil, errors.New("unexpected recovery start")
	}}}, Options{Retry: true, ForceStop: force})
	require.ErrorIs(t, err, pkgshutdown.ErrForcedClose)
}

func TestRetryRecoveryForceReachesOwnerWhileCloseWaitsForSave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		force := make(chan struct{})
		closing, savedOrForced := make(chan struct{}), make(chan struct{})
		lifecycle := &retryLifecycleStub{
			run: func(context.Context, int) (logfilelifecycle.RunReport, error) {
				return logfilelifecycle.RunReport{DirectoryPath: "/logs"}, nil
			},
			close: func() error { close(closing); <-savedOrForced; return pkgshutdown.ErrForcedClose },
			// 実際のForceCloseと同じく、繰り返し呼ばれても一度だけ解除します。
			force: sync.OnceFunc(func() { close(savedOrForced) }),
		}
		var builds atomic.Int64
		returned := make(chan error, 1)
		go func() {
			_, err := runRetryRecovery(context.Background(), []retryRecoverySpec{retryStubSpec(lifecycle, &builds)}, Options{Retry: true, ForceStop: force})
			returned <- err
		}()
		<-closing
		close(force)
		require.ErrorIs(t, <-returned, pkgshutdown.ErrForcedClose)
		require.Equal(t, 1, lifecycle.closes)
		require.GreaterOrEqual(t, lifecycle.forces, 1)
	})
}

func TestRetryRecoveryBuildFailureClosesPartialOwnerAndRebuilds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		unused := &retryLifecycleStub{run: func(context.Context, int) (logfilelifecycle.RunReport, error) {
			t.Error("構築失敗のownerを実行しない")
			return logfilelifecycle.RunReport{}, nil
		}}
		active := &retryLifecycleStub{run: func(context.Context, int) (logfilelifecycle.RunReport, error) {
			return logfilelifecycle.RunReport{DirectoryPath: "/logs"}, nil
		}}
		builds := 0
		report, err := runRetryRecovery(context.Background(), []retryRecoverySpec{{name: "file-logger", build: func(logfilelifecycle.RunReporter) ([]recoveryLifecycle, error) {
			builds++
			if builds == 1 {
				return []recoveryLifecycle{unused}, errors.New("temporary construction failure")
			}
			return []recoveryLifecycle{active}, nil
		}}}, Options{Retry: true})
		require.NoError(t, err)
		require.Empty(t, report.Errors)
		require.Equal(t, 2, builds)
		require.Equal(t, 1, unused.closes)
		require.Equal(t, 1, active.closes)
	})
}

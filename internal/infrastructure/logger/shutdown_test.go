package logger

import (
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

// 初回のbackend Closeが失敗しても、呼び出し元はCloseを繰り返しません。Controllerが
// 時間を空けてbackend Closeを再試行し、成功した時点で一度のCloseが戻ります。
// 再試行の間隔がReportInterval以上であることはshutdownパッケージの
// TestControllerRetriesIncompleteCleanupで確認し、ここでは待機の後に再試行したことだけを
// 確認します。
func TestTopLevelGracefulCloseRetriesWithoutCallerLoop(t *testing.T) {
	tests := []struct {
		name string
		new  func() (interface{ Close() error }, func() []time.Time)
	}{
		{"sync", func() (interface{ Close() error }, func() []time.Time) {
			backend := &closeStageTestSyncBackend{closeErrors: []error{errors.New("temporary"), nil}}
			return &topLevelLoggerRuntime{topLevelLoggerDelegate: backend}, func() []time.Time { return backend.closeTimes }
		}},
		{"async", func() (interface{ Close() error }, func() []time.Time) {
			backend := &closeStageTestAsyncBackend{closeErrors: []error{errors.New("temporary"), nil}}
			return &topLevelAsyncLoggerRuntime{topLevelAsyncLoggerDelegate: backend}, func() []time.Time { return backend.closeTimes }
		}},
		{"delivery", func() (interface{ Close() error }, func() []time.Time) {
			backend := &closeStageTestDeliveryBackend{closeErrors: []error{errors.New("temporary"), nil}}
			return &topLevelDeliveryLoggerRuntime{topLevelDeliveryLoggerDelegate: backend}, func() []time.Time { return backend.closeTimes }
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				logger, closeTimes := tc.new()
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
				// Closeの完了はController workerの終了後なので、呼出し記録を競合なく読めます。
				attempts := closeTimes()
				if len(attempts) != 2 {
					t.Fatalf("backend Close calls = %d, want a failed attempt and one retry", len(attempts))
				}
				if !attempts[1].After(attempts[0]) {
					t.Fatalf("retried backend Close without waiting: attempts=%v", attempts)
				}
			})
		})
	}
}

type forceTestBackend struct {
	pkglogger.Logger
	entered chan struct{}
	release chan struct{}
	forced  chan struct{}
	once    sync.Once
}

func (b *forceTestBackend) StopAccepting() {}
func (b *forceTestBackend) Close() error   { close(b.entered); <-b.release; return nil }
func (b *forceTestBackend) ForceClose() error {
	b.once.Do(func() { close(b.forced) })
	return pkgshutdown.ErrForcedClose
}

func TestTopLevelPublicForceCloseReachesOwnerAndReleasesWait(t *testing.T) {
	for _, test := range []struct {
		name string
		new  func(*forceTestBackend) (io.Closer, *shutdown.Controller)
	}{
		{"sync", func(b *forceTestBackend) (io.Closer, *shutdown.Controller) {
			r := &topLevelLoggerRuntime{topLevelLoggerDelegate: b}
			return r, &r.closeController
		}},
		{"async", func(b *forceTestBackend) (io.Closer, *shutdown.Controller) {
			r := &topLevelAsyncLoggerRuntime{topLevelAsyncLoggerDelegate: &forceTestAsyncBackend{lifecycle: b}}
			return r, &r.closeController
		}},
		{"delivery", func(b *forceTestBackend) (io.Closer, *shutdown.Controller) {
			r := &topLevelDeliveryLoggerRuntime{topLevelDeliveryLoggerDelegate: &forceTestDeliveryBackend{lifecycle: b}}
			return r, &r.closeController
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := &forceTestBackend{entered: make(chan struct{}), release: make(chan struct{}), forced: make(chan struct{})}
				unblock := sync.OnceFunc(func() { close(b.release) })
				defer unblock()
				logger, controller := test.new(b)
				done := make(chan error, 1)
				go func() { done <- logger.Close() }()
				<-b.entered
				time.Sleep(2 * shutdown.ReportInterval)
				synctest.Wait()
				if len(done) != 0 || controller.IsComplete() {
					t.Fatal("normal Close did not wait for accepted backend cleanup")
				}
				if err := applogging.ForceClose(logger); !errors.Is(err, pkgshutdown.ErrForcedClose) {
					t.Fatal(err)
				}
				if err := <-done; !errors.Is(err, pkgshutdown.ErrForcedClose) {
					t.Fatal(err)
				}
				<-b.forced
				select {
				case <-controller.Done():
					t.Fatal("live backend cleanup abandoned")
				default:
				}
				unblock()
				<-controller.Done()
				if err := logger.Close(); !errors.Is(err, pkgshutdown.ErrForcedClose) {
					t.Fatalf("repeated Close lost forced result: %v", err)
				}
			})
		})
	}
}

type forceTestAsyncBackend struct {
	pkglogger.AsyncLogger
	lifecycle *forceTestBackend
}

func (b *forceTestAsyncBackend) StopAccepting()    { b.lifecycle.StopAccepting() }
func (b *forceTestAsyncBackend) Close() error      { return b.lifecycle.Close() }
func (b *forceTestAsyncBackend) ForceClose() error { return b.lifecycle.ForceClose() }

type forceTestDeliveryBackend struct {
	pkglogger.DeliveryLogger
	lifecycle *forceTestBackend
}

func (b *forceTestDeliveryBackend) StopAccepting()    { b.lifecycle.StopAccepting() }
func (b *forceTestDeliveryBackend) Close() error      { return b.lifecycle.Close() }
func (b *forceTestDeliveryBackend) ForceClose() error { return b.lifecycle.ForceClose() }

package opensearch

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFallbackHandlerControllerCloseDrainsEnqueuedBatchAndClosesLogger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		manager := newFallbackControllerFlushManagerProbe()
		fallback := newFallbackControllerTestLogger(t, cfg, manager)

		controller := &fallbackHandlerController{
			initializationAttempted: true,
			logger:                  fallback,
		}
		// cleanupは登録と逆順に実行するため、失敗時も先に最終batchを解放してからCloseし、
		// bubble内のgoroutineをdeadlockではなく停止させます。
		t.Cleanup(func() { _ = controller.Close() })
		t.Cleanup(manager.releaseIndex)
		tracer, meter := newTestObserver()

		controller.Handle(context.Background(), &logging.LogEntry{
			Level:   pkglogger.LogLevelInfo,
			Message: "drain enqueued fallback",
		}, tracer, meter)
		select {
		case <-manager.indexStarted:
			t.Fatal("fallback handler performed synchronous backend I/O")
		default:
		}

		closeDone := make(chan error, 1)
		go func() { closeDone <- controller.Close() }()
		select {
		case <-manager.indexStarted:
		case <-time.After(time.Second):
			t.Fatal("Close did not start final fallback batch")
		}

		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the final batch completed: %v", err)
		default:
		}

		manager.releaseIndex()
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatalf("Close: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not complete")
		}

		manager.mu.Lock()
		defer manager.mu.Unlock()
		if manager.closeCalls != 1 {
			t.Fatalf("underlying flush manager Close calls = %d, want 1", manager.closeCalls)
		}
		if manager.closeWhileIndexActive {
			t.Fatal("underlying logger was closed while fallback batch was active")
		}
	})
}

func TestFallbackHandlerControllerCloseBeforeInitializationRejectsFutureHandlers(t *testing.T) {
	controller := &fallbackHandlerController{}

	if err := controller.Close(); err != nil {
		t.Fatalf("Close before initialization: %v", err)
	}
	controller.Handle(
		context.Background(),
		&logging.LogEntry{Level: pkglogger.LogLevelInfo},
		nil,
		nil,
	)
	controller.HandleError(
		context.Background(),
		context.Canceled,
		&logging.LogEntry{Level: pkglogger.LogLevelError},
		nil,
		nil,
	)

	if controller.initializationAttempted {
		t.Fatal("fallback logger was initialized after Close")
	}
	if !controller.closing {
		t.Fatal("controller is not marked closing")
	}
}

func TestNamedFallbackHandlersExposeControllerCloser(t *testing.T) {
	constructors := map[string]func() any{
		"error": func() any {
			return NewNamedErrorFallbackHandler(nil, nil, nil, nil, nil)
		},
		"warn": func() any {
			return NewNamedWarnFallbackHandler(nil, nil, nil, nil, nil)
		},
		"info": func() any {
			return NewNamedInfoFallbackHandler(nil, nil, nil, nil, nil)
		},
		"debug": func() any {
			return NewNamedDebugFallbackHandler(nil, nil, nil, nil, nil)
		},
	}

	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			var closer any
			switch named := construct().(type) {
			case logging.NamedLoggerErrorHandler:
				if named.Handler == nil {
					t.Fatal("Handler is nil")
				}
				closer = named.Closer
			case logging.NamedLoggerHandler:
				if named.Handler == nil {
					t.Fatal("Handler is nil")
				}
				closer = named.Closer
			default:
				t.Fatalf("unexpected named handler type %T", named)
			}
			controller, ok := closer.(*fallbackHandlerController)
			if !ok {
				t.Fatalf("Closer type = %T, want *fallbackHandlerController", closer)
			}
			if err := controller.Close(); err != nil {
				t.Fatalf("uninitialized Close: %v", err)
			}
		})
	}
}

func newFallbackControllerTestLogger(
	t *testing.T,
	cfg *MockOpenSearchConfigProvider,
	manager FlushManager,
) *FallbackLogger {
	t.Helper()
	logger, _ := newTestLogger(t, cfg, nil)
	logger.mu.Lock()
	logger.flushManager = manager
	logger.mu.Unlock()
	return &FallbackLogger{OpenSearchLogger: logger}
}

type fallbackControllerFlushManagerProbe struct {
	mu                    sync.Mutex
	indexStarted          chan struct{}
	indexRelease          chan struct{}
	indexStartedOnce      sync.Once
	indexReleaseOnce      sync.Once
	indexActive           bool
	closeCalls            int
	closeWhileIndexActive bool
}

func newFallbackControllerFlushManagerProbe() *fallbackControllerFlushManagerProbe {
	return &fallbackControllerFlushManagerProbe{
		indexStarted: make(chan struct{}),
		indexRelease: make(chan struct{}),
	}
}

// releaseIndexは、待機中の最終batch送信を一度だけ解放します。
func (m *fallbackControllerFlushManagerProbe) releaseIndex() {
	m.indexReleaseOnce.Do(func() { close(m.indexRelease) })
}

func (m *fallbackControllerFlushManagerProbe) Flush(context.Context, []*logging.LogEntry) error {
	return nil
}

func (m *fallbackControllerFlushManagerProbe) Index(context.Context, *logging.LogEntry) error {
	return nil
}

func (m *fallbackControllerFlushManagerProbe) IndexWithReport(
	context.Context,
	*logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	return pkglogger.DeliveryReport{}, nil
}

func (m *fallbackControllerFlushManagerProbe) FlushWithNoFallback(
	context.Context,
	[]*logging.LogEntry,
) {
	m.mu.Lock()
	m.indexActive = true
	m.mu.Unlock()
	m.indexStartedOnce.Do(func() { close(m.indexStarted) })
	<-m.indexRelease
	m.mu.Lock()
	m.indexActive = false
	m.mu.Unlock()
}

func (m *fallbackControllerFlushManagerProbe) Close(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	m.closeWhileIndexActive = m.indexActive
	return nil
}

func (m *fallbackControllerFlushManagerProbe) Config() OpenSearchFlushManagerRuntimeConfigProvider {
	return nil
}

func (m *fallbackControllerFlushManagerProbe) LogEntryToDocumentConverter() LogEntryToDocumentConverter {
	return nil
}

func (m *fallbackControllerFlushManagerProbe) Resource() resource.ResourceProvider {
	return nil
}

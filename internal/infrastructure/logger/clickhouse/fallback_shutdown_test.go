package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

type completedClickHouseFallbackManager struct {
	*lifecycleClickHouseFlushManager
}

func (p *completedClickHouseFallbackManager) ShutdownComplete() bool {
	return p.snapshot().closeCalls > 0
}

func TestClickHouseFallbackCloseSuppressesCompletedManagerError(t *testing.T) {
	manager := &completedClickHouseFallbackManager{newLifecycleClickHouseFlushManager(false)}
	manager.closeErrors = []error{errors.New("completed manager result")}
	fallback := newTimerTestClickHouseFallback(t, &recordingClickHouseClient{}, newTestClickHouseLoggerBaseConfig())
	fallback.mu.Lock()
	fallback.flushManager = manager
	fallback.mu.Unlock()
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fallback.ShutdownComplete() {
		t.Fatal("completed manager error stranded fallback cleanup")
	}
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls := manager.snapshot().closeCalls; calls != 1 {
		t.Fatalf("manager cleanup calls = %d, want 1", calls)
	}
}

func TestClickHouseFallbackCloseRetriesIncompleteManagerWithoutResending(t *testing.T) {
	cfg := newTestClickHouseLoggerBaseConfig()
	cfg.FlushInterval = time.Hour
	cfg.MaxEntries = 10
	fallback := newTimerTestClickHouseFallback(t, &recordingClickHouseClient{}, cfg)
	cleanupErr := errors.New("manager cleanup incomplete")
	manager := newLifecycleClickHouseFlushManager(false)
	manager.closeErrors = []error{cleanupErr, nil}
	fallback.mu.Lock()
	fallback.flushManager = manager
	fallback.mu.Unlock()
	fallback.enqueueProcess(context.Background(), &logging.LogEntry{Message: "final batch"})
	if err := fallback.Close(context.Background()); !errors.Is(err, cleanupErr) {
		t.Fatalf("first Close error = %v, want incomplete cleanup", err)
	}
	if fallback.ShutdownComplete() {
		t.Fatal("fallback declared cleanup complete after incomplete manager close")
	}
	fallback.enqueueProcess(context.Background(), &logging.LogEntry{Message: "rejected during shutdown"})
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fallback.ShutdownComplete() {
		t.Fatal("fallback cleanup did not complete after retry")
	}
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := manager.snapshot()
	if snapshot.closeCalls != 2 || snapshot.flushCalls != 1 {
		t.Fatalf("manager close calls=%d send calls=%d, want two cleanup attempts and one send", snapshot.closeCalls, snapshot.flushCalls)
	}
}

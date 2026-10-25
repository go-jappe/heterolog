package opensearch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

type completedOpenSearchFallbackBatchProbe struct{ *openSearchFallbackBatchProbe }

// 終了済みの結果errorと、後片付け未完了のerrorを区別するための試験用契約です。
func (p *completedOpenSearchFallbackBatchProbe) ShutdownComplete() bool {
	_, calls := p.snapshot()
	return calls > 0
}

func TestOpenSearchFallbackCloseRetriesIncompleteManagerWithoutResending(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 10
	cfg.flushInterval = time.Hour
	manager := newOpenSearchFallbackBatchProbe()
	fallback := newOpenSearchFallbackBatchTestLogger(t, cfg, manager)
	cleanupErr := errors.New("manager cleanup incomplete")
	manager.closeErr = cleanupErr
	fallback.enqueueProcess(&logging.LogEntry{Message: "final batch"})
	if err := fallback.Close(context.Background()); !errors.Is(err, cleanupErr) {
		t.Fatalf("first Close error = %v, want incomplete cleanup", err)
	}
	if fallback.ShutdownComplete() {
		t.Fatal("fallback declared cleanup complete after incomplete manager close")
	}
	// 受付停止は維持し、失敗した終了処理だけを再試行します。
	fallback.enqueueProcess(&logging.LogEntry{Message: "rejected during shutdown"})
	manager.mu.Lock()
	manager.closeErr = nil
	manager.mu.Unlock()
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fallback.ShutdownComplete() {
		t.Fatal("fallback cleanup did not complete after retry")
	}
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	batches, calls := manager.snapshot()
	if calls != 2 || len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("manager close calls=%d batches=%v, want two cleanup attempts and one send", calls, batches)
	}
}

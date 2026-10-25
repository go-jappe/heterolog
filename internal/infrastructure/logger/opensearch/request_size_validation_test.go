package opensearch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestValidateBufferFlushBytesRejectsNonPositiveValues(t *testing.T) {
	for _, value := range []int{0, -1} {
		if err := validateBufferFlushBytes(value); !errors.Is(err, ErrInvalidBufferFlushBytes) {
			t.Fatalf("validateBufferFlushBytes(%d) error = %v, want ErrInvalidBufferFlushBytes", value, err)
		}
	}
}

func TestOpenSearchLoggerFlushesAtBufferFlushBytesWithoutUsingItAsBulkLimit(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 100
	cfg.flushInterval = time.Hour
	logger, client := newTestLogger(t, cfg, nil)

	occurredAt := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	first := &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "first buffered document",
	}
	second := &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "second buffered document",
	}
	firstBytes, err := logger.measureBufferedEntryBytes(first)
	if err != nil {
		t.Fatalf("measure first buffered entry: %v", err)
	}
	secondBytes, err := logger.measureBufferedEntryBytes(second)
	if err != nil {
		t.Fatalf("measure second buffered entry: %v", err)
	}
	cfg.bufferFlushBytes = firstBytes + secondBytes
	// BufferFlushBytesは送信開始のsoft thresholdであり、Bulk requestの
	// hard limitではないことを別の値で固定します。
	cfg.maxBulkRequestBytes = cfg.bufferFlushBytes * 2

	if err := logger.enqueue(context.Background(), first); err != nil {
		t.Fatalf("enqueue first entry: %v", err)
	}
	client.mu.RLock()
	calledAfterFirst := client.IndexDocumentCalled || client.IndexDocumentsCalled
	client.mu.RUnlock()
	if calledAfterFirst {
		t.Fatal("entry below BufferFlushBytes triggered a backend request")
	}

	if err := logger.enqueue(context.Background(), second); err != nil {
		t.Fatalf("enqueue second entry: %v", err)
	}
	client.mu.RLock()
	bulkCalled := client.IndexDocumentsCalled
	bulkDocuments := len(client.IndexDocumentsArgs)
	client.mu.RUnlock()
	if !bulkCalled || bulkDocuments != 2 {
		t.Fatalf("bulk call = %t with %d documents, want one two-document flush", bulkCalled, bulkDocuments)
	}

	logger.mu.Lock()
	bufferedEntries := len(logger.entries)
	bufferedBytes := logger.bufferBytes
	logger.mu.Unlock()
	if bufferedEntries != 0 || bufferedBytes != 0 {
		t.Fatalf("buffer after successful flush = %d entries/%d bytes, want empty", bufferedEntries, bufferedBytes)
	}
}

func TestOpenSearchLoggerDropsSnapshotAfterFailedThresholdFlush(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 100
	cfg.flushInterval = time.Hour
	logger, client := newTestLogger(t, cfg, nil)

	occurredAt := time.Date(2026, 8, 19, 12, 1, 0, 0, time.UTC)
	entries := []*logging.LogEntry{
		{OccurredAt: occurredAt, Level: pkglogger.LogLevelInfo, Message: "first detached document"},
		{OccurredAt: occurredAt, Level: pkglogger.LogLevelInfo, Message: "second detached document"},
	}
	wantBytes := 0
	for _, entry := range entries {
		entryBytes, err := logger.measureBufferedEntryBytes(entry)
		if err != nil {
			t.Fatalf("measure buffered entry: %v", err)
		}
		wantBytes += entryBytes
	}
	cfg.bufferFlushBytes = wantBytes
	cfg.maxBulkRequestBytes = wantBytes * 2
	client.mu.Lock()
	client.returnError = true
	client.mu.Unlock()

	if err := logger.enqueue(context.Background(), entries[0]); err != nil {
		t.Fatalf("enqueue first entry: %v", err)
	}
	if err := logger.enqueue(context.Background(), entries[1]); err == nil {
		t.Fatal("threshold flush error = nil, want backend failure")
	}

	logger.mu.Lock()
	gotEntries := len(logger.entries)
	gotBytes := logger.bufferBytes
	logger.mu.Unlock()
	if gotEntries != 0 || gotBytes != 0 {
		t.Fatalf("buffer after failed one-shot flush = %d entries/%d bytes, want empty", gotEntries, gotBytes)
	}
}

func TestOpenSearchImmediateIndexDoesNotUseMaxBulkRequestBytes(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.noBulkFlushModeEnabled = true
	cfg.maxBulkRequestBytes = 1
	logger, client := newTestLogger(t, cfg, nil)

	if err := logger.testInfo(context.Background(), "Index is bounded by the upstream log-entry limit"); err != nil {
		t.Fatalf("Info() error = %v, want Index attempt", err)
	}
	client.mu.RLock()
	indexCalled := client.IndexDocumentCalled
	bulkCalled := client.IndexDocumentsCalled
	client.mu.RUnlock()
	if !indexCalled || bulkCalled {
		t.Fatalf("Index called = %t, Bulk called = %t, want only Index", indexCalled, bulkCalled)
	}
}

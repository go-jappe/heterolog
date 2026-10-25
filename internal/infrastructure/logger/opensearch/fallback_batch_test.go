package opensearch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestOpenSearchFallbackQueueDropsOldestAndCloseFlushesOnce(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 2
	cfg.flushInterval = time.Hour
	manager := newOpenSearchFallbackBatchProbe()
	fallback := newOpenSearchFallbackBatchTestLogger(t, cfg, manager)
	occurredAt := time.Date(2026, 8, 19, 1, 2, 3, 0, time.UTC)

	first := &logging.LogEntry{OccurredAt: occurredAt, Message: "first"}
	second := &logging.LogEntry{OccurredAt: occurredAt, Message: "second"}
	third := &logging.LogEntry{OccurredAt: occurredAt, Message: "third"}
	fallback.enqueueProcess(first)
	fallback.enqueueProcess(second)
	fallback.enqueueProcess(third)

	fallback.mu.Lock()
	queued := append([]*logging.LogEntry(nil), fallback.entries...)
	queuedBytes := fallback.bufferBytes
	fallback.mu.Unlock()
	if len(queued) != 2 || queued[0] != second || queued[1] != third {
		t.Fatalf("bounded queue = %#v, want [second third]", queued)
	}
	wantBytes := 0
	for _, entry := range []*logging.LogEntry{second, third} {
		entryBytes, err := fallback.measureBufferedEntryBytes(entry)
		if err != nil {
			t.Fatalf("measure retained fallback entry: %v", err)
		}
		wantBytes += entryBytes
	}
	if queuedBytes != wantBytes {
		t.Fatalf("bounded queue bytes = %d, want retained entries size %d", queuedBytes, wantBytes)
	}
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	batches, closeCalls := manager.snapshot()
	if len(batches) != 1 || len(batches[0]) != 2 || batches[0][0] != second || batches[0][1] != third {
		t.Fatalf("flushed batches = %#v, want one [second third] batch", batches)
	}
	if closeCalls != 1 {
		t.Fatalf("flush manager Close calls = %d, want 1", closeCalls)
	}
}

func TestOpenSearchFallbackUsesBatchWorkerWhenPrimaryNoBulkModeIsEnabled(t *testing.T) {
	// FlushIntervalのtickerはbubbleの時間で進むため、実時間の経過に依存せずintervalを検証します。
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.noBulkFlushModeEnabled = true
		cfg.maxBulkDocument = 10
		cfg.flushInterval = time.Millisecond
		manager := newOpenSearchFallbackBatchProbe()
		fallback := newOpenSearchFallbackBatchTestLogger(t, cfg, manager)
		fallback.startFallbackWorker()
		t.Cleanup(func() { _ = fallback.Close(context.Background()) })

		fallback.enqueueProcess(&logging.LogEntry{OccurredAt: fallback.Clock.Now(), Message: "fallback remains batched"})
		select {
		case <-manager.started:
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("fallback batch worker did not start when primary no-bulk mode was enabled")
		}

		batches, _ := manager.snapshot()
		if len(batches) != 1 || len(batches[0]) != 1 {
			t.Fatalf("no-bulk primary fallback batches = %#v, want one single-entry batch", batches)
		}
	})
}

func TestOpenSearchFallbackWorkersKeepInstanceBatchesSeparate(t *testing.T) {
	firstCfg := NewMockOpenSearchConfigProvider()
	firstCfg.maxBulkDocument = 1
	firstCfg.flushInterval = time.Hour
	firstManager := newOpenSearchFallbackBatchProbe()
	firstFallback := newOpenSearchFallbackBatchTestLogger(t, firstCfg, firstManager)
	firstFallback.startFallbackWorker()
	t.Cleanup(func() { _ = firstFallback.Close(context.Background()) })

	secondCfg := NewMockOpenSearchConfigProvider()
	secondCfg.maxBulkDocument = 1
	secondCfg.flushInterval = time.Hour
	secondManager := newOpenSearchFallbackBatchProbe()
	secondFallback := newOpenSearchFallbackBatchTestLogger(t, secondCfg, secondManager)
	secondFallback.startFallbackWorker()
	t.Cleanup(func() { _ = secondFallback.Close(context.Background()) })

	firstEntry := &logging.LogEntry{OccurredAt: firstFallback.Clock.Now(), Message: "first instance"}
	secondEntry := &logging.LogEntry{OccurredAt: secondFallback.Clock.Now(), Message: "second instance"}
	firstFallback.enqueueProcess(firstEntry)
	secondFallback.enqueueProcess(secondEntry)
	select {
	case <-firstManager.started:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("first fallback worker did not flush")
	}
	select {
	case <-secondManager.started:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("second fallback worker did not flush")
	}

	firstBatches, _ := firstManager.snapshot()
	if len(firstBatches) != 1 || len(firstBatches[0]) != 1 || firstBatches[0][0] != firstEntry {
		t.Fatalf("first instance batches = %#v, want only first entry", firstBatches)
	}
	secondBatches, _ := secondManager.snapshot()
	if len(secondBatches) != 1 || len(secondBatches[0]) != 1 || secondBatches[0][0] != secondEntry {
		t.Fatalf("second instance batches = %#v, want only second entry", secondBatches)
	}
}

func TestOpenSearchFallbackWorkerFlushesByInterval(t *testing.T) {
	// FlushIntervalのtickerはbubbleの時間で進むため、実時間の経過に依存せずintervalを検証します。
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.maxBulkDocument = 10
		cfg.flushInterval = time.Millisecond
		manager := newOpenSearchFallbackBatchProbe()
		fallback := newOpenSearchFallbackBatchTestLogger(t, cfg, manager)
		fallback.startFallbackWorker()
		t.Cleanup(func() { _ = fallback.Close(context.Background()) })

		fallback.enqueueProcess(&logging.LogEntry{OccurredAt: fallback.Clock.Now(), Message: "interval"})
		select {
		case <-manager.started:
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("fallback interval did not flush")
		}

		batches, _ := manager.snapshot()
		if len(batches) != 1 || len(batches[0]) != 1 {
			t.Fatalf("interval batches = %#v, want one single-entry batch", batches)
		}
	})
}

func TestOpenSearchFallbackEnqueueWakesWorkerAtTimeThreshold(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 100
	cfg.flushInterval = time.Hour
	manager := newOpenSearchFallbackBatchProbe()
	fallback := newOpenSearchFallbackBatchTestLogger(t, cfg, manager)
	lastFlush := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	fallback.mu.Lock()
	fallback.lastFlushTime = lastFlush
	fallback.timeProvider = func() time.Time { return lastFlush.Add(2 * time.Hour) }
	fallback.mu.Unlock()
	fallback.startFallbackWorker()
	t.Cleanup(func() { _ = fallback.Close(context.Background()) })

	fallback.enqueueProcess(&logging.LogEntry{
		OccurredAt: fallback.Clock.Now(),
		Message:    "time-threshold wake",
	})
	select {
	case <-manager.started:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("elapsed flush interval did not wake the fallback batch worker")
	}

	batches, _ := manager.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("time-triggered batches = %#v, want one single-entry batch", batches)
	}
}

func TestOpenSearchFallbackEnqueueWakesWorkerAtBufferBytesThreshold(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 100
	cfg.flushInterval = time.Hour
	manager := newOpenSearchFallbackBatchProbe()
	fallback := newOpenSearchFallbackBatchTestLogger(t, cfg, manager)
	entry := &logging.LogEntry{
		OccurredAt: fallback.Clock.Now(),
		Message:    "byte-threshold wake",
	}
	entryBytes, err := fallback.measureBufferedEntryBytes(entry)
	if err != nil {
		t.Fatalf("measure fallback entry: %v", err)
	}
	cfg.bufferFlushBytes = entryBytes
	cfg.maxBulkRequestBytes = entryBytes * 2
	fallback.startFallbackWorker()
	t.Cleanup(func() { _ = fallback.Close(context.Background()) })

	fallback.enqueueProcess(entry)
	select {
	case <-manager.started:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("BufferFlushBytes did not wake the fallback batch worker")
	}

	batches, _ := manager.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 || batches[0][0] != entry {
		t.Fatalf("byte-triggered batches = %#v, want the enqueued entry", batches)
	}
}

func TestOpenSearchFallbackFlushDoesNotRestoreSnapshotWhenManagerIsMissing(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 10
	cfg.flushInterval = time.Hour
	fallback := newOpenSearchFallbackBatchTestLogger(t, cfg, nil)

	entry := &logging.LogEntry{OccurredAt: fallback.Clock.Now(), Message: "drop after one fallback attempt"}
	if _, err := fallback.enqueueFallbackEntry(entry); err != nil {
		t.Fatalf("enqueue fallback entry: %v", err)
	}
	if err := fallback.doFallbackFlush(context.Background()); err == nil {
		t.Fatal("fallback flush without manager error = nil, want initialization error")
	}

	fallback.mu.Lock()
	queued := len(fallback.entries)
	fallback.mu.Unlock()
	if queued != 0 {
		t.Fatalf("fallback queue after failed one-shot attempt = %d, want 0", queued)
	}
}

func TestOpenSearchFallbackCloseSuppressesCompletedManagerError(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 10
	cfg.flushInterval = time.Hour
	closeErr := errors.New("fallback manager close failed")
	manager := &completedOpenSearchFallbackBatchProbe{newOpenSearchFallbackBatchProbe()}
	manager.closeErr = closeErr
	fallback := newOpenSearchFallbackBatchTestLogger(t, cfg, manager)
	fallback.enqueueProcess(&logging.LogEntry{OccurredAt: fallback.Clock.Now(), Message: "final fallback batch"})

	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("fallback Close() error = %v, want nil", err)
	}
	fallback.mu.Lock()
	closeState := fallback.closeState
	fallback.mu.Unlock()
	if closeState != loggercommon.LoggerCloseStateCompleted {
		t.Fatalf("fallback close state = %v, want completed", closeState)
	}
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("second fallback Close() error = %v, want nil", err)
	}

	batches, closeCalls := manager.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("final fallback batches = %#v, want one single-entry batch", batches)
	}
	if closeCalls != 1 {
		t.Fatalf("fallback manager Close calls = %d, want 1", closeCalls)
	}
}

func newOpenSearchFallbackBatchTestLogger(
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

type openSearchFallbackBatchProbe struct {
	mu         sync.Mutex
	batches    [][]*logging.LogEntry
	closeCalls int
	closeErr   error
	started    chan struct{}
	startOnce  sync.Once
}

func newOpenSearchFallbackBatchProbe() *openSearchFallbackBatchProbe {
	return &openSearchFallbackBatchProbe{started: make(chan struct{})}
}

func (m *openSearchFallbackBatchProbe) Flush(context.Context, []*logging.LogEntry) error { return nil }
func (m *openSearchFallbackBatchProbe) Index(context.Context, *logging.LogEntry) error   { return nil }
func (m *openSearchFallbackBatchProbe) IndexWithReport(
	context.Context,
	*logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	return pkglogger.DeliveryReport{}, nil
}
func (m *openSearchFallbackBatchProbe) FlushWithNoFallback(
	_ context.Context,
	entries []*logging.LogEntry,
) {
	m.mu.Lock()
	m.batches = append(m.batches, append([]*logging.LogEntry(nil), entries...))
	m.mu.Unlock()
	m.startOnce.Do(func() { close(m.started) })
}
func (m *openSearchFallbackBatchProbe) Close(context.Context) error {
	m.mu.Lock()
	m.closeCalls++
	err := m.closeErr
	m.mu.Unlock()
	return err
}
func (m *openSearchFallbackBatchProbe) Config() OpenSearchFlushManagerRuntimeConfigProvider {
	return nil
}
func (m *openSearchFallbackBatchProbe) LogEntryToDocumentConverter() LogEntryToDocumentConverter {
	return nil
}
func (m *openSearchFallbackBatchProbe) Resource() resource.ResourceProvider { return nil }
func (m *openSearchFallbackBatchProbe) snapshot() ([][]*logging.LogEntry, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	batches := make([][]*logging.LogEntry, len(m.batches))
	for index, batch := range m.batches {
		batches[index] = append([]*logging.LogEntry(nil), batch...)
	}
	return batches, m.closeCalls
}

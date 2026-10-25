package loki

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLokiFallbackQueueDropsOldestAndCloseFlushesOnce(t *testing.T) {
	cfg := newTestLokiLoggerConfigWithBufferLimits(1, 1024*1024, 2, time.Hour, false)
	manager := newLokiFallbackBatchProbe()
	fallback := newLokiFallbackBatchTestLogger(cfg, manager)

	first := newTestLokiLogEntry(pkglogger.LogLevelInfo, "first")
	second := newTestLokiLogEntry(pkglogger.LogLevelInfo, "second has more bytes")
	third := newTestLokiLogEntry(pkglogger.LogLevelInfo, "third")
	fallback.enqueueProcess(first)
	fallback.enqueueProcess(second)
	fallback.enqueueProcess(third)

	fallback.mu.Lock()
	queued := append([]*logging.LogEntry(nil), fallback.entries...)
	queuedEntryBytes := append([]int(nil), fallback.entryBytes...)
	queuedBytes := fallback.bufferBytes
	fallback.mu.Unlock()
	if len(queued) != 2 || queued[0] != second || queued[1] != third {
		t.Fatalf("bounded queue = %#v, want [second third]", queued)
	}
	secondBytes, err := fallback.measureBufferedEntryBytes(second)
	if err != nil {
		t.Fatalf("measure second entry: %v", err)
	}
	thirdBytes, err := fallback.measureBufferedEntryBytes(third)
	if err != nil {
		t.Fatalf("measure third entry: %v", err)
	}
	if len(queuedEntryBytes) != 2 ||
		queuedEntryBytes[0] != secondBytes ||
		queuedEntryBytes[1] != thirdBytes {
		t.Fatalf("bounded queue entry bytes = %v, want [%d %d]", queuedEntryBytes, secondBytes, thirdBytes)
	}
	if want := secondBytes + thirdBytes; queuedBytes != want {
		t.Fatalf("bounded queue bytes = %d, want %d", queuedBytes, want)
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

func TestLokiFallbackWorkerFlushesByInterval(t *testing.T) {
	// FlushIntervalのtickerはbubbleの時間で進むため、実時間の経過に依存せずintervalを検証します。
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestLokiLoggerConfig(10, time.Millisecond, false)
		manager := newLokiFallbackBatchProbe()
		fallback := newLokiFallbackBatchTestLogger(cfg, manager)
		fallback.startFallbackWorker()
		t.Cleanup(func() { _ = fallback.Close(context.Background()) })

		fallback.enqueueProcess(newTestLokiLogEntry(pkglogger.LogLevelInfo, "interval"))
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

func TestLokiFallbackWorkerFlushesAtEntryThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestLokiLoggerConfig(2, time.Hour, false)
		manager := newLokiFallbackBatchProbe()
		fallback := newLokiFallbackBatchTestLogger(cfg, manager)
		fallback.startFallbackWorker()
		t.Cleanup(func() { _ = fallback.Close(context.Background()) })

		fallback.enqueueProcess(newTestLokiLogEntry(pkglogger.LogLevelInfo, "first"))
		// workerが進める所まで進んだ後も、件数閾値に届かない間はflushを始めません。
		synctest.Wait()
		select {
		case <-manager.started:
			t.Fatal("fallback flushed before the entry threshold was reached")
		default:
		}

		fallback.enqueueProcess(newTestLokiLogEntry(pkglogger.LogLevelInfo, "second"))
		select {
		case <-manager.started:
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("fallback entry threshold did not wake the batch worker")
		}

		batches, _ := manager.snapshot()
		if len(batches) != 1 || len(batches[0]) != 2 {
			t.Fatalf("threshold-triggered batches = %#v, want one two-entry batch", batches)
		}
	})
}

func TestLokiFallbackWorkerFlushesAtFormattedLineByteThreshold(t *testing.T) {
	entry := newTestLokiLogEntry(pkglogger.LogLevelInfo, "byte threshold")
	entryBytes := len(formatter.NewPlainTextFormatter(timestamp.RFC3339Formatter()).Format(entry))
	cfg := newTestLokiLoggerConfigWithBufferLimits(
		100,
		entryBytes,
		100,
		time.Hour,
		false,
	)
	manager := newLokiFallbackBatchProbe()
	fallback := newLokiFallbackBatchTestLogger(cfg, manager)
	fallback.startFallbackWorker()
	t.Cleanup(func() { _ = fallback.Close(context.Background()) })

	fallback.enqueueProcess(entry)
	select {
	case <-manager.started:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("fallback byte threshold did not wake the batch worker")
	}

	batches, _ := manager.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 || batches[0][0] != entry {
		t.Fatalf("byte-threshold batches = %#v, want one single-entry batch", batches)
	}
	fallback.mu.Lock()
	bufferedEntries := len(fallback.entries)
	bufferedEntryBytes := len(fallback.entryBytes)
	bufferedBytes := fallback.bufferBytes
	fallback.mu.Unlock()
	if bufferedEntries != 0 || bufferedEntryBytes != 0 || bufferedBytes != 0 {
		t.Fatalf(
			"fallback byte state after flush = (entries=%d, entryBytes=%d, bytes=%d), want zero",
			bufferedEntries,
			bufferedEntryBytes,
			bufferedBytes,
		)
	}
}

func TestLokiFallbackEnqueueWakesWhenFlushIntervalAlreadyElapsed(t *testing.T) {
	cfg := newTestLokiLoggerConfig(10, time.Hour, false)
	manager := newLokiFallbackBatchProbe()
	fallback := newLokiFallbackBatchTestLogger(cfg, manager)
	now := time.Date(2026, 8, 19, 4, 5, 6, 0, time.UTC)
	fallback.timeProvider = func() time.Time { return now }
	fallback.lastFlushTime = now.Add(-cfg.FlushInterval())
	fallback.startFallbackWorker()
	t.Cleanup(func() { _ = fallback.Close(context.Background()) })

	fallback.enqueueProcess(newTestLokiLogEntry(pkglogger.LogLevelInfo, "elapsed interval"))
	select {
	case <-manager.started:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("fallback elapsed interval did not wake the batch worker")
	}

	batches, _ := manager.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("elapsed-interval batches = %#v, want one single-entry batch", batches)
	}
}

func TestLokiFallbackUsesBatchWorkerWhenPrimaryNoBulkModeIsEnabled(t *testing.T) {
	cfg := newTestLokiLoggerConfig(1, time.Hour, true)
	manager := newLokiFallbackBatchProbe()
	fallback := newLokiFallbackBatchTestLogger(cfg, manager)
	fallback.startFallbackWorker()
	t.Cleanup(func() { _ = fallback.Close(context.Background()) })

	fallback.enqueueProcess(newTestLokiLogEntry(pkglogger.LogLevelInfo, "fallback remains batched"))
	select {
	case <-manager.started:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("fallback batch worker did not start when primary no-bulk mode was enabled")
	}

	batches, _ := manager.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("no-bulk primary fallback batches = %#v, want one single-entry batch", batches)
	}
}

func TestLokiFallbackFailedAttemptDoesNotRestoreBatch(t *testing.T) {
	cfg := newTestLokiLoggerConfig(10, time.Hour, false)
	client := &recordingFlushManagerLokiClient{pushErr: errors.New("loki unavailable")}
	fallback := newLokiFallbackBatchTestLogger(cfg, newTestLokiFlushManager(client))
	entry := newTestLokiLogEntry(pkglogger.LogLevelError, "discard after one failed attempt")

	fallback.enqueueProcess(entry)
	if err := fallback.doFallbackFlush(context.Background()); err != nil {
		t.Fatalf("fallback doFallbackFlush() error = %v", err)
	}
	if got := lifecycleLokiEntryCount(fallback.LokiLogger); got != 0 {
		t.Fatalf("fallback buffered entries after failed attempt = %d, want 0", got)
	}
	fallback.mu.Lock()
	bufferedEntryBytes := len(fallback.entryBytes)
	bufferedBytes := fallback.bufferBytes
	fallback.mu.Unlock()
	if bufferedEntryBytes != 0 || bufferedBytes != 0 {
		t.Fatalf(
			"fallback byte state after failed attempt = (entries=%d, bytes=%d), want (0, 0)",
			bufferedEntryBytes,
			bufferedBytes,
		)
	}
	if client.pushCalls != 1 {
		t.Fatalf("fallback push calls = %d, want 1", client.pushCalls)
	}

	if err := fallback.doFallbackFlush(context.Background()); err != nil {
		t.Fatalf("second fallback doFallbackFlush() error = %v", err)
	}
	if client.pushCalls != 1 {
		t.Fatalf("fallback retried discarded batch: push calls = %d, want 1", client.pushCalls)
	}
}

func TestLokiFallbackMissingFlushManagerDiscardsBatchAndUpdatesAttemptTime(t *testing.T) {
	cfg := newTestLokiLoggerConfig(10, time.Hour, false)
	fallback := newLokiFallbackBatchTestLogger(cfg, nil)
	attemptedAt := time.Date(2026, 8, 19, 5, 6, 7, 0, time.UTC)
	fallback.timeProvider = func() time.Time { return attemptedAt }
	fallback.lastFlushTime = attemptedAt.Add(-cfg.FlushInterval())

	fallback.enqueueProcess(newTestLokiLogEntry(pkglogger.LogLevelInfo, "discard without flush manager"))
	err := fallback.doFallbackFlush(context.Background())
	if err == nil {
		t.Fatal("doFallbackFlush() error = nil, want missing FlushManager error")
	}
	if got := lifecycleLokiEntryCount(fallback.LokiLogger); got != 0 {
		t.Fatalf("fallback buffered entries after missing-manager attempt = %d, want 0", got)
	}
	if !fallback.lastFlushTime.Equal(attemptedAt) {
		t.Fatalf("fallback lastFlushTime = %s, want attempt time %s", fallback.lastFlushTime, attemptedAt)
	}
	fallback.mu.Lock()
	intervalElapsed := fallback.flushIntervalElapsedLocked()
	fallback.mu.Unlock()
	if intervalElapsed {
		t.Fatal("fallback interval remained elapsed after missing-manager attempt")
	}
}

func TestLokiFallbackCloseSuppressesCompletedManagerError(t *testing.T) {
	cfg := newTestLokiLoggerConfig(10, time.Hour, false)
	manager := &completedLokiFallbackBatchProbe{newLokiFallbackBatchProbe()}
	manager.closeErr = errors.New("fallback flush manager close failed")
	fallback := newLokiFallbackBatchTestLogger(cfg, manager)

	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("fallback Close() error = %v, want nil", err)
	}
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("second fallback Close() error = %v, want nil", err)
	}

	_, closeCalls := manager.snapshot()
	if closeCalls != 1 {
		t.Fatalf("fallback flush manager Close calls = %d, want 1", closeCalls)
	}
}

func newLokiFallbackBatchTestLogger(
	cfg LokiConfigProvider,
	manager FlushManager,
) *FallbackLogger {
	tracer := observer.NopTracer{}
	meter := observer.NopMeter{}
	logger := &LokiLogger{
		LoggerBase: &LoggerBase{
			config:       cfg,
			Clock:        clock.NewUTCClock(),
			LogFormatter: lokiTestPlainTextFormatter,
			Tracer:       tracer,
			Meter:        meter,
		},
		entries:          make([]*logging.LogEntry, 0, cfg.FallbackQueueMaxEntries()),
		entryBytes:       make([]int, 0, cfg.FallbackQueueMaxEntries()),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		flushManager:     manager,
		Tracer:           tracer,
		Meter:            meter,
	}
	logger.lastFlushTime = logger.timeProvider()
	return &FallbackLogger{LokiLogger: logger}
}

type lokiFallbackBatchProbe struct {
	mu         sync.Mutex
	batches    [][]*logging.LogEntry
	closeCalls int
	closeErr   error
	started    chan struct{}
	startOnce  sync.Once
}

func newLokiFallbackBatchProbe() *lokiFallbackBatchProbe {
	return &lokiFallbackBatchProbe{started: make(chan struct{})}
}

func (m *lokiFallbackBatchProbe) Flush(context.Context, []*logging.LogEntry) error { return nil }
func (m *lokiFallbackBatchProbe) Push(context.Context, *logging.LogEntry) error    { return nil }
func (m *lokiFallbackBatchProbe) PushWithReport(
	context.Context,
	*logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	return pkglogger.DeliveryReport{}, nil
}
func (m *lokiFallbackBatchProbe) FlushWithNoFallback(
	_ context.Context,
	entries []*logging.LogEntry,
) {
	m.mu.Lock()
	m.batches = append(m.batches, append([]*logging.LogEntry(nil), entries...))
	m.mu.Unlock()
	m.startOnce.Do(func() { close(m.started) })
}
func (m *lokiFallbackBatchProbe) Close(context.Context) error {
	m.mu.Lock()
	m.closeCalls++
	err := m.closeErr
	m.mu.Unlock()
	return err
}
func (m *lokiFallbackBatchProbe) snapshot() ([][]*logging.LogEntry, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	batches := make([][]*logging.LogEntry, len(m.batches))
	for index, batch := range m.batches {
		batches[index] = append([]*logging.LogEntry(nil), batch...)
	}
	return batches, m.closeCalls
}

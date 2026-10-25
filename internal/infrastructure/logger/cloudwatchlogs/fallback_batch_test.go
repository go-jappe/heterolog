package cloudwatchlogs

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
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestCloudWatchLogsFallbackQueueDropsOldestAndCloseFlushesOnce(t *testing.T) {
	cfg := newTestCloudWatchLogsLoggerConfig(2, time.Hour, false)
	manager := newCloudWatchLogsFallbackBatchProbe()
	fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, manager)

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
	if want := cloudWatchLogsBufferedEntriesBytes(
		[]*logging.LogEntry{second, third},
		formatter.NewPlainTextFormatter(timestamp.RFC3339Formatter()),
	); queuedBytes != want {
		t.Fatalf("bounded queue bytes = %d, want retained entries size %d", queuedBytes, want)
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

func TestCloudWatchLogsFallbackQueueHardCapIsIndependentFromFlushThreshold(t *testing.T) {
	cfg := newTestCloudWatchLogsLoggerConfigWithBufferLimits(
		2,
		64*1024*1024,
		3,
		time.Hour,
		false,
	)
	fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, nil)
	occurredAt := time.Date(2026, 8, 19, 1, 3, 0, 0, time.UTC)
	entries := []*logging.LogEntry{
		{OccurredAt: occurredAt, Message: "first"},
		{OccurredAt: occurredAt, Message: "second"},
		{OccurredAt: occurredAt, Message: "third"},
		{OccurredAt: occurredAt, Message: "fourth"},
	}
	for _, entry := range entries {
		fallback.enqueueProcess(entry)
	}

	fallback.mu.Lock()
	queued := append([]*logging.LogEntry(nil), fallback.entries...)
	queuedBytes := fallback.bufferBytes
	fallback.mu.Unlock()
	wantEntries := entries[1:]
	if len(queued) != len(wantEntries) {
		t.Fatalf("bounded queue length = %d, want hard cap %d", len(queued), len(wantEntries))
	}
	for index := range wantEntries {
		if queued[index] != wantEntries[index] {
			t.Fatalf("bounded queue[%d] = %p, want retained entry %p", index, queued[index], wantEntries[index])
		}
	}
	if wantBytes := cloudWatchLogsBufferedEntriesBytes(
		wantEntries,
		formatter.NewPlainTextFormatter(timestamp.RFC3339Formatter()),
	); queuedBytes != wantBytes {
		t.Fatalf("bounded queue bytes = %d, want retained entries size %d", queuedBytes, wantBytes)
	}
}

func TestCloudWatchLogsFallbackWorkerFlushesByInterval(t *testing.T) {
	// FlushIntervalのtickerはbubbleの時間で進むため、実時間の経過に依存せずintervalを検証します。
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestCloudWatchLogsLoggerConfig(10, time.Millisecond, false)
		manager := newCloudWatchLogsFallbackBatchProbe()
		fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, manager)
		fallback.startFallbackWorker()
		t.Cleanup(func() { _ = fallback.Close(context.Background()) })

		fallback.enqueueProcess(&logging.LogEntry{Message: "interval"})
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

func TestCloudWatchLogsFallbackWorkerFlushesAtEventThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestCloudWatchLogsLoggerConfig(2, time.Hour, false)
		manager := newCloudWatchLogsFallbackBatchProbe()
		fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, manager)
		fallback.startFallbackWorker()
		t.Cleanup(func() { _ = fallback.Close(context.Background()) })

		fallback.enqueueProcess(&logging.LogEntry{Message: "first"})
		// workerが進める所まで進んだ後も、件数閾値に届かない間はflushを始めません。
		synctest.Wait()
		select {
		case <-manager.started:
			t.Fatal("fallback flushed before the event threshold was reached")
		default:
		}

		fallback.enqueueProcess(&logging.LogEntry{Message: "second"})
		select {
		case <-manager.started:
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("fallback event threshold did not wake the batch worker")
		}

		batches, _ := manager.snapshot()
		if len(batches) != 1 || len(batches[0]) != 2 {
			t.Fatalf("threshold batches = %#v, want one two-entry batch", batches)
		}
	})
}

func TestCloudWatchLogsFallbackWorkerFlushesAtBufferBytesThreshold(t *testing.T) {
	occurredAt := time.Date(2026, 8, 19, 2, 3, 4, 0, time.UTC)
	entry := &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "byte-threshold-あ",
	}
	entryBytes := cloudWatchLogsBufferedEntryBytes(
		entry,
		formatter.NewPlainTextFormatter(timestamp.RFC3339Formatter()),
	)
	cfg := newTestCloudWatchLogsLoggerConfigWithBufferLimits(
		100,
		entryBytes,
		200,
		time.Hour,
		false,
	)
	manager := newCloudWatchLogsFallbackBatchProbe()
	fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, manager)
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
		t.Fatalf("byte-threshold batches = %#v, want the enqueued entry", batches)
	}
	fallback.mu.Lock()
	bufferedBytes := fallback.bufferBytes
	fallback.mu.Unlock()
	if bufferedBytes != 0 {
		t.Fatalf("fallback buffer bytes after flush = %d, want 0", bufferedBytes)
	}
}

func TestCloudWatchLogsFallbackUsesBatchWorkerWhenPrimaryNoBulkModeIsEnabled(t *testing.T) {
	// FlushIntervalのtickerはbubbleの時間で進むため、実時間の経過に依存せずintervalを検証します。
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestCloudWatchLogsLoggerConfig(10, time.Millisecond, true)
		manager := newCloudWatchLogsFallbackBatchProbe()
		fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, manager)
		fallback.startFallbackWorker()
		t.Cleanup(func() { _ = fallback.Close(context.Background()) })

		fallback.enqueueProcess(&logging.LogEntry{Message: "fallback remains batched"})
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

func TestCloudWatchLogsFallbackDoesNotRestoreSnapshotAfterNoFallbackAttempt(t *testing.T) {
	cfg := newTestCloudWatchLogsLoggerConfig(10, time.Hour, false)
	manager := newCloudWatchLogsFallbackBatchProbe()
	fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, manager)
	attemptedAt := time.Date(2026, 8, 19, 3, 4, 5, 0, time.UTC)
	fallback.timeProvider = func() time.Time { return attemptedAt }
	fallback.lastFlushTime = attemptedAt.Add(-time.Minute)

	fallback.enqueueProcess(&logging.LogEntry{Message: "discard after one attempt"})
	if err := fallback.doFallbackFlush(context.Background()); err != nil {
		t.Fatalf("doFallbackFlush: %v", err)
	}
	if got := cloudWatchLogsBufferedMessages(fallback.CloudWatchLogsLogger); len(got) != 0 {
		t.Fatalf("buffer after no-fallback attempt = %#v, want empty", got)
	}
	if !fallback.lastFlushTime.Equal(attemptedAt) {
		t.Fatalf("lastFlushTime = %s, want attempt time %s", fallback.lastFlushTime, attemptedAt)
	}

	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	batches, _ := manager.snapshot()
	if len(batches) != 1 {
		t.Fatalf("no-fallback attempts = %d, want exactly 1", len(batches))
	}
}

func TestCloudWatchLogsFallbackDropsSnapshotWhenFlushManagerIsMissing(t *testing.T) {
	cfg := newTestCloudWatchLogsLoggerConfig(10, time.Hour, false)
	fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, nil)
	attemptedAt := time.Date(2026, 8, 19, 4, 5, 6, 0, time.UTC)
	fallback.timeProvider = func() time.Time { return attemptedAt }
	fallback.lastFlushTime = attemptedAt.Add(-time.Minute)

	fallback.enqueueProcess(&logging.LogEntry{Message: "drop without flush manager"})
	if err := fallback.doFallbackFlush(context.Background()); err == nil {
		t.Fatal("doFallbackFlush error = nil, want missing flush manager error")
	}
	if got := cloudWatchLogsBufferedMessages(fallback.CloudWatchLogsLogger); len(got) != 0 {
		t.Fatalf("buffer after missing-manager attempt = %#v, want empty", got)
	}
	if !fallback.lastFlushTime.Equal(attemptedAt) {
		t.Fatalf("lastFlushTime = %s, want attempt time %s", fallback.lastFlushTime, attemptedAt)
	}
	if err := fallback.doFallbackFlush(context.Background()); err != nil {
		t.Fatalf("second doFallbackFlush with empty queue: %v", err)
	}
}

func TestCloudWatchLogsFallbackCloseSuppressesCompletedManagerError(t *testing.T) {
	cfg := newTestCloudWatchLogsLoggerConfig(10, time.Hour, false)
	manager := &completedCloudWatchLogsFallbackBatchProbe{newCloudWatchLogsFallbackBatchProbe()}
	manager.closeErr = errors.New("fallback manager close failed")
	fallback := newCloudWatchLogsFallbackBatchTestLogger(cfg, manager)
	fallback.enqueueProcess(&logging.LogEntry{Message: "flush before suppressed close error"})

	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v, want nil", err)
	}
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
	batches, closeCalls := manager.snapshot()
	if len(batches) != 1 {
		t.Fatalf("fallback batches = %d, want 1", len(batches))
	}
	if closeCalls != 1 {
		t.Fatalf("flush manager Close calls = %d, want 1 after completed Close", closeCalls)
	}
}

func newCloudWatchLogsFallbackBatchTestLogger(
	cfg CloudWatchLogsConfigProvider,
	manager FlushManager,
) *FallbackLogger {
	logger := &CloudWatchLogsLogger{
		LoggerBase: &LoggerBase{
			config:       cfg,
			LogFormatter: cloudWatchLogsTestPlainTextFormatter,
		},
		entries:          make([]*logging.LogEntry, 0, cfg.FallbackQueueMaxEvents()),
		timeProvider:     time.Now,
		flushIntervalGen: time.NewTicker,
		flushManager:     manager,
		Tracer:           observer.NopTracer{},
	}
	logger.lastFlushTime = logger.timeProvider()
	return &FallbackLogger{CloudWatchLogsLogger: logger}
}

type cloudWatchLogsFallbackBatchProbe struct {
	mu         sync.Mutex
	batches    [][]*logging.LogEntry
	closeCalls int
	closeErr   error
	started    chan struct{}
	startOnce  sync.Once
}

func newCloudWatchLogsFallbackBatchProbe() *cloudWatchLogsFallbackBatchProbe {
	return &cloudWatchLogsFallbackBatchProbe{started: make(chan struct{})}
}

func (m *cloudWatchLogsFallbackBatchProbe) Flush(
	_ context.Context,
	entries []*logging.LogEntry,
) error {
	return nil
}
func (m *cloudWatchLogsFallbackBatchProbe) Put(context.Context, *logging.LogEntry) error {
	return nil
}
func (m *cloudWatchLogsFallbackBatchProbe) PutWithReport(
	context.Context,
	*logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	return pkglogger.DeliveryReport{}, nil
}
func (m *cloudWatchLogsFallbackBatchProbe) FlushWithNoFallback(
	_ context.Context,
	entries []*logging.LogEntry,
) {
	m.mu.Lock()
	m.batches = append(m.batches, append([]*logging.LogEntry(nil), entries...))
	m.mu.Unlock()
	m.startOnce.Do(func() { close(m.started) })
}
func (m *cloudWatchLogsFallbackBatchProbe) Close(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	return m.closeErr
}
func (m *cloudWatchLogsFallbackBatchProbe) snapshot() ([][]*logging.LogEntry, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	batches := make([][]*logging.LogEntry, len(m.batches))
	for index, batch := range m.batches {
		batches[index] = append([]*logging.LogEntry(nil), batch...)
	}
	return batches, m.closeCalls
}

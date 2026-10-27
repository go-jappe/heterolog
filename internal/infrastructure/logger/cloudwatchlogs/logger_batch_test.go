package cloudwatchlogs

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestCloudWatchLogsLoggerFlushesAtAWSCalculatedBufferBytes(t *testing.T) {
	occurredAt := time.Date(2026, 8, 19, 12, 34, 56, 0, time.UTC)
	fixedClock := fixedCloudWatchLogsClock{t: occurredAt}
	first := &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "first-あ",
	}
	second := &logging.LogEntry{
		OccurredAt: occurredAt,
		Level:      pkglogger.LogLevelInfo,
		Message:    "second-い",
	}
	timestampFormatter := timestamp.RFC3339Formatter()
	logFormatter := formatter.NewPlainTextFormatter(timestampFormatter)
	firstBytes := cloudWatchLogsBufferedEntryBytes(first, logFormatter)
	secondBytes := cloudWatchLogsBufferedEntryBytes(second, logFormatter)
	if got, want := firstBytes, len(formatCloudWatchLogsEntryAt(first, occurredAt, logFormatter))+26; got != want {
		t.Fatalf("first buffered bytes = %d, want message UTF-8 bytes + 26 = %d", got, want)
	}

	cfg := newTestCloudWatchLogsLoggerConfigWithBufferLimits(
		100,
		firstBytes+secondBytes,
		100,
		time.Hour,
		false,
	)
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	logger := &CloudWatchLogsLogger{
		LoggerBase: &LoggerBase{
			Clock:        fixedClock,
			LogFormatter: logFormatter,
			config:       cfg,
		},
		entries:      make([]*logging.LogEntry, 0, cfg.BufferFlushMaxEvents()),
		timeProvider: func() time.Time { return occurredAt },
		flushManager: manager,
		Tracer:       observer.NopTracer{},
		Meter:        observer.NopMeter{},
	}
	logger.lastFlushTime = occurredAt

	if err := logger.enqueue(context.Background(), first); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if got := manager.snapshot().flushCalls; got != 0 {
		t.Fatalf("flush calls below byte threshold = %d, want 0", got)
	}
	logger.mu.Lock()
	bufferedBytesAfterFirst := logger.bufferBytes
	logger.mu.Unlock()
	if bufferedBytesAfterFirst != firstBytes {
		t.Fatalf("buffer bytes after first entry = %d, want %d", bufferedBytesAfterFirst, firstBytes)
	}

	if err := logger.enqueue(context.Background(), second); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}
	snapshot := manager.snapshot()
	if snapshot.flushCalls != 1 || len(snapshot.flushEntries) != 1 || len(snapshot.flushEntries[0]) != 2 {
		t.Fatalf("byte-threshold flush snapshot = %+v, want one two-entry flush", snapshot)
	}
	logger.mu.Lock()
	bufferedEntries := len(logger.entries)
	bufferedBytes := logger.bufferBytes
	logger.mu.Unlock()
	if bufferedEntries != 0 || bufferedBytes != 0 {
		t.Fatalf("buffer after successful byte-threshold flush = %d entries/%d bytes, want empty", bufferedEntries, bufferedBytes)
	}
}

func TestCloudWatchLogsLoggerDropsSnapshotAfterFailedThresholdFlush(t *testing.T) {
	occurredAt := time.Date(2026, 8, 19, 12, 35, 0, 0, time.UTC)
	fixedClock := fixedCloudWatchLogsClock{t: occurredAt}
	entries := []*logging.LogEntry{
		{OccurredAt: occurredAt, Level: pkglogger.LogLevelInfo, Message: "first retained"},
		{OccurredAt: occurredAt, Level: pkglogger.LogLevelInfo, Message: "second retained"},
	}
	wantBytes := cloudWatchLogsBufferedEntriesBytes(
		entries,
		formatter.NewPlainTextFormatter(timestamp.RFC3339Formatter()),
	)
	cfg := newTestCloudWatchLogsLoggerConfigWithBufferLimits(
		100,
		wantBytes,
		100,
		time.Hour,
		false,
	)
	flushErr := errors.New("backend unavailable")
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	manager.flushErrors = []error{flushErr}
	logger := &CloudWatchLogsLogger{
		LoggerBase: &LoggerBase{
			Clock:        fixedClock,
			LogFormatter: cloudWatchLogsTestPlainTextFormatter,
			config:       cfg,
		},
		entries:      make([]*logging.LogEntry, 0, cfg.BufferFlushMaxEvents()),
		timeProvider: func() time.Time { return occurredAt },
		flushManager: manager,
		Tracer:       observer.NopTracer{},
		Meter:        observer.NopMeter{},
	}
	logger.lastFlushTime = occurredAt

	if err := logger.enqueue(context.Background(), entries[0]); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if err := logger.enqueue(context.Background(), entries[1]); !errors.Is(err, flushErr) {
		t.Fatalf("enqueue second error = %v, want %v", err, flushErr)
	}
	logger.mu.Lock()
	gotEntries := len(logger.entries)
	gotBytes := logger.bufferBytes
	logger.mu.Unlock()
	if gotEntries != 0 || gotBytes != 0 {
		t.Fatalf("buffer after failed flush = %d entries/%d bytes, want empty", gotEntries, gotBytes)
	}
}

func TestCloudWatchLogsLoggerDropsSnapshotWhenFlushManagerIsMissing(t *testing.T) {
	cfg := newTestCloudWatchLogsLoggerConfig(10, time.Hour, false)
	first := &logging.LogEntry{Message: "first"}
	second := &logging.LogEntry{Message: "second"}
	logger := &CloudWatchLogsLogger{
		LoggerBase: &LoggerBase{
			Clock:        fixedCloudWatchLogsClock{t: time.Date(2026, 8, 19, 4, 5, 6, 0, time.UTC)},
			LogFormatter: cloudWatchLogsTestPlainTextFormatter,
			config:       cfg,
		},
		entries:      []*logging.LogEntry{first, second},
		timeProvider: time.Now,
		Tracer:       observer.NopTracer{},
		Meter:        observer.NopMeter{},
	}

	if err := logger.doFlush(context.Background()); err == nil {
		t.Fatal("doFlush error = nil, want missing flush manager error")
	}
	logger.mu.Lock()
	buffered := len(logger.entries)
	logger.mu.Unlock()
	if buffered != 0 {
		t.Fatalf("buffer after missing-manager flush = %d entries, want empty", buffered)
	}
}

func TestCloudWatchLogsLoggerPassesOneBufferSnapshotToFlushManager(t *testing.T) {
	logger := newLifecycleCloudWatchLogsLogger(t, time.Hour)
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(
			pkglogger.LogLevelInfo,
			"second stream",
			time.Date(2026, 8, 13, 13, 5, 0, 0, time.UTC),
		),
		newTestCloudWatchLogsEntryAt(
			pkglogger.LogLevelInfo,
			"first stream",
			time.Date(2026, 8, 13, 12, 5, 0, 0, time.UTC),
		),
	}
	logger.mu.Unlock()

	if err := logger.doFlush(context.Background()); err != nil {
		t.Fatalf("doFlush() error = %v", err)
	}

	snapshot := manager.snapshot()
	if snapshot.flushCalls != 1 {
		t.Fatalf("Flush calls = %d, want 1", snapshot.flushCalls)
	}
	if got := cloudWatchLogsTestBatchMessages(snapshot.flushEntries); !equalCloudWatchLogsStrings(
		got,
		[][]string{{"second stream", "first stream"}},
	) {
		t.Fatalf("Flush inputs = %#v, want the original snapshot in one call", got)
	}
	if got := cloudWatchLogsBufferedMessages(logger); len(got) != 0 {
		t.Fatalf("buffer after successful flush = %#v, want empty", got)
	}
}

func TestCloudWatchLogsLoggerDoesNotSplitTimestampSpan(t *testing.T) {
	logger := newLifecycleCloudWatchLogsLogger(t, time.Hour)
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	first := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(
			pkglogger.LogLevelInfo,
			"more than 24 hours later",
			first.Add(24*time.Hour+time.Millisecond),
		),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "first", first),
	}
	logger.mu.Unlock()

	if err := logger.doFlush(context.Background()); err != nil {
		t.Fatalf("doFlush() error = %v", err)
	}

	snapshot := manager.snapshot()
	if snapshot.flushCalls != 1 {
		t.Fatalf("Flush calls = %d, want 1", snapshot.flushCalls)
	}
	if got := cloudWatchLogsTestBatchMessages(snapshot.flushEntries); !equalCloudWatchLogsStrings(
		got,
		[][]string{{"more than 24 hours later", "first"}},
	) {
		t.Fatalf("Flush inputs = %#v, want one unchanged snapshot", got)
	}
}

func TestCloudWatchLogsLoggerDoesNotSplitRequestBytes(t *testing.T) {
	logger := newLifecycleCloudWatchLogsLogger(t, time.Hour)
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, strings.Repeat("a", 600_000), now),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, strings.Repeat("b", 600_000), now.Add(time.Second)),
	}
	logger.mu.Unlock()

	if err := logger.doFlush(context.Background()); err != nil {
		t.Fatalf("doFlush() error = %v", err)
	}

	snapshot := manager.snapshot()
	if snapshot.flushCalls != 1 {
		t.Fatalf("Flush calls = %d, want 1", snapshot.flushCalls)
	}
	if len(snapshot.flushEntries[0]) != 2 {
		t.Fatalf("Flush input count = %d, want 2", len(snapshot.flushEntries[0]))
	}
}

// request単位の結果はmanagerが管理します。managerの集約errorによって、loggerが
// 完了済みのentryを含むsnapshot全体を再送してはいけません。
func TestCloudWatchLogsLoggerDoesNotRequeueWholeSnapshotWhenFlushFails(t *testing.T) {
	logger := newLifecycleCloudWatchLogsLogger(t, time.Hour)
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	flushErr := errors.New("second request durable handling failed")
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	manager.flushErrors = []error{flushErr}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(
			pkglogger.LogLevelInfo,
			"third 14",
			time.Date(2026, 8, 13, 14, 5, 0, 0, time.UTC),
		),
		newTestCloudWatchLogsEntryAt(
			pkglogger.LogLevelInfo,
			"completed 12",
			time.Date(2026, 8, 13, 12, 5, 0, 0, time.UTC),
		),
		newTestCloudWatchLogsEntryAt(
			pkglogger.LogLevelInfo,
			"failed 13",
			time.Date(2026, 8, 13, 13, 5, 0, 0, time.UTC),
		),
	}
	logger.mu.Unlock()

	if err := logger.doFlush(context.Background()); !errors.Is(err, flushErr) {
		t.Fatalf("doFlush() error = %v, want %v", err, flushErr)
	}
	if got := cloudWatchLogsBufferedMessages(logger); len(got) != 0 {
		t.Fatalf("buffer after failed flush = %#v, want empty", got)
	}

	firstAttempt := manager.snapshot()
	if got := cloudWatchLogsTestBatchMessages(firstAttempt.flushEntries); !equalCloudWatchLogsStrings(
		got,
		[][]string{{"third 14", "completed 12", "failed 13"}},
	) {
		t.Fatalf("first Flush input = %#v", got)
	}

	if firstAttempt.flushCalls != 1 {
		t.Fatalf("Flush calls = %d, want one attempt", firstAttempt.flushCalls)
	}
}

func TestCloudWatchLogsLoggerDropsInvalidSnapshotWhenDurableHandlingFails(t *testing.T) {
	logger := newLifecycleCloudWatchLogsLogger(t, time.Hour)
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	durableErr := errors.New("invalid record durable handling failed")
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	manager.flushErrors = []error{durableErr}
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = []*logging.LogEntry{
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "sent successfully", now),
		newTestCloudWatchLogsEntryAt(pkglogger.LogLevelInfo, "", now.Add(time.Second)),
	}
	logger.mu.Unlock()

	if err := logger.doFlush(context.Background()); !errors.Is(err, durableErr) {
		t.Fatalf("doFlush() error = %v, want %v", err, durableErr)
	}
	if got := cloudWatchLogsBufferedMessages(logger); len(got) != 0 {
		t.Fatalf("buffer after durable failure = %#v, want empty", got)
	}

	snapshot := manager.snapshot()
	if got := cloudWatchLogsTestBatchMessages(snapshot.flushEntries); !equalCloudWatchLogsStrings(
		got,
		[][]string{{"sent successfully", ""}},
	) {
		t.Fatalf("Flush input = %#v, want one unchanged snapshot", got)
	}
}

func TestCloudWatchLogsLoggerDoesNotRetryFailedSnapshot(t *testing.T) {
	logger := newLifecycleCloudWatchLogsLogger(t, time.Hour)
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	flushErr := errors.New("flush stopped")
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	manager.flushErrors = []error{flushErr}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = []*logging.LogEntry{
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "first"),
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "second"),
	}
	logger.mu.Unlock()

	err := logger.doFlush(context.Background())
	if !errors.Is(err, flushErr) {
		t.Fatalf("doFlush() error = %v, want %v", err, flushErr)
	}
	if got := cloudWatchLogsBufferedMessages(logger); len(got) != 0 {
		t.Fatalf("buffer after failed flush = %#v, want empty", got)
	}
}

func TestCloudWatchLogsLoggerKeepsOnlyEntriesAddedDuringFailedFlush(t *testing.T) {
	logger := newLifecycleCloudWatchLogsLogger(t, time.Hour)
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	flushErr := errors.New("first request failed")
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	manager.blockFirst = true
	manager.flushErrors = []error{flushErr}
	// 先に登録したCloseより前に実行し、失敗時もblockしたflushを解放します。
	t.Cleanup(manager.releaseFlush)
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = []*logging.LogEntry{
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "detached snapshot"),
	}
	logger.mu.Unlock()

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- logger.doFlush(context.Background())
	}()
	waitCloudWatchLogsLifecycleSignal(t, manager.started, "Flush did not start")

	logger.mu.Lock()
	logger.entries = append(
		logger.entries,
		newTestCloudWatchLogsEntry(pkglogger.LogLevelInfo, "added during flush"),
	)
	logger.mu.Unlock()
	manager.releaseFlush()

	select {
	case err := <-flushDone:
		if !errors.Is(err, flushErr) {
			t.Fatalf("doFlush() error = %v, want %v", err, flushErr)
		}
	case <-time.After(lifecycleHangTimeout):
		t.Fatal("doFlush() did not return")
	}

	if got, want := cloudWatchLogsBufferedMessages(logger), []string{
		"added during flush",
	}; !slices.Equal(got, want) {
		t.Fatalf("buffer after failed flush = %#v, want %#v", got, want)
	}
}

func cloudWatchLogsTestBatchMessages(batches [][]*logging.LogEntry) [][]string {
	messages := make([][]string, len(batches))
	for batchIndex, entries := range batches {
		messages[batchIndex] = make([]string, len(entries))
		for entryIndex, entry := range entries {
			if entry != nil {
				messages[batchIndex][entryIndex] = entry.Message
			}
		}
	}
	return messages
}

func cloudWatchLogsBufferedMessages(logger *CloudWatchLogsLogger) []string {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	messages := make([]string, len(logger.entries))
	for index, entry := range logger.entries {
		if entry != nil {
			messages[index] = entry.Message
		}
	}
	return messages
}

func equalCloudWatchLogsStrings(got, want [][]string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if !slices.Equal(got[index], want[index]) {
			return false
		}
	}
	return true
}

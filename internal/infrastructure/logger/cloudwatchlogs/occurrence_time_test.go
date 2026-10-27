package cloudwatchlogs

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestBufferedEntryKeepsOccurrenceTimeCapturedBeforeBoundary(t *testing.T) {
	occurredAt := time.Date(2026, 8, 13, 14, 59, 59, 0, time.UTC)
	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	timestampFormatter, err := timestamp.NewFormatter(
		location,
		"2006-01-02T15:04:05-07:00",
	)
	if err != nil {
		t.Fatalf("timestamp.NewFormatter() error = %v", err)
	}
	client := &recordingCloudWatchLogsClient{}
	logger, err := NewCloudWatchLogsLogger(
		context.Background(),
		newTestCloudWatchLogsLoggerConfig(100, time.Hour, false),
		client,
		newTestCloudWatchLogsFlushManagerConfig(),
		newTestCloudWatchLogsResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{
			Clock:        clock.NewUTCClock(),
			LogFormatter: formatter.NewPlainTextFormatter(timestampFormatter),
		},
	)
	if err != nil {
		t.Fatalf("NewCloudWatchLogsLogger() error = %v", err)
	}
	if err := handleTestLogAt(
		context.Background(),
		logger,
		occurredAt,
		pkglogger.LogLevelInfo,
		"before stream boundary",
	); err != nil {
		t.Fatalf("HandleLogEntry() error = %v", err)
	}
	if len(client.events) != 0 {
		t.Fatalf("events before Close = %d, want buffered", len(client.events))
	}
	if err := logger.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if len(client.events) != 1 {
		t.Fatalf("sent events = %d, want 1", len(client.events))
	}
	event := client.events[0]
	if !event.Timestamp.Equal(occurredAt) {
		t.Fatalf("event occurrence = %s, want log-call time %s", event.Timestamp, occurredAt)
	}
	wantLocalTime := occurredAt.In(time.FixedZone("JST", 9*60*60)).Format("2006-01-02T15:04:05-07:00")
	if !strings.Contains(event.Message, wantLocalTime) {
		t.Fatalf(
			"event message = %q, want configured timezone timestamp %q",
			event.Message,
			wantLocalTime,
		)
	}
	if event.Timestamp.Location() != time.UTC {
		t.Fatalf("event timestamp location = %s, want UTC protocol instant", event.Timestamp.Location())
	}
}

func TestCloudWatchStreamUsesOccurrenceTimezoneIndependentOfDisplay(t *testing.T) {
	occurredAt := time.Date(2026, 8, 13, 15, 30, 0, 0, time.UTC)
	location, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatalf("LoadLocation() error = %v", err)
	}
	timestampFormatter, err := timestamp.NewFormatter(location, time.RFC3339)
	if err != nil {
		t.Fatalf("timestamp.NewFormatter() error = %v", err)
	}
	entry := &logging.LogEntry{
		OccurredAt: occurredAt,
		Message:    "timezone routing",
	}
	record := convertLogEntriesToEventRecords(
		[]*logging.LogEntry{entry},
		formatter.NewPlainTextFormatter(timestampFormatter),
	)[0]
	resolver, err := infracloudwatchlogs.NewLogStreamNameResolver(
		"application/task",
		pkgcloudwatchlogs.StreamRoutingDaily,
	)
	if err != nil {
		t.Fatalf("NewLogStreamNameResolver() error = %v", err)
	}
	streamName, err := resolver.ResolveLogStreamNameForEvent(record.event.Timestamp)
	if err != nil {
		t.Fatalf("ResolveLogStreamNameForEvent() error = %v", err)
	}
	if streamName != "application/task/2026/08/13" {
		t.Fatalf("stream name = %q, want occurrence UTC date suffix", streamName)
	}
	if !strings.Contains(record.event.Message, "2026-08-14T00:30:00+09:00") {
		t.Fatalf("event message = %q, want JST representation of occurrence", record.event.Message)
	}

	fm := &CloudWatchLogsFlushManager{}
	document := fm.convertEventRecordToDocument(record, streamName)
	if got := document["timestamp"]; got != occurredAt.Format(time.RFC3339Nano) {
		t.Fatalf("durable timestamp = %v, want %s", got, occurredAt.Format(time.RFC3339Nano))
	}
	if got := document["line"]; got != record.event.Message {
		t.Fatalf("durable line = %v, want sent line %q", got, record.event.Message)
	}
}

func TestEnsureEntryOccurredAtPreservesExistingOccurrenceTime(t *testing.T) {
	existingOccurrence := time.Date(2026, 8, 13, 23, 59, 59, 0, time.UTC)
	fallbackClock := fixedCloudWatchLogsClock{t: existingOccurrence.Add(time.Hour)}
	entry := &logging.LogEntry{
		OccurredAt: existingOccurrence,
	}

	logging.EnsureEntryOccurredAt(entry, fallbackClock)

	if got := entry.OccurredAt; !got.Equal(existingOccurrence) {
		t.Fatalf("OccurredAt = %s, want preserved source occurrence %s", got, existingOccurrence)
	}
}

func TestEnsureEntryOccurredAtCapturesMissingOccurrenceFromExplicitClock(t *testing.T) {
	sourceOccurrence := time.Date(2026, 8, 13, 23, 59, 59, 0, time.UTC)
	sourceClock := fixedCloudWatchLogsClock{t: sourceOccurrence}
	entry := &logging.LogEntry{}

	logging.EnsureEntryOccurredAt(entry, sourceClock)

	if got := entry.OccurredAt; !got.Equal(sourceOccurrence) {
		t.Fatalf("OccurredAt = %s, want source clock time %s", got, sourceOccurrence)
	}
}

type mutableCloudWatchLogsClock struct {
	mu  sync.Mutex
	now time.Time
}

func newMutableCloudWatchLogsClock(now time.Time) *mutableCloudWatchLogsClock {
	return &mutableCloudWatchLogsClock{now: now}
}

func (c *mutableCloudWatchLogsClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mutableCloudWatchLogsClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

var _ clock.Clock = (*mutableCloudWatchLogsClock)(nil)

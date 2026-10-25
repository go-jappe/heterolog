package clickhouse

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestClickHouseBufferedEntryKeepsExplicitOccurrenceAndConfiguredTimezone(t *testing.T) {
	occurredAt := time.Date(
		2026,
		8,
		14,
		0,
		30,
		0,
		0,
		time.FixedZone("explicit occurrence", 9*60*60),
	)
	wantUTC := time.Date(2026, 8, 13, 15, 30, 0, 0, time.UTC)
	baseCfg := newTestClickHouseLoggerBaseConfig()
	baseCfg.TimeConfig = pkglogger.TimeConfig{
		IsLocalTimeEnabled: true,
		TimeZone:           "Asia/Tokyo",
	}
	baseCfg.LogBodyConfig.LogLineDateFormat = "2006-01-02T15:04:05-07:00"
	cfg := NewClickHouseLoggerConfigAdapter(baseCfg)
	client := &recordingClickHouseClient{}
	logger, err := NewClickHouseLogger(
		context.Background(),
		cfg,
		client,
		newTestClickHouseFlushManagerConfig(),
		newTestClickHouseResourceProvider(),
		nil,
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewClickHouseLogger() error = %v", err)
	}
	if err := handleTestLogAt(
		context.Background(),
		logger,
		occurredAt,
		pkglogger.LogLevelInfo,
		"delayed ClickHouse flush",
	); err != nil {
		t.Fatalf("HandleLogEntry() error = %v", err)
	}
	logger.mu.Lock()
	if len(logger.entries) != 1 {
		entryCount := len(logger.entries)
		logger.mu.Unlock()
		t.Fatalf("buffered entries = %d, want 1", entryCount)
	}
	bufferedOccurredAt := logger.entries[0].OccurredAt
	logger.mu.Unlock()
	if !bufferedOccurredAt.Equal(wantUTC) || bufferedOccurredAt.Location() != time.UTC {
		t.Fatalf("buffered occurrence = %s (%s), want %s (UTC)", bufferedOccurredAt, bufferedOccurredAt.Location(), wantUTC)
	}
	if client.batchCount() != 0 {
		t.Fatalf("batch count before Close = %d, want buffered", client.batchCount())
	}
	if err := logger.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if client.batchCount() != 1 || len(client.batches[0]) != 1 {
		t.Fatalf("inserted batches = %#v, want one record", client.batches)
	}
	record := client.batches[0][0]
	if !record.Timestamp.Equal(wantUTC) || record.Timestamp.Location() != time.UTC {
		t.Fatalf("record timestamp = %s (%s), want occurrence %s (UTC)", record.Timestamp, record.Timestamp.Location(), wantUTC)
	}
	if !strings.Contains(record.Line, "2026-08-14T00:30:00+09:00") {
		t.Fatalf("record line = %q, want JST representation of occurrence", record.Line)
	}
}

func TestDeliveryClickHouseBufferedEntryKeepsExplicitOccurrenceAsUTC(t *testing.T) {
	occurredAt := time.Date(
		2026,
		8,
		14,
		0,
		30,
		0,
		123,
		time.FixedZone("explicit delivery occurrence", 9*60*60),
	)
	wantUTC := time.Date(2026, 8, 13, 15, 30, 0, 123, time.UTC)
	client := &recordingClickHouseClient{}
	logger := newTestDeliveryClickHouseLogger(t, client)

	report, err := handleTestDeliveryLogAt(
		context.Background(),
		logger,
		occurredAt,
		pkglogger.LogLevelInfo,
		"delayed delivery ClickHouse flush",
	)
	if err != nil {
		t.Fatalf("HandleLogEntry() error = %v", err)
	}
	if len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliveryEnqueued {
		t.Fatalf("HandleLogEntry() report = %+v, want one enqueued target", report)
	}

	logger.mu.Lock()
	if len(logger.entries) != 1 {
		entryCount := len(logger.entries)
		logger.mu.Unlock()
		t.Fatalf("buffered entries = %d, want 1", entryCount)
	}
	bufferedOccurredAt := logger.entries[0].OccurredAt
	logger.mu.Unlock()
	if !bufferedOccurredAt.Equal(wantUTC) || bufferedOccurredAt.Location() != time.UTC {
		t.Fatalf("buffered occurrence = %s (%s), want %s (UTC)", bufferedOccurredAt, bufferedOccurredAt.Location(), wantUTC)
	}

	if err := logger.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if client.batchCount() != 1 || len(client.batches[0]) != 1 {
		t.Fatalf("inserted batches = %#v, want one record", client.batches)
	}
	record := client.batches[0][0]
	if !record.Timestamp.Equal(wantUTC) || record.Timestamp.Location() != time.UTC {
		t.Fatalf("record timestamp = %s (%s), want occurrence %s (UTC)", record.Timestamp, record.Timestamp.Location(), wantUTC)
	}
}

func TestClickHouseDurablePayloadKeepsInsertedOccurrence(t *testing.T) {
	occurredAt := time.Date(2026, 8, 13, 15, 30, 0, 123, time.UTC)
	client := &recordingClickHouseClient{
		err: &pkgclickhouse.HTTPStatusError{
			Code:   http.StatusTooManyRequests,
			Status: "429 Too Many Requests",
		},
	}
	baseCfg := newTestClickHouseLoggerBaseConfig()
	baseCfg.TimeConfig = pkglogger.TimeConfig{
		IsLocalTimeEnabled: true,
		TimeZone:           "Asia/Tokyo",
	}
	baseCfg.LogBodyConfig.LogLineDateFormat = time.RFC3339Nano
	logger := newTestClickHouseLoggerWithConfigAndOptions(
		t,
		client,
		baseCfg,
		WithClock(clock.NewFixedClock(occurredAt)),
	)
	flushManager := logger.flushManager.(*ClickHouseFlushManager)
	entry := newTestLogEntry(pkglogger.LogLevelError, "durable occurrence", nil)
	entry.OccurredAt = occurredAt

	result, err := insertClickHouseLogEntriesAsSingleRequestForTest(
		context.Background(),
		flushManager,
		[]*logging.LogEntry{entry},
	)
	if err != nil {
		t.Fatalf("insertClickHouseLogEntriesAsSingleRequestForTest() error = %v", err)
	}
	if len(result.RetryableRecords) != 1 {
		t.Fatalf("retryable records = %d, want 1", len(result.RetryableRecords))
	}
	payload := result.RetryableRecords[0].Payload
	if got := payload["timestamp"]; got != occurredAt.Format(time.RFC3339Nano) {
		t.Fatalf("durable timestamp = %v, want %s", got, occurredAt.Format(time.RFC3339Nano))
	}
	line, _ := payload["line"].(string)
	if !strings.Contains(line, "2026-08-14T00:30:00.000000123+09:00") {
		t.Fatalf("durable line = %q, want JST representation of occurrence", line)
	}
}

package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestClickHouseLoggerHandleLogEntryPreservesApplicationFieldsInJSONEachRow(t *testing.T) {
	client := &recordingClickHouseClient{}
	logger := newTestClickHouseLogger(t, client)
	fields := clickHouseApplicationFields(t)

	calls := []func() error{
		func() error { return handleTestDebug(context.Background(), logger, "typed debug", fields...) },
		func() error { return handleTestInfo(context.Background(), logger, "typed info", fields...) },
		func() error { return handleTestWarn(context.Background(), logger, "typed warn", fields...) },
		func() error { return handleTestError(context.Background(), logger, "typed error", fields...) },
		func() error {
			return handleTestErrorWithStack(
				context.Background(),
				logger,
				"typed stack",
				errors.New("stack failure"),
				fields...,
			)
		},
	}
	for _, call := range calls {
		require.NoError(t, call())
	}
	require.NoError(t, logger.Close(context.Background()))

	records := clickHouseRecordedLogRecords(t, client)
	require.Len(t, records, len(calls))
	for _, record := range records {
		requireClickHouseApplicationFields(t, record)
	}
}

func TestDeliveryClickHouseLoggerHandleLogEntryPreservesApplicationFieldsInJSONEachRow(t *testing.T) {
	client := &recordingClickHouseClient{}
	logger := newTestDeliveryClickHouseLogger(t, client)
	fields := clickHouseApplicationFields(t)

	calls := []func() (pkglogger.DeliveryReport, error){
		func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryDebug(context.Background(), logger, "typed delivery debug", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryInfo(context.Background(), logger, "typed delivery info", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryWarn(context.Background(), logger, "typed delivery warn", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryError(context.Background(), logger, "typed delivery error", fields...)
		},
		func() (pkglogger.DeliveryReport, error) {
			return handleTestDeliveryErrorWithStack(
				context.Background(),
				logger,
				"typed delivery stack",
				errors.New("delivery stack failure"),
				fields...,
			)
		},
	}
	for _, call := range calls {
		report, err := call()
		require.NoError(t, err)
		require.Len(t, report.Targets, 1)
		require.Equal(t, pkglogger.DeliveryEnqueued, report.Targets[0].Status)
	}
	require.NoError(t, logger.Close())

	records := clickHouseRecordedLogRecords(t, client)
	require.Len(t, records, len(calls))
	for _, record := range records {
		requireClickHouseApplicationFields(t, record)
	}
}

func clickHouseApplicationFields(t *testing.T) []pkglogger.Field {
	t.Helper()

	profileField, err := pkglogger.Any("profile", map[string]any{
		"tier":    "premium",
		"enabled": true,
	})
	require.NoError(t, err)

	return []pkglogger.Field{
		pkglogger.MustFieldKey("user_id").String("hogehoge"),
		pkglogger.MustFieldKey("authenticated").Bool(true),
		pkglogger.MustFieldKey("attempt").Int(2),
		profileField,
	}
}

func clickHouseRecordedLogRecords(
	t *testing.T,
	client *recordingClickHouseClient,
) []pkgclickhouse.LogRecord {
	t.Helper()

	client.mu.Lock()
	defer client.mu.Unlock()

	var records []pkgclickhouse.LogRecord
	for _, batch := range client.batches {
		records = append(records, batch...)
	}
	return records
}

func requireClickHouseApplicationFields(t *testing.T, record pkgclickhouse.LogRecord) {
	t.Helper()

	require.Equal(t, "hogehoge", record.Fields["user_id"])
	require.Equal(t, true, record.Fields["authenticated"])
	require.Equal(t, 2, record.Fields["attempt"])
	require.Equal(t, map[string]any{
		"tier":    "premium",
		"enabled": true,
	}, record.Fields["profile"])

	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var row map[string]any
	require.NoError(t, decoder.Decode(&row))

	fields, ok := row["fields"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "hogehoge", fields["user_id"])
	require.Equal(t, true, fields["authenticated"])
	require.Equal(t, json.Number("2"), fields["attempt"])
	require.Equal(t, map[string]any{
		"tier":    "premium",
		"enabled": true,
	}, fields["profile"])
}

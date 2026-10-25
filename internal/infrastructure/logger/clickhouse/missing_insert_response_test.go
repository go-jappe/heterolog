package clickhouse

import (
	"context"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseFlushManagerQuarantinesInsertWithoutResponse(t *testing.T) {
	client := &missingInsertResponseClient{}
	flushManager := NewClickHouseFlushManager(
		newTestClickHouseFlushManagerBase(t),
		client,
		newTestClickHouseFlushManagerConfig(),
		nil,
		newTestClickHouseResourceProvider(),
		nil,
		nil,
	)
	entries := []*logging.LogEntry{
		newTestLogEntry(pkglogger.LogLevelInfo, "first", nil),
		newTestLogEntry(pkglogger.LogLevelInfo, "second", nil),
	}

	result, err := flushManager.insertEntryRecords(
		context.Background(),
		flushManager.convertLogEntriesToEntryRecords(entries),
		"missing-response-token",
	)

	// 応答のないINSERTは成功として扱わず、spool再送と同じく自動再送しないquarantineへ隔離します。
	require.NoError(t, err)
	require.Equal(t, 1, client.calls)
	assert.ErrorIs(t, result.TerminalError, infraclickhouse.ErrMissingInsertResponse)
	assert.True(t, result.HasFailedRecords())
	assert.Empty(t, result.RetryableRecords)
	assert.Empty(t, result.DeadRecords)
	require.Len(t, result.QuarantineRecords, len(entries))
	assert.False(t, result.Meta.AttemptedAt.IsZero(), "the INSERT was sent before its outcome became unknown")
	for index, record := range result.QuarantineRecords {
		assert.Equal(t, entries[index].Message, record.Payload["message"])
		assert.Equal(t, loggerfailure.FailureCode(retryable.RetryReasonUnknownOutcome), record.FailureCode)
		assert.Equal(t, infraclickhouse.DurableReplayModeNone, record.Payload[infraclickhouse.DurablePayloadFieldReplayMode])
		assert.Equal(t, "missing-response-token", record.Payload[pkgclickhouse.SettingInsertDeduplicationToken])
	}
}

// missingInsertResponseClientは、errorを返さずにInsertResponseも返さないclientです。
type missingInsertResponseClient struct {
	calls int
}

func (c *missingInsertResponseClient) InsertLogsWithOptions(
	context.Context,
	[]pkgclickhouse.LogRecord,
	pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	c.calls++
	return nil, nil
}

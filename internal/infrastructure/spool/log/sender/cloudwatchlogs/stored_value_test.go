package cloudwatchlogs

import (
	"context"
	"errors"
	"testing"
	"time"

	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 保存されたpayloadをdecode・検証できないrecordは、従来どおりinvalid（quarantine）に分類し、
// 種類のsentinelとfield名で判定できます。同じspool fileの有効なrecordは送信します。保存された
// 値は、errors.Asで取り出せるdecode・解析のerrorや型付きerror（stream名の文字を持つ
// *LogStreamNameCharacterErrorを含む）には残しません（表示文は対象外です）。
func TestCloudWatchLogsSpoolSenderKeepsStoredPayloadValuesOutOfInvalidRecordErrors(t *testing.T) {
	const stored = "stored-value-marker"
	now := time.Date(2026, 8, 14, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	badTimestamp := mustMarshalCloudWatchLogsDurablePayload(t, map[string]any{
		"log_group": "/app/test", "log_stream": "stream-a", "timestamp": stored, "line": "bad timestamp", "message": "bad timestamp",
	})
	records := []spool.SpoolRecord{
		{RequestID: "type-mismatch", Payload: []byte(`["` + stored + `"]`)},
		{RequestID: "bad-timestamp", Payload: badTimestamp},
		cloudWatchLogsSpoolRecordAtWithStream(t, "unsafe-stream", "unsafe stream", now, stored+"\n"),
		cloudWatchLogsSpoolRecordAt(t, "valid", "valid", now),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	decodeOutcome := requireOutcomeForInputIndex(t, report, 0)
	assert.Equal(t, spool.RecordOutcomeStatusInvalid, decodeOutcome.Status)
	require.ErrorIs(t, decodeOutcome.Err, ErrDecodeSpoolPayload)
	timestampOutcome := requireOutcomeForInputIndex(t, report, 1)
	assert.Equal(t, spool.RecordOutcomeStatusInvalid, timestampOutcome.Status)
	require.ErrorIs(t, timestampOutcome.Err, ErrInvalidSpoolPayloadField)
	var fieldErr *SpoolPayloadFieldError
	require.ErrorAs(t, timestampOutcome.Err, &fieldErr)
	assert.Equal(t, "timestamp", fieldErr.Field)
	streamOutcome := requireOutcomeForInputIndex(t, report, 2)
	assert.Equal(t, spool.RecordOutcomeStatusInvalid, streamOutcome.Status)
	require.ErrorIs(t, streamOutcome.Err, infracloudwatchlogs.ErrLogStreamNameControlCharacter)
	var characterErr *infracloudwatchlogs.LogStreamNameCharacterError
	assert.False(t, errors.As(streamOutcome.Err, &characterErr), "stored stream name character must not be extractable: %v", streamOutcome.Err)
	for inputIndex, outcomeErr := range []error{decodeOutcome.Err, timestampOutcome.Err, streamOutcome.Err} {
		location, found := errortree.ExtractableValue(outcomeErr, stored)
		assert.False(t, found, "input %d: %s keeps the stored payload value: %v", inputIndex, location, outcomeErr)
	}
	assertOutcome(t, report, 3, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 1, client.sendCalls)
	require.Len(t, client.events, 1)
	assert.Equal(t, "valid", client.events[0].Message)
}

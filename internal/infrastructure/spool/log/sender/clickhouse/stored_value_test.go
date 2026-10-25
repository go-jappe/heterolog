package clickhouse

import (
	"context"
	"testing"
	"time"

	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 保存されたdocumentをdecode・検証できないrecordは、従来どおりinvalid（quarantine）に分類し、
// 種類のsentinelとfield名で判定できます。同じspool fileの有効なrecordは送信します。保存された値は、
// errors.Asで取り出せるdecodeのerrorや型付きerrorのfieldには残しません（表示文は対象外です）。
func TestSendKeepsStoredPayloadValuesOutOfInvalidRecordErrors(t *testing.T) {
	const stored = "stored-value-marker"
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	invalid := func(requestID string, change func(map[string]any)) spool.SpoolRecord {
		doc := clickHouseDurableDocument("invalid", "token-"+requestID, "record-"+requestID, infraclickhouse.DurableReplayModeBatch)
		change(doc)
		return spool.SpoolRecord{RequestID: requestID, Payload: mustMarshalClickHouseDurablePayload(t, doc)}
	}
	records := []spool.SpoolRecord{
		invalid("timestamp", func(doc map[string]any) { doc["timestamp"] = stored }),
		invalid("token", func(doc map[string]any) { doc[pkgclickhouse.SettingInsertDeduplicationToken] = " " + stored + " " }),
		invalid("record-id", func(doc map[string]any) { doc[infraclickhouse.DurablePayloadFieldRecordID] = " " + stored + " " }),
		invalid("replay-mode", func(doc map[string]any) { doc[infraclickhouse.DurablePayloadFieldReplayMode] = stored }),
		clickHouseSpoolRecord(t, "valid", "valid message", "token-valid"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	for inputIndex, want := range []struct {
		kind  error
		field string
	}{
		{kind: ErrDecodeSpoolPayload},
		{kind: ErrSpoolPayloadFieldWhitespace, field: pkgclickhouse.SettingInsertDeduplicationToken},
		{kind: ErrSpoolPayloadFieldWhitespace, field: infraclickhouse.DurablePayloadFieldRecordID},
		{kind: ErrUnknownSpoolPayloadReplayMode, field: infraclickhouse.DurablePayloadFieldReplayMode},
	} {
		assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusInvalid, time.Time{})
		outcomeErr := requireOutcomeForInputIndex(t, report, inputIndex).Err
		require.ErrorIs(t, outcomeErr, want.kind)
		if want.field != "" {
			var fieldErr *SpoolPayloadFieldError
			require.ErrorAs(t, outcomeErr, &fieldErr)
			assert.Equal(t, want.field, fieldErr.Field)
		}
		location, found := errortree.ExtractableValue(outcomeErr, stored)
		assert.False(t, found, "input %d: %s keeps the stored payload value: %v", inputIndex, location, outcomeErr)
	}
	assertOutcome(t, report, 4, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 1, client.calls)
	require.Len(t, client.batches[0], 1)
	assert.Equal(t, "valid message", client.batches[0][0].Message)
}

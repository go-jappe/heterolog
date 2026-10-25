package loki

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 保存されたpayloadをdecode・検証できないrecordは、従来どおりinvalid（quarantine）に分類し、
// 種類のsentinel（不正なlabel名はpkg/lokiのErrInvalidLabelNameも）で判定できます。同じspool
// fileの有効なrecordは送信します。保存された値は、errors.Asで取り出せるdecodeのerrorや
// 型付きerror（*pkgloki.InvalidLabelNameError）には残しません（表示文は対象外です）。
func TestLokiSpoolSenderKeepsStoredPayloadValuesOutOfInvalidRecordErrors(t *testing.T) {
	const stored = "stored-value-marker"
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	sender := newTestLokiSpoolSender(t, now, client)
	badTimestamp := labeledLogEntryDocument("bad timestamp")
	badTimestamp["timestamp"] = stored
	badLabel := labeledLogEntryDocument("bad label")
	badLabel["labels"] = map[string]string{stored: "value"}
	records := []spool.SpoolRecord{
		{RequestID: "bad-timestamp", Payload: mustMarshalLabeledLogEntry(t, badTimestamp)},
		{RequestID: "bad-label", Payload: mustMarshalLabeledLogEntry(t, badLabel)},
		lokiSpoolRecord(t, "valid", "valid line"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	decodeOutcome := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	require.ErrorIs(t, decodeOutcome.Err, ErrDecodeSpoolPayload)
	labelOutcome := assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
	require.ErrorIs(t, labelOutcome.Err, ErrInvalidSpoolPayloadLabels)
	require.ErrorIs(t, labelOutcome.Err, pkgloki.ErrInvalidLabelName)
	var labelErr *pkgloki.InvalidLabelNameError
	assert.False(t, errors.As(labelOutcome.Err, &labelErr), "stored label name must not be extractable: %v", labelOutcome.Err)
	for inputIndex, outcomeErr := range []error{decodeOutcome.Err, labelOutcome.Err} {
		location, found := errortree.ExtractableValue(outcomeErr, stored)
		assert.False(t, found, "input %d: %s keeps the stored payload value: %v", inputIndex, location, outcomeErr)
	}
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 1, client.pushCalls)
	require.Len(t, client.streams, 1)
	require.Len(t, client.streams[0].Entries, 1)
	assert.Equal(t, "valid line", client.streams[0].Entries[0].Line)
}

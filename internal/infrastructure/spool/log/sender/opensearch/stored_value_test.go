package opensearch

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 保存されたpayloadをdecode・検証できないrecordは、従来どおりinvalid（quarantine）に分類し、
// 種類のsentinel・field名・値を含まない検証の理由で判定できます。保存された値は、errors.Asで
// 取り出せるdecodeのerrorや型付きerrorのfieldには残しません（表示文は対象外です）。
func TestSendKeepsStoredPayloadValuesOutOfInvalidRecordErrors(t *testing.T) {
	t.Parallel()

	const stored = "stored-value-marker"
	records := []spool.SpoolRecord{
		{RequestID: "type-mismatch", Payload: []byte(`{"index":{"name":"` + stored + `"},"id":"doc","doc":{},"op":"index"}`)},
		{RequestID: "syntax", Payload: []byte(`{"index":"` + stored + `" "id":"doc"}`)},
		spoolRecordForDocument(t, "padded-index", pkgopensearch.DocumentWriteRequest{
			Index: " " + stored + " ", ID: "doc", Doc: map[string]any{"message": "m"}, Op: pkgopensearch.WriteOpIndex,
		}),
		spoolRecordForDocument(t, "unsupported-op", pkgopensearch.DocumentWriteRequest{
			Index: "logs", ID: "doc", Doc: map[string]any{"message": "m"}, Op: pkgopensearch.WriteOp(stored),
		}),
	}
	client := &fakeOpenSearchClient{}

	report, err := newTestSender(time.Time{}, client).Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assert.Zero(t, client.indexDocumentsCallCount)
	for inputIndex, want := range []struct {
		kind   error
		field  string
		reason error
	}{
		{kind: ErrDecodeSpoolPayload},
		{kind: ErrDecodeSpoolPayload},
		{kind: ErrInvalidSpoolPayloadField, field: "index", reason: pkgopensearch.ErrIndexHasSurroundingWhitespace},
		{kind: ErrInvalidSpoolPayloadField, field: "op"},
	} {
		outcome := assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusInvalid, time.Time{})
		require.ErrorIs(t, outcome.Err, want.kind)
		if want.reason != nil {
			require.ErrorIs(t, outcome.Err, want.reason)
		}
		if want.field != "" {
			var fieldErr *SpoolPayloadFieldError
			require.ErrorAs(t, outcome.Err, &fieldErr)
			assert.Equal(t, want.field, fieldErr.Field)
		}
		location, found := errortree.ExtractableValue(outcome.Err, stored)
		assert.False(t, found, "input %d: %s keeps the stored payload value: %v", inputIndex, location, outcome.Err)
	}
}

package clickhouse

import (
	"encoding/json"
	"testing"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDurablePayloadMarshalsAsFlatDocument(t *testing.T) {
	timestamp := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	payload, err := json.Marshal(DurablePayload{
		LogRecord: pkgclickhouse.LogRecord{
			Timestamp: timestamp,
			Level:     "INFO",
			Message:   "flat clickhouse durable payload",
		},
		InsertDeduplicationToken: "root-token",
		RecordID:                 "record-id",
		ReplayMode:               DurableReplayModeRecordIsolation,
	})
	require.NoError(t, err)

	var document map[string]any
	require.NoError(t, json.Unmarshal(payload, &document))
	assert.Equal(t, timestamp.Format(time.RFC3339Nano), document["timestamp"])
	assert.Equal(t, "INFO", document["level"])
	assert.Equal(t, "flat clickhouse durable payload", document["message"])
	assert.Equal(t, "root-token", document[pkgclickhouse.SettingInsertDeduplicationToken])
	assert.Equal(t, "record-id", document[DurablePayloadFieldRecordID])
	assert.Equal(t, string(DurableReplayModeRecordIsolation), document[DurablePayloadFieldReplayMode])
	assert.NotContains(t, document, "LogRecord")
}

func TestDeriveRecordIsolationTokenIsStableAndRecordSpecific(t *testing.T) {
	first, err := DeriveRecordIsolationToken("root-token", "record-id")
	require.NoError(t, err)
	second, err := DeriveRecordIsolationToken("root-token", "record-id")
	require.NoError(t, err)
	otherRecord, err := DeriveRecordIsolationToken("root-token", "other-record-id")
	require.NoError(t, err)
	otherRoot, err := DeriveRecordIsolationToken("other-root-token", "record-id")
	require.NoError(t, err)

	assert.Equal(t, "847a090a440d7c736ca0181c870d52f2ade88381174ec46f3992c9b09a63f16a", first)
	assert.Equal(t, first, second)
	assert.NotEqual(t, first, otherRecord)
	assert.NotEqual(t, first, otherRoot)
	assert.Len(t, first, 64)
}

func TestDeriveRecordIsolationTokenRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		name      string
		rootToken string
		recordID  string
		wantErr   error
	}{
		{name: "empty root token", recordID: "record-id", wantErr: ErrEmptyRootInsertDeduplicationToken},
		{name: "blank root token", rootToken: "  ", recordID: "record-id", wantErr: ErrEmptyRootInsertDeduplicationToken},
		{name: "root token has surrounding whitespace", rootToken: " root-token ", recordID: "record-id", wantErr: ErrRootInsertDeduplicationTokenWhitespace},
		{name: "empty record ID", rootToken: "root-token", wantErr: ErrEmptyDurableRecordID},
		{name: "blank record ID", rootToken: "root-token", recordID: "  ", wantErr: ErrEmptyDurableRecordID},
		{name: "record ID has surrounding whitespace", rootToken: "root-token", recordID: " record-id ", wantErr: ErrDurableRecordIDWhitespace},
		{name: "root token contains NUL", rootToken: "root\x00token", recordID: "record-id", wantErr: ErrRootInsertDeduplicationTokenNUL},
		{name: "record ID contains NUL", rootToken: "root-token", recordID: "record\x00id", wantErr: ErrDurableRecordIDNUL},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DeriveRecordIsolationToken(tt.rootToken, tt.recordID)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestDurableReplayModeRecognizesOnlyPublishedModes(t *testing.T) {
	assert.True(t, DurableReplayModeBatch.IsKnown())
	assert.True(t, DurableReplayModeRecordIsolation.IsKnown())
	assert.True(t, DurableReplayModeNone.IsKnown())
	assert.False(t, DurableReplayMode("").IsKnown())
	assert.False(t, DurableReplayMode("future-mode").IsKnown())
}

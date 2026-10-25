package worker

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

// SpoolRecordとして不正な行は、ErrInvalidSpoolRecordと共に、値を含まない不正の理由を
// errors.Isで判定できる形で返します。
func TestDecodeSpoolRecordLineKeepsValidationReasonAsCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		line string
		want error
	}{
		{name: "zero occurredAt", line: `{"OccurredAt":"0001-01-01T00:00:00Z","Payload":{"message":"m"}}`, want: errSpoolRecordOccurredAtZero},
		{name: "missing payload", line: `{"OccurredAt":"2026-10-21T00:00:00Z"}`, want: errSpoolRecordPayloadEmpty},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeSpoolRecordLine([]byte(tc.line), 3)
			require.ErrorIs(t, err, pkgspool.ErrInvalidSpoolRecord)
			require.ErrorIs(t, err, pkgspool.ErrInvalidSpoolFile)
			require.ErrorIs(t, err, tc.want)
		})
	}

	// JSON行のdecodeでは常に有効なJSONのpayloadになるため、検証関数で直接確認します。
	err := validateSpoolRecord(pkgspool.SpoolRecord{
		OccurredAt: time.Date(2026, time.October, 21, 0, 0, 0, 0, time.UTC),
		Payload:    json.RawMessage(`{"message":`),
	})
	require.ErrorIs(t, err, errSpoolRecordPayloadNotJSON)
}

// 行をSpoolRecordへdecodeできないerrorは、保存された値（*time.ParseErrorのValueなど）を
// 保持し得るため原因として辿れません。種類はErrInvalidSpoolRecordで判定します。
func TestDecodeSpoolRecordLineDoesNotExposeStoredValueAsCause(t *testing.T) {
	_, err := decodeSpoolRecordLine([]byte(`{"OccurredAt":"stored-value","Payload":{"message":"m"}}`), 1)
	require.ErrorIs(t, err, pkgspool.ErrInvalidSpoolRecord)
	var parseErr *time.ParseError
	require.False(t, errors.As(err, &parseErr), "decode error must not expose the stored value: %v", err)
}

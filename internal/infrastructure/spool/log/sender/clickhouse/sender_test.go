package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraclickhouse "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseSpoolSenderSendsDocumentPayloadWithDeduplicationToken(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	record := clickHouseSpoolRecord(t, "request-1", "hello clickhouse spool", "token-1")

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assert.Len(t, report, 1)
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusSuccess))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 1, client.calls)
	require.Len(t, client.batches, 1)
	require.Len(t, client.batches[0], 1)
	assert.Equal(t, "hello clickhouse spool", client.batches[0][0].Message)
	require.Len(t, client.options, 1)
	assert.Equal(t, "token-1", client.options[0].InsertDeduplicationToken)
}

func TestNewClickHouseSpoolSenderRejectsNonPositiveRequestLimit(t *testing.T) {
	t.Parallel()

	for _, maxBytes := range []int{0, -1} {
		_, err := NewClickHouseSpoolSender(
			fixedClock{now: time.Unix(1, 0).UTC()},
			&recordingClickHouseSpoolClient{},
			maxBytes,
		)
		require.ErrorIs(t, err, ErrInvalidMaxInsertRequestBytes)
	}
}

func TestClickHouseSpoolSenderRejectsPayloadWithoutClickHouseFields(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	record := spool.SpoolRecord{
		RequestID: "invalid-clickhouse-payload",
		Payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
			"labels": map[string]string{"service": "loki"},
			"line":   "must not send",
		}),
	}

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.Zero(t, client.calls)
}

func TestClickHouseSpoolSenderGroupsByDeduplicationToken(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-1", "first", "token-1"),
		clickHouseSpoolRecord(t, "request-2", "second", "token-2"),
		clickHouseSpoolRecord(t, "request-3", "third", "token-1"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assert.Len(t, report, 3)
	assert.Equal(t, 3, countOutcomeStatus(report, spool.RecordOutcomeStatusSuccess))
	assertOutcomeStatuses(t, report, []spool.RecordOutcomeStatus{
		spool.RecordOutcomeStatusSuccess,
		spool.RecordOutcomeStatusSuccess,
		spool.RecordOutcomeStatusSuccess,
	})
	require.Equal(t, 2, client.calls)
	require.Len(t, client.options, 2)
	assert.Equal(t, "token-1", client.options[0].InsertDeduplicationToken)
	assert.Equal(t, "token-2", client.options[1].InsertDeduplicationToken)
	require.Len(t, client.batches[0], 2)
	require.Len(t, client.batches[1], 1)
}

func TestClickHouseSpoolSenderDoesNotSplitLegacyOversizedTokenGroup(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-1", "first legacy row", "legacy-token"),
		clickHouseSpoolRecord(t, "request-2", "second legacy row", "legacy-token"),
	}
	firstPayload, err := (&ClickHouseSpoolSender{}).convertSpoolRecordToDurablePayload(records[0])
	require.NoError(t, err)
	secondPayload, err := (&ClickHouseSpoolSender{}).convertSpoolRecordToDurablePayload(records[1])
	require.NoError(t, err)
	firstBody, err := pkgclickhouse.MarshalJSONEachRow([]pkgclickhouse.LogRecord{firstPayload.LogRecord})
	require.NoError(t, err)
	secondBody, err := pkgclickhouse.MarshalJSONEachRow([]pkgclickhouse.LogRecord{secondPayload.LogRecord})
	require.NoError(t, err)
	maxBytes := len(firstBody)
	if len(secondBody) > maxBytes {
		maxBytes = len(secondBody)
	}

	client := &recordingClickHouseSpoolClient{}
	sender, err := NewClickHouseSpoolSender(fixedClock{now: now}, client, maxBytes)
	require.NoError(t, err)
	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
	require.Zero(t, client.calls)
}

func TestClickHouseSpoolSenderMarksSingleOversizedRecordInvalidWithoutAttempt(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	record := clickHouseSpoolRecord(t, "request-1", "oversized durable row", "token-1")
	payload, err := (&ClickHouseSpoolSender{}).convertSpoolRecordToDurablePayload(record)
	require.NoError(t, err)
	body, err := pkgclickhouse.MarshalJSONEachRow([]pkgclickhouse.LogRecord{payload.LogRecord})
	require.NoError(t, err)

	client := &recordingClickHouseSpoolClient{}
	sender, err := NewClickHouseSpoolSender(fixedClock{now: now}, client, len(body)-1)
	require.NoError(t, err)

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	require.Zero(t, client.calls)
}

func TestClickHouseSpoolSenderMarksPlannerErrorInvalidWithoutAttempt(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	sender.maxInsertRequestBytes = 0
	record := clickHouseSpoolRecord(t, "request-1", "invalid plan", "token-1")

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	require.Zero(t, client.calls)
}

func TestClickHouseSpoolSenderMarksTokenDerivationErrorInvalidWithoutAttempt(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	record := clickHouseRecordIsolationSpoolRecord(
		t,
		"request-1",
		"invalid isolation token",
		"root-token\x00invalid",
		"record-id-1",
	)

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	require.Zero(t, client.calls)
}

func TestClickHouseSpoolSenderSeparatesClientMarshalFailureWithoutBackendAttempt(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	marshalErr := &pkgclickhouse.LogRecordMarshalError{
		Index: 1,
		Err:   errors.New("simulated JSONEachRow encoding failure"),
	}
	client := &recordingClickHouseSpoolClient{err: marshalErr}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-1", "valid sibling", "token-1"),
		clickHouseSpoolRecord(t, "request-2", "invalid record", "token-1"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, time.Time{})
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 0).Err, marshalErr)
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 1).Err, marshalErr)
	require.Equal(t, 1, client.calls)
}

func TestClickHouseSpoolSenderSendsRecordIsolationAsStableSingletons(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	rootToken := "root-token-1"
	records := []spool.SpoolRecord{
		clickHouseRecordIsolationSpoolRecord(
			t,
			"request-1",
			"same message",
			rootToken,
			"record-id-1",
		),
		clickHouseRecordIsolationSpoolRecord(
			t,
			"request-2",
			"same message",
			rootToken,
			"record-id-2",
		),
	}
	wantFirstToken, err := infraclickhouse.DeriveRecordIsolationToken(rootToken, "record-id-1")
	require.NoError(t, err)
	wantSecondToken, err := infraclickhouse.DeriveRecordIsolationToken(rootToken, "record-id-2")
	require.NoError(t, err)

	firstReport, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, firstReport.Validate(len(records)))
	assertOutcome(t, firstReport, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, firstReport, 1, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 2, client.calls)
	require.Len(t, client.batches, 2)
	require.Len(t, client.batches[0], 1)
	require.Len(t, client.batches[1], 1)
	assert.Equal(t, "same message", client.batches[0][0].Message)
	assert.Equal(t, "same message", client.batches[1][0].Message)
	assert.Equal(t, wantFirstToken, client.options[0].InsertDeduplicationToken)
	assert.Equal(t, wantSecondToken, client.options[1].InsertDeduplicationToken)
	assert.NotEqual(t, client.options[0].InsertDeduplicationToken, client.options[1].InsertDeduplicationToken)

	secondReport, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, secondReport.Validate(len(records)))
	require.Equal(t, 4, client.calls)
	assert.Equal(t, wantFirstToken, client.options[2].InsertDeduplicationToken)
	assert.Equal(t, wantSecondToken, client.options[3].InsertDeduplicationToken)
}

func TestClickHouseSpoolSenderDoesNotGroupDuplicateRecordIsolationTokens(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseRecordIsolationSpoolRecord(t, "request-1", "duplicated", "root-token", "record-id"),
		clickHouseRecordIsolationSpoolRecord(t, "request-2", "duplicated", "root-token", "record-id"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 2, client.calls)
	require.Len(t, client.batches, 2)
	require.Len(t, client.batches[0], 1)
	require.Len(t, client.batches[1], 1)
	assert.Equal(t, client.options[0].InsertDeduplicationToken, client.options[1].InsertDeduplicationToken)
}

func TestClickHouseSpoolSenderQuarantinesRejectedIsolatedRecordAndDeliversOthers(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	client.onInsert = func(call int) (*pkgclickhouse.InsertResponse, error) {
		require.Len(t, client.batches[call-1], 1)
		if client.batches[call-1][0].Message == "invalid UUID" {
			return nil, &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusInternalServerError,
				Status:        "500 Internal Server Error",
				ExceptionCode: 27,
				ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID,
			}
		}
		return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
	}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseRecordIsolationSpoolRecord(t, "request-1", "valid one", "root-token", "record-id-1"),
		clickHouseRecordIsolationSpoolRecord(t, "request-2", "invalid UUID", "root-token", "record-id-2"),
		clickHouseRecordIsolationSpoolRecord(t, "request-3", "valid two", "root-token", "record-id-3"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 一件でも値を拒否されたrecordは入力不備として、配送不能とせずquarantineへ隔離します。
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	quarantined := requireOutcomeForInputIndex(t, report, 1)
	var rejected *pkgclickhouse.HTTPStatusError
	require.ErrorAs(t, quarantined.Err, &rejected)
	assert.Equal(t, pkgclickhouse.ErrorTypeCannotParseUUID, rejected.ClickHouseErrorType())
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
	assert.Equal(t, 3, client.calls)
}

func TestClickHouseSpoolSenderIsolatesLateBatchContentFailureWithDerivedTokens(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	contentErr := &pkgclickhouse.HTTPStatusError{
		Code:          http.StatusInternalServerError,
		Status:        "500 Internal Server Error",
		ExceptionCode: 27,
		ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID,
	}
	client := &recordingClickHouseSpoolClient{}
	client.onInsert = func(call int) (*pkgclickhouse.InsertResponse, error) {
		for _, record := range client.batches[call-1] {
			if record.Message == "invalid UUID" {
				return nil, contentErr
			}
		}
		return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
	}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-1", "valid", "root-token"),
		clickHouseSpoolRecord(t, "request-2", "invalid UUID", "root-token"),
		clickHouseSpoolRecord(t, "request-3", "also valid", "root-token"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	// batch INSERTの拒否後は一件ずつ再送し、正常なrecordを配送します。
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	quarantined := requireOutcomeForInputIndex(t, report, 1)
	assert.ErrorIs(t, quarantined.Err, contentErr)
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 4, client.calls)
	assert.Len(t, client.batches[0], 3)
	assert.Equal(t, "root-token", client.options[0].InsertDeduplicationToken)
	// 一件ずつの再送は、record isolation modeと同じ規則で導出したtokenを使います。
	for index, requestID := range []string{"request-1", "request-2", "request-3"} {
		require.Len(t, client.batches[index+1], 1)
		wantToken, deriveErr := infraclickhouse.DeriveRecordIsolationToken("root-token", "record-"+requestID)
		require.NoError(t, deriveErr)
		assert.Equal(t, wantToken, client.options[index+1].InsertDeduplicationToken)
	}
}

func TestClickHouseSpoolSenderIsolatesLateBatchRejectedAsTooLarge(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	client.onInsert = func(call int) (*pkgclickhouse.InsertResponse, error) {
		batch := client.batches[call-1]
		// 中継先の上限が計画時の上限より小さく、二件以上のINSERTと単独の大きなrecordを拒否します。
		if len(batch) > 1 || batch[0].Message == "too large even alone" {
			return nil, &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusRequestEntityTooLarge,
				Status: "413 Request Entity Too Large",
			}
		}
		return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
	}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-1", "first", "root-token"),
		clickHouseSpoolRecord(t, "request-2", "too large even alone", "root-token"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 一件でも上限を超えるrecordは、上限設定の変更で配送できる可能性があるため隔離します。
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	tooLarge := requireOutcomeForInputIndex(t, report, 1)
	var rejected *pkgclickhouse.HTTPStatusError
	require.ErrorAs(t, tooLarge.Err, &rejected)
	assert.Equal(t, http.StatusRequestEntityTooLarge, rejected.StatusCode())
	require.Equal(t, 3, client.calls)
	assert.Len(t, client.batches[0], 2)
	assert.NotEqual(t, "root-token", client.options[1].InsertDeduplicationToken)
	assert.NotEqual(t, client.options[1].InsertDeduplicationToken, client.options[2].InsertDeduplicationToken)
}

func TestClickHouseSpoolSenderKeepsLateBatchSchemaFailureDeadWithoutIsolation(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{err: &pkgclickhouse.HTTPStatusError{
		Code:          http.StatusNotFound,
		Status:        "404 Not Found",
		ExceptionType: pkgclickhouse.ErrorTypeUnknownTable,
	}}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-1", "first", "root-token"),
		clickHouseSpoolRecord(t, "request-2", "second", "root-token"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	// record内容に依存しない宛先の不備は、一件ずつ再送しても解消しないため配送不能です。
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusPermanent, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusPermanent, now)
	require.Equal(t, 1, client.calls)
	assert.Len(t, client.batches[0], 2)
}

func TestClickHouseSpoolSenderReturnsUnsentIsolatedRecordsToRetryableWhenContextEnds(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &recordingClickHouseSpoolClient{}
	client.onInsert = func(call int) (*pkgclickhouse.InsertResponse, error) {
		if len(client.batches[call-1]) > 1 {
			return nil, &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusInternalServerError,
				Status:        "500 Internal Server Error",
				ExceptionType: pkgclickhouse.ErrorTypeCannotParseNumber,
			}
		}
		// 一件目を個別に配送した直後に、呼び出し元が停止します。
		cancel()
		return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
	}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-1", "first", "root-token"),
		clickHouseSpoolRecord(t, "request-2", "second", "root-token"),
		clickHouseSpoolRecord(t, "request-3", "unrelated", "other-token"),
	}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 個別に送る前に停止したrecordは、batch INSERTの送信時刻を残して再送対象にします。
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, now)
	unsent := requireOutcomeForInputIndex(t, report, 1)
	assert.ErrorIs(t, unsent.Err, context.Canceled)
	// 送信していない別groupは、送信時刻を記録せずに再送対象にします。
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.Equal(t, 2, client.calls)
}

func TestClickHouseSpoolSenderKeepsRecordIsolationTokenAcrossRetryableAttempts(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	retryableErr := &pkgclickhouse.HTTPStatusError{
		Code:   http.StatusServiceUnavailable,
		Status: "503 Service Unavailable",
	}
	client := &recordingClickHouseSpoolClient{err: retryableErr}
	sender := newTestClickHouseSpoolSender(t, now, client)
	record := clickHouseRecordIsolationSpoolRecord(
		t,
		"request-1",
		"retry me",
		"root-token",
		"record-id-1",
	)

	for range 2 {
		report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})
		require.NoError(t, err)
		require.NoError(t, report.Validate(1))
		outcome := requireOutcomeForInputIndex(t, report, 0)
		assert.Equal(t, spool.RecordOutcomeStatusRetryable, outcome.Status)
		assert.Equal(t, now, outcome.AttemptedAt)
		assert.ErrorIs(t, outcome.Err, retryableErr)
	}

	require.Len(t, client.options, 2)
	assert.NotEqual(t, "root-token", client.options[0].InsertDeduplicationToken)
	assert.Equal(t, client.options[0].InsertDeduplicationToken, client.options[1].InsertDeduplicationToken)
}

func TestClickHouseSpoolSenderReturnsCanceledRecordsAsRetryableBeforeDecode(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	record := spool.SpoolRecord{RequestID: "not-decoded"}

	report, err := sender.Send(ctx, []spool.SpoolRecord{record})

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(1))
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusRetryable))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 0).Err, context.Canceled)
	assert.Zero(t, client.calls)
}

func TestClickHouseSpoolSenderReturnsRemainingGroupsAsRetryableWhenContextEndsBetweenGroups(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	client := &recordingClickHouseSpoolClient{
		onInsert: func(call int) (*pkgclickhouse.InsertResponse, error) {
			cancel()
			return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
		},
	}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-success", "first", "token-1"),
		clickHouseSpoolRecord(t, "request-unattempted", "second", "token-2"),
	}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 1).Err, context.Canceled)
	assert.Equal(t, 1, client.calls)
}

func TestClickHouseSpoolSenderPreservesOriginalIndexesAcrossGroupedPartialResult(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{
		onInsert: func(call int) (*pkgclickhouse.InsertResponse, error) {
			if call == 2 {
				return nil, nil
			}
			return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
		},
	}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-success-1", "first", "token-1"),
		clickHouseSpoolRecord(t, "request-attempted-unknown", "second", "token-2"),
		clickHouseSpoolRecord(t, "request-success-2", "third", "token-1"),
		clickHouseSpoolRecord(t, "request-unattempted", "fourth", "token-3"),
	}

	report, err := sender.Send(context.Background(), records)

	require.ErrorIs(t, err, infraclickhouse.ErrMissingInsertResponse)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 応答のないINSERTは成否を確認できないため、自動再送せずquarantineへ隔離します。
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
	// 送信しなかったgroupは再送対象として残し、送信試行時刻も記録しません。
	assertOutcome(t, report, 3, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 1).Err, infraclickhouse.ErrMissingInsertResponse)
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 3).Err, infraclickhouse.ErrMissingInsertResponse)
	assert.Equal(t, 2, client.calls)
}

func TestClickHouseSpoolSenderPreservesCompletedGroupWhenLaterGroupContextIsCanceled(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	client := &recordingClickHouseSpoolClient{
		onInsert: func(call int) (*pkgclickhouse.InsertResponse, error) {
			if call == 2 {
				cancel()
				return nil, context.Canceled
			}
			return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
		},
	}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-success", "first", "token-1"),
		clickHouseSpoolRecord(t, "request-attempted-unknown", "second", "token-2"),
		clickHouseSpoolRecord(t, "request-unattempted", "third", "token-3"),
	}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 1).Err, context.Canceled)
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 2).Err, context.Canceled)
	assert.Equal(t, 2, client.calls)
}

func TestClickHouseSpoolSenderClassifiesInsertFailure(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantRetryable int
		wantPermanent int
		wantInvalid   int
	}{
		{
			name: "retryable",
			err: &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusTooManyRequests,
				Status: "429 Too Many Requests",
			},
			wantRetryable: 1,
		},
		{
			name: "permanent",
			err: &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusBadRequest,
				Status: "400 Bad Request",
			},
			wantPermanent: 1,
		},
		{
			name:          "circuit breaker open is retryable",
			err:           retryable.NewCircuitBreakerOpenError("clickhouse insert", time.Now().Add(time.Minute)),
			wantRetryable: 1,
		},
		{
			name:        "unknown result is quarantined",
			err:         errors.New("unclassified result"),
			wantInvalid: 1,
		},
		{
			name: "unknown exception in successful HTTP response is quarantined",
			err: &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusOK,
				Status:        "200 OK",
				ExceptionCode: 999,
				ExceptionType: pkgclickhouse.ErrorType("SOME_TRANSIENT_EXCEPTION"),
			},
			wantInvalid: 1,
		},
		{
			name: "input exception in successful HTTP response is quarantined",
			err: &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusOK,
				Status:        "200 OK",
				ExceptionCode: 27,
				ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID,
			},
			wantInvalid: 1,
		},
		{
			name: "schema exception in successful HTTP response is permanent",
			err: &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusOK,
				Status:        "200 OK",
				ExceptionCode: 60,
				ExceptionType: pkgclickhouse.ErrorTypeUnknownTable,
			},
			wantPermanent: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
			client := &recordingClickHouseSpoolClient{err: tt.err}
			sender := newTestClickHouseSpoolSender(t, now, client)

			report, err := sender.Send(context.Background(), []spool.SpoolRecord{
				clickHouseSpoolRecord(t, "request-1", "failed", "token-1"),
			})

			require.NoError(t, err)
			require.NoError(t, report.Validate(1))
			assert.Len(t, report, 1)
			assert.Equal(t, tt.wantRetryable, countOutcomeStatus(report, spool.RecordOutcomeStatusRetryable))
			assert.Equal(t, tt.wantPermanent, countOutcomeStatus(report, spool.RecordOutcomeStatusPermanent))
			assert.Equal(t, tt.wantInvalid, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
			outcome := requireOutcomeForInputIndex(t, report, 0)
			assert.True(t, outcome.AttemptedAt.Equal(now))
			assert.ErrorIs(t, outcome.Err, tt.err)
			assert.Equal(t, 1, client.calls)
		})
	}
}

func TestClickHouseSpoolSenderRequiresDeduplicationToken(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	client := &recordingClickHouseSpoolClient{}
	sender := newTestClickHouseSpoolSender(t, now, client)
	record := clickHouseSpoolRecord(t, "request-1", "missing token", "")

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assert.Len(t, report, 1)
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.Equal(t, 0, client.calls)
}

func TestConvertSpoolRecordToLogRecordDecodesDirectDurableDocument(t *testing.T) {
	timestamp := time.Date(2026, 6, 29, 11, 59, 0, 123, time.UTC)
	payload := mustMarshalClickHouseDurablePayload(t, map[string]any{
		"timestamp": timestamp.Format(time.RFC3339Nano),
		"level":     "ERROR",
		"message":   "direct durable payload",
		"line":      "formatted line",
		"fields": map[string]any{
			"attempt": json.Number("2"),
		},
		pkgclickhouse.SettingInsertDeduplicationToken: "token-direct",
		infraclickhouse.DurablePayloadFieldRecordID:   "record-direct",
		infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeBatch,
	})

	doc, err := (&ClickHouseSpoolSender{}).convertSpoolRecordToDurablePayload(
		spool.SpoolRecord{Payload: payload},
	)

	require.NoError(t, err)
	assert.Equal(t, timestamp, doc.Timestamp)
	assert.Equal(t, "ERROR", doc.Level)
	assert.Equal(t, "direct durable payload", doc.Message)
	assert.Equal(t, "formatted line", doc.Line)
	assert.Equal(t, json.Number("2"), doc.Fields["attempt"])
	assert.Equal(t, "token-direct", doc.InsertDeduplicationToken)
	assert.Equal(t, "record-direct", doc.RecordID)
	assert.Equal(t, infraclickhouse.DurableReplayModeBatch, doc.ReplayMode)
}

func TestConvertSpoolRecordToLogRecordRejectsInvalidDurableDocument(t *testing.T) {
	validTimestamp := time.Date(2026, 6, 29, 11, 59, 0, 0, time.UTC).Format(time.RFC3339Nano)

	tests := []struct {
		name      string
		payload   []byte
		wantErr   error
		wantField string
		// storedValueは、errors.Asで取り出せるfieldに残してはならない保存された値です。
		storedValue string
	}{
		{
			name:    "empty payload",
			wantErr: ErrEmptySpoolPayload,
		},
		{
			name:    "malformed json",
			payload: []byte("{"),
			wantErr: ErrDecodeSpoolPayload,
		},
		{
			name:    "null document",
			payload: []byte("null"),
			wantErr: ErrNilSpoolPayloadDocument,
		},
		{
			name: "missing deduplication token",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				"timestamp": validTimestamp,
				infraclickhouse.DurablePayloadFieldRecordID:   "record-1",
				infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeBatch,
			}),
			wantErr:   ErrMissingSpoolPayloadField,
			wantField: pkgclickhouse.SettingInsertDeduplicationToken,
		},
		{
			name: "non-string deduplication token",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				"timestamp": validTimestamp,
				pkgclickhouse.SettingInsertDeduplicationToken: 1,
				infraclickhouse.DurablePayloadFieldRecordID:   "record-1",
				infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeBatch,
			}),
			wantErr: ErrDecodeSpoolPayload,
		},
		{
			name: "deduplication token with surrounding whitespace",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				"timestamp": validTimestamp,
				pkgclickhouse.SettingInsertDeduplicationToken: " token-1 ",
				infraclickhouse.DurablePayloadFieldRecordID:   "record-1",
				infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeBatch,
			}),
			wantErr:     ErrSpoolPayloadFieldWhitespace,
			wantField:   pkgclickhouse.SettingInsertDeduplicationToken,
			storedValue: " token-1 ",
		},
		{
			name: "missing record ID",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				"timestamp": validTimestamp,
				pkgclickhouse.SettingInsertDeduplicationToken: "token-1",
				infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeBatch,
			}),
			wantErr:   ErrMissingSpoolPayloadField,
			wantField: infraclickhouse.DurablePayloadFieldRecordID,
		},
		{
			name: "record ID with surrounding whitespace",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				"timestamp": validTimestamp,
				pkgclickhouse.SettingInsertDeduplicationToken: "token-1",
				infraclickhouse.DurablePayloadFieldRecordID:   " record-1 ",
				infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeBatch,
			}),
			wantErr:     ErrSpoolPayloadFieldWhitespace,
			wantField:   infraclickhouse.DurablePayloadFieldRecordID,
			storedValue: " record-1 ",
		},
		{
			name: "missing replay mode",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				"timestamp": validTimestamp,
				pkgclickhouse.SettingInsertDeduplicationToken: "token-1",
				infraclickhouse.DurablePayloadFieldRecordID:   "record-1",
			}),
			wantErr:   ErrUnknownSpoolPayloadReplayMode,
			wantField: infraclickhouse.DurablePayloadFieldReplayMode,
		},
		{
			name: "unknown replay mode",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				"timestamp": validTimestamp,
				pkgclickhouse.SettingInsertDeduplicationToken: "token-1",
				infraclickhouse.DurablePayloadFieldRecordID:   "record-1",
				infraclickhouse.DurablePayloadFieldReplayMode: "future-mode",
			}),
			wantErr:     ErrUnknownSpoolPayloadReplayMode,
			wantField:   infraclickhouse.DurablePayloadFieldReplayMode,
			storedValue: "future-mode",
		},
		{
			name: "none replay mode",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				"timestamp": validTimestamp,
				pkgclickhouse.SettingInsertDeduplicationToken: "token-1",
				infraclickhouse.DurablePayloadFieldRecordID:   "record-1",
				infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeNone,
			}),
			wantErr:     ErrNotReplayableSpoolPayload,
			wantField:   infraclickhouse.DurablePayloadFieldReplayMode,
			storedValue: string(infraclickhouse.DurableReplayModeNone),
		},
		{
			name: "zero timestamp",
			payload: mustMarshalClickHouseDurablePayload(t, map[string]any{
				pkgclickhouse.SettingInsertDeduplicationToken: "token-1",
				infraclickhouse.DurablePayloadFieldRecordID:   "record-1",
				infraclickhouse.DurablePayloadFieldReplayMode: infraclickhouse.DurableReplayModeBatch,
			}),
			wantErr: ErrZeroSpoolPayloadTimestamp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := (&ClickHouseSpoolSender{}).convertSpoolRecordToDurablePayload(
				spool.SpoolRecord{Payload: tt.payload},
			)

			require.ErrorIs(t, err, tt.wantErr)
			var fieldErr *SpoolPayloadFieldError
			if tt.wantField == "" {
				assert.False(t, errors.As(err, &fieldErr), "payload-level error must not name a field: %v", err)
				return
			}
			require.ErrorAs(t, err, &fieldErr)
			assert.Equal(t, tt.wantField, fieldErr.Field)
			if tt.storedValue != "" {
				// 保存されたpayloadの値は、errors.Asで取り出せるfieldに残しません。
				location, found := errortree.ExportedFieldContaining(err, tt.storedValue)
				assert.False(t, found, "%s keeps the stored value", location)
			}
		})
	}
}

func newTestClickHouseSpoolSender(
	t *testing.T,
	now time.Time,
	client *recordingClickHouseSpoolClient,
) *ClickHouseSpoolSender {
	t.Helper()

	sender, err := NewClickHouseSpoolSender(fixedClock{now: now}, client, 10*1024*1024)
	require.NoError(t, err)
	return sender
}

func clickHouseSpoolRecord(
	t *testing.T,
	requestID string,
	message string,
	token string,
) spool.SpoolRecord {
	t.Helper()

	doc := clickHouseDurableDocument(
		message,
		token,
		"record-"+requestID,
		infraclickhouse.DurableReplayModeBatch,
	)
	payload := mustMarshalClickHouseDurablePayload(t, doc)

	return spool.SpoolRecord{
		OccurredAt: time.Date(2026, 6, 29, 11, 59, 1, 0, time.UTC),
		RequestID:  requestID,
		Payload:    payload,
	}
}

func clickHouseRecordIsolationSpoolRecord(
	t *testing.T,
	requestID string,
	message string,
	rootToken string,
	recordID string,
) spool.SpoolRecord {
	t.Helper()

	doc := clickHouseDurableDocument(
		message,
		rootToken,
		recordID,
		infraclickhouse.DurableReplayModeRecordIsolation,
	)
	return spool.SpoolRecord{
		OccurredAt: time.Date(2026, 6, 29, 11, 59, 1, 0, time.UTC),
		RequestID:  requestID,
		Payload:    mustMarshalClickHouseDurablePayload(t, doc),
	}
}

func clickHouseDurableDocument(
	message string,
	token string,
	recordID string,
	replayMode infraclickhouse.DurableReplayMode,
) map[string]any {
	doc := map[string]any{
		"timestamp": time.Date(2026, 6, 29, 11, 59, 0, 0, time.UTC).Format(time.RFC3339Nano),
		"level":     "INFO",
		"message":   message,
		"line":      message,
		infraclickhouse.DurablePayloadFieldRecordID:   recordID,
		infraclickhouse.DurablePayloadFieldReplayMode: replayMode,
	}
	if token != "" {
		doc[pkgclickhouse.SettingInsertDeduplicationToken] = token
	}
	return doc
}

func mustMarshalClickHouseDurablePayload(t *testing.T, data map[string]any) []byte {
	t.Helper()

	payload, err := json.Marshal(data)
	require.NoError(t, err)
	return payload
}

func assertOutcome(
	t *testing.T,
	report spool.ExportReport,
	inputIndex int,
	status spool.RecordOutcomeStatus,
	attemptedAt time.Time,
) {
	t.Helper()
	outcome := requireOutcomeForInputIndex(t, report, inputIndex)
	assert.Equal(t, inputIndex, outcome.InputIndex)
	assert.Equal(t, status, outcome.Status)
	assert.True(t, outcome.AttemptedAt.Equal(attemptedAt))
	if status == spool.RecordOutcomeStatusSuccess {
		assert.NoError(t, outcome.Err)
	} else {
		assert.Error(t, outcome.Err)
	}
}

func requireOutcomeForInputIndex(
	t *testing.T,
	report spool.ExportReport,
	inputIndex int,
) spool.RecordOutcome {
	t.Helper()
	var matches []spool.RecordOutcome
	for _, outcome := range report {
		if outcome.InputIndex == inputIndex {
			matches = append(matches, outcome)
		}
	}
	require.Len(t, matches, 1, "input index %d", inputIndex)
	return matches[0]
}

func countOutcomeStatus(report spool.ExportReport, status spool.RecordOutcomeStatus) int {
	count := 0
	for _, outcome := range report {
		if outcome.Status == status {
			count++
		}
	}
	return count
}

func assertOutcomeStatuses(
	t *testing.T,
	report spool.ExportReport,
	want []spool.RecordOutcomeStatus,
) {
	t.Helper()
	require.Len(t, report, len(want))
	for inputIndex, status := range want {
		outcome := requireOutcomeForInputIndex(t, report, inputIndex)
		assert.Equal(t, status, outcome.Status, "input index %d", inputIndex)
		if status == spool.RecordOutcomeStatusSuccess {
			assert.NoError(t, outcome.Err, "input index %d", inputIndex)
		} else {
			assert.Error(t, outcome.Err, "input index %d", inputIndex)
		}
	}
}

type recordingClickHouseSpoolClient struct {
	calls    int
	err      error
	batches  [][]pkgclickhouse.LogRecord
	options  []pkgclickhouse.InsertOptions
	onInsert func(call int) (*pkgclickhouse.InsertResponse, error)
}

func (c *recordingClickHouseSpoolClient) InsertLogsWithOptions(
	ctx context.Context,
	records []pkgclickhouse.LogRecord,
	opts pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	_ = ctx
	c.calls++
	c.batches = append(c.batches, append([]pkgclickhouse.LogRecord(nil), records...))
	c.options = append(c.options, opts)
	if c.onInsert != nil {
		return c.onInsert(c.calls)
	}
	if c.err != nil {
		return nil, c.err
	}
	return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
}

func (c *recordingClickHouseSpoolClient) InsertLogs(
	ctx context.Context,
	records []pkgclickhouse.LogRecord,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogsWithOptions(ctx, records, pkgclickhouse.InsertOptions{})
}

func (c *recordingClickHouseSpoolClient) InsertLog(
	ctx context.Context,
	record pkgclickhouse.LogRecord,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogs(ctx, []pkgclickhouse.LogRecord{record})
}

func (c *recordingClickHouseSpoolClient) InsertLogWithOptions(
	ctx context.Context,
	record pkgclickhouse.LogRecord,
	opts pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	return c.InsertLogsWithOptions(ctx, []pkgclickhouse.LogRecord{record}, opts)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

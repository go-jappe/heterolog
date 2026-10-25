package loki

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLokiSpoolSenderSendsDocumentPayloadAsLokiStream(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	sender := newTestLokiSpoolSender(t, now, client)
	records := []spool.SpoolRecord{lokiSpoolRecord(t, "request-1", "hello loki spool")}
	inputBeforeSend := cloneSpoolRecords(records)

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assert.Len(t, report, 1)
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 送信試行時刻はoutcomeで返し、Sendへ渡したrecordの再送メタデータは変更しません。
	assert.Zero(t, records[0].ResendAttemptCount)
	assert.True(t, records[0].FirstResendAttemptedAt.IsZero())
	assert.True(t, records[0].LastResendAttemptedAt.IsZero())
	assert.Equal(t, inputBeforeSend, records)

	require.Equal(t, 1, client.pushCalls)
	require.Len(t, client.streams, 1)
	assert.Equal(t, "loki-test", client.streams[0].Labels["appName"])
	require.Len(t, client.streams[0].Entries, 1)
	assert.Equal(t, "hello loki spool", client.streams[0].Entries[0].Line)
	assert.True(t, client.streams[0].Entries[0].Timestamp.Equal(now.Add(-time.Minute)))
}

func TestNewLokiSpoolSenderRejectsInvalidPushLimits(t *testing.T) {
	sender, err := NewLokiSpoolSender(
		fixedClock{now: time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)},
		&recordingLokiSpoolClient{},
		infraloki.PushLimits{},
	)

	require.ErrorIs(t, err, infraloki.ErrInvalidPushLimits)
	assert.Nil(t, sender)
}

func TestLokiSpoolSenderReadsHardCodedDirectPayload(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	sender := newTestLokiSpoolSender(t, now, client)
	record := spool.SpoolRecord{
		RequestID: "direct-request",
		Payload: []byte(
			`{"labels":{"appName":"direct-app","loglevel":"info","env":"test"},"timestamp":"2026-06-23T12:59:00Z","line":"direct loki"}`,
		),
	}

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	require.Len(t, client.streams, 1)
	assert.Equal(t, "direct-app", client.streams[0].Labels["appName"])
	require.Len(t, client.streams[0].Entries, 1)
	assert.Equal(t, "direct loki", client.streams[0].Entries[0].Line)
}

func TestLokiSpoolSenderRejectsEnvelopePayload(t *testing.T) {
	client := &recordingLokiSpoolClient{}
	sender := newTestLokiSpoolSender(t, time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC), client)
	record := spool.SpoolRecord{
		RequestID: "envelope",
		Payload: []byte(
			`{"version":2,"backend":"loki","data":{"labels":{"appName":"must-not-send"},"timestamp":"2026-06-23T12:59:00Z","line":"must not send"}}`,
		),
	}

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	invalidOutcome := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.ErrorIs(t, invalidOutcome.Err, ErrEmptySpoolPayloadLabels)
	assert.Zero(t, client.pushCalls)
}

func TestLokiSpoolSenderClassifiesPushFailure(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantRetryable int
		wantPermanent int
		wantPushCalls int
		wantInvalid   int
		records       []spool.SpoolRecord
	}{
		{
			name: "retryable",
			err: &pkgloki.HTTPStatusError{
				Code:   http.StatusServiceUnavailable,
				Status: "503 Service Unavailable",
			},
			wantRetryable: 1,
			wantPushCalls: 1,
		},
		{
			name: "ingestion blocked remains retryable for future durable send",
			err: &pkgloki.HTTPStatusError{
				Code:   260,
				Status: "260 Ingestion Blocked",
			},
			wantRetryable: 1,
			wantPushCalls: 1,
		},
		{
			name:          "circuit breaker open remains retryable for future durable send",
			err:           retryable.NewCircuitBreakerOpenError("loki push", time.Now().Add(time.Minute)),
			wantRetryable: 1,
			wantPushCalls: 1,
		},
		{
			name:          "unknown result is quarantined",
			err:           errors.New("unclassified result"),
			wantInvalid:   1,
			wantPushCalls: 1,
		},
		{
			name: "bad request is quarantined",
			err: &pkgloki.HTTPStatusError{
				Code:   http.StatusBadRequest,
				Status: "400 Bad Request",
			},
			wantInvalid:   1,
			wantPushCalls: 1,
		},
		{
			name: "single entry rejected as too large is quarantined",
			err: &pkgloki.HTTPStatusError{
				Code:   http.StatusRequestEntityTooLarge,
				Status: "413 Request Entity Too Large",
			},
			wantInvalid:   1,
			wantPushCalls: 1,
		},
		{
			name: "unauthorized is permanent",
			err: &pkgloki.HTTPStatusError{
				Code:   http.StatusUnauthorized,
				Status: "401 Unauthorized",
			},
			wantPermanent: 1,
			wantPushCalls: 1,
		},
		{
			name:          "invalid skips backend",
			records:       []spool.SpoolRecord{{RequestID: "bad", Payload: []byte("{")}},
			wantInvalid:   1,
			wantPushCalls: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			client := &recordingLokiSpoolClient{err: tt.err}
			sender := newTestLokiSpoolSender(t, time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC), client)
			records := tt.records
			if records == nil {
				records = []spool.SpoolRecord{lokiSpoolRecord(t, "request-1", "hello loki spool")}
			}

			report, err := sender.Send(context.Background(), records)

			require.NoError(t, err)
			require.NoError(t, report.Validate(len(records)))
			assert.Len(t, report, len(records))
			assert.Equal(t, tt.wantRetryable, countOutcomeStatus(report, spool.RecordOutcomeStatusRetryable))
			assert.Equal(t, tt.wantPermanent, countOutcomeStatus(report, spool.RecordOutcomeStatusPermanent))
			assert.Equal(t, tt.wantInvalid, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
			assert.Equal(t, tt.wantPushCalls, client.pushCalls)
			switch {
			case tt.wantRetryable > 0:
				outcome := assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, sender.Clock.Now())
				assert.ErrorIs(t, outcome.Err, tt.err)
			case tt.wantPermanent > 0:
				outcome := assertOutcome(t, report, 0, spool.RecordOutcomeStatusPermanent, sender.Clock.Now())
				assert.ErrorIs(t, outcome.Err, tt.err)
			case tt.wantInvalid > 0 && tt.err != nil:
				outcome := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, sender.Clock.Now())
				assert.ErrorIs(t, outcome.Err, tt.err)
			case tt.wantInvalid > 0:
				outcome := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
				assert.ErrorIs(t, outcome.Err, ErrDecodeSpoolPayload)
			}
		})
	}
}

func TestLokiSpoolSenderQuarantinesRequestRejectedWithBadRequestWithoutIsolation(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	client.pushResult = func(_ context.Context, streams []pkgloki.Stream) error {
		for _, line := range lokiStreamLines(streams) {
			if line == "invalid for Loki" {
				return &pkgloki.HTTPStatusError{
					Code:   http.StatusBadRequest,
					Status: "400 Bad Request",
				}
			}
		}
		return nil
	}
	sender := newTestLokiSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		lokiSpoolRecord(t, "request-1", "first valid line"),
		{RequestID: "decode-invalid", Payload: []byte("{")},
		lokiSpoolRecord(t, "request-2", "invalid for Loki"),
		lokiSpoolRecord(t, "request-3", "second valid line"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	// Lokiは400でも有効なstreamを受理している可能性があり、どのentryが受理されたか
	// 確認できません。分割して再送すると重複し得るため、送信した全entryを隔離します。
	for _, inputIndex := range []int{0, 2, 3} {
		quarantined := assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusInvalid, now)
		var rejected *pkgloki.HTTPStatusError
		require.ErrorAs(t, quarantined.Err, &rejected)
		assert.Equal(t, http.StatusBadRequest, rejected.Code)
	}
	invalid := assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.ErrorIs(t, invalid.Err, ErrDecodeSpoolPayload)
	assert.Equal(t, 1, client.pushCalls)
}

func TestLokiSpoolSenderSplitsRequestRejectedAsTooLarge(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	client.pushResult = func(_ context.Context, streams []pkgloki.Stream) error {
		lines := lokiStreamLines(streams)
		// 中継先の上限が計画時の上限より小さく、二件以上のpushと単独の大きなentryを拒否します。
		if len(lines) > 1 || (len(lines) == 1 && lines[0] == "too large even alone") {
			return &pkgloki.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		return nil
	}
	sender := newTestLokiSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		lokiSpoolRecord(t, "request-1", "line 1"),
		lokiSpoolRecord(t, "request-2", "too large even alone"),
		lokiSpoolRecord(t, "request-3", "line 3"),
		lokiSpoolRecord(t, "request-4", "line 4"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	for _, inputIndex := range []int{0, 2, 3} {
		assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusSuccess, now)
	}
	// 一件でも拒否されたentryだけを、配送不能とせずquarantineへ隔離します。
	tooLarge := assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	var rejected *pkgloki.HTTPStatusError
	require.ErrorAs(t, tooLarge.Err, &rejected)
	assert.Equal(t, http.StatusRequestEntityTooLarge, rejected.Code)
	// 最初はrequest全体を送り、分割後は拒否されたentryを単独で送って確認します。
	// 他のentryは一度だけ配送し、分割の順序と粒度には依存しません。
	pushed := lokiPushedLines(client.pushes)
	require.NotEmpty(t, pushed)
	assert.Equal(t, []string{"line 1", "too large even alone", "line 3", "line 4"}, pushed[0])
	assert.Contains(t, pushed, []string{"too large even alone"}, "the rejected entry is confirmed alone")
	var accepted []string
	for _, lines := range pushed {
		if len(lines) == 1 && lines[0] != "too large even alone" {
			accepted = append(accepted, lines[0])
		}
	}
	assert.ElementsMatch(t, []string{"line 1", "line 3", "line 4"}, accepted)
}

func TestLokiSpoolSenderReturnsUnsentSplitRequestsToRetryableWhenContextEnds(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &recordingLokiSpoolClient{}
	client.pushResult = func(_ context.Context, streams []pkgloki.Stream) error {
		if len(lokiStreamLines(streams)) > 1 {
			return &pkgloki.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		// 分割後の一件目を送信した直後に、呼び出し元が停止します。
		cancel()
		return nil
	}
	sender := newTestLokiSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		lokiSpoolRecord(t, "request-1", "line 1"),
		lokiSpoolRecord(t, "request-2", "line 2"),
	}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 413ではどのentryも受理されていないため、未送信の後半は再送対象として残します。
	unsent := assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, now)
	assert.ErrorIs(t, unsent.Err, context.Canceled)
	assert.Equal(t, 2, client.pushCalls)
}

func TestLokiSpoolSenderSplitsByConfiguredRequestLimitBeforePush(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	client.pushResult = func(_ context.Context, streams []pkgloki.Stream) error {
		if len(lokiStreamLines(streams)) > 2 {
			t.Fatalf("planned request contains more than two records: %v", lokiStreamLines(streams))
		}
		return nil
	}
	requestLimit := lokiPushRequestSizeForLines(t, "line 1", "line 2")
	sender := newTestLokiSpoolSenderWithLimits(t, now, client, infraloki.PushLimits{
		MaxLineBytes:        64,
		MaxPushRequestBytes: requestLimit,
	})
	records := []spool.SpoolRecord{
		lokiSpoolRecord(t, "request-1", "line 1"),
		lokiSpoolRecord(t, "request-2", "line 2"),
		lokiSpoolRecord(t, "request-3", "line 3"),
		lokiSpoolRecord(t, "request-4", "line 4"),
		lokiSpoolRecord(t, "request-5", "line 5"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	for inputIndex := range records {
		assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusSuccess, now)
	}
	assert.Equal(t, 3, client.pushCalls)
	require.Len(t, client.pushes, 3)
	assert.Equal(t, []string{"line 1", "line 2"}, lokiStreamLines(client.pushes[0]))
	assert.Equal(t, []string{"line 3", "line 4"}, lokiStreamLines(client.pushes[1]))
	assert.Equal(t, []string{"line 5"}, lokiStreamLines(client.pushes[2]))
}

func TestLokiSpoolSenderClassifiesRequestWideAuthorizationFailureWithoutIsolation(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{err: &pkgloki.HTTPStatusError{
		Code:   http.StatusUnauthorized,
		Status: "401 Unauthorized",
	}}
	sender := newTestLokiSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		lokiSpoolRecord(t, "request-1", "line 1"),
		lokiSpoolRecord(t, "request-2", "line 2"),
		lokiSpoolRecord(t, "request-3", "line 3"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	for inputIndex := range records {
		assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusPermanent, now)
	}
	assert.Equal(t, 1, client.pushCalls)
}

func TestLokiSpoolSenderReturnsRemainingPlannedRequestsToRetryableWhenContextEnds(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &recordingLokiSpoolClient{}
	client.pushResult = func(_ context.Context, _ []pkgloki.Stream) error {
		if client.pushCalls == 1 {
			// 最初の計画済みrequestの成功応答後に親contextが終了した状況を再現する。
			cancel()
			return nil
		}
		t.Fatalf("unexpected Push call %d after context cancellation", client.pushCalls)
		return nil
	}
	requestLimit := lokiPushRequestSizeForLines(t, "line 1", "line 2")
	sender := newTestLokiSpoolSenderWithLimits(t, now, client, infraloki.PushLimits{
		MaxLineBytes:        64,
		MaxPushRequestBytes: requestLimit,
	})
	records := []spool.SpoolRecord{
		lokiSpoolRecord(t, "request-1", "line 1"),
		lokiSpoolRecord(t, "request-2", "line 2"),
		lokiSpoolRecord(t, "request-3", "line 3"),
		lokiSpoolRecord(t, "request-4", "line 4"),
	}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusSuccess, now)
	for _, inputIndex := range []int{2, 3} {
		outcome := assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusRetryable, time.Time{})
		assert.ErrorIs(t, outcome.Err, context.Canceled)
	}
	assert.Equal(t, 1, client.pushCalls)
}

func TestLokiSpoolSenderRejectsOversizedLineBeforePush(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	sender := newTestLokiSpoolSenderWithLimits(t, now, client, infraloki.PushLimits{
		MaxLineBytes:        8,
		MaxPushRequestBytes: 1024,
	})
	records := []spool.SpoolRecord{
		lokiSpoolRecord(t, "oversized", "123456789"),
		lokiSpoolRecord(t, "valid", "valid"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	rejected := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.ErrorIs(t, rejected.Err, infraloki.ErrLogLineTooLarge)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 1, client.pushCalls)
	assert.Equal(t, []string{"valid"}, lokiStreamLines(client.streams))
}

func TestLokiSpoolSenderRejectsSingleRecordThatExceedsRequestLimitBeforePush(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	line := "small"
	requestSize := lokiPushRequestSizeForLines(t, line)
	sender := newTestLokiSpoolSenderWithLimits(t, now, client, infraloki.PushLimits{
		MaxLineBytes:        len(line) + 1,
		MaxPushRequestBytes: requestSize - 1,
	})

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{
		lokiSpoolRecord(t, "request-too-large", line),
	})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	rejected := assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assert.ErrorIs(t, rejected.Err, infraloki.ErrPushRecordTooLarge)
	assert.Zero(t, client.pushCalls)
}

func TestLokiSpoolSenderRejectsInvalidLabelRecordWithoutAffectingValidRecords(t *testing.T) {
	tests := []struct {
		name             string
		invalidLabelName string
	}{
		{name: "invalid character", invalidLabelName: "bad-label"},
		{name: "starts with digit", invalidLabelName: "1bad_label"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
			client := &recordingLokiSpoolClient{}
			sender := newTestLokiSpoolSender(t, now, client)
			invalidPayload := labeledLogEntryDocument("must not be sent")
			invalidPayload["labels"] = map[string]string{tt.invalidLabelName: "value"}
			records := []spool.SpoolRecord{
				lokiSpoolRecord(t, "valid-1", "first valid line"),
				{RequestID: "invalid", Payload: mustMarshalLabeledLogEntry(t, invalidPayload)},
				lokiSpoolRecord(t, "valid-2", "second valid line"),
			}

			report, err := sender.Send(context.Background(), records)

			require.NoError(t, err)
			require.NoError(t, report.Validate(len(records)))
			assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
			invalidOutcome := assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
			assert.ErrorIs(t, invalidOutcome.Err, ErrInvalidSpoolPayloadLabels)
			assert.ErrorIs(t, invalidOutcome.Err, pkgloki.ErrInvalidLabelName)
			// 保存されたlabel名は、errors.Asで取り出せる型付きerrorやfieldに残しません。
			var labelErr *pkgloki.InvalidLabelNameError
			assert.False(t, errors.As(invalidOutcome.Err, &labelErr), "stored label name must not be extractable: %v", invalidOutcome.Err)
			location, found := errortree.ExportedFieldContaining(invalidOutcome.Err, tt.invalidLabelName)
			assert.False(t, found, "%s keeps the stored label name", location)
			assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
			require.Equal(t, 1, client.pushCalls)
			require.Len(t, client.streams, 1)
			require.Len(t, client.streams[0].Entries, 2)
			assert.Equal(t, "first valid line", client.streams[0].Entries[0].Line)
			assert.Equal(t, "second valid line", client.streams[0].Entries[1].Line)
		})
	}
}

func TestLokiSpoolSenderAcceptsServerValidLabelForms(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 23, 13, 30, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	sender := newTestLokiSpoolSender(t, now, client)
	payload := labeledLogEntryDocument("accepted")
	payload["labels"] = map[string]string{
		"__internal_style": "",
		"app:component":    "service",
	}

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{{
		RequestID: "valid-label-forms",
		Payload:   mustMarshalLabeledLogEntry(t, payload),
	}})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 1, client.pushCalls)
	require.Len(t, client.streams, 1)
	assert.Equal(t, "", client.streams[0].Labels["__internal_style"])
	assert.Equal(t, "service", client.streams[0].Labels["app:component"])
}

func TestLokiSpoolSenderReturnsUnattemptedRecordsToRetryableWhenContextEndsBeforePush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := &recordingLokiSpoolClient{}
	sender := newTestLokiSpoolSender(t, time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC), client)
	invalidRecord := spool.SpoolRecord{RequestID: "invalid", Payload: []byte("{")}
	validRecord := lokiSpoolRecord(t, "retryable", "push was not attempted")
	records := []spool.SpoolRecord{invalidRecord, validRecord}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	require.Len(t, report, len(records))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	retryableOutcome := assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.ErrorIs(t, retryableOutcome.Err, context.Canceled)
	assert.Zero(t, client.pushCalls)
}

func TestLokiSpoolSenderQuarantinesAttemptedRecordsWhenContextEndsDuringPush(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	client := &recordingLokiSpoolClient{
		err: context.Canceled,
		onPush: func() {
			cancel()
		},
	}
	sender := newTestLokiSpoolSender(t, now, client)
	invalidRecord := spool.SpoolRecord{RequestID: "invalid", Payload: []byte("{")}
	validRecord := lokiSpoolRecord(t, "unknown", "unknown push result")
	records := []spool.SpoolRecord{invalidRecord, validRecord}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	require.Len(t, report, len(records))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	unknownOutcome := assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	assert.ErrorIs(t, unknownOutcome.Err, context.Canceled)
	assert.Equal(t, 1, client.pushCalls)
}

func TestLokiSpoolSenderKeepsInputOutcomesWhileGroupingDuplicatePayloads(t *testing.T) {
	now := time.Date(2026, 6, 23, 14, 0, 0, 0, time.UTC)
	client := &recordingLokiSpoolClient{}
	sender := newTestLokiSpoolSender(t, now, client)

	labelARecord := lokiSpoolRecord(t, "same-request", "same line")
	labelBPayload := labeledLogEntryDocument("label b line")
	labelBPayload["labels"] = map[string]string{
		"appName":  "loki-test-b",
		"loglevel": "info",
		"env":      "test",
	}
	labelBRecord := spool.SpoolRecord{
		OccurredAt: labelARecord.OccurredAt,
		RequestID:  "request-b",
		Payload:    mustMarshalLabeledLogEntry(t, labelBPayload),
	}
	invalidRecord := spool.SpoolRecord{RequestID: "invalid", Payload: []byte("{")}
	records := []spool.SpoolRecord{
		labelARecord,
		invalidRecord,
		labelBRecord,
		labelARecord, // 完全に同一のrecordも別々の入力slotとして扱う。
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	require.Len(t, report, len(records))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 3, spool.RecordOutcomeStatusSuccess, now)

	require.Len(t, client.streams, 2)
	assert.Equal(t, "loki-test", client.streams[0].Labels["appName"])
	require.Len(t, client.streams[0].Entries, 2)
	assert.Equal(t, "same line", client.streams[0].Entries[0].Line)
	assert.Equal(t, "same line", client.streams[0].Entries[1].Line)
	assert.Equal(t, "loki-test-b", client.streams[1].Labels["appName"])
	require.Len(t, client.streams[1].Entries, 1)
}

type recordingLokiSpoolClient struct {
	err        error
	pushCalls  int
	streams    []pkgloki.Stream
	pushes     [][]pkgloki.Stream
	onPush     func()
	pushResult func(context.Context, []pkgloki.Stream) error
}

func (c *recordingLokiSpoolClient) Push(ctx context.Context, streams []pkgloki.Stream) error {
	c.pushCalls++
	c.streams = append([]pkgloki.Stream(nil), streams...)
	c.pushes = append(c.pushes, append([]pkgloki.Stream(nil), streams...))
	if c.onPush != nil {
		c.onPush()
	}
	if c.pushResult != nil {
		return c.pushResult(ctx, streams)
	}
	return c.err
}

func (c *recordingLokiSpoolClient) PushEntry(
	ctx context.Context,
	labels map[string]string,
	entry pkgloki.Entry,
) error {
	return c.Push(ctx, []pkgloki.Stream{{Labels: labels, Entries: []pkgloki.Entry{entry}}})
}

func (c *recordingLokiSpoolClient) PushLog(
	ctx context.Context,
	labels map[string]string,
	line string,
) error {
	return c.PushEntry(ctx, labels, pkgloki.Entry{Timestamp: time.Now(), Line: line})
}

func newTestLokiSpoolSender(
	t *testing.T,
	now time.Time,
	client *recordingLokiSpoolClient,
) *LokiSpoolSender {
	t.Helper()

	return newTestLokiSpoolSenderWithLimits(t, now, client, infraloki.PushLimits{
		MaxLineBytes:        256 * 1024,
		MaxPushRequestBytes: 4 * 1024 * 1024,
	})
}

func newTestLokiSpoolSenderWithLimits(
	t *testing.T,
	now time.Time,
	client *recordingLokiSpoolClient,
	limits infraloki.PushLimits,
) *LokiSpoolSender {
	t.Helper()

	sender, err := NewLokiSpoolSender(fixedClock{now: now}, client, limits)
	require.NoError(t, err)
	return sender
}

func lokiPushRequestSizeForLines(t *testing.T, lines ...string) int {
	t.Helper()

	entries := make([]pkgloki.Entry, 0, len(lines))
	for _, line := range lines {
		entries = append(entries, pkgloki.Entry{
			Timestamp: time.Date(2026, 6, 23, 12, 59, 0, 0, time.UTC),
			Line:      line,
		})
	}
	size, err := pkgloki.PushRequestSize([]pkgloki.Stream{{
		Labels: map[string]string{
			"appName":  "loki-test",
			"loglevel": "info",
			"env":      "test",
		},
		Entries: entries,
	}})
	require.NoError(t, err)
	return size
}

func lokiSpoolRecord(t *testing.T, requestID string, line string) spool.SpoolRecord {
	t.Helper()

	payload := mustMarshalLabeledLogEntry(t, labeledLogEntryDocument(line))

	return spool.SpoolRecord{
		OccurredAt: time.Date(2026, 6, 23, 12, 59, 0, 0, time.UTC),
		RequestID:  requestID,
		Payload:    payload,
	}
}

func labeledLogEntryDocument(line string) map[string]any {
	return map[string]any{
		"labels": map[string]string{
			"appName":  "loki-test",
			"loglevel": "info",
			"env":      "test",
		},
		"timestamp": time.Date(2026, 6, 23, 12, 59, 0, 0, time.UTC).Format(time.RFC3339Nano),
		"line":      line,
		// Logger側のdurable payloadには再送に必要な3フィールド以外の
		// 診断情報も含まれるため、senderがそれらを許容することも確認する。
		"level":      "INFO",
		"message":    line,
		"request_id": "request-1",
		"fields":     map[string]any{"source": "sender-test"},
	}
}

func mustMarshalLabeledLogEntry(t *testing.T, data map[string]any) []byte {
	t.Helper()

	payload, err := json.Marshal(data)
	require.NoError(t, err)
	return payload
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

func assertOutcome(
	t *testing.T,
	report spool.ExportReport,
	wantInputIndex int,
	wantStatus spool.RecordOutcomeStatus,
	wantAttemptedAt time.Time,
) spool.RecordOutcome {
	t.Helper()
	var matched []spool.RecordOutcome
	for _, outcome := range report {
		if outcome.InputIndex == wantInputIndex {
			matched = append(matched, outcome)
		}
	}
	require.Len(t, matched, 1, "outcomes for input index %d", wantInputIndex)
	actual := matched[0]
	assert.Equal(t, wantInputIndex, actual.InputIndex)
	assert.Equal(t, wantStatus, actual.Status)
	assert.True(
		t,
		actual.AttemptedAt.Equal(wantAttemptedAt),
		"attempted at = %v, want %v",
		actual.AttemptedAt,
		wantAttemptedAt,
	)
	if wantStatus == spool.RecordOutcomeStatusSuccess {
		assert.NoError(t, actual.Err)
	} else {
		assert.Error(t, actual.Err)
	}
	return actual
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

func lokiPushedLines(pushes [][]pkgloki.Stream) [][]string {
	lines := make([][]string, 0, len(pushes))
	for _, streams := range pushes {
		lines = append(lines, lokiStreamLines(streams))
	}
	return lines
}

func lokiStreamLines(streams []pkgloki.Stream) []string {
	lines := make([]string, 0)
	for _, stream := range streams {
		for _, entry := range stream.Entries {
			lines = append(lines, entry.Line)
		}
	}
	return lines
}

// cloneSpoolRecordsは、Sendへ渡すrecordとpayloadの内容を送信前の値として複製します。
// 送信後に渡したsliceの要素と比較し、senderが入力recordを変更しないことを確認します。
func cloneSpoolRecords(records []spool.SpoolRecord) []spool.SpoolRecord {
	cloned := make([]spool.SpoolRecord, len(records))
	for index, record := range records {
		record.Payload = append(json.RawMessage(nil), record.Payload...)
		cloned[index] = record
	}
	return cloned
}

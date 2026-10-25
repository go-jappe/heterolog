package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infracloudwatchlogs "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsSpoolSenderSendsDocumentPayloadAsEvents(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{cloudWatchLogsSpoolRecord(t, "request-1", "hello cloudwatch spool")}
	inputBeforeSend := cloneSpoolRecords(records)

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assert.Len(t, report, 1)
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusSuccess))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 送信試行時刻はoutcomeで返し、Sendへ渡したrecordの再送メタデータは変更しません。
	assert.Zero(t, records[0].ResendAttemptCount)
	assert.True(t, records[0].FirstResendAttemptedAt.IsZero())
	assert.True(t, records[0].LastResendAttemptedAt.IsZero())
	assert.Equal(t, inputBeforeSend, records)

	require.Equal(t, 1, client.sendCalls)
	require.Len(t, client.events, 1)
	assert.Equal(t, "hello cloudwatch spool", client.events[0].Message)
	assert.True(t, client.events[0].Timestamp.Equal(now.Add(-time.Minute)))
	require.Equal(t, []time.Time{now}, client.sentAts)
}

func TestCloudWatchLogsSpoolSenderReadsHardCodedDirectPayload(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	record := spool.SpoolRecord{
		RequestID: "direct-request",
		Payload: []byte(
			`{"log_group":"/app/test","log_stream":"stream-a","timestamp":"2026-06-23T12:59:00Z","line":"direct cloudwatch","message":"direct cloudwatch"}`,
		),
	}

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusSuccess))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	require.Len(t, client.events, 1)
	assert.Equal(t, "direct cloudwatch", client.events[0].Message)
}

func TestCloudWatchLogsSpoolSenderClassifiesPutFailure(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantRetryable int
		wantPermanent int
		wantPutCalls  int
		wantInvalid   int
		records       []spool.SpoolRecord
	}{
		{
			name: "retryable",
			err: &pkgcloudwatchlogs.HTTPStatusError{
				Code:   http.StatusTooManyRequests,
				Status: "429 Too Many Requests",
			},
			wantRetryable: 1,
			wantPutCalls:  1,
		},
		{
			name: "permanent",
			err: &pkgcloudwatchlogs.HTTPStatusError{
				Code:   http.StatusBadRequest,
				Status: "400 Bad Request",
			},
			wantPermanent: 1,
			wantPutCalls:  1,
		},
		{
			name:          "circuit breaker open is retryable",
			err:           retryable.NewCircuitBreakerOpenError("cloudwatch logs send", time.Now().Add(time.Minute)),
			wantRetryable: 1,
			wantPutCalls:  1,
		},
		{
			name:         "unknown result is quarantined",
			err:          errors.New("unclassified result"),
			wantInvalid:  1,
			wantPutCalls: 1,
		},
		{
			name:         "invalid skips backend",
			records:      []spool.SpoolRecord{{RequestID: "bad", Payload: []byte("{")}},
			wantInvalid:  1,
			wantPutCalls: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			client := &recordingCloudWatchLogsSpoolSender{err: tt.err}
			sender := newTestCloudWatchLogsSpoolSender(
				t,
				time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC),
				client,
			)
			records := tt.records
			if records == nil {
				records = []spool.SpoolRecord{cloudWatchLogsSpoolRecord(t, "request-1", "hello cloudwatch spool")}
			}

			report, err := sender.Send(context.Background(), records)

			require.NoError(t, err)
			require.NoError(t, report.Validate(len(records)))
			assert.Len(t, report, len(records))
			assert.Equal(t, tt.wantRetryable, countOutcomeStatus(report, spool.RecordOutcomeStatusRetryable))
			assert.Equal(t, tt.wantPermanent, countOutcomeStatus(report, spool.RecordOutcomeStatusPermanent))
			assert.Equal(t, tt.wantInvalid, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
			for _, outcome := range report {
				assert.Error(t, outcome.Err)
				if tt.err != nil {
					assert.ErrorIs(t, outcome.Err, tt.err)
				}
			}
			assert.Equal(t, tt.wantPutCalls, client.sendCalls)
		})
	}
}

func TestCloudWatchLogsSpoolSenderClassifiesPartialRejectedLogEvents(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{
		output: &pkgcloudwatchlogs.PutLogEventsOutput{
			RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{
				TooOldLogEventEndIndex:   intPointer(1),
				TooNewLogEventStartIndex: intPointer(2),
			},
		},
	}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	oldTime := now.Add(-time.Hour)
	acceptedTime := now.Add(-time.Minute)
	futureTime := now.Add(time.Hour)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "ok", "accepted", acceptedTime),
		cloudWatchLogsSpoolRecordAt(t, "future", "too new", futureTime),
		cloudWatchLogsSpoolRecordAt(t, "old", "too old", oldTime),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assert.Len(t, report, 3)
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, now)
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusPermanent, now)
	assert.NoError(t, requireOutcomeForInputIndex(t, report, 0).Err)
	requireRejectedLogEvent(t, requireOutcomeForInputIndex(t, report, 1).Err, 1, pkgcloudwatchlogs.RejectedLogEventReasonTooNew)
	requireRejectedLogEvent(t, requireOutcomeForInputIndex(t, report, 2).Err, 2, pkgcloudwatchlogs.RejectedLogEventReasonTooOld)
	require.Equal(t, 1, client.sendCalls)
	require.Len(t, client.events, 3)
	assert.Equal(t, "too old", client.events[0].Message)
	assert.Equal(t, "accepted", client.events[1].Message)
	assert.Equal(t, "too new", client.events[2].Message)
}

func TestCloudWatchLogsSpoolSenderKeepsTooNewOnlyRejectionRecordSpecific(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{
		output: &pkgcloudwatchlogs.PutLogEventsOutput{
			RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{
				TooNewLogEventStartIndex: intPointer(1),
			},
		},
	}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "accepted", "accepted", now),
		cloudWatchLogsSpoolRecordAt(t, "future", "future", now.Add(time.Hour)),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, now)
	requireRejectedLogEvent(t, requireOutcomeForInputIndex(t, report, 1).Err, 1, pkgcloudwatchlogs.RejectedLogEventReasonTooNew)
}

func TestCloudWatchLogsSpoolSenderKeepsExpiredRejectionRecordSpecific(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{
		output: &pkgcloudwatchlogs.PutLogEventsOutput{
			RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{
				ExpiredLogEventEndIndex: intPointer(1),
			},
		},
	}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "accepted", "accepted", now.Add(-time.Minute)),
		cloudWatchLogsSpoolRecordAt(t, "expired", "expired", now.Add(-time.Hour)),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusPermanent, now)
	assert.NoError(t, requireOutcomeForInputIndex(t, report, 0).Err)
	requireRejectedLogEvent(t, requireOutcomeForInputIndex(t, report, 1).Err, 1, pkgcloudwatchlogs.RejectedLogEventReasonExpired)
	require.Len(t, client.events, 2)
	assert.Equal(t, "expired", client.events[0].Message)
	assert.Equal(t, "accepted", client.events[1].Message)
}

func TestCloudWatchLogsSpoolSenderRejectsStoredEventsRequiringMultipleTimeSpanRequests(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "last", "last", now.Add(48*time.Hour)),
		cloudWatchLogsSpoolRecordAt(t, "first", "first", now),
	}

	report, err := sender.Send(context.Background(), records)

	require.ErrorIs(t, err, ErrRequestBoundary)
	require.NoError(t, report.Validate(len(records)))
	assert.Len(t, report, 2)
	assert.Equal(t, 2, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
	assertOutcomeStatuses(t, report, []spool.RecordOutcomeStatus{
		spool.RecordOutcomeStatusInvalid,
		spool.RecordOutcomeStatusInvalid,
	})
	require.Zero(t, client.sendCalls)
	requireRequestBoundary(t, err, RequestBoundaryError{Kind: ErrMultiplePutLogEventsRequests, Requests: 2})
}

func TestCloudWatchLogsSpoolSenderRejectsStoredEventsRequiringMultipleByteLimitedRequests(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	firstMessage := strings.Repeat("a", 600_000)
	secondMessage := strings.Repeat("b", 600_000)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "second", secondMessage, now.Add(time.Second)),
		cloudWatchLogsSpoolRecordAt(t, "first", firstMessage, now),
	}

	report, err := sender.Send(context.Background(), records)

	require.ErrorIs(t, err, ErrRequestBoundary)
	require.NoError(t, report.Validate(len(records)))
	assert.Equal(t, 2, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
	require.Zero(t, client.sendCalls)
	requireRequestBoundary(t, err, RequestBoundaryError{Kind: ErrMultiplePutLogEventsRequests, Requests: 2})
}

func TestCloudWatchLogsSpoolSenderRejectsMultipleStoredStreams(t *testing.T) {
	attemptedAt := time.Date(2026, 8, 13, 20, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, attemptedAt, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAtWithStream(t, "first", "first", attemptedAt, "stream-a"),
		cloudWatchLogsSpoolRecordAtWithStream(t, "second", "second", attemptedAt.Add(time.Second), "stream-b"),
	}

	report, err := sender.Send(context.Background(), records)

	require.ErrorIs(t, err, ErrRequestBoundary)
	require.NoError(t, report.Validate(len(records)))
	for inputIndex := range records {
		assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusInvalid, time.Time{})
	}
	require.Zero(t, client.sendCalls)
	requireRequestBoundary(t, err, RequestBoundaryError{Kind: ErrMixedLogStreams, InputIndex: 1})
	// 保存されたstream名は、errors.Asで取り出せる公開fieldに残しません。
	for _, stored := range []string{"stream-a", "stream-b"} {
		location, found := errortree.ExportedFieldContaining(err, stored)
		assert.False(t, found, "%s keeps the stored stream name", location)
	}
}

func TestCloudWatchLogsSpoolSenderRejectsMixedStoredStreamsWithoutSending(t *testing.T) {
	now := time.Date(2026, 8, 13, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAtWithStream(t, "first", "first", now.Add(-time.Hour), "stream-a/12"),
		cloudWatchLogsSpoolRecordAtWithStream(t, "second", "second", now, "stream-a/13"),
	}

	report, err := sender.Send(context.Background(), records)

	require.ErrorIs(t, err, ErrRequestBoundary)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
	require.Zero(t, client.sendCalls)
}

func TestCloudWatchLogsSpoolSenderSendsOldEventWithoutLocalAgeCutoff(t *testing.T) {
	now := time.Date(2026, 8, 13, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	record := cloudWatchLogsSpoolRecordAtWithStream(
		t,
		"old",
		"old",
		now.AddDate(0, 0, -30),
		"stream-a/2026/07/14",
	)

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	require.Len(t, client.requests, 1)
	assert.Equal(t, "stream-a/2026/07/14", client.requests[0].LogStreamName)
}

func TestCloudWatchLogsSpoolSenderClassifiesInvalidEventRecordsBeforeClient(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "empty", "", now),
		cloudWatchLogsSpoolRecordAtWithMessage(t, "empty-message", "[INFO] 2026-06-23T13:00:01Z - ", "", now.Add(time.Second)),
		cloudWatchLogsSpoolRecordAt(t, "valid", "valid", now.Add(2*time.Second)),
		cloudWatchLogsSpoolRecordAt(t, "large", strings.Repeat("x", pkgcloudwatchlogs.MaxPutLogEventMessageBytes+1), now.Add(3*time.Second)),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assert.Len(t, report, 4)
	assert.Equal(t, 3, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusSuccess))
	assertOutcomeStatuses(t, report, []spool.RecordOutcomeStatus{
		spool.RecordOutcomeStatusInvalid,
		spool.RecordOutcomeStatusInvalid,
		spool.RecordOutcomeStatusSuccess,
		spool.RecordOutcomeStatusInvalid,
	})
	require.Equal(t, 1, client.sendCalls)
	require.Len(t, client.events, 1)
	assert.Equal(t, "valid", client.events[0].Message)
}

func TestCloudWatchLogsSpoolSenderAppliesPerEventMessageLimitPerRecord(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	largest := strings.Repeat("x", pkgcloudwatchlogs.MaxPutLogEventMessageBytes)

	// 上限ちょうどのmessageは、classifierとplannerの両方が一回のPutLogEventsとして受け付けます。
	largestClient := &recordingCloudWatchLogsSpoolSender{}
	report, err := newTestCloudWatchLogsSpoolSender(t, now, largestClient).Send(
		context.Background(),
		[]spool.SpoolRecord{cloudWatchLogsSpoolRecordAt(t, "largest", largest, now)},
	)
	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	require.Equal(t, 1, largestClient.sendCalls)
	require.Len(t, largestClient.events, 1)
	assert.Len(t, largestClient.events[0].Message, pkgcloudwatchlogs.MaxPutLogEventMessageBytes)

	// 1 byte超えるrecordだけをinvalidにし、同じspool fileの他のrecordは送信します。
	client := &recordingCloudWatchLogsSpoolSender{}
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "valid", "valid", now),
		cloudWatchLogsSpoolRecordAt(t, "too-large", largest+"x", now.Add(time.Second)),
	}
	report, err = newTestCloudWatchLogsSpoolSender(t, now, client).Send(context.Background(), records)
	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, time.Time{})
	var validationErr *infracloudwatchlogs.EventValidationError
	require.ErrorAs(t, requireOutcomeForInputIndex(t, report, 1).Err, &validationErr)
	assert.Equal(t, infracloudwatchlogs.FailureCodeInvalidEventTooLarge, validationErr.FailureCode)
	assert.Equal(t, pkgcloudwatchlogs.MaxPutLogEventMessageBytes+1, validationErr.MessageBytes)
	require.Equal(t, 1, client.sendCalls)
	require.Len(t, client.events, 1)
	assert.Equal(t, "valid", client.events[0].Message)
}

func TestCloudWatchLogsSpoolSenderClassifiesInvalidPayloadsBeforeClient(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	nilDocPayload := mustMarshalCloudWatchLogsDurablePayload(t, nil)
	envelopePayload := []byte(`{"version":2,"backend":"cloudwatchlogs","data":{"timestamp":"2026-06-23T13:00:00Z","line":"must not send","message":"must not send"}}`)
	records := []spool.SpoolRecord{
		{RequestID: "empty-payload"},
		{RequestID: "malformed-json", Payload: []byte("{")},
		{RequestID: "nil-doc", Payload: nilDocPayload},
		{RequestID: "envelope", Payload: envelopePayload},
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assert.Len(t, report, 4)
	assert.Equal(t, 4, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
	assertOutcomeStatuses(t, report, []spool.RecordOutcomeStatus{
		spool.RecordOutcomeStatusInvalid,
		spool.RecordOutcomeStatusInvalid,
		spool.RecordOutcomeStatusInvalid,
		spool.RecordOutcomeStatusInvalid,
	})
	assert.Equal(t, 0, client.sendCalls)
}

func TestCloudWatchLogsSpoolSenderRejectsUnsafeStoredStreamNameBeforeSending(t *testing.T) {
	now := time.Date(2026, 8, 14, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	record := cloudWatchLogsSpoolRecordAtWithStream(
		t,
		"unsafe-stream",
		"message",
		now,
		"app\nworker",
	)

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.NoError(t, err)
	require.NoError(t, report.Validate(1))
	outcome := requireOutcomeForInputIndex(t, report, 0)
	assert.Equal(t, spool.RecordOutcomeStatusInvalid, outcome.Status)
	assert.ErrorIs(t, outcome.Err, infracloudwatchlogs.ErrLogStreamNameControlCharacter)
	// 保存されたstream名の文字は、errors.Asで取り出せる型付きerrorに残しません。
	var characterErr *infracloudwatchlogs.LogStreamNameCharacterError
	assert.False(t, errors.As(outcome.Err, &characterErr), "stored stream name character must not be extractable: %v", outcome.Err)
	assert.Zero(t, client.sendCalls)
}

func TestCloudWatchLogsSpoolSenderReturnsContextErrorBeforeClassifyingInvalidRecords(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	report, err := sender.Send(ctx, []spool.SpoolRecord{
		{RequestID: "empty-payload"},
	})

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(1))
	assert.Len(t, report, 1)
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusRetryable))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, time.Time{})
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 0).Err, context.Canceled)
	assert.Equal(t, 0, client.sendCalls)
}

func TestCloudWatchLogsSpoolSenderRejectsNilOutputSuccess(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{returnNilOutput: true}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	record := cloudWatchLogsSpoolRecord(t, "request-1", "hello cloudwatch spool")

	report, err := sender.Send(context.Background(), []spool.SpoolRecord{record})

	require.ErrorIs(t, err, ErrInvalidPutLogEventsResponse)
	require.ErrorIs(t, err, infracloudwatchlogs.ErrMissingPutLogEventsOutput)
	require.NoError(t, report.Validate(1))
	assert.Len(t, report, 1)
	assert.Equal(t, 1, countOutcomeStatus(report, spool.RecordOutcomeStatusInvalid))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, now)
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 0).Err, infracloudwatchlogs.ErrMissingPutLogEventsOutput)
	assert.Equal(t, 1, client.sendCalls)
}

func TestCloudWatchLogsSpoolSenderQuarantinesAttemptedRecordsWhenContextEndsDuringPut(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	client := &recordingCloudWatchLogsSpoolSender{
		err: context.Canceled,
		onSend: func() {
			cancel()
		},
	}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	invalidRecord := spool.SpoolRecord{RequestID: "invalid", Payload: []byte("{")}
	validRecord := cloudWatchLogsSpoolRecord(t, "unknown-result", "unknown put result")
	records := []spool.SpoolRecord{invalidRecord, validRecord}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, time.Time{})
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 1).Err, context.Canceled)
	assert.Equal(t, 1, client.sendCalls)
}

type recordingCloudWatchLogsSpoolSender struct {
	err             error
	output          *pkgcloudwatchlogs.PutLogEventsOutput
	returnNilOutput bool
	sendCalls       int
	events          []pkgcloudwatchlogs.Event
	onSend          func()
	sentAts         []time.Time
	sendFunc        func(pkgcloudwatchlogs.PutLogEventsRequest) (*pkgcloudwatchlogs.PutLogEventsOutput, error)
	requests        []pkgcloudwatchlogs.PutLogEventsRequest
}

func (s *recordingCloudWatchLogsSpoolSender) PutLogEvents(
	ctx context.Context,
	request pkgcloudwatchlogs.PutLogEventsRequest,
) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
	_ = ctx
	s.sendCalls++
	request.Events = append([]pkgcloudwatchlogs.Event(nil), request.Events...)
	s.requests = append(s.requests, request)
	s.events = append([]pkgcloudwatchlogs.Event(nil), request.Events...)
	s.sentAts = append(s.sentAts, request.AttemptedAt)
	if s.onSend != nil {
		s.onSend()
	}
	if s.sendFunc != nil {
		return s.sendFunc(request)
	}
	if s.returnNilOutput {
		return nil, s.err
	}
	if s.output != nil || s.err != nil {
		return s.output, s.err
	}
	return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
}

func newTestCloudWatchLogsSpoolSender(
	t *testing.T,
	now time.Time,
	client *recordingCloudWatchLogsSpoolSender,
) *CloudWatchLogsSpoolSender {
	t.Helper()

	sender, err := NewCloudWatchLogsSpoolSender(fixedClock{now: now}, client)
	require.NoError(t, err)
	return sender
}

func cloudWatchLogsSpoolRecord(t *testing.T, requestID string, line string) spool.SpoolRecord {
	t.Helper()
	return cloudWatchLogsSpoolRecordAt(
		t,
		requestID,
		line,
		time.Date(2026, 6, 23, 12, 59, 0, 0, time.UTC),
	)
}

func cloudWatchLogsSpoolRecordAt(
	t *testing.T,
	requestID string,
	line string,
	timestamp time.Time,
) spool.SpoolRecord {
	t.Helper()
	return cloudWatchLogsSpoolRecordAtWithMessageAndStream(t, requestID, line, line, timestamp, "stream-a")
}

func cloudWatchLogsSpoolRecordAtWithStream(
	t *testing.T,
	requestID string,
	line string,
	timestamp time.Time,
	streamName string,
) spool.SpoolRecord {
	t.Helper()
	return cloudWatchLogsSpoolRecordAtWithMessageAndStream(
		t,
		requestID,
		line,
		line,
		timestamp,
		streamName,
	)
}

func cloudWatchLogsSpoolRecordAtWithMessage(
	t *testing.T,
	requestID string,
	line string,
	message string,
	timestamp time.Time,
) spool.SpoolRecord {
	t.Helper()
	return cloudWatchLogsSpoolRecordAtWithMessageAndStream(
		t,
		requestID,
		line,
		message,
		timestamp,
		"stream-a",
	)
}

func cloudWatchLogsSpoolRecordAtWithMessageAndStream(
	t *testing.T,
	requestID string,
	line string,
	message string,
	timestamp time.Time,
	streamName string,
) spool.SpoolRecord {
	t.Helper()

	payload := mustMarshalCloudWatchLogsDurablePayload(
		t,
		map[string]any{
			"log_group":  "/app/test",
			"log_stream": streamName,
			"timestamp":  timestamp.Format(time.RFC3339Nano),
			"line":       line,
			"message":    message,
		},
	)

	return spool.SpoolRecord{
		OccurredAt: timestamp,
		RequestID:  requestID,
		Payload:    payload,
	}
}

func mustMarshalCloudWatchLogsDurablePayload(t *testing.T, data map[string]any) []byte {
	t.Helper()

	payload, err := json.Marshal(data)
	require.NoError(t, err)
	return payload
}

func intPointer(v int) *int {
	return &v
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

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

func TestCloudWatchLogsSpoolSenderQuarantinesUncertainResponses(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	for _, index := range []int{-1, 1, 2} {
		client := &recordingCloudWatchLogsSpoolSender{output: &pkgcloudwatchlogs.PutLogEventsOutput{RejectedLogEventsInfo: &pkgcloudwatchlogs.RejectedLogEventsInfo{TooNewLogEventStartIndex: &index}}}
		sender := newTestCloudWatchLogsSpoolSender(t, now, client)
		report, err := sender.Send(context.Background(), []spool.SpoolRecord{cloudWatchLogsSpoolRecord(t, "retained", "uncertain response")})
		require.Error(t, err)
		require.NoError(t, report.Validate(1))
		assertOutcome(t, report, 0, spool.RecordOutcomeStatusInvalid, now)
		require.Equal(t, 1, client.sendCalls)
	}
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

func TestCloudWatchLogsSpoolSenderSplitsRequestRejectedAsTooLarge(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	client := &recordingCloudWatchLogsSpoolSender{}
	client.sendFunc = func(request pkgcloudwatchlogs.PutLogEventsRequest) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
		// 中継先の上限がAWSの上限より小さく、二件以上のrequestと単独の大きなeventを拒否します。
		if len(request.Events) > 1 || request.Events[0].Message == "too large even alone" {
			return nil, &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
	}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "first", "first", now.Add(-3*time.Second)),
		cloudWatchLogsSpoolRecordAt(t, "large", "too large even alone", now.Add(-2*time.Second)),
		cloudWatchLogsSpoolRecordAt(t, "third", "third", now.Add(-time.Second)),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 一件でも拒否されたeventは、上限設定の変更で配送できる可能性があるため隔離します。
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusInvalid, now)
	var tooLarge *pkgcloudwatchlogs.HTTPStatusError
	require.ErrorAs(t, requireOutcomeForInputIndex(t, report, 1).Err, &tooLarge)
	assert.Equal(t, http.StatusRequestEntityTooLarge, tooLarge.Code)
	assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
	require.NotEmpty(t, client.requests)
	var sent [][]string
	var accepted []string
	for _, request := range client.requests {
		var messages []string
		for _, event := range request.Events {
			messages = append(messages, event.Message)
		}
		sent = append(sent, messages)
		// 分割後もrouting済みのstreamと初回の送信時刻を引き継ぎます。
		assert.Equal(t, "stream-a", request.LogStreamName)
		assert.True(t, request.AttemptedAt.Equal(now))
		// 分割後のrequestも、eventを時系列順に並べます。
		assert.IsNonDecreasing(t, eventTimestamps(request.Events), "request events %v", messages)
		if len(messages) == 1 && messages[0] != "too large even alone" {
			accepted = append(accepted, messages[0])
		}
	}
	// 最初はbatch全体を送り、分割後は拒否されたeventを単独で送って確認します。
	// 他のeventは一度だけ配送し、分割の順序と粒度には依存しません。
	assert.Equal(t, []string{"first", "too large even alone", "third"}, sent[0])
	assert.Contains(t, sent, []string{"too large even alone"}, "the rejected event is confirmed alone")
	assert.ElementsMatch(t, []string{"first", "third"}, accepted)
}

// eventTimestampsは、requestに含めたeventの送信用timestampを並び順のまま返します。
func eventTimestamps(events []pkgcloudwatchlogs.Event) []int64 {
	timestamps := make([]int64, 0, len(events))
	for _, event := range events {
		timestamps = append(timestamps, event.Timestamp.UnixNano())
	}
	return timestamps
}

func TestCloudWatchLogsSpoolSenderReturnsUnsentSplitEventsToRetryableWhenContextEnds(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &recordingCloudWatchLogsSpoolSender{}
	client.sendFunc = func(request pkgcloudwatchlogs.PutLogEventsRequest) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
		if len(request.Events) > 1 {
			return nil, &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		}
		// 分割後の一件目を送信した直後に、呼び出し元が停止します。
		cancel()
		return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
	}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "first", "first", now.Add(-2*time.Second)),
		cloudWatchLogsSpoolRecordAt(t, "second", "second", now.Add(-time.Second)),
	}

	report, err := sender.Send(ctx, records)

	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusSuccess, now)
	// 413ではどのeventも受理されていないため、未送信の後半は再送対象として残します。
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, now)
	assert.ErrorIs(t, requireOutcomeForInputIndex(t, report, 1).Err, context.Canceled)
	assert.Equal(t, 2, client.sendCalls)
}

// requireRejectedLogEventは、受付期間外の拒否をerrors.Asで取り出し、拒否理由を比較します。
func requireRejectedLogEvent(
	t *testing.T,
	err error,
	inputIndex int,
	reason pkgcloudwatchlogs.RejectedLogEventReason,
) {
	t.Helper()
	require.ErrorIs(t, err, ErrLogEventRejected)
	var rejected *RejectedLogEventError
	require.ErrorAs(t, err, &rejected)
	assert.Equal(t, inputIndex, rejected.InputIndex)
	assert.Equal(t, reason, rejected.Reason)
}

// requireRequestBoundaryは、request境界の違反をerrors.Asで取り出し、理由と、違反した入力位置・
// request数・原因を比較します。保存されたstream名は公開fieldに持たないため、ここでは比較しません。
func requireRequestBoundary(t *testing.T, err error, want RequestBoundaryError) {
	t.Helper()
	require.ErrorIs(t, err, ErrRequestBoundary)
	require.ErrorIs(t, err, want.Kind)
	var boundary *RequestBoundaryError
	require.ErrorAs(t, err, &boundary)
	assert.Equal(t, want.Kind, boundary.Kind)
	assert.Equal(t, want.InputIndex, boundary.InputIndex)
	assert.Equal(t, want.Requests, boundary.Requests)
	assert.Equal(t, want.Err, boundary.Err)
}

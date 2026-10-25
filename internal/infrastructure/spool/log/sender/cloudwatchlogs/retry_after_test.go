package cloudwatchlogs

import (
	"context"
	"net/http"
	"testing"
	"time"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudWatchLogsSpoolSenderKeepsRetryAfterOfThrottledRequest(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	// pkg/cloudwatchlogsはRetry-Afterの秒数をRetryAfterDurへ設定します。
	throttleErr := &pkgcloudwatchlogs.HTTPStatusError{
		Code:          http.StatusBadRequest,
		Status:        "400 Bad Request",
		AwsErrorType:  "ThrottlingException",
		RetryAfterDur: 30 * time.Second,
	}
	client := &recordingCloudWatchLogsSpoolSender{err: throttleErr}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "first", "first", now.Add(-2*time.Second)),
		cloudWatchLogsSpoolRecordAt(t, "second", "second", now.Add(-time.Second)),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	for inputIndex := range records {
		assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusRetryable, now)
		outcome := requireOutcomeForInputIndex(t, report, inputIndex)
		assert.ErrorIs(t, outcome.Err, throttleErr)
		assert.Equal(t, 30*time.Second, outcome.RetryAfter)
	}
	assert.Equal(t, 1, client.sendCalls)
}

func TestCloudWatchLogsSpoolSenderDefersSplitHalfAfterRetryAfter(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	throttleErr := &pkgcloudwatchlogs.HTTPStatusError{
		Code:          http.StatusServiceUnavailable,
		Status:        "503 Service Unavailable",
		AwsErrorType:  "ServiceUnavailableException",
		RetryAfterDur: 30 * time.Second,
	}
	client := &recordingCloudWatchLogsSpoolSender{}
	client.sendFunc = func(pkgcloudwatchlogs.PutLogEventsRequest) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
		switch client.sendCalls {
		case 1:
			return nil, &pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		case 2:
			return nil, throttleErr
		default:
			return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
		}
	}
	sender := newTestCloudWatchLogsSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		cloudWatchLogsSpoolRecordAt(t, "first", "first", now.Add(-2*time.Second)),
		cloudWatchLogsSpoolRecordAt(t, "second", "second", now.Add(-time.Second)),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, now)
	assert.Equal(t, 30*time.Second, requireOutcomeForInputIndex(t, report, 0).RetryAfter)
	// 分けた後半は未送信のまま、初回requestの送信時刻を残して再送対象にします。
	assertOutcome(t, report, 1, spool.RecordOutcomeStatusRetryable, now)
	deferred := requireOutcomeForInputIndex(t, report, 1)
	assert.ErrorIs(t, deferred.Err, spool.ErrDeferredByRetryAfter)
	assert.ErrorIs(t, deferred.Err, throttleErr)
	assert.Equal(t, 2, client.sendCalls)
}

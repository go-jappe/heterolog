package clickhouse

import (
	"context"
	"net/http"
	"testing"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClickHouseSpoolSenderDefersRemainingGroupsAfterRetryAfter(t *testing.T) {
	for _, tt := range []struct {
		name       string
		retryAfter time.Duration
		wantCalls  int
	}{
		{name: "retry-after", retryAfter: 30 * time.Second, wantCalls: 1},
		// Retry-Afterがなければ、従来どおり同じ周期で残りのgroupも送信します。
		{name: "without retry-after", wantCalls: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
			throttleErr := &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusServiceUnavailable,
				Status: "503 Service Unavailable",
				After:  tt.retryAfter,
			}
			client := &recordingClickHouseSpoolClient{}
			client.onInsert = func(call int) (*pkgclickhouse.InsertResponse, error) {
				if call == 1 {
					return nil, throttleErr
				}
				return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
			}
			sender := newTestClickHouseSpoolSender(t, now, client)
			// tokenごとに別INSERTへ分け、先頭groupの応答で残りを止めるか確認します。
			records := []spool.SpoolRecord{
				clickHouseSpoolRecord(t, "request-1", "first", "token-1"),
				clickHouseSpoolRecord(t, "request-2", "second", "token-2"),
				clickHouseSpoolRecord(t, "request-3", "third", "token-3"),
			}

			report, err := sender.Send(context.Background(), records)

			require.NoError(t, err)
			require.NoError(t, report.Validate(len(records)))
			assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, now)
			throttled := requireOutcomeForInputIndex(t, report, 0)
			assert.ErrorIs(t, throttled.Err, throttleErr)
			assert.Equal(t, tt.retryAfter, throttled.RetryAfter)
			require.Equal(t, tt.wantCalls, client.calls)
			if tt.retryAfter == 0 {
				assertOutcome(t, report, 1, spool.RecordOutcomeStatusSuccess, now)
				assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
				return
			}
			for _, inputIndex := range []int{1, 2} {
				assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusRetryable, time.Time{})
				deferred := requireOutcomeForInputIndex(t, report, inputIndex)
				assert.ErrorIs(t, deferred.Err, spool.ErrDeferredByRetryAfter)
				assert.ErrorIs(t, deferred.Err, throttleErr)
				assert.Zero(t, deferred.RetryAfter)
			}
		})
	}
}

func TestClickHouseSpoolSenderDefersIsolatedRecordsAfterRetryAfter(t *testing.T) {
	now := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	contentErr := &pkgclickhouse.HTTPStatusError{
		Code:          http.StatusInternalServerError,
		Status:        "500 Internal Server Error",
		ExceptionCode: 27,
		ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID,
	}
	throttleErr := &pkgclickhouse.HTTPStatusError{
		Code:   http.StatusTooManyRequests,
		Status: "429 Too Many Requests",
		After:  30 * time.Second,
	}
	client := &recordingClickHouseSpoolClient{}
	client.onInsert = func(call int) (*pkgclickhouse.InsertResponse, error) {
		switch call {
		case 1:
			return nil, contentErr
		case 2:
			return nil, throttleErr
		default:
			return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
		}
	}
	sender := newTestClickHouseSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		clickHouseSpoolRecord(t, "request-1", "first", "root-token"),
		clickHouseSpoolRecord(t, "request-2", "second", "root-token"),
		clickHouseSpoolRecord(t, "request-3", "third", "root-token"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, now)
	assert.Equal(t, 30*time.Second, requireOutcomeForInputIndex(t, report, 0).RetryAfter)
	// 一件ずつの再送もbatch INSERTの継続なので、未送信のrecordもその送信時刻を残します。
	for _, inputIndex := range []int{1, 2} {
		assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusRetryable, now)
		deferred := requireOutcomeForInputIndex(t, report, inputIndex)
		assert.ErrorIs(t, deferred.Err, spool.ErrDeferredByRetryAfter)
		assert.ErrorIs(t, deferred.Err, contentErr)
	}
	assert.Equal(t, 2, client.calls)
}

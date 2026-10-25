package loki

import (
	"context"
	"net/http"
	"testing"
	"time"

	infraloki "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLokiSpoolSenderDefersRemainingRequestsAfterRetryAfter(t *testing.T) {
	for _, tt := range []struct {
		name       string
		retryAfter time.Duration
		wantPushes int
	}{
		{name: "retry-after", retryAfter: 30 * time.Second, wantPushes: 1},
		// Retry-Afterがなければ、従来どおり同じ周期で残りのrequestも送信します。
		{name: "without retry-after", wantPushes: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
			throttleErr := &pkgloki.HTTPStatusError{
				Code:               http.StatusTooManyRequests,
				Status:             "429 Too Many Requests",
				RetryAfterDuration: tt.retryAfter,
			}
			client := &recordingLokiSpoolClient{}
			client.pushResult = func(context.Context, []pkgloki.Stream) error {
				if client.pushCalls == 1 {
					return throttleErr
				}
				return nil
			}
			// 一件ずつ別requestへ計画し、先頭requestの応答で残りを止めるか確認します。
			sender := newTestLokiSpoolSenderWithLimits(t, now, client, infraloki.PushLimits{
				MaxLineBytes:        64,
				MaxPushRequestBytes: lokiPushRequestSizeForLines(t, "line 1"),
			})
			records := []spool.SpoolRecord{
				lokiSpoolRecord(t, "request-1", "line 1"),
				lokiSpoolRecord(t, "request-2", "line 2"),
				lokiSpoolRecord(t, "request-3", "line 3"),
			}

			report, err := sender.Send(context.Background(), records)

			require.NoError(t, err)
			require.NoError(t, report.Validate(len(records)))
			throttled := assertOutcome(t, report, 0, spool.RecordOutcomeStatusRetryable, now)
			assert.ErrorIs(t, throttled.Err, throttleErr)
			assert.Equal(t, tt.retryAfter, throttled.RetryAfter)
			require.Equal(t, tt.wantPushes, client.pushCalls)
			if tt.retryAfter == 0 {
				assertOutcome(t, report, 1, spool.RecordOutcomeStatusSuccess, now)
				assertOutcome(t, report, 2, spool.RecordOutcomeStatusSuccess, now)
				return
			}
			for _, inputIndex := range []int{1, 2} {
				deferred := assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusRetryable, time.Time{})
				assert.ErrorIs(t, deferred.Err, spool.ErrDeferredByRetryAfter)
				assert.ErrorIs(t, deferred.Err, throttleErr)
				assert.Zero(t, deferred.RetryAfter)
			}
		})
	}
}

func TestLokiSpoolSenderDefersSplitHalfAfterRetryAfter(t *testing.T) {
	now := time.Date(2026, 6, 23, 13, 0, 0, 0, time.UTC)
	throttleErr := &pkgloki.HTTPStatusError{
		Code:               http.StatusServiceUnavailable,
		Status:             "503 Service Unavailable",
		RetryAfterDuration: 30 * time.Second,
	}
	client := &recordingLokiSpoolClient{}
	client.pushResult = func(context.Context, []pkgloki.Stream) error {
		switch client.pushCalls {
		case 1:
			return &pkgloki.HTTPStatusError{Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}
		case 2:
			return throttleErr
		default:
			return nil
		}
	}
	sender := newTestLokiSpoolSender(t, now, client)
	records := []spool.SpoolRecord{
		lokiSpoolRecord(t, "request-1", "line 1"),
		lokiSpoolRecord(t, "request-2", "line 2"),
		lokiSpoolRecord(t, "request-3", "line 3"),
		lokiSpoolRecord(t, "request-4", "line 4"),
	}

	report, err := sender.Send(context.Background(), records)

	require.NoError(t, err)
	require.NoError(t, report.Validate(len(records)))
	for _, inputIndex := range []int{0, 1} {
		throttled := assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusRetryable, now)
		assert.Equal(t, 30*time.Second, throttled.RetryAfter)
	}
	// 分けた後半は未送信のまま、初回requestの送信時刻を残して再送対象にします。
	for _, inputIndex := range []int{2, 3} {
		deferred := assertOutcome(t, report, inputIndex, spool.RecordOutcomeStatusRetryable, now)
		assert.ErrorIs(t, deferred.Err, spool.ErrDeferredByRetryAfter)
	}
	assert.Equal(t, [][]string{
		{"line 1", "line 2", "line 3", "line 4"},
		{"line 1", "line 2"},
	}, lokiPushedLines(client.pushes))
}

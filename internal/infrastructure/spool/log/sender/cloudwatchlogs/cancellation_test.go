package cloudwatchlogs

import (
	"context"
	"errors"
	"testing"
	"time"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestCanceledPutPreservesKnownOutcomeAndClientTimeoutPolicy(t *testing.T) {
	for _, tt := range []struct {
		name   string
		err    error
		cancel bool
		want   spool.RecordOutcomeStatus
	}{
		{"success after cancellation", nil, true, spool.RecordOutcomeStatusSuccess},
		{"canceled in flight", context.Canceled, true, spool.RecordOutcomeStatusInvalid},
		{"deadline in flight", context.DeadlineExceeded, true, spool.RecordOutcomeStatusInvalid},
		{"unknown after cancellation", errors.New("unknown response"), true, spool.RecordOutcomeStatusInvalid},
		{"permanent after cancellation", &pkgcloudwatchlogs.HTTPStatusError{Code: 400}, true, spool.RecordOutcomeStatusPermanent},
		{"throttled after cancellation", &pkgcloudwatchlogs.HTTPStatusError{Code: 429}, true, spool.RecordOutcomeStatusRetryable},
		{"client timeout with live parent", context.DeadlineExceeded, false, spool.RecordOutcomeStatusRetryable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &recordingCloudWatchLogsSpoolSender{err: tt.err, onSend: func() {
				if tt.cancel {
					cancel()
				}
			}}
			now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
			sender := newTestCloudWatchLogsSpoolSender(t, now, client)
			report, err := sender.Send(ctx, []spool.SpoolRecord{cloudWatchLogsSpoolRecord(t, "one", "original payload")})
			if tt.cancel && tt.err != nil {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, report.Validate(1))
			assertOutcome(t, report, 0, tt.want, now)
			outcome := requireOutcomeForInputIndex(t, report, 0)
			if tt.err != nil {
				require.ErrorIs(t, outcome.Err, tt.err)
			}
			require.Equal(t, 1, client.sendCalls)
		})
	}
}

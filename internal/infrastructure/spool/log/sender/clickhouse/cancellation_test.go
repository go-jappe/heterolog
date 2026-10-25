package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestCanceledInsertPreservesKnownOutcomeAndClientTimeoutPolicy(t *testing.T) {
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
		{"permanent after cancellation", &pkgclickhouse.HTTPStatusError{Code: 400}, true, spool.RecordOutcomeStatusPermanent},
		{"throttled after cancellation", &pkgclickhouse.HTTPStatusError{Code: 429}, true, spool.RecordOutcomeStatusRetryable},
		{"client timeout with live parent", context.DeadlineExceeded, false, spool.RecordOutcomeStatusRetryable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &recordingClickHouseSpoolClient{onInsert: func(int) (*pkgclickhouse.InsertResponse, error) {
				if tt.cancel {
					cancel()
				}
				return &pkgclickhouse.InsertResponse{StatusCode: 200}, tt.err
			}}
			now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
			sender := newTestClickHouseSpoolSender(t, now, client)
			report, err := sender.Send(ctx, []spool.SpoolRecord{clickHouseSpoolRecord(t, "one", "original payload", "stable-token")})
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
			require.Equal(t, 1, client.calls)
		})
	}
}

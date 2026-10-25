package opensearch

import (
	"context"
	"errors"
	"testing"
	"time"

	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestCanceledBulkPreservesKnownOutcomeAndClientTimeoutPolicy(t *testing.T) {
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
		{"permanent after cancellation", &pkgopensearch.HTTPStatusError{Code: 400}, true, spool.RecordOutcomeStatusPermanent},
		{"throttled after cancellation", &pkgopensearch.HTTPStatusError{Code: 429}, true, spool.RecordOutcomeStatusRetryable},
		{"client timeout with live parent", context.DeadlineExceeded, false, spool.RecordOutcomeStatusRetryable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &fakeOpenSearchClient{indexDocumentsErr: tt.err, indexDocumentsResponse: successBulkResponse("logs", "stable-id"), onIndexDocuments: func(context.Context, []pkgopensearch.DocumentWriteRequest) {
				if tt.cancel {
					cancel()
				}
			}}
			now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
			sender := newTestSender(now, client)
			report, err := sender.Send(ctx, []spool.SpoolRecord{validSpoolRecord(t, "one", "logs", "stable-id")})
			if tt.cancel && tt.err != nil {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, report.Validate(1))
			outcome := assertOutcome(t, report, 0, tt.want, now)
			if tt.err != nil {
				require.ErrorIs(t, outcome.Err, tt.err)
			}
			require.Equal(t, 1, client.indexDocumentsCallCount)
		})
	}
}

package opensearch

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	wrapper "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	backend "github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

type waitRetryConfig struct{ mode pkglogger.RetryClientMode }

func (c waitRetryConfig) MaxAttempts() int         { return 3 }
func (c waitRetryConfig) BaseDelay() time.Duration { return 100 * time.Millisecond }
func (c waitRetryConfig) MaxDelay() time.Duration  { return 100 * time.Millisecond }
func (c waitRetryConfig) RetryClientSelection() pkglogger.RetryClientSelection {
	return (pkglogger.LokiRetryableClientCaseTransportErrorConfig{RetryClientMode: c.mode}).RetryClientSelection()
}

// Use the real retry chain and sender so a wait cancellation must preserve both
// request acceptance evidence and the durable disposition of the original record.
func TestRetryWaitCancellationPreservesSpoolOutcome(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{pkglogger.RetryClientModeRetry, pkglogger.RetryClientModeRetryPolicy} {
		for _, scenario := range []string{"known refusal during wait", "known refusal at cancellation", "EOF during wait", "in-flight cancellation", "invalid response", "before dispatch"} {
			t.Run(string(mode)+"/"+scenario, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					statusErr := &backend.HTTPStatusError{Code: 503}
					unknownErr := &backend.ResponseDecodeError{Err: io.ErrUnexpectedEOF}
					var lastErr error
					attempt := func() error {
						switch scenario {
						case "known refusal during wait", "EOF during wait":
							go func() { time.Sleep(10 * time.Millisecond); cancel() }()
							lastErr = statusErr
							if scenario == "EOF during wait" {
								lastErr = io.EOF
							}
						case "known refusal at cancellation":
							cancel()
							lastErr = statusErr
						case "invalid response":
							cancel()
							lastErr = unknownErr
						default:
							cancel()
							lastErr = ctx.Err()
						}
						return lastErr
					}
					raw := &fakeOpenSearchClient{indexDocuments: func(_ context.Context, docs []backend.DocumentWriteRequest) (*backend.BulkResponse, error) {
						require.Len(t, docs, 1)
						require.Equal(t, "keep record", docs[0].Doc["request_id"])
						return nil, attempt()
					}}
					client, err := wrapper.BuildClientChain(raw, waitRetryConfig{mode: mode}, nil, nil)
					require.NoError(t, err)
					sender, err := NewOpensearchSpoolSender(fixedClock{now: time.Now()}, client, 1<<20)
					require.NoError(t, err)
					record := validSpoolRecord(t, "keep record", "logs", "document-1")
					beforePayload := append([]byte(nil), record.Payload...)
					if scenario == "before dispatch" {
						cancel()
					}
					report, err := sender.Send(ctx, []spool.SpoolRecord{record})
					require.ErrorIs(t, err, context.Canceled)
					require.NoError(t, report.Validate(1))
					require.Len(t, report, 1)
					require.Equal(t, 0, report[0].InputIndex)
					require.Equal(t, beforePayload, []byte(record.Payload), "original recovery record must not be rewritten")
					want := spool.RecordOutcomeStatusInvalid
					calls := 1
					if scenario == "before dispatch" {
						want = spool.RecordOutcomeStatusRetryable
						calls = 0
						require.True(t, report[0].AttemptedAt.IsZero())
					} else {
						require.False(t, report[0].AttemptedAt.IsZero())
						require.ErrorIs(t, report[0].Err, lastErr)
					}
					if scenario == "known refusal during wait" || scenario == "known refusal at cancellation" {
						want = spool.RecordOutcomeStatusRetryable
						var got *backend.HTTPStatusError
						require.True(t, errors.As(report[0].Err, &got))
						require.Same(t, statusErr, got)
					}
					require.Equal(t, calls, raw.indexDocumentsCallCount, "cancellation must not start another attempt")
					require.Equal(t, want, report[0].Status)
					require.ErrorIs(t, report[0].Err, context.Canceled)

				})
			})
		}
	}
}

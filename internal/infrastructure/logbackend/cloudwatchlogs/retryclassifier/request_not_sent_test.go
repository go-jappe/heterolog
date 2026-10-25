package retryclassifier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/stretchr/testify/require"
)

func TestCredentialFailureRetainsNotSentEvidenceAndStopsImmediateRetry(t *testing.T) {
	putErr := &cw.HTTPStatusError{Code: 400, AwsErrorType: cw.ErrorTypeResourceNotFoundException}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, io.EOF, errors.New("credentials unavailable")} {
		for _, prior := range []error{nil, putErr} {
			t.Run(fmt.Sprintf("%v/prior=%t", cause, prior != nil), func(t *testing.T) {
				unsent := &cw.RequestNotSentError{Operation: "PutLogEvents", Err: cause, PreviousPutError: prior}
				err := fmt.Errorf("client wrapper: %w", unsent)
				wantReason, wantCode := retryable.RetryReasonRequestNotSent, 0
				if prior != nil {
					wantReason, wantCode = retryable.RetryReasonServiceTransient, 400
				}
				for _, stopped := range []bool{false, true} {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if stopped {
						cancel()
					}
					if stopped {
						require.False(t, ClassifyRetry(ctx, err).Retry)
					} else {
						require.Equal(t, ClassifyRetry(ctx, cause), ClassifyRetry(ctx, err), "credential cause controls immediate retry, not the earlier missing stream")
					}
					decision := ClassifyDurableRequestOutcome(ctx, err)
					require.True(t, decision.Retry)
					require.False(t, decision.Quarantine)
					require.Equal(t, wantReason, decision.Reason)
					require.Equal(t, wantCode, decision.Code)
					if stopped {
						wait := retryable.InterruptedWait(err, ctx.Err())
						require.Equal(t, decision, ClassifyDurableRequestOutcome(ctx, wait))
						require.ErrorIs(t, wait, cause)
						require.ErrorIs(t, wait, context.Canceled)
					}
				}
			})
		}
	}
}

func TestNotSentEvidenceDoesNotOverrideAnUncertainLogAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	unsent := &cw.RequestNotSentError{Operation: "PutLogEvents", Err: context.Canceled}
	unknown := &cw.ResponseDecodeError{Operation: "PutLogEvents", Err: io.ErrUnexpectedEOF}
	for _, err := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		errors.Join(unsent, unknown),
		errors.Join(unknown, unsent),
		fmt.Errorf("wrapped: %w", errors.Join(unsent, context.Canceled)),
		&cw.ResponseDecodeError{Operation: "PutLogEvents", Err: unsent},
		retryable.InterruptedWait(errors.Join(unsent, unknown), context.Canceled),
		errors.Join(retryable.InterruptedWait(unsent, context.Canceled), unknown),
	} {
		decision := ClassifyDurableRequestOutcome(ctx, err)
		require.True(t, decision.Quarantine, "%v", err)
		require.False(t, decision.Retry, "%v", err)
	}
}

func TestCredentialFailureKeepsPriorResponseEvidence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	unsent := &cw.RequestNotSentError{Operation: "PutLogEvents", Err: context.Canceled}
	for _, prior := range []error{io.EOF, context.DeadlineExceeded, &cw.HTTPStatusError{Code: 503}} {
		err := PreservePriorAttemptOutcome(prior, unsent)
		require.ErrorIs(t, err, prior)
		require.ErrorIs(t, err, unsent)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, ClassifyRetry(ctx, err).Retry)
		for _, stopped := range []context.Context{context.Background(), ctx} {
			decision := ClassifyDurableRequestOutcome(stopped, fmt.Errorf("wrapper: %w", err))
			if _, known := prior.(*cw.HTTPStatusError); known {
				require.True(t, decision.Retry)
				require.False(t, decision.Quarantine)
				require.Equal(t, 503, decision.Code, "an earlier HTTP refusal cannot be reported as locally rejected")
			} else {
				require.False(t, decision.Retry)
				require.True(t, decision.Quarantine)
			}
		}
		joined := errors.Join(err, &cw.ResponseDecodeError{Err: io.ErrUnexpectedEOF})
		require.True(t, ClassifyDurableRequestOutcome(ctx, joined).Quarantine)
	}
	refused := &cw.HTTPStatusError{Code: 400, AwsErrorType: cw.ErrorTypeResourceNotFoundException}
	afterCreation := &cw.RequestNotSentError{Operation: "PutLogEvents", Err: context.Canceled, PreviousPutError: refused}
	err := PreservePriorAttemptOutcome(io.EOF, afterCreation)
	require.True(t, ClassifyDurableRequestOutcome(ctx, err).Quarantine)
	require.ErrorIs(t, err, refused)
}

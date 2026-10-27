package retryclassifier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	cw "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/stretchr/testify/require"
)

func TestAutoCreateFailureClassifiesCreationWithoutLosingLogRejection(t *testing.T) {
	putErr := &cw.HTTPStatusError{Code: 400, AwsErrorType: cw.ErrorTypeResourceNotFoundException}
	for _, tt := range []struct {
		name                         string
		cause                        error
		immediateRetry, durableRetry bool
		code                         int
	}{
		{"canceled creation", context.Canceled, false, true, 400},
		{"creation deadline", context.DeadlineExceeded, true, true, 400},
		{"creation EOF", io.EOF, true, true, 400},
		{"unknown creation result", errors.New("management operation failed"), false, true, 400},
		{"creation denied", &cw.HTTPStatusError{Code: 403}, false, false, 403},
		{"creation invalid", &cw.HTTPStatusError{Code: 400}, false, false, 400},
		{"creation transient", &cw.HTTPStatusError{Code: 503, RetryAfterDur: time.Second}, true, true, 503},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := fmt.Errorf("client wrapper: %w", &cw.AutoCreateLogStreamError{PutError: putErr, CreateError: tt.cause})
			require.Equal(t, tt.immediateRetry, ClassifyRetry(context.Background(), err).Retry)
			for _, stopped := range []bool{false, true} {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if stopped {
					cancel()
				}
				if stopped {
					require.False(t, ClassifyRetry(ctx, err).Retry, "never retry immediately with a stopped caller")
				}
				decision := ClassifyDurableRequestOutcome(ctx, err)
				require.Equal(t, tt.durableRetry, decision.Retry)
				require.False(t, decision.Quarantine, "the first log request was explicitly rejected")
				require.Equal(t, tt.code, decision.Code)
				if tt.code == 503 {
					require.Equal(t, time.Second, decision.RetryAfter)
				}
			}
		})
	}
	// 一般的な送信先の欠落には、引き続き恒久失敗として扱う既存のpolicyを使います。
	missing := ClassifyDurableRequestOutcome(context.Background(), putErr)
	require.False(t, missing.Retry)
	require.False(t, missing.Quarantine)
	require.Equal(t, 400, missing.Code)
}

func TestAutoCreateEvidenceDoesNotOverrideAnotherUnknownLogAttempt(t *testing.T) {
	putErr := &cw.HTTPStatusError{Code: 400, AwsErrorType: cw.ErrorTypeResourceNotFoundException}
	creationErr := &cw.AutoCreateLogStreamError{PutError: putErr, CreateError: context.Canceled}
	unknown := &cw.ResponseDecodeError{Operation: "PutLogEvents", Err: io.ErrUnexpectedEOF}
	for _, err := range []error{errors.Join(creationErr, unknown), errors.Join(unknown, creationErr), fmt.Errorf("wrapper: %w", errors.Join(creationErr, context.Canceled))} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		decision := ClassifyDurableRequestOutcome(ctx, err)
		require.True(t, decision.Quarantine)
		require.False(t, decision.Retry)
	}
	// 一時的な作成失敗の後でcancelされたretry待機にも、両方の操作がclientでwrapされて
	// いるにもかかわらず、受理されなかったことの確認済みの証拠があります。
	prior := &cw.AutoCreateLogStreamError{PutError: putErr, CreateError: &cw.HTTPStatusError{Code: 503}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	decision := ClassifyDurableRequestOutcome(ctx, retryable.InterruptedWait(prior, context.Canceled))
	require.True(t, decision.Retry)
	require.False(t, decision.Quarantine)
	require.Equal(t, 503, decision.Code)
}

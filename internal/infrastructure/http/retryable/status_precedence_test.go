package retryable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestClassifyRetryKeepsHTTPStatusAcrossErrorWrappers(t *testing.T) {
	for _, code := range []int{http.StatusBadRequest, http.StatusForbidden, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		for _, message := range []string{"connection refused", "unexpected EOF", "i/o timeout"} {
			statusErr := &statusPrecedenceError{code: code, message: message, after: 3 * time.Second}
			for name, err := range map[string]error{
				"direct": statusErr,
				"fmt":    fmt.Errorf("request failed: %w", statusErr),
				"url":    &url.Error{Op: "Post", URL: "https://example.test", Err: statusErr},
				// statusは、外側のwrapperや、joinされた別の枝にある場合があります。
				// url.Errorをunwrapしても、messageによる推測を再び有効にしてはいけません。
				"status wrapping url": &statusPrecedenceError{
					code: code, message: message, after: 3 * time.Second,
					cause: &url.Error{Op: "Post", URL: "https://example.test", Err: errors.New(message)},
				},
				"joined url": errors.Join(statusErr, &url.Error{Op: "Post", URL: "https://example.test", Err: errors.New(message)}),
			} {
				t.Run(fmt.Sprintf("%d/%s/%s", code, message, name), func(t *testing.T) {
					want := ClassifyRetryByStatus(code, statusErr.after)
					if got := ClassifyRetry(context.Background(), err); got != want {
						t.Fatalf("known HTTP status changed by error text: got=%+v want=%+v", got, want)
					}
				})
			}
		}
	}
}

func TestClassifyRetryHTTPStatusPreservesTypedErrorPrecedence(t *testing.T) {
	for _, tt := range []struct {
		name  string
		cause error
		want  RetryDecision
	}{
		{name: "canceled", cause: context.Canceled, want: RetryDecision{}},
		{name: "deadline", cause: context.DeadlineExceeded, want: RetryDecision{Retry: true, Reason: RetryReasonCtxDeadline}},
		{name: "network timeout", cause: &net.DNSError{IsTimeout: true}, want: RetryDecision{Retry: true, Reason: RetryReasonNetTimeout}},
		{name: "EOF", cause: io.EOF, want: RetryDecision{Retry: true, Reason: RetryReasonEOF}},
		{name: "unexpected EOF", cause: io.ErrUnexpectedEOF, want: RetryDecision{Retry: true, Reason: RetryReasonEOF}},
		{name: "circuit breaker", cause: NewCircuitBreakerOpenError("send", time.Time{}), want: RetryDecision{Reason: RetryReasonCircuitBreakerOpen}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := &statusPrecedenceError{code: http.StatusBadRequest, message: "connection refused", cause: tt.cause}
			if got := ClassifyRetry(context.Background(), err); got != tt.want {
				t.Fatalf("typed error precedence changed: got=%+v want=%+v", got, tt.want)
			}
		})
	}
	for name, ctx := range map[string]context.Context{"canceled": canceledContext(), "expired": expiredContext()} {
		t.Run(name, func(t *testing.T) {
			err := &statusPrecedenceError{code: http.StatusServiceUnavailable, message: "connection refused", cause: io.EOF}
			if got := ClassifyRetry(ctx, err); got.Retry || got.Quarantine || got.Code != 0 {
				t.Fatalf("stopped caller must override HTTP and transport errors: %+v", got)
			}
		})
	}
}

func TestClassifyRetryCancellationDoesNotFallThroughToHTTPStatus(t *testing.T) {
	statusErr := &statusPrecedenceError{code: http.StatusServiceUnavailable, message: "connection refused", cause: context.Canceled}
	for name, err := range map[string]error{
		"wrapped": fmt.Errorf("send: %w", statusErr),
		"url":     &url.Error{Op: "Post", URL: "https://example.test", Err: statusErr},
		"joined":  errors.Join(&statusPrecedenceError{code: http.StatusServiceUnavailable}, context.Canceled),
	} {
		t.Run(name, func(t *testing.T) {
			if got := ClassifyRetry(context.Background(), err); got != (RetryDecision{}) {
				t.Fatalf("cancellation must prevent retry despite HTTP 503: %+v", got)
			}
		})
	}
}

func TestClassifyRetryStillRecognizesDisconnectWithoutHTTPStatus(t *testing.T) {
	for _, message := range []string{"connection refused", "unexpected EOF", "i/o timeout"} {
		err := &url.Error{Op: "Post", URL: "https://example.test", Err: errors.New(message)}
		want := RetryDecision{Retry: true, Reason: RetryReasonDisconnect}
		if got := ClassifyRetry(context.Background(), err); got != want {
			t.Fatalf("transport fallback lost for %q: got=%+v want=%+v", message, got, want)
		}
	}
}

type statusPrecedenceError struct {
	code    int
	message string
	after   time.Duration
	cause   error
}

func (e *statusPrecedenceError) Error() string             { return e.message }
func (e *statusPrecedenceError) Unwrap() error             { return e.cause }
func (e *statusPrecedenceError) StatusCode() int           { return e.code }
func (e *statusPrecedenceError) RetryAfter() time.Duration { return e.after }

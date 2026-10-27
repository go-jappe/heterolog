package retryclassifier

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

func TestClassifyRetryHTTPStatusMatrix(t *testing.T) {
	t.Parallel()

	retryAfter := 3 * time.Second
	tests := []struct {
		name       string
		statusCode int
		wantRetry  bool
		wantReason retryable.RetryReason
	}{
		{name: "request timeout", statusCode: http.StatusRequestTimeout, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "too many requests", statusCode: http.StatusTooManyRequests, wantRetry: true, wantReason: retryable.RetryReasonHTTPThrottle},
		{name: "internal server error", statusCode: http.StatusInternalServerError, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "bad gateway", statusCode: http.StatusBadGateway, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "service unavailable", statusCode: http.StatusServiceUnavailable, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "gateway timeout", statusCode: http.StatusGatewayTimeout, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "bad request", statusCode: http.StatusBadRequest, wantRetry: false, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, wantRetry: false, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "forbidden", statusCode: http.StatusForbidden, wantRetry: false, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "not found", statusCode: http.StatusNotFound, wantRetry: false, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "request entity too large", statusCode: http.StatusRequestEntityTooLarge, wantRetry: false, wantReason: retryable.RetryReasonHTTPStatus},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			decision := ClassifyRetry(context.Background(), &pkgcloudwatchlogs.HTTPStatusError{
				Code:          tt.statusCode,
				Status:        http.StatusText(tt.statusCode),
				Body:          []byte("remote diagnostic: connection refused; unexpected EOF"),
				RetryAfterDur: retryAfter,
			})

			want := retryable.RetryDecision{Retry: tt.wantRetry, Reason: tt.wantReason, Code: tt.statusCode}
			if tt.wantRetry {
				want.RetryAfter = retryAfter
			}
			if decision != want {
				t.Fatalf("ClassifyRetry() = %+v, want %+v", decision, want)
			}
		})
	}
}

func TestClassifyRetryAWSErrorTypeMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		errorType  string
		statusCode int
		wantRetry  bool
		wantReason retryable.RetryReason
		wantClass  retryable.RetryPolicyClass
	}{
		{
			name:       "throttling exception",
			errorType:  "ThrottlingException",
			statusCode: http.StatusBadRequest,
			wantRetry:  true,
			wantReason: retryable.RetryReasonHTTPThrottle,
			wantClass:  retryable.RetryPolicyClassThrottled,
		},
		{
			name:       "too many requests exception",
			errorType:  "TooManyRequestsException",
			statusCode: http.StatusBadRequest,
			wantRetry:  true,
			wantReason: retryable.RetryReasonHTTPThrottle,
			wantClass:  retryable.RetryPolicyClassThrottled,
		},
		{
			// AWS error typeに名前空間が付いても、request頻度制限として扱います。
			name:       "throttling exception with namespace suffix",
			errorType:  "ThrottlingException:http://internal.example/",
			statusCode: http.StatusBadRequest,
			wantRetry:  true,
			wantReason: retryable.RetryReasonHTTPThrottle,
			wantClass:  retryable.RetryPolicyClassThrottled,
		},
		{
			// error typeはHTTP statusの分類より優先します。
			name:       "throttling exception over server error",
			errorType:  "ThrottlingException",
			statusCode: http.StatusInternalServerError,
			wantRetry:  true,
			wantReason: retryable.RetryReasonHTTPThrottle,
			wantClass:  retryable.RetryPolicyClassThrottled,
		},
		{
			name:       "request limit exceeded",
			errorType:  "RequestLimitExceeded",
			statusCode: http.StatusBadRequest,
			wantRetry:  true,
			wantReason: retryable.RetryReasonHTTPThrottle,
			wantClass:  retryable.RetryPolicyClassThrottled,
		},
		{
			name:       "request limit exceeded exception",
			errorType:  "RequestLimitExceededException",
			statusCode: http.StatusBadRequest,
			wantRetry:  true,
			wantReason: retryable.RetryReasonHTTPThrottle,
			wantClass:  retryable.RetryPolicyClassThrottled,
		},
		{
			name:       "operation aborted",
			errorType:  pkgcloudwatchlogs.ErrorTypeOperationAbortedException,
			statusCode: http.StatusBadRequest,
			wantRetry:  true,
			wantReason: retryable.RetryReasonServiceTransient,
			wantClass:  retryable.RetryPolicyClassTransient,
		},
		{
			name:       "request aborted",
			errorType:  pkgcloudwatchlogs.ErrorTypeRequestAbortedException,
			statusCode: http.StatusBadRequest,
			wantRetry:  true,
			wantReason: retryable.RetryReasonDisconnect,
			wantClass:  retryable.RetryPolicyClassSingleProbe,
		},
		{
			name:       "resource limit exceeded is not request throttling",
			errorType:  "LimitExceededException",
			statusCode: http.StatusBadRequest,
			wantRetry:  false,
			wantReason: retryable.RetryReasonHTTPStatus,
			wantClass:  retryable.RetryPolicyClassNoRetry,
		},
		{
			name:       "invalid parameter",
			errorType:  "InvalidParameterException",
			statusCode: http.StatusBadRequest,
			wantRetry:  false,
			wantReason: retryable.RetryReasonHTTPStatus,
			wantClass:  retryable.RetryPolicyClassNoRetry,
		},
		{
			name:       "resource not found",
			errorType:  pkgcloudwatchlogs.ErrorTypeResourceNotFoundException,
			statusCode: http.StatusBadRequest,
			wantRetry:  false,
			wantReason: retryable.RetryReasonHTTPStatus,
			wantClass:  retryable.RetryPolicyClassNoRetry,
		},
		{
			name:       "unrecognized client",
			errorType:  "UnrecognizedClientException",
			statusCode: http.StatusBadRequest,
			wantRetry:  false,
			wantReason: retryable.RetryReasonHTTPStatus,
			wantClass:  retryable.RetryPolicyClassNoRetry,
		},
		{
			name:       "access denied",
			errorType:  "AccessDeniedException",
			statusCode: http.StatusForbidden,
			wantRetry:  false,
			wantReason: retryable.RetryReasonHTTPStatus,
			wantClass:  retryable.RetryPolicyClassNoRetry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			statusErr := &pkgcloudwatchlogs.HTTPStatusError{
				Code:         tt.statusCode,
				Status:       http.StatusText(tt.statusCode),
				AwsErrorType: tt.errorType,
				AwsMessage:   "remote diagnostic: connection refused; unexpected EOF",
			}
			decision := ClassifyRetry(context.Background(), fmt.Errorf("send cloudwatch logs request: %w", statusErr))

			want := retryable.RetryDecision{Retry: tt.wantRetry, Reason: tt.wantReason, Code: tt.statusCode}
			if decision != want {
				t.Fatalf("ClassifyRetry() = %+v, want %+v", decision, want)
			}
			if class := retryable.PolicyClass(decision); class != tt.wantClass {
				t.Fatalf("PolicyClass = %q, want %q", class, tt.wantClass)
			}
		})
	}
}

// clientはredirectを追従せず3xxをHTTPStatusErrorとして返します。redirectは宛先・設定の
// 誤りで、同じ要求を再送しても配送できないため、即時retryもquarantineもせず、配送不能が
// 確定した失敗として扱います。
func TestRedirectStatusesArePermanentFailures(t *testing.T) {
	for _, status := range []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		err := &pkgcloudwatchlogs.HTTPStatusError{Code: status, Status: http.StatusText(status), Body: []byte("redirected by gateway")}
		want := retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: status}
		if got := ClassifyRetry(context.Background(), err); got != want {
			t.Fatalf("ClassifyRetry(%d) = %+v, want %+v", status, got, want)
		}
		if got := ClassifyDurableRequestFailure(context.Background(), err); got != want {
			t.Fatalf("ClassifyDurableRequestFailure(%d) = %+v, want %+v", status, got, want)
		}
	}
}

func TestClassifyDurableRequestFailureKeepsKnownInvalidParameter(t *testing.T) {
	for _, message := range []string{"invalid input", "unexpected EOF", "connection refused"} {
		err := fmt.Errorf("send failed: %w", &pkgcloudwatchlogs.HTTPStatusError{
			Code: http.StatusBadRequest, AwsErrorType: "InvalidParameterException", AwsMessage: message,
		})
		got := ClassifyDurableRequestFailure(context.Background(), err)
		want := retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusBadRequest}
		if got != want {
			t.Fatalf("known invalid input was reclassified by %q: got=%+v want=%+v", message, got, want)
		}
	}
}

// 呼び出し元が停止した後は、retryableなerror typeでも即時retryしません。
func TestClassifyRetryDoesNotRetryAfterParentContextEnds(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancelExpired()

	for _, ctx := range []context.Context{canceled, expired} {
		for _, err := range []error{
			&pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusBadRequest, AwsErrorType: pkgcloudwatchlogs.ErrorTypeRequestAbortedException},
			&pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusBadRequest, AwsErrorType: "ThrottlingException"},
			&pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusTooManyRequests, AwsErrorType: "ThrottlingException"},
		} {
			if decision := ClassifyRetry(ctx, err); decision != (retryable.RetryDecision{}) {
				t.Fatalf("ClassifyRetry(%v) after ctx err %v = %+v, want no retry decision", err, ctx.Err(), decision)
			}
		}
	}
}

// Circuit Breaker openは今回のattemptが抑止されたことを表すため将来再送の対象とし、
// 分類できないerrorは結果を確認するまでquarantineへ隔離します。
func TestClassifyDurableRequestFailureRetriesCircuitBreakerOpenAndQuarantinesUnknownError(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		err  error
		want retryable.RetryDecision
	}{
		{
			name: "circuit breaker open",
			err:  retryable.NewCircuitBreakerOpenError("cloudwatch logs send", time.Unix(1_700_000_000, 0)),
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonCircuitBreakerOpen},
		},
		{
			name: "unclassified error",
			err:  errors.New("unclassified result"),
			want: retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyDurableRequestFailure(context.Background(), tt.err); got != tt.want {
				t.Fatalf("ClassifyDurableRequestFailure() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestClassifyDurableRequestFailureQuarantinesSingleRequestRejectedAsTooLarge(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("put log events: %w", &pkgcloudwatchlogs.HTTPStatusError{
		Code: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large",
	})
	got := ClassifyDurableRequestFailure(context.Background(), err)
	// 分割できない一件の413は、上限設定の変更で配送できる可能性があるため配送不能にしません。
	want := retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonRequestTooLarge, Code: http.StatusRequestEntityTooLarge}
	if got != want {
		t.Fatalf("ClassifyDurableRequestFailure() = %+v, want %+v", got, want)
	}
	if !IsRequestEntityTooLarge(err) {
		t.Fatal("IsRequestEntityTooLarge() = false for wrapped HTTP 413")
	}
	for _, other := range []error{
		nil,
		&pkgcloudwatchlogs.HTTPStatusError{Code: http.StatusBadRequest},
		fmt.Errorf("joined: %w", errors.Join(err, context.Canceled)),
	} {
		if IsRequestEntityTooLarge(other) {
			t.Fatalf("IsRequestEntityTooLarge(%v) = true, want false", other)
		}
	}
}

func TestClassifyDurableRequestFailureKeepsExplicitResponseAfterCallerStops(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancelExpired()

	for _, ctx := range []context.Context{canceled, expired} {
		// 受け取った応答の分類は、呼び出し元が停止した後も維持します。
		throttled := ClassifyDurableRequestFailure(ctx, &pkgcloudwatchlogs.HTTPStatusError{
			Code: http.StatusBadRequest, AwsErrorType: "ThrottlingException",
		})
		if want := (retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPThrottle, Code: http.StatusBadRequest}); throttled != want {
			t.Fatalf("throttled after ctx err %v = %+v, want retry %+v", ctx.Err(), throttled, want)
		}
		invalid := ClassifyDurableRequestFailure(ctx, &pkgcloudwatchlogs.HTTPStatusError{
			Code: http.StatusBadRequest, AwsErrorType: "InvalidParameterException",
		})
		if invalid != (retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusBadRequest}) {
			t.Fatalf("invalid parameter after ctx err %v = %+v, want dead", ctx.Err(), invalid)
		}
		// 応答を得ないまま停止したrequestは、結果不明としてquarantineへ隔離します。
		inFlight := ClassifyDurableRequestFailure(ctx, fmt.Errorf("put: %w", ctx.Err()))
		if want := (retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome}); inFlight != want {
			t.Fatalf("in-flight stop after ctx err %v = %+v, want quarantine %+v", ctx.Err(), inFlight, want)
		}
	}
}

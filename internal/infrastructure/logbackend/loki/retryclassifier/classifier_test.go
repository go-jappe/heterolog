package retryclassifier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestClassifyRetryByHTTPStatus(t *testing.T) {
	retryAfter := 3 * time.Second
	tests := []struct {
		name       string
		statusCode int
		retryAfter time.Duration
		wantRetry  bool
		wantReason retryable.RetryReason
	}{
		{name: "request timeout", statusCode: http.StatusRequestTimeout, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "throttled", statusCode: http.StatusTooManyRequests, retryAfter: retryAfter, wantRetry: true, wantReason: retryable.RetryReasonHTTPThrottle},
		{name: "internal error", statusCode: http.StatusInternalServerError, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "bad gateway", statusCode: http.StatusBadGateway, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "service unavailable", statusCode: http.StatusServiceUnavailable, retryAfter: retryAfter, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "gateway timeout", statusCode: http.StatusGatewayTimeout, wantRetry: true, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "bad request", statusCode: http.StatusBadRequest, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "forbidden", statusCode: http.StatusForbidden, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "not found", statusCode: http.StatusNotFound, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "request too large", statusCode: http.StatusRequestEntityTooLarge, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "ingestion blocked is not immediate retry", statusCode: 260, wantReason: retryable.RetryReasonHTTPStatus},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fmt.Errorf("wrapped Loki error: %w", &pkgloki.HTTPStatusError{
				Code:               tt.statusCode,
				Body:               []byte("remote diagnostic: connection refused; unexpected EOF"),
				RetryAfterDuration: tt.retryAfter,
			})

			decision := ClassifyRetry(context.Background(), err)

			want := retryable.RetryDecision{Retry: tt.wantRetry, Reason: tt.wantReason, Code: tt.statusCode, RetryAfter: tt.retryAfter}
			if decision != want {
				t.Fatalf("ClassifyRetry() = %+v, want %+v", decision, want)
			}
		})
	}
}

func TestClassifyDurableRequestFailurePreservesRetryAfterForIngestionBlocked(t *testing.T) {
	wantRetryAfter := 5 * time.Second
	decision := ClassifyDurableRequestFailure(context.Background(), &pkgloki.HTTPStatusError{
		Code:               260,
		RetryAfterDuration: wantRetryAfter,
	})

	want := retryable.RetryDecision{Retry: true, Reason: RetryReasonIngestionBlocked, Code: 260, RetryAfter: wantRetryAfter}
	if decision != want {
		t.Fatalf("decision = %+v, want future durable retry %+v", decision, want)
	}
}

func TestClassifyDurableRequestFailureSeparatesRejectedPushFromDestinationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		statusCode     int
		wantQuarantine bool
		wantReason     retryable.RetryReason
	}{
		// 入力不備または一部受理の可能性がある拒否は、内容を確認するまで隔離します。
		{name: "bad request", statusCode: http.StatusBadRequest, wantQuarantine: true, wantReason: RetryReasonPushRejected},
		{name: "request too large", statusCode: http.StatusRequestEntityTooLarge, wantQuarantine: true, wantReason: retryable.RetryReasonRequestTooLarge},
		// record内容に依存しない宛先・設定の拒否は配送不能のままです。
		{name: "unprocessable entity", statusCode: http.StatusUnprocessableEntity, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "forbidden", statusCode: http.StatusForbidden, wantReason: retryable.RetryReasonHTTPStatus},
		{name: "not found", statusCode: http.StatusNotFound, wantReason: retryable.RetryReasonHTTPStatus},
	}

	for _, tt := range tests {
		testCase := tt
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			decision := ClassifyDurableRequestFailure(
				context.Background(),
				&pkgloki.HTTPStatusError{Code: testCase.statusCode, Body: []byte("request rejected: unexpected EOF")},
			)

			want := retryable.RetryDecision{Quarantine: testCase.wantQuarantine, Reason: testCase.wantReason, Code: testCase.statusCode}
			if decision != want {
				t.Fatalf("decision = %+v, want %+v", decision, want)
			}
		})
	}
}

// Circuit Breaker openは今回のattemptが抑止されたことを表すため将来再送の対象とし、
// 分類できないerrorは結果を確認するまでquarantineへ隔離する。応答を得られなかったtransport
// errorや再試行可能なHTTP statusは、即時retryの分類のまま将来再送の対象とする。
func TestClassifyDurableRequestFailureKeepsReplayableFailuresAndQuarantinesUnknownOutcome(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		err  error
		want retryable.RetryDecision
	}{
		{
			name: "circuit breaker open",
			err:  retryable.NewCircuitBreakerOpenError("loki push", time.Unix(1_700_000_000, 0)),
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonCircuitBreakerOpen},
		},
		{
			name: "ingestion blocked without retry-after",
			err:  &pkgloki.HTTPStatusError{Code: 260, Status: "260 Ingestion Blocked"},
			want: retryable.RetryDecision{Retry: true, Reason: RetryReasonIngestionBlocked, Code: 260},
		},
		{
			name: "service unavailable",
			err:  &pkgloki.HTTPStatusError{Code: http.StatusServiceUnavailable, Status: "503 Service Unavailable"},
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable},
		},
		{
			name: "transport eof",
			err:  io.ErrUnexpectedEOF,
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonEOF},
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

func TestClassifyDurableRequestFailureKeepsExplicitResponseAfterCallerStops(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()

	for _, ctx := range []context.Context{canceled, expired} {
		// 受け取った応答の分類は、呼び出し元が停止した後も維持します。
		for _, tt := range []struct {
			name string
			err  error
			want retryable.RetryDecision
		}{
			{
				name: "ingestion blocked",
				err:  &pkgloki.HTTPStatusError{Code: 260, Status: "260 Ingestion Blocked", RetryAfterDuration: time.Second},
				want: retryable.RetryDecision{Retry: true, Reason: RetryReasonIngestionBlocked, Code: 260, RetryAfter: time.Second},
			},
			{
				name: "bad request",
				err:  &pkgloki.HTTPStatusError{Code: http.StatusBadRequest},
				want: retryable.RetryDecision{Quarantine: true, Reason: RetryReasonPushRejected, Code: http.StatusBadRequest},
			},
			{
				name: "unauthorized",
				err:  &pkgloki.HTTPStatusError{Code: http.StatusUnauthorized},
				want: retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusUnauthorized},
			},
			{
				// 応答を得ないまま呼び出し元が停止したpushは、結果不明としてquarantineへ隔離します。
				name: "in-flight stop",
				err:  fmt.Errorf("push: %w", ctx.Err()),
				want: retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome},
			},
		} {
			if got := ClassifyDurableRequestFailure(ctx, tt.err); got != tt.want {
				t.Fatalf("%s after ctx err %v = %+v, want %+v", tt.name, ctx.Err(), got, tt.want)
			}
		}
	}
}

func TestIsRequestEntityTooLargeRecognizesWrappedLokiStatus(t *testing.T) {
	t.Parallel()

	tooLarge := &pkgloki.HTTPStatusError{Code: http.StatusRequestEntityTooLarge}
	if !IsRequestEntityTooLarge(fmt.Errorf("push: %w", tooLarge)) {
		t.Fatal("wrapped HTTP 413 was not recognized")
	}
	for _, err := range []error{nil, errors.New("413"), &pkgloki.HTTPStatusError{Code: http.StatusBadRequest}} {
		if IsRequestEntityTooLarge(err) {
			t.Fatalf("IsRequestEntityTooLarge(%v) = true, want false", err)
		}
	}
}

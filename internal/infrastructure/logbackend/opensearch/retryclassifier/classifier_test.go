package retryclassifier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

// 成功HTTP応答のdecode失敗は結果不明のため即時retryせず、transport errorとHTTP statusは
// 共有classifierの分類を維持する。
func TestClassifyRetrySeparatesResponseDecodeFromSharedClassification(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		err  error
		want retryable.RetryDecision
	}{
		{
			name: "response decode EOF",
			err:  &pkgopensearch.ResponseDecodeError{Operation: "bulk", Err: io.ErrUnexpectedEOF},
			want: retryable.RetryDecision{Reason: FailureReasonResponseDecode},
		},
		{
			name: "missing response",
			err:  &pkgopensearch.ResponseDecodeError{Operation: "index", Err: ErrMissingResponse},
			want: retryable.RetryDecision{Reason: FailureReasonResponseDecode},
		},
		{
			name: "service unavailable",
			err:  &pkgopensearch.HTTPStatusError{Code: http.StatusServiceUnavailable, After: 2 * time.Second},
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable, RetryAfter: 2 * time.Second},
		},
		{
			name: "transport EOF",
			err:  io.ErrUnexpectedEOF,
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonEOF},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyRetry(context.Background(), tt.err); got != tt.want {
				t.Fatalf("ClassifyRetry() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// Circuit Breaker openは今回のattemptが抑止されたことを表すため将来再送の対象とし、
// 応答のdecode失敗や分類できないerrorは結果を確認するまでquarantineへ隔離する。
func TestClassifyDurableRequestFailureRetriesCircuitBreakerOpenAndQuarantinesUnknownOutcome(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		err  error
		want retryable.RetryDecision
	}{
		{
			name: "circuit breaker open",
			err:  retryable.NewCircuitBreakerOpenError("opensearch flush", time.Unix(1_700_000_000, 0)),
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonCircuitBreakerOpen},
		},
		{
			name: "response decode",
			err:  &pkgopensearch.ResponseDecodeError{Operation: "bulk", Err: io.ErrUnexpectedEOF},
			want: retryable.RetryDecision{Quarantine: true, Reason: FailureReasonResponseDecode},
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

func TestClassifyDurableRequestFailureSeparatesRecordScopedRejections(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name       string
		err        error
		want       retryable.RetryDecision
		wantScoped bool
	}{
		// 分割できない一件の413は、上限設定の変更で配送できる可能性があるため隔離します。
		{
			name:       "request too large",
			err:        openSearchStatusError(http.StatusRequestEntityTooLarge, ""),
			want:       retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonRequestTooLarge, Code: http.StatusRequestEntityTooLarge},
			wantScoped: true,
		},
		// document内容の不正は入力不備として隔離します。
		{
			name:       "mapper parsing",
			err:        openSearchStatusError(http.StatusBadRequest, "mapper_parsing_exception"),
			want:       retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonRecordRejected, Code: http.StatusBadRequest},
			wantScoped: true,
		},
		{
			name:       "strict mapping",
			err:        openSearchStatusError(http.StatusBadRequest, "strict_dynamic_mapping_exception"),
			want:       retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonRecordRejected, Code: http.StatusBadRequest},
			wantScoped: true,
		},
		// index・request設定の不備は配送不能のままです。
		{
			name: "illegal argument",
			err:  openSearchStatusError(http.StatusBadRequest, "illegal_argument_exception"),
			want: retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusBadRequest},
		},
		{
			name: "forbidden",
			err:  openSearchStatusError(http.StatusForbidden, ""),
			want: retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusForbidden},
		},
		{
			name: "unavailable",
			err:  openSearchStatusError(http.StatusServiceUnavailable, ""),
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable},
		},
		// 呼び出し元の停止と同時に返ったerrorは、どのrequestの結果か確定できません。
		{
			name: "joined with cancellation",
			err:  errors.Join(openSearchStatusError(http.StatusBadRequest, "mapper_parsing_exception"), context.Canceled),
			want: retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, ctx := range []context.Context{context.Background(), canceled} {
				// 明示的な応答の分類は、呼び出し元が停止した後も維持します。
				if decision := ClassifyDurableRequestFailure(ctx, tc.err); decision != tc.want {
					t.Fatalf("ctx err %v: decision = %+v, want %+v", ctx.Err(), decision, tc.want)
				}
			}
			if got := IsRecordScopedRequestRejection(tc.err); got != tc.wantScoped {
				t.Fatalf("IsRecordScopedRequestRejection() = %t, want %t", got, tc.wantScoped)
			}
		})
	}
}

func TestClassifyDurableRequestFailureQuarantinesInFlightStopAfterCallerStops(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancelExpired()

	for _, ctx := range []context.Context{canceled, expired} {
		// 応答を得ないまま停止したrequestは、結果不明としてquarantineへ隔離します。
		got := ClassifyDurableRequestFailure(ctx, fmt.Errorf("bulk: %w", ctx.Err()))
		if want := (retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome}); got != want {
			t.Fatalf("in-flight stop after ctx err %v = %+v, want %+v", ctx.Err(), got, want)
		}
	}
}

func TestIsRecordPayloadErrorType(t *testing.T) {
	t.Parallel()

	for _, errorType := range []pkgopensearch.ErrorType{
		pkgopensearch.ErrorTypeMapperParsing,
		pkgopensearch.ErrorTypeDocumentParsing,
		pkgopensearch.ErrorTypeXContentParse,
		pkgopensearch.ErrorTypeParse,
		pkgopensearch.ErrorTypeStrictDynamicMapping,
	} {
		if !IsRecordPayloadErrorType(errorType) {
			t.Fatalf("IsRecordPayloadErrorType(%q) = false, want true", errorType)
		}
	}
	// index・request設定の不正にも使われるerror typeは、document内容の不正とみなしません。
	for _, errorType := range []pkgopensearch.ErrorType{"illegal_argument_exception", "index_not_found_exception", ""} {
		if IsRecordPayloadErrorType(errorType) {
			t.Fatalf("IsRecordPayloadErrorType(%q) = true, want false", errorType)
		}
	}
}

func openSearchStatusError(code int, errorType string) error {
	body := []byte(`{"status":` + strconv.Itoa(code) + `}`)
	if errorType != "" {
		body = []byte(`{"error":{"type":"` + errorType + `","reason":"rejected"},"status":` + strconv.Itoa(code) + `}`)
	}
	return fmt.Errorf("index documents: %w", &pkgopensearch.HTTPStatusError{Code: code, Body: body})
}

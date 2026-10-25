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
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

func permanentHTTPStatus(code int) retryable.RetryDecision {
	return retryable.RetryDecision{Reason: retryable.RetryReasonHTTPStatus, Code: code}
}

// ClickHouseのerror typeを、共通のHTTP status・transport分類より優先して即時retryを判定する。
func TestClassifyRetryAppliesClickHouseErrorTypes(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want retryable.RetryDecision
	}{
		{
			name: "generic service unavailable",
			ctx:  context.Background(),
			err:  &pkgclickhouse.HTTPStatusError{Code: http.StatusServiceUnavailable, Body: []byte("server is overloaded")},
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable},
		},
		{
			name: "schema error in body",
			ctx:  context.Background(),
			err: &pkgclickhouse.HTTPStatusError{
				Code: http.StatusInternalServerError,
				Body: []byte("Code: 60. DB::Exception: Table default.logs does not exist. (UNKNOWN_TABLE) (version 26.5.3.52)"),
			},
			want: permanentHTTPStatus(http.StatusInternalServerError),
		},
		{
			name: "input parse error in body",
			ctx:  context.Background(),
			err:  &pkgclickhouse.HTTPStatusError{Code: http.StatusInternalServerError, Body: []byte("Code: 27. DB::Exception: Cannot parse input. (CANNOT_PARSE_NUMBER)")},
			want: permanentHTTPStatus(http.StatusInternalServerError),
		},
		{
			name: "setting constraint",
			ctx:  context.Background(),
			err: &pkgclickhouse.HTTPStatusError{
				Code: http.StatusBadRequest,
				Body: []byte("Code: 452. DB::Exception: Setting insert_deduplication_token violates constraint. (SETTING_CONSTRAINT_VIOLATION)"),
			},
			want: permanentHTTPStatus(http.StatusBadRequest),
		},
		{
			name: "unknown exception after HTTP 200",
			ctx:  context.Background(),
			err:  &pkgclickhouse.HTTPStatusError{Code: http.StatusOK, ExceptionCode: 999, ExceptionType: pkgclickhouse.ErrorType("SOME_TRANSIENT_EXCEPTION")},
			want: retryable.RetryDecision{Reason: retryable.RetryReasonUnknownOutcome, Code: http.StatusOK},
		},
		{
			name: "known input exception after HTTP 200",
			ctx:  context.Background(),
			err:  &pkgclickhouse.HTTPStatusError{Code: http.StatusOK, ExceptionCode: 27, ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID},
			want: permanentHTTPStatus(http.StatusOK),
		},
		{
			name: "wrapped permanent exception",
			ctx:  context.Background(),
			err: fmt.Errorf("insert failed: %w", &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusInternalServerError,
				ExceptionCode: 27,
				ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID,
			}),
			want: permanentHTTPStatus(http.StatusInternalServerError),
		},
		{
			name: "unknown server error",
			ctx:  context.Background(),
			err:  &pkgclickhouse.HTTPStatusError{Code: http.StatusInternalServerError, Body: []byte("Code: 999. DB::Exception: temporary backend failure. (SOME_UNKNOWN_ERROR)")},
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusInternalServerError},
		},
		{
			name: "transport",
			ctx:  context.Background(),
			err:  io.ErrUnexpectedEOF,
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonEOF},
		},
		{
			name: "missing response",
			ctx:  context.Background(),
			err:  ErrMissingInsertResponse,
			want: retryable.RetryDecision{Reason: retryable.RetryReasonUnknownOutcome},
		},
		{
			name: "canceled caller",
			ctx:  canceled,
			err:  &pkgclickhouse.HTTPStatusError{Code: http.StatusInternalServerError, Body: []byte("Code: 60. DB::Exception: (UNKNOWN_TABLE)")},
			want: retryable.RetryDecision{},
		},
		{
			name: "expired caller",
			ctx:  expired,
			err:  &pkgclickhouse.HTTPStatusError{Code: http.StatusOK, ExceptionCode: 999, ExceptionType: pkgclickhouse.ErrorType("SOME_TRANSIENT_EXCEPTION")},
			want: retryable.RetryDecision{Reason: retryable.RetryReasonCtxDeadline},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyRetry(tt.ctx, tt.err); got != tt.want {
				t.Fatalf("ClassifyRetry() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestClassifyRetryTreatsKnownJSONEachRowErrorsAsPermanent(t *testing.T) {
	t.Parallel()

	for _, errorType := range []pkgclickhouse.ErrorType{
		pkgclickhouse.ErrorTypeCannotParseText,
		pkgclickhouse.ErrorTypeIncorrectNumberOfColumns,
		pkgclickhouse.ErrorTypeSizesOfColumnsDoesntMatch,
		pkgclickhouse.ErrorTypeNotFoundColumnInBlock,
		pkgclickhouse.ErrorTypeDuplicateColumn,
		pkgclickhouse.ErrorTypeSizeOfFixedStringDoesntMatch,
		pkgclickhouse.ErrorTypeCannotParseEscapeSequence,
		pkgclickhouse.ErrorTypeIllegalColumn,
		pkgclickhouse.ErrorTypeFormatIsNotSuitableForInput,
		pkgclickhouse.ErrorTypeEmptyDataPassed,
		pkgclickhouse.ErrorTypeCannotInsertValueOfDifferentSizeIntoTuple,
		pkgclickhouse.ErrorTypeIncompatibleColumns,
		pkgclickhouse.ErrorTypeCannotReadArrayFromText,
		pkgclickhouse.ErrorTypeTooLargeStringSize,
		pkgclickhouse.ErrorTypeCannotParseUUID,
		pkgclickhouse.ErrorTypeBadQueryParameter,
		pkgclickhouse.ErrorTypeCannotParseBool,
		pkgclickhouse.ErrorTypeViolatedConstraint,
		pkgclickhouse.ErrorTypeInvalidSettingValue,
		pkgclickhouse.ErrorTypeCannotParseIPv4,
		pkgclickhouse.ErrorTypeCannotParseIPv6,
		pkgclickhouse.ErrorTypeUnknownElementOfEnum,
		pkgclickhouse.ErrorTypeIncompatibleSchema,
	} {
		t.Run(string(errorType), func(t *testing.T) {
			t.Parallel()
			got := ClassifyRetry(context.Background(), &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusInternalServerError,
				ExceptionType: errorType,
			})
			if want := permanentHTTPStatus(http.StatusInternalServerError); got != want {
				t.Fatalf("ClassifyRetry() = %+v, want %+v", got, want)
			}
		})
	}
}

// Circuit Breaker openは今回のattemptが抑止されたことを表すため将来再送の対象とし、
// 分類できないerrorは結果を確認するまでquarantineへ隔離する。
func TestClassifyDurableInsertFailureRetriesCircuitBreakerOpenAndQuarantinesUnknownError(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		err  error
		want retryable.RetryDecision
	}{
		{
			name: "circuit breaker open",
			err:  retryable.NewCircuitBreakerOpenError("clickhouse insert", time.Unix(1_700_000_000, 0)),
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
			for _, inputCount := range []int{1, 2} {
				if got := ClassifyDurableInsertFailure(context.Background(), tt.err, inputCount); got != tt.want {
					t.Fatalf("ClassifyDurableInsertFailure(inputCount=%d) = %+v, want %+v", inputCount, got, tt.want)
				}
			}
		})
	}
}

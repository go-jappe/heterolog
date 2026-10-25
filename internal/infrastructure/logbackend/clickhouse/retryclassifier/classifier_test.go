package retryclassifier

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

func TestClassifyDurableInsertFailureRequiresIsolationForKnownMultiRecordInputErrors(t *testing.T) {
	recordErrorTypes := []pkgclickhouse.ErrorType{
		pkgclickhouse.ErrorTypeCannotParseInputAssertionFailed,
		pkgclickhouse.ErrorTypeCannotParseQuotedString,
		pkgclickhouse.ErrorTypeCannotParseNumber,
		pkgclickhouse.ErrorTypeCannotParseDate,
		pkgclickhouse.ErrorTypeCannotParseDateTime,
		pkgclickhouse.ErrorTypeIncorrectData,
		pkgclickhouse.ErrorTypeCannotParseText,
		pkgclickhouse.ErrorTypeSizeOfFixedStringDoesntMatch,
		pkgclickhouse.ErrorTypeCannotParseEscapeSequence,
		pkgclickhouse.ErrorTypeCannotInsertValueOfDifferentSizeIntoTuple,
		pkgclickhouse.ErrorTypeCannotReadArrayFromText,
		pkgclickhouse.ErrorTypeTooLargeStringSize,
		pkgclickhouse.ErrorTypeCannotParseUUID,
		pkgclickhouse.ErrorTypeCannotParseBool,
		pkgclickhouse.ErrorTypeViolatedConstraint,
		pkgclickhouse.ErrorTypeCannotParseIPv4,
		pkgclickhouse.ErrorTypeCannotParseIPv6,
		pkgclickhouse.ErrorTypeUnknownElementOfEnum,
		// 入力値を列の型へ変換できないerrorもrecord内容に起因します。
		pkgclickhouse.ErrorTypeTypeMismatch,
		pkgclickhouse.ErrorTypeCannotConvertType,
	}

	for _, errorType := range recordErrorTypes {
		t.Run(string(errorType), func(t *testing.T) {
			const retryAfter = 3 * time.Second
			decision := ClassifyDurableInsertFailure(context.Background(), &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusInternalServerError,
				Status:        "500 Internal Server Error",
				ExceptionType: errorType,
				After:         retryAfter,
			}, 2)

			want := retryable.RetryDecision{
				Retry:      true,
				Reason:     RetryReasonRecordIsolationRequired,
				Code:       http.StatusInternalServerError,
				RetryAfter: retryAfter,
			}
			if decision != want {
				t.Fatalf("decision = %+v, want record isolation %+v", decision, want)
			}
		})
	}
}

func TestClassifyDurableInsertFailureKeepsRequestWideErrorsDead(t *testing.T) {
	requestWideErrorTypes := []pkgclickhouse.ErrorType{
		pkgclickhouse.ErrorTypeUnknownTable,
		pkgclickhouse.ErrorTypeUnknownDatabase,
		pkgclickhouse.ErrorTypeUnknownIdentifier,
		pkgclickhouse.ErrorTypeUnknownFunction,
		pkgclickhouse.ErrorTypeUnknownType,
		pkgclickhouse.ErrorTypeNoSuchColumnInTable,
		pkgclickhouse.ErrorTypeThereIsNoColumn,
		pkgclickhouse.ErrorTypeSyntaxError,
		pkgclickhouse.ErrorTypeIllegalTypeOfArgument,
		pkgclickhouse.ErrorTypeBadArguments,
		pkgclickhouse.ErrorTypeBadCast,
		pkgclickhouse.ErrorTypeUnknownFormat,
		pkgclickhouse.ErrorTypeBlocksHaveDifferentStructure,
		pkgclickhouse.ErrorTypeTooFewColumns,
		pkgclickhouse.ErrorTypeTooManyColumns,
		pkgclickhouse.ErrorTypeNumberOfColumnsDoesntMatch,
		pkgclickhouse.ErrorTypeUnknownSetting,
		pkgclickhouse.ErrorTypeReadonlySetting,
		pkgclickhouse.ErrorTypeSettingConstraintViolation,
		pkgclickhouse.ErrorTypeIncorrectNumberOfColumns,
		pkgclickhouse.ErrorTypeSizesOfColumnsDoesntMatch,
		pkgclickhouse.ErrorTypeNotFoundColumnInBlock,
		pkgclickhouse.ErrorTypeDuplicateColumn,
		pkgclickhouse.ErrorTypeIllegalColumn,
		pkgclickhouse.ErrorTypeFormatIsNotSuitableForInput,
		pkgclickhouse.ErrorTypeEmptyDataPassed,
		pkgclickhouse.ErrorTypeIncompatibleColumns,
		pkgclickhouse.ErrorTypeBadQueryParameter,
		pkgclickhouse.ErrorTypeInvalidSettingValue,
		pkgclickhouse.ErrorTypeIncompatibleSchema,
	}

	for _, errorType := range requestWideErrorTypes {
		t.Run(string(errorType), func(t *testing.T) {
			// record内容に依存しない宛先・schema・settingの不備は、件数にかかわらず配送不能です。
			for _, inputCount := range []int{1, 2} {
				decision := ClassifyDurableInsertFailure(context.Background(), &pkgclickhouse.HTTPStatusError{
					Code:          http.StatusInternalServerError,
					Status:        "500 Internal Server Error",
					ExceptionType: errorType,
				}, inputCount)

				if want := permanentHTTPStatus(http.StatusInternalServerError); decision != want {
					t.Fatalf("inputCount=%d decision = %+v, want dead %+v", inputCount, decision, want)
				}
			}
			immediate := ClassifyRetry(context.Background(), &pkgclickhouse.HTTPStatusError{Code: http.StatusInternalServerError, ExceptionType: errorType})
			if want := permanentHTTPStatus(http.StatusInternalServerError); immediate != want {
				t.Fatalf("ClassifyRetry() = %+v, want %+v", immediate, want)
			}
		})
	}
}

func TestClassifyDurableInsertFailureRequiresIsolationForMultiRecordRequestEntityTooLarge(t *testing.T) {
	decision := ClassifyDurableInsertFailure(context.Background(), &pkgclickhouse.HTTPStatusError{
		Code:   http.StatusRequestEntityTooLarge,
		Status: "413 Request Entity Too Large",
	}, 2)

	want := retryable.RetryDecision{Retry: true, Reason: RetryReasonRecordIsolationRequired, Code: http.StatusRequestEntityTooLarge}
	if decision != want {
		t.Fatalf("decision = %+v, want record isolation %+v", decision, want)
	}
}

func TestClassifyDurableInsertFailureRequiresIsolationForMultiRecordMarshalError(t *testing.T) {
	err := fmt.Errorf("build insert body: %w", &pkgclickhouse.LogRecordMarshalError{
		Index: 1,
		Err:   fmt.Errorf("unsupported field value"),
	})

	decision := ClassifyDurableInsertFailure(context.Background(), err, 2)

	// 送信前のerrorなので、HTTP statusとretry-afterは持ちません。
	if want := (retryable.RetryDecision{Retry: true, Reason: RetryReasonRecordIsolationRequired}); decision != want {
		t.Fatalf("decision = %+v, want record isolation %+v", decision, want)
	}
}

func TestClassifyDurableInsertFailureQuarantinesSingleRecordRejections(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		want     retryable.RetryReason
		wantCode int
	}{
		{
			name: "input value",
			err: &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusInternalServerError,
				Status:        "500 Internal Server Error",
				ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID,
			},
			want:     retryable.RetryReasonRecordRejected,
			wantCode: http.StatusInternalServerError,
		},
		{
			name: "type conversion",
			err: &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusBadRequest,
				Status:        "400 Bad Request",
				ExceptionType: pkgclickhouse.ErrorTypeTypeMismatch,
			},
			want:     retryable.RetryReasonRecordRejected,
			wantCode: http.StatusBadRequest,
		},
		{
			// 一件でも上限を超える場合は、上限設定の変更で配送できる可能性があるため配送不能にしません。
			name: "request entity too large",
			err: &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusRequestEntityTooLarge,
				Status: "413 Request Entity Too Large",
			},
			want:     retryable.RetryReasonRequestTooLarge,
			wantCode: http.StatusRequestEntityTooLarge,
		},
		{
			name: "record marshal",
			err: &pkgclickhouse.LogRecordMarshalError{
				Index: 0,
				Err:   fmt.Errorf("unsupported field value"),
			},
			want: retryable.RetryReasonRecordRejected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := ClassifyDurableInsertFailure(context.Background(), tt.err, 1)

			want := retryable.RetryDecision{Quarantine: true, Reason: tt.want, Code: tt.wantCode}
			if decision != want {
				t.Fatalf("decision = %+v, want a single-record rejection quarantined %+v", decision, want)
			}
		})
	}
}

func TestClassifyDurableInsertFailureReadsWrappedBodyError(t *testing.T) {
	err := fmt.Errorf("insert failed: %w", &pkgclickhouse.HTTPStatusError{
		Code:   http.StatusInternalServerError,
		Status: "500 Internal Server Error",
		Body:   []byte("Code: 72. DB::Exception: Cannot parse number. (CANNOT_PARSE_NUMBER)"),
	})

	decision := ClassifyDurableInsertFailure(context.Background(), err, 2)

	want := retryable.RetryDecision{Retry: true, Reason: RetryReasonRecordIsolationRequired, Code: http.StatusInternalServerError}
	if decision != want {
		t.Fatalf("expected wrapped body error to require isolation %+v, got %+v", want, decision)
	}
}

func TestClassifyDurableInsertFailurePreservesBaseRetryableClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want retryable.RetryDecision
	}{
		{
			name: "service unavailable",
			err: &pkgclickhouse.HTTPStatusError{
				Code:   http.StatusServiceUnavailable,
				Status: "503 Service Unavailable",
			},
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonHTTPStatus, Code: http.StatusServiceUnavailable},
		},
		{
			name: "transport eof",
			err:  io.ErrUnexpectedEOF,
			want: retryable.RetryDecision{Retry: true, Reason: retryable.RetryReasonEOF},
		},
		{
			name: "unknown exception after successful HTTP status",
			err: &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusOK,
				Status:        "200 OK",
				ExceptionCode: 999,
				ExceptionType: pkgclickhouse.ErrorType("SOME_TRANSIENT_EXCEPTION"),
			},
			want: retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome, Code: http.StatusOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := ClassifyDurableInsertFailure(context.Background(), tt.err, 2)

			if decision != tt.want {
				t.Fatalf("decision = %+v, want %+v", decision, tt.want)
			}
		})
	}
}

func TestClassifyDurableInsertFailureDoesNotPromoteGenericBadRequest(t *testing.T) {
	decision := ClassifyDurableInsertFailure(context.Background(), &pkgclickhouse.HTTPStatusError{
		Code:   http.StatusBadRequest,
		Status: "400 Bad Request",
	}, 2)

	if want := permanentHTTPStatus(http.StatusBadRequest); decision != want {
		t.Fatalf("generic HTTP 400 without a record error type = %+v, want dead %+v", decision, want)
	}
}

func TestClassifyDurableInsertFailureKeepsExplicitResponseAfterParentContextEnds(t *testing.T) {
	tests := []struct {
		name       string
		newContext func() (context.Context, context.CancelFunc)
	}{
		{
			name: "canceled",
			newContext: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
		},
		{
			name: "deadline exceeded",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.newContext()
			defer cancel()
			inputErr := &pkgclickhouse.HTTPStatusError{
				Code:          http.StatusInternalServerError,
				Status:        "500 Internal Server Error",
				ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID,
			}

			// 受け取ったrecord内容の拒否は、呼び出し元が停止した後も同じ分類です。
			isolation := retryable.RetryDecision{Retry: true, Reason: RetryReasonRecordIsolationRequired, Code: http.StatusInternalServerError}
			if decision := ClassifyDurableInsertFailure(ctx, inputErr, 2); decision != isolation {
				t.Fatalf("multi-record input error = %+v, want record isolation %+v", decision, isolation)
			}
			rejected := retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonRecordRejected, Code: http.StatusInternalServerError}
			if decision := ClassifyDurableInsertFailure(ctx, inputErr, 1); decision != rejected {
				t.Fatalf("single-record input error = %+v, want quarantine %+v", decision, rejected)
			}
			schemaErr := &pkgclickhouse.HTTPStatusError{Code: http.StatusNotFound, ExceptionType: pkgclickhouse.ErrorTypeUnknownTable}
			if decision := ClassifyDurableInsertFailure(ctx, schemaErr, 2); decision != permanentHTTPStatus(http.StatusNotFound) {
				t.Fatalf("schema error = %+v, want dead %+v", decision, permanentHTTPStatus(http.StatusNotFound))
			}
			// 応答を得ないまま停止したINSERTは、結果不明としてquarantineへ隔離します。
			inFlight := ClassifyDurableInsertFailure(ctx, fmt.Errorf("insert: %w", ctx.Err()), 2)
			if want := (retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome}); inFlight != want {
				t.Fatalf("in-flight stop = %+v, want quarantine %+v", inFlight, want)
			}
		})
	}
}

func TestClassifyMissingInsertResponseAsUnknownOutcome(t *testing.T) {
	for _, err := range []error{
		ErrMissingInsertResponse,
		fmt.Errorf("insert request: %w", ErrMissingInsertResponse),
	} {
		t.Run(err.Error(), func(t *testing.T) {
			// 応答がないため即時retryしません。retryしない分類なので、circuit breakerは
			// backend障害として記録しません。復旧確認でもbackendが応答した証拠にならないため、
			// circuit breaker clientはbreakerを閉じずにprobe枠だけを解放します。
			immediate := ClassifyRetry(context.Background(), err)
			if want := (retryable.RetryDecision{Reason: retryable.RetryReasonUnknownOutcome}); immediate != want {
				t.Fatalf("ClassifyRetry() = %+v, want no retry with unknown outcome %+v", immediate, want)
			}
			// 送信済みrequestの結果を確認できないため、件数にかかわらず自動再送せずquarantineへ隔離します。
			for _, inputCount := range []int{1, 2} {
				decision := ClassifyDurableInsertFailure(context.Background(), err, inputCount)
				if want := (retryable.RetryDecision{Quarantine: true, Reason: retryable.RetryReasonUnknownOutcome}); decision != want {
					t.Fatalf("ClassifyDurableInsertFailure(inputCount=%d) = %+v, want quarantine %+v", inputCount, decision, want)
				}
			}
		})
	}
}

package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/retryclassifier"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
)

// facadeはretryclassifierの分類をそのまま公開します。分類規則はretryclassifierの試験で確認します。
func TestClassifierFacadeDelegatesToRetryClassifier(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	for _, err := range []error{
		&pkgclickhouse.HTTPStatusError{Code: http.StatusServiceUnavailable},
		&pkgclickhouse.HTTPStatusError{Code: http.StatusInternalServerError, ExceptionType: pkgclickhouse.ErrorTypeCannotParseUUID},
		&pkgclickhouse.HTTPStatusError{Code: http.StatusInternalServerError, ExceptionType: pkgclickhouse.ErrorTypeUnknownTable},
		&pkgclickhouse.HTTPStatusError{Code: http.StatusRequestEntityTooLarge},
		&pkgclickhouse.LogRecordMarshalError{Index: 0, Err: errors.New("unsupported field value")},
		ErrMissingInsertResponse,
		retryable.NewCircuitBreakerOpenError("clickhouse insert", time.Unix(1_700_000_000, 0)),
		errors.New("unclassified result"),
	} {
		if got, want := ClassifyRetry(ctx, err), retryclassifier.ClassifyRetry(ctx, err); got != want {
			t.Fatalf("ClassifyRetry(%v) = %+v, want %+v", err, got, want)
		}
		for _, inputCount := range []int{1, 2} {
			got := ClassifyDurableInsertFailure(ctx, err, inputCount)
			if want := retryclassifier.ClassifyDurableInsertFailure(ctx, err, inputCount); got != want {
				t.Fatalf("ClassifyDurableInsertFailure(%v, %d) = %+v, want %+v", err, inputCount, got, want)
			}
		}
	}
	if RetryReasonRecordIsolationRequired != retryclassifier.RetryReasonRecordIsolationRequired ||
		ErrMissingInsertResponse != retryclassifier.ErrMissingInsertResponse {
		t.Fatal("facade constants must be the retryclassifier values")
	}
}

package loki

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/retryclassifier"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

// facadeはretryclassifierの分類をそのまま公開します。分類規則はretryclassifierの試験で確認します。
func TestClassifierFacadeDelegatesToRetryClassifier(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{context.Background(), canceled} {
		for _, err := range []error{
			nil,
			&pkgloki.HTTPStatusError{Code: 260, RetryAfterDuration: time.Second},
			&pkgloki.HTTPStatusError{Code: http.StatusBadRequest},
			fmt.Errorf("push: %w", &pkgloki.HTTPStatusError{Code: http.StatusRequestEntityTooLarge}),
			&pkgloki.HTTPStatusError{Code: http.StatusUnauthorized},
			&pkgloki.HTTPStatusError{Code: http.StatusServiceUnavailable},
			io.ErrUnexpectedEOF,
			retryable.NewCircuitBreakerOpenError("loki push", time.Unix(1_700_000_000, 0)),
			errors.New("unclassified result"),
			fmt.Errorf("push: %w", context.Canceled),
		} {
			if got, want := ClassifyDurableRequestFailure(ctx, err), retryclassifier.ClassifyDurableRequestFailure(ctx, err); got != want {
				t.Fatalf("ClassifyDurableRequestFailure(%v) with ctx err %v = %+v, want %+v", err, ctx.Err(), got, want)
			}
			if got, want := IsRequestEntityTooLarge(err), retryclassifier.IsRequestEntityTooLarge(err); got != want {
				t.Fatalf("IsRequestEntityTooLarge(%v) = %t, want %t", err, got, want)
			}
		}
	}
}

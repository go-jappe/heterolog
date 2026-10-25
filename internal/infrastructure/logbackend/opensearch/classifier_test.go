package opensearch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/retryclassifier"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

// facadeはretryclassifierの分類をそのまま公開します。分類規則はretryclassifierの試験で確認します。
func TestClassifierFacadeDelegatesToRetryClassifier(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{context.Background(), canceled} {
		for _, err := range []error{
			nil,
			&pkgopensearch.ResponseDecodeError{Operation: "bulk", Err: io.ErrUnexpectedEOF},
			&pkgopensearch.HTTPStatusError{Code: http.StatusServiceUnavailable, After: 2 * time.Second},
			fmt.Errorf("index documents: %w", &pkgopensearch.HTTPStatusError{Code: http.StatusRequestEntityTooLarge}),
			fmt.Errorf("index documents: %w", &pkgopensearch.HTTPStatusError{
				Code: http.StatusBadRequest,
				Body: []byte(`{"error":{"type":"mapper_parsing_exception","reason":"rejected"},"status":400}`),
			}),
			&pkgopensearch.HTTPStatusError{Code: http.StatusForbidden},
			io.ErrUnexpectedEOF,
			retryable.NewCircuitBreakerOpenError("opensearch flush", time.Unix(1_700_000_000, 0)),
			errors.New("unclassified result"),
			fmt.Errorf("bulk: %w", context.Canceled),
		} {
			if got, want := ClassifyRetry(ctx, err), retryclassifier.ClassifyRetry(ctx, err); got != want {
				t.Fatalf("ClassifyRetry(%v) with ctx err %v = %+v, want %+v", err, ctx.Err(), got, want)
			}
			if got, want := ClassifyDurableRequestFailure(ctx, err), retryclassifier.ClassifyDurableRequestFailure(ctx, err); got != want {
				t.Fatalf("ClassifyDurableRequestFailure(%v) with ctx err %v = %+v, want %+v", err, ctx.Err(), got, want)
			}
			if got, want := IsRecordScopedRequestRejection(err), retryclassifier.IsRecordScopedRequestRejection(err); got != want {
				t.Fatalf("IsRecordScopedRequestRejection(%v) = %t, want %t", err, got, want)
			}
		}
	}
	for _, errorType := range []pkgopensearch.ErrorType{pkgopensearch.ErrorTypeMapperParsing, "illegal_argument_exception"} {
		if got, want := IsRecordPayloadErrorType(errorType), retryclassifier.IsRecordPayloadErrorType(errorType); got != want {
			t.Fatalf("IsRecordPayloadErrorType(%q) = %t, want %t", errorType, got, want)
		}
	}
	if FailureReasonResponseDecode != retryclassifier.FailureReasonResponseDecode {
		t.Fatal("facade constant must be the retryclassifier value")
	}
}

func TestIsIdempotentCreateConflict(t *testing.T) {
	t.Parallel()

	createRequest := pkgopensearch.DocumentWriteRequest{
		Index: "logs",
		ID:    "fixed-id",
		Op:    pkgopensearch.WriteOpCreate,
	}
	tests := []struct {
		name    string
		request pkgopensearch.DocumentWriteRequest
		err     error
		want    bool
	}{
		{
			name:    "create HTTP 409",
			request: createRequest,
			err:     &pkgopensearch.HTTPStatusError{Code: http.StatusConflict},
			want:    true,
		},
		{
			name:    "wrapped create HTTP 409",
			request: createRequest,
			err: fmt.Errorf(
				"index request failed: %w",
				&pkgopensearch.HTTPStatusError{Code: http.StatusConflict},
			),
			want: true,
		},
		{
			name: "index HTTP 409",
			request: pkgopensearch.DocumentWriteRequest{
				Index: "logs",
				ID:    "fixed-id",
				Op:    pkgopensearch.WriteOpIndex,
			},
			err:  &pkgopensearch.HTTPStatusError{Code: http.StatusConflict},
			want: false,
		},
		{
			name: "create without fixed ID",
			request: pkgopensearch.DocumentWriteRequest{
				Index: "logs",
				Op:    pkgopensearch.WriteOpCreate,
			},
			err:  &pkgopensearch.HTTPStatusError{Code: http.StatusConflict},
			want: false,
		},
		{
			name:    "create HTTP 400",
			request: createRequest,
			err:     &pkgopensearch.HTTPStatusError{Code: http.StatusBadRequest},
			want:    false,
		},
		{
			name:    "non HTTP error",
			request: createRequest,
			err:     errors.New("transport failure"),
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsIdempotentCreateConflict(tt.request, tt.err); got != tt.want {
				t.Fatalf("IsIdempotentCreateConflict() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestIsRequestEntityTooLarge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "HTTP 413",
			err:  &pkgopensearch.HTTPStatusError{Code: http.StatusRequestEntityTooLarge},
			want: true,
		},
		{
			name: "wrapped HTTP 413",
			err: fmt.Errorf(
				"bulk request failed: %w",
				&pkgopensearch.HTTPStatusError{Code: http.StatusRequestEntityTooLarge},
			),
			want: true,
		},
		{
			name: "different HTTP status",
			err:  &pkgopensearch.HTTPStatusError{Code: http.StatusBadRequest},
			want: false,
		},
		{
			name: "non HTTP status error",
			err:  errors.New("transport failure"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRequestEntityTooLarge(tt.err); got != tt.want {
				t.Fatalf("IsRequestEntityTooLarge() = %t, want %t", got, tt.want)
			}
		})
	}
}

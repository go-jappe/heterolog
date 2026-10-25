package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

func TestClassifyRetryTreatsWrappedS3RequestTimeoutAsTransient(t *testing.T) {
	t.Parallel()

	retryAfter := 3 * time.Second
	err := fmt.Errorf("upload object: %w", &pkgs3.HTTPStatusError{
		Code:               http.StatusBadRequest,
		ErrorCode:          pkgs3.ErrorCodeRequestTimeout,
		RetryAfterDuration: retryAfter,
	})

	decision := ClassifyRetry(context.Background(), err)
	if !decision.Retry {
		t.Fatal("Retry = false, want true")
	}
	if decision.Reason != retryable.RetryReasonServiceTransient {
		t.Fatalf("Reason = %q, want %q", decision.Reason, retryable.RetryReasonServiceTransient)
	}
	if decision.Code != http.StatusBadRequest {
		t.Fatalf("Code = %d, want %d", decision.Code, http.StatusBadRequest)
	}
	if decision.RetryAfter != retryAfter {
		t.Fatalf("RetryAfter = %s, want %s", decision.RetryAfter, retryAfter)
	}
}

func TestClassifyRetryTreatsTemporaryRedirectAndOperationAbortedAsTransient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		errorCode  pkgs3.ErrorCode
	}{
		{
			name:       "temporary redirect while bucket DNS is updating",
			statusCode: http.StatusTemporaryRedirect,
			errorCode:  pkgs3.ErrorCodeTemporaryRedirect,
		},
		{
			name:       "operation aborted by a conflicting operation",
			statusCode: http.StatusConflict,
			errorCode:  pkgs3.ErrorCodeOperationAborted,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := ClassifyRetry(context.Background(), fmt.Errorf(
				"wrapped S3 response: %w",
				&pkgs3.HTTPStatusError{Code: test.statusCode, ErrorCode: test.errorCode},
			))
			if !decision.Retry || decision.Reason != retryable.RetryReasonServiceTransient {
				t.Fatalf("decision = %+v, want service transient retry", decision)
			}
			if decision.Code != test.statusCode {
				t.Fatalf("Code = %d, want %d", decision.Code, test.statusCode)
			}
		})
	}
}

func TestClassifyRetryDoesNotOverwriteContextTermination(t *testing.T) {
	t.Parallel()

	requestTimeout := &pkgs3.HTTPStatusError{
		Code:      http.StatusBadRequest,
		ErrorCode: pkgs3.ErrorCodeRequestTimeout,
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	decision := ClassifyRetry(canceledCtx, requestTimeout)
	if decision.Retry || decision.Reason != retryable.RetryReasonNone {
		t.Fatalf("canceled context decision = %+v, want no retry", decision)
	}

	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	decision = ClassifyRetry(deadlineCtx, requestTimeout)
	if decision.Retry || decision.Reason != retryable.RetryReasonCtxDeadline {
		t.Fatalf("expired context decision = %+v, want context deadline without retry", decision)
	}

	decision = ClassifyRetry(
		context.Background(),
		errors.Join(context.DeadlineExceeded, requestTimeout),
	)
	if !decision.Retry || decision.Reason != retryable.RetryReasonCtxDeadline {
		t.Fatalf("wrapped context deadline decision = %+v, want original deadline classification", decision)
	}
}

func TestClassifyRetryLeavesConditionalConflictNonRetryableForUnconditionalPut(t *testing.T) {
	t.Parallel()

	decision := ClassifyRetry(context.Background(), &pkgs3.HTTPStatusError{
		Code:      http.StatusConflict,
		ErrorCode: pkgs3.ErrorCodeConditionalRequestConflict,
	})
	if decision.Retry {
		t.Fatalf("decision = %+v, want non-retryable unconditional PutObject conflict", decision)
	}
	if decision.Reason != retryable.RetryReasonHTTPStatus || decision.Code != http.StatusConflict {
		t.Fatalf("decision = %+v, want ordinary HTTP 409 classification", decision)
	}
}

func TestClassifyRetryRequiresMatchingS3StatusAndErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		errorCode  pkgs3.ErrorCode
	}{
		{
			name:       "temporary redirect code with wrong status",
			statusCode: http.StatusBadRequest,
			errorCode:  pkgs3.ErrorCodeTemporaryRedirect,
		},
		{
			name:       "redirect status without S3 code",
			statusCode: http.StatusTemporaryRedirect,
		},
		{
			name:       "operation aborted code with wrong status",
			statusCode: http.StatusBadRequest,
			errorCode:  pkgs3.ErrorCodeOperationAborted,
		},
		{
			name:       "conflict status without S3 code",
			statusCode: http.StatusConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := ClassifyRetry(context.Background(), &pkgs3.HTTPStatusError{
				Code:      test.statusCode,
				ErrorCode: test.errorCode,
			})
			if decision.Retry {
				t.Fatalf("decision = %+v, want non-retryable mismatched S3 response", decision)
			}
		})
	}
}

func TestClassifyRetryKeepsSharedHTTPStatusClassification(t *testing.T) {
	t.Parallel()

	retryAfter := 2 * time.Second
	decision := ClassifyRetry(context.Background(), &pkgs3.HTTPStatusError{
		Code:               http.StatusServiceUnavailable,
		ErrorCode:          "SlowDown",
		RetryAfterDuration: retryAfter,
	})
	if !decision.Retry || decision.Reason != retryable.RetryReasonHTTPStatus {
		t.Fatalf("decision = %+v, want shared HTTP status retry", decision)
	}
	if decision.Code != http.StatusServiceUnavailable || decision.RetryAfter != retryAfter {
		t.Fatalf("decision = %+v, want status and Retry-After preserved", decision)
	}
}

func TestClassifyDurableUploadFailureKeepsTransientAndUnknownFailuresRetryable(t *testing.T) {
	t.Parallel()

	retryAfter := 4 * time.Second
	tests := []struct {
		name       string
		err        error
		wantReason string
		wantCode   int
		wantAfter  time.Duration
	}{
		{
			name:       "transport EOF",
			err:        io.ErrUnexpectedEOF,
			wantReason: string(retryable.RetryReasonEOF),
		},
		{
			name: "throttled response",
			err: &pkgs3.HTTPStatusError{
				Code:               http.StatusTooManyRequests,
				RetryAfterDuration: retryAfter,
			},
			wantReason: string(retryable.RetryReasonHTTPThrottle),
			wantCode:   http.StatusTooManyRequests,
			wantAfter:  retryAfter,
		},
		{
			name:       "unlisted server error",
			err:        &pkgs3.HTTPStatusError{Code: http.StatusHTTPVersionNotSupported},
			wantReason: string(retryable.RetryReasonHTTPStatus),
			wantCode:   http.StatusHTTPVersionNotSupported,
		},
		{
			name: "S3 request timeout",
			err: &pkgs3.HTTPStatusError{
				Code:      http.StatusBadRequest,
				ErrorCode: pkgs3.ErrorCodeRequestTimeout,
			},
			wantReason: string(retryable.RetryReasonServiceTransient),
			wantCode:   http.StatusBadRequest,
		},
		{
			name: "S3 temporary redirect",
			err: &pkgs3.HTTPStatusError{
				Code:      http.StatusTemporaryRedirect,
				ErrorCode: pkgs3.ErrorCodeTemporaryRedirect,
			},
			wantReason: string(retryable.RetryReasonServiceTransient),
			wantCode:   http.StatusTemporaryRedirect,
		},
		{
			name: "S3 operation aborted",
			err: &pkgs3.HTTPStatusError{
				Code:      http.StatusConflict,
				ErrorCode: pkgs3.ErrorCodeOperationAborted,
			},
			wantReason: string(retryable.RetryReasonServiceTransient),
			wantCode:   http.StatusConflict,
		},
		{
			name:       "circuit breaker open",
			err:        retryable.NewCircuitBreakerOpenError("s3 upload", time.Now().Add(time.Minute)),
			wantReason: string(retryable.RetryReasonCircuitBreakerOpen),
		},
		{
			name:       "unknown error",
			err:        errors.New("unclassified upload failure"),
			wantReason: durableFailureReasonUnknown,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := ClassifyDurableUploadFailure(context.Background(), test.err)
			if decision.FailureCategory != uploader.FailureCategoryRetryable {
				t.Fatalf("FailureCategory = %q, want %q", decision.FailureCategory, uploader.FailureCategoryRetryable)
			}
			if decision.Reason != test.wantReason {
				t.Fatalf("Reason = %q, want %q", decision.Reason, test.wantReason)
			}
			if decision.Code != test.wantCode {
				t.Fatalf("Code = %d, want %d", decision.Code, test.wantCode)
			}
			if decision.RetryAfter != test.wantAfter {
				t.Fatalf("RetryAfter = %s, want %s", decision.RetryAfter, test.wantAfter)
			}
		})
	}
}

func TestClassifyDurableUploadFailureTreatsKnownNonTransientS3ResponsesAsPermanent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		errorCode  pkgs3.ErrorCode
	}{
		{
			name:       "access denied",
			statusCode: http.StatusForbidden,
			errorCode:  "AccessDenied",
		},
		{
			name:       "bucket not found",
			statusCode: http.StatusNotFound,
			errorCode:  "NoSuchBucket",
		},
		{
			name:       "conditional conflict",
			statusCode: http.StatusConflict,
			errorCode:  pkgs3.ErrorCodeConditionalRequestConflict,
		},
		// UploadはPUTのredirectを追従しません。redirectは宛先・設定の誤りで配送不能が
		// 確定した応答として、再送し続けずに保持します。
		{
			name:       "permanent redirect to the bucket region",
			statusCode: http.StatusMovedPermanently,
			errorCode:  "PermanentRedirect",
		},
		{
			name:       "proxy redirect to a login page",
			statusCode: http.StatusFound,
		},
		{
			name:       "see other redirect",
			statusCode: http.StatusSeeOther,
		},
		{
			name:       "temporary redirect without S3 code",
			statusCode: http.StatusTemporaryRedirect,
		},
		{
			name:       "permanent redirect with preserved method",
			statusCode: http.StatusPermanentRedirect,
			errorCode:  "PermanentRedirect",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := ClassifyDurableUploadFailure(context.Background(), &pkgs3.HTTPStatusError{
				Code:      test.statusCode,
				ErrorCode: test.errorCode,
			})
			if decision.FailureCategory != uploader.FailureCategoryPermanent {
				t.Fatalf("FailureCategory = %q, want %q", decision.FailureCategory, uploader.FailureCategoryPermanent)
			}
			if decision.Reason != string(retryable.RetryReasonHTTPStatus) {
				t.Fatalf("Reason = %q, want %q", decision.Reason, retryable.RetryReasonHTTPStatus)
			}
			if decision.Code != test.statusCode {
				t.Fatalf("Code = %d, want %d", decision.Code, test.statusCode)
			}
		})
	}
}

func TestClassifyDurableUploadFailurePreservesCompressedLogWhenContextEnds(t *testing.T) {
	t.Parallel()

	permanentResponse := &pkgs3.HTTPStatusError{
		Code:      http.StatusForbidden,
		ErrorCode: "AccessDenied",
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name       string
		ctx        context.Context
		err        error
		wantReason string
	}{
		{
			name:       "parent context canceled after response",
			ctx:        canceledCtx,
			err:        permanentResponse,
			wantReason: durableFailureReasonContextCanceled,
		},
		{
			name:       "wrapped cancellation",
			ctx:        context.Background(),
			err:        fmt.Errorf("upload stopped: %w", context.Canceled),
			wantReason: durableFailureReasonContextCanceled,
		},
		{
			name:       "wrapped deadline",
			ctx:        context.Background(),
			err:        fmt.Errorf("upload stopped: %w", context.DeadlineExceeded),
			wantReason: string(retryable.RetryReasonCtxDeadline),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision := ClassifyDurableUploadFailure(test.ctx, test.err)
			if decision.FailureCategory != uploader.FailureCategoryRetryable {
				t.Fatalf("FailureCategory = %q, want %q", decision.FailureCategory, uploader.FailureCategoryRetryable)
			}
			if decision.Reason != test.wantReason {
				t.Fatalf("Reason = %q, want %q", decision.Reason, test.wantReason)
			}
		})
	}
}

// UploadはPUTのredirectを追従せずHTTPStatusErrorを返します。redirect先のGETが成功しても
// upload成功にはならず、durable分類では宛先・設定の誤りとしてpermanentになります。
// S3がbucketのDNS更新中に返すTemporaryRedirectだけは一時状態としてretryableです。
func TestClassifyDurableUploadFailureTreatsUploadRedirectsAsPermanent(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		errorCode pkgs3.ErrorCode
		want      uploader.FailureCategory
	}{
		{"wrong region", http.StatusMovedPermanently, "PermanentRedirect", uploader.FailureCategoryPermanent},
		{"login page", http.StatusFound, "", uploader.FailureCategoryPermanent},
		{"see other", http.StatusSeeOther, "", uploader.FailureCategoryPermanent},
		{"proxy temporary redirect", http.StatusTemporaryRedirect, "", uploader.FailureCategoryPermanent},
		{"permanent redirect", http.StatusPermanentRedirect, "PermanentRedirect", uploader.FailureCategoryPermanent},
		{"bucket DNS propagation", http.StatusTemporaryRedirect, pkgs3.ErrorCodeTemporaryRedirect, uploader.FailureCategoryRetryable},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if r.URL.Path != "/archive-bucket/app.log.gz" {
					// login pageなどのredirect先は、本文なしのGETにも成功を返します。
					w.WriteHeader(http.StatusOK)
					return
				}
				w.Header().Set("Location", "/login")
				w.WriteHeader(test.status)
				if test.errorCode != "" {
					_, _ = io.WriteString(w, "<Error><Code>"+string(test.errorCode)+"</Code></Error>")
				}
			}))
			defer server.Close()
			filePath := filepath.Join(t.TempDir(), "app.log.gz")
			if err := os.WriteFile(filePath, []byte("compressed log"), 0o600); err != nil {
				t.Fatal(err)
			}
			client, err := pkgs3.New(pkgs3.Config{
				S3Config: objectstorage.S3Config{
					Region: "ap-northeast-1", Endpoint: server.URL, Bucket: "archive-bucket", ForcePathStyle: true,
					AccessKeyID: "test-access", SecretAccessKey: "test-secret",
				},
				HTTPClient: server.Client(),
			})
			if err != nil {
				t.Fatal(err)
			}

			uploadErr := client.Upload(context.Background(), uploadertest.Input(t, filePath, "app.log.gz"))
			if uploadErr == nil {
				t.Fatal("Upload() error = nil, want redirect returned as an HTTP status error")
			}
			decision := ClassifyDurableUploadFailure(context.Background(), uploadErr)
			if decision.FailureCategory != test.want || decision.Code != test.status {
				t.Fatalf("decision = %+v, want category %q for HTTP %d", decision, test.want, test.status)
			}
		})
	}
}

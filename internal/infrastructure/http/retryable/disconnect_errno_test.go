package retryable

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
)

// opaqueTransportErrorは、切断を表す文言を含まないmessageでerror番号を包みます。
type opaqueTransportError struct{ cause error }

func (e opaqueTransportError) Error() string { return "transport failed" }
func (e opaqueTransportError) Unwrap() error { return e.cause }

func TestClassifyRetryRecognizesDisconnectErrnoWithoutMessage(t *testing.T) {
	t.Parallel()

	for _, errno := range []syscall.Errno{syscall.ECONNRESET, syscall.ECONNABORTED, syscall.ECONNREFUSED, syscall.EPIPE} {
		for name, err := range map[string]error{
			"opaque wrapper": opaqueTransportError{cause: errno},
			"net operation": &url.Error{Op: "Post", URL: "https://example.test", Err: &net.OpError{
				Op: "write", Net: "tcp", Err: os.NewSyscallError("write", opaqueTransportError{cause: errno}),
			}},
		} {
			t.Run(fmt.Sprintf("%d/%s", errno, name), func(t *testing.T) {
				want := RetryDecision{Retry: true, Reason: RetryReasonDisconnect}
				if got := ClassifyRetry(context.Background(), err); got != want {
					t.Fatalf("ClassifyRetry(%v) = %+v, want %+v", err, got, want)
				}
			})
		}
	}
}

// 型付きの切断は、EOFと同じくHTTP statusより優先します。文言による推定とは異なり、
// statusを持つerrorでも判定を変えません。
func TestClassifyRetryDisconnectErrnoKeepsTypedErrorPrecedence(t *testing.T) {
	t.Parallel()

	err := &statusPrecedenceError{code: http.StatusBadRequest, message: "rejected", cause: opaqueTransportError{cause: syscall.ECONNRESET}}
	want := RetryDecision{Retry: true, Reason: RetryReasonDisconnect}
	if got := ClassifyRetry(context.Background(), err); got != want {
		t.Fatalf("ClassifyRetry() = %+v, want %+v", got, want)
	}
	// 呼び出し元の停止は、型付きの切断より優先します。
	if got := ClassifyRetry(canceledContext(), opaqueTransportError{cause: syscall.ECONNRESET}); got != (RetryDecision{}) {
		t.Fatalf("ClassifyRetry() after cancellation = %+v, want no retry", got)
	}
}

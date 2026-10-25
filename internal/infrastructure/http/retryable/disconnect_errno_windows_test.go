//go:build windows

package retryable

import (
	"context"
	"net"
	"os"
	"syscall"
	"testing"
)

// Windowsのsocket操作が返すWinsockのerror番号は、POSIXと異なる文言でも切断として扱います。
func TestClassifyRetryRecognizesWinsockDisconnectErrno(t *testing.T) {
	t.Parallel()

	for _, errno := range []syscall.Errno{syscall.WSAECONNRESET, syscall.WSAECONNABORTED, wsaeConnRefused} {
		err := &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("wsarecv", errno)}
		want := RetryDecision{Retry: true, Reason: RetryReasonDisconnect}
		if got := ClassifyRetry(context.Background(), err); got != want {
			t.Fatalf("ClassifyRetry(%v) = %+v, want %+v", err, got, want)
		}
	}
}

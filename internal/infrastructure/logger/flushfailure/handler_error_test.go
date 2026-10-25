package flushfailure

import (
	"errors"
	"testing"
)

// FailureHandlerErrorの文面は、Compositeが内部ログへ渡すerrorとして運用者が読みます。どの子handlerの
// どの処理が失敗したかを文面だけで判別できるよう、位置・型・処理・原因（panicでは回復した値）を示す
// 書式をこの試験で固定します。他の試験はerrors.Asでfieldを確認します。
func TestFailureHandlerErrorFormat(t *testing.T) {
	cause := errors.New("child failure cause")
	handler := failureHandlerFunc(nil)
	for _, tt := range []struct {
		name string
		err  *FailureHandlerError
		want string
	}{
		{
			name: "handle",
			err:  &FailureHandlerError{Index: 1, Handler: handler, Operation: FailureHandlerOperationHandle, Err: cause},
			want: "failure handler[1] (flushfailure.failureHandlerFunc) failed: child failure cause",
		},
		{
			name: "panic",
			err:  &FailureHandlerError{Index: 0, Handler: handler, Operation: FailureHandlerOperationPanic, PanicValue: "handler panic"},
			want: "failure handler[0] (flushfailure.failureHandlerFunc) panic: handler panic",
		},
		{
			name: "report handler acquire",
			err: &FailureHandlerError{
				Index: 2, Handler: handler, Report: true, Operation: FailureHandlerOperationAcquire, Err: cause,
			},
			want: "failure report handler[2] (flushfailure.failureHandlerFunc) semaphore acquire failed: child failure cause",
		},
		{
			name: "report handler panic",
			err: &FailureHandlerError{
				Index: 0, Handler: handler, Report: true, Operation: FailureHandlerOperationPanic, PanicValue: 42,
			},
			want: "failure report handler[0] (flushfailure.failureHandlerFunc) panic: 42",
		},
		{
			name: "shutdown",
			err:  &FailureHandlerError{Index: 3, Handler: handler, Operation: FailureHandlerOperationShutdown, Err: cause},
			want: "failure handler[3] (flushfailure.failureHandlerFunc) shutdown failed: child failure cause",
		},
		{
			name: "force close",
			err:  &FailureHandlerError{Index: 0, Handler: handler, Operation: FailureHandlerOperationForceClose, Err: cause},
			want: "failure handler[0] (flushfailure.failureHandlerFunc) force close failed: child failure cause",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

package flushfailure

import "fmt"

// FailureHandlerOperationは、Compositeが子handlerに対して行い失敗した処理です。
type FailureHandlerOperation string

const (
	// FailureHandlerOperationHandleは、子handlerの失敗処理がerrorを返したことを表します。
	FailureHandlerOperationHandle FailureHandlerOperation = "failed"
	// FailureHandlerOperationPanicは、子handlerの失敗処理がpanicしたことを表します。
	FailureHandlerOperationPanic FailureHandlerOperation = "panic"
	// FailureHandlerOperationAcquireは、並行数の上限内で処理を開始できなかったことを表します。
	FailureHandlerOperationAcquire FailureHandlerOperation = "semaphore acquire failed"
	// FailureHandlerOperationShutdownは、子handlerの終了処理がerrorを返したことを表します。
	FailureHandlerOperationShutdown FailureHandlerOperation = "shutdown failed"
	// FailureHandlerOperationForceCloseは、子handlerの強制終了がerrorを返したことを表します。
	FailureHandlerOperationForceClose FailureHandlerOperation = "force close failed"
)

// FailureHandlerErrorは、Compositeの子handlerの処理が失敗したことを表します。
// Indexは構築時に指定した子handlerの位置、Handlerは失敗した子handlerです。
// ReportはDeliveryReportを返す子handlerかどうかを表します。
// Errは原因のerrorで、panicの場合はnilです。PanicValueはpanicで回復した値です。
type FailureHandlerError struct {
	Index      int
	Handler    any
	Report     bool
	Operation  FailureHandlerOperation
	PanicValue any
	Err        error
}

func (e *FailureHandlerError) Error() string {
	kind := "failure handler"
	if e.Report {
		kind = "failure report handler"
	}
	if e.Operation == FailureHandlerOperationPanic {
		return fmt.Sprintf("%s[%d] (%T) panic: %v", kind, e.Index, e.Handler, e.PanicValue)
	}
	return fmt.Sprintf("%s[%d] (%T) %s: %v", kind, e.Index, e.Handler, e.Operation, e.Err)
}

func (e *FailureHandlerError) Unwrap() error {
	return e.Err
}

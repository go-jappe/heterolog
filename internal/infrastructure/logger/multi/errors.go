package multi

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/logger"
)

// ErrNoMultiLoggerは、nilのtop-level loggerへlog要求が渡されたことを表します。
var ErrNoMultiLogger = errors.New("no multi logger available")

// ErrLoggerPanickedは、child handlerのpanicを回復し、その操作を失敗として扱ったことを
// 表します。回復した値はPanicErrorで取り出せます。
var ErrLoggerPanicked = errors.New("logger panicked")

// OperationErrorは、top-level loggerの一つの操作が失敗したことを表します。Opは
// "multi.Info"や"delivery.ErrorWithStack"などの操作名で、Errは原因です。
// Close開始後の拒否はErrにapplogging.ErrLoggerClosedを含みます。
type OperationError struct {
	Op  string
	Err error
}

func (e *OperationError) Error() string { return e.Op + ": " + e.Err.Error() }

func (e *OperationError) Unwrap() error { return e.Err }

// HandlerErrorは、一つのchild handlerの操作が失敗したことを表します。Opは操作名、
// Handlerは失敗したchildです。errors.Asでchildを特定し、Errで原因を判定できます。
type HandlerError struct {
	Op      string
	Handler any
	Err     error
}

func (e *HandlerError) Error() string {
	return fmt.Sprintf("%s: handler=%T: %v", e.Op, e.Handler, e.Err)
}

func (e *HandlerError) Unwrap() error { return e.Err }

// PanicErrorは、child handlerから回復したpanicの値を保持します。Loggerは
// "logger"または"delivery logger"で、errors.IsではErrLoggerPanickedと一致します。
// 値がerrorの場合は、その値も原因としてerrors.Is/Asで判定できます。
type PanicError struct {
	Logger string
	Value  any
}

func (e *PanicError) Error() string { return fmt.Sprintf("%s panicked: %v", e.Logger, e.Value) }

func (e *PanicError) Is(target error) bool { return target == ErrLoggerPanicked }

func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// TargetDeliveryErrorは、childが返した一つのtarget結果を登録済みの送達として扱えない
// ことを表します。errors.Isではlogger.ErrTargetDeliveryNotRegisteredと一致し、Backendと
// Statusでtargetを、Errでtargetが報告した原因を特定できます。
type TargetDeliveryError struct {
	Backend string
	Status  logger.DeliveryStatus
	Err     error
}

func (e *TargetDeliveryError) Error() string {
	text := fmt.Sprintf(
		"%v: backend=%q status=%s",
		logger.ErrTargetDeliveryNotRegistered,
		e.Backend,
		e.Status,
	)
	if e.Err == nil {
		return text
	}
	if registeredDeliveryStatus(e.Status) {
		return text + " contains target error: " + e.Err.Error()
	}
	return text + ": " + e.Err.Error()
}

func (e *TargetDeliveryError) Is(target error) bool {
	return target == logger.ErrTargetDeliveryNotRegistered
}

func (e *TargetDeliveryError) Unwrap() error { return e.Err }

// registeredDeliveryStatusは、errorを伴わなければ登録済みとして扱う送達状態かを返します。
func registeredDeliveryStatus(status logger.DeliveryStatus) bool {
	switch status {
	case logger.DeliverySucceeded, logger.DeliveryEnqueued, logger.DeliverySkipped:
		return true
	default:
		return false
	}
}

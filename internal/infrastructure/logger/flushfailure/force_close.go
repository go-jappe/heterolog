package flushfailure

import (
	"errors"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
)

// ForceCloseは新規failure処理を止め、対応するchildの復旧処理を中止します。
// 実行中handlerの終了はDrainが引き続き待ちます。保存済みspoolの再送は開始しません。
func (c *CompositeFailureHandler) ForceClose() error {
	if c == nil {
		return nil
	}
	c.lifecycleMu.Lock()
	if c.state == compositeLifecycleStateDrained {
		c.lifecycleMu.Unlock()
		return nil
	}
	c.state = compositeLifecycleStateDraining
	handlers := append([]FailureHandler(nil), c.Handlers...)
	c.lifecycleMu.Unlock()
	errs := []error{shutdown.ErrForcedClose}
	for index, handler := range handlers {
		if err := loggercommon.ForceCloseIfSupported(handler); err != nil {
			errs = append(errs, &FailureHandlerError{
				Index: index, Handler: handler, Operation: FailureHandlerOperationForceClose, Err: err,
			})
		}
	}
	return errors.Join(errs...)
}

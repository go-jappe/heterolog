package flushfailure

import (
	"context"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
)

// SpoolHandlerRegistrarは、Buildが構築したspool handlerのShutdown責任を受け取ります。
// nilを返した場合だけ所有権が移り、Buildはhandlerを借用viewとしてCompositeへ渡します。
// errorを返した場合、BuildはhandlerをShutdownしてCompositeへは渡しません。
type SpoolHandlerRegistrar func(*spoolstorehandler.SpoolStoreHandler) error

// borrowedSpoolFailureHandlerはHandleFailureだけをCompositeへ公開します。
// HandlerShutdownを実装しないことで、Compositeが借用中のhandlerを閉じることを防ぎ、
// registrar側のownerがCompositeのDrain後にShutdownできるようにします。
type borrowedSpoolFailureHandler struct {
	handler *spoolstorehandler.SpoolStoreHandler
}

var _ FailureHandler = borrowedSpoolFailureHandler{}

func newBorrowedSpoolFailureHandler(
	handler *spoolstorehandler.SpoolStoreHandler,
) FailureHandler {
	return borrowedSpoolFailureHandler{handler: handler}
}

func (h borrowedSpoolFailureHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	return h.handler.HandleFailure(ctx, result)
}

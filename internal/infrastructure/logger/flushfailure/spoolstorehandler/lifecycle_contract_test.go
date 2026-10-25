package spoolstorehandler_test

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure/spoolstorehandler"
)

// raw SpoolStoreHandlerのownerが終了待機を実行できることをcompile timeで保証します。
// standalone factoryではCompositeがownerとなり、top-level loggerのpaired pipelineでは
// CompositeへShutdownを持たない借用viewを渡してpipeline側がこの契約を呼びます。
var _ flushfailure.HandlerShutdown = (*spoolstorehandler.SpoolStoreHandler)(nil)

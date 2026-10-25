package flushfailure

import (
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// InternalHandlers はdurable failure store自身が出す内部ログの委譲先です。
type InternalHandlers struct {
	OnError logging.LoggerErrorHandler
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc
}

// InternalLogHandlerFactoryは、storeのhandler名設定から内部ログ用の通知関数群を組み立てます。
// durableTypeは、構築側がspoolとfile archiveのhandlerの所有元を区別するために渡します。
// 通知の実行は、返されたInternalHandlersの各関数を呼ぶ側が担当します。
type InternalLogHandlerFactory func(
	durableType pkgdurable.DurableType,
	config loggerhandler.ConfigProvider,
) InternalHandlers

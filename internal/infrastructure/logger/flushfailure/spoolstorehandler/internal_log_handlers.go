package spoolstorehandler

import "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"

// InternalLogHandlersは、spool store自身が出すerror、warn、info、debugを委譲する
// handler群です。呼び出し側が設定名から解決済みの関数だけを設定します。
type InternalLogHandlers struct {
	OnError logging.LoggerErrorHandler
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc
}

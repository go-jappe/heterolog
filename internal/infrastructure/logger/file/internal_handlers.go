package file

import "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"

// InternalHandlersは、一つのFileLoggerがinternal logを委譲するhandler群です。
// handlerはlevelごとに独立しており、fallback先のlogger状態もこの単位で所有します。
type InternalHandlers struct {
	OnError logging.LoggerErrorHandler
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc
}

// InternalHandlerRoleは、InternalHandlersを所有する物理FileLoggerのroleです。
// error/debug分離が無効な場合は実際に共有されるdefaultだけがfactoryへ渡されます。
type InternalHandlerRole string

// InternalHandlerFactoryは、roleで識別したFileLoggerごとに専用のInternalHandlersを作ります。
// 戻り値のhandler状態は別のFileLoggerと共有してはいけません。一方、Fileをfallback先に
// 選んだhandlerは、同じLoggerSetから同じroleのFileLoggerを取得できます。
type InternalHandlerFactory func(InternalHandlerRole) InternalHandlers

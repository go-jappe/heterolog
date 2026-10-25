package stdout

import "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"

type internalLogHandlers struct {
	onError logging.LoggerErrorHandler
	onWarn  logging.LoggerHandlerFunc
	onInfo  logging.LoggerHandlerFunc
	onDebug logging.LoggerHandlerFunc
}

// LoggerOptionは通常・delivery loggerの構築時に通知先を指定します。
type LoggerOption func(*internalLogHandlers)

// WithInternalLogHandlersは内部ログの通知先を構築時に設定します。
// 未指定またはnilの通知先へは通知しません。
func WithInternalLogHandlers(
	onError logging.LoggerErrorHandler,
	onWarn, onInfo, onDebug logging.LoggerHandlerFunc,
) LoggerOption {
	return func(handlers *internalLogHandlers) {
		*handlers = internalLogHandlers{onError: onError, onWarn: onWarn, onInfo: onInfo, onDebug: onDebug}
	}
}

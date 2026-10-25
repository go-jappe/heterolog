package fsarchivehandler

import (
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

type stackTraceConfigAdapter struct {
	IsDebugConfig                  bool
	ErrorTraceDepthDebugModeConfig int
	ErrorTraceDepthConfig          int
}

// NewStackTraceConfigAdapter はfailure archiveの設定を内部ログ用stack trace設定に変換します。
func NewStackTraceConfigAdapter(cfg loggerdurable.FileArchiveConfigProvider) logging.StackTraceConfigProvider {
	return &stackTraceConfigAdapter{
		IsDebugConfig:                  cfg.IsDebug(),
		ErrorTraceDepthDebugModeConfig: cfg.ErrorTraceDepthDebugMode(),
		ErrorTraceDepthConfig:          cfg.ErrorTraceDepth(),
	}
}

func (a *stackTraceConfigAdapter) IsDebug() bool {
	return a.IsDebugConfig
}
func (a *stackTraceConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.ErrorTraceDepthDebugModeConfig
}
func (a *stackTraceConfigAdapter) ErrorTraceDepth() int {
	return a.ErrorTraceDepthConfig
}

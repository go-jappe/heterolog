package file

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type internalLogFileLayout interface {
	IsSeparateErrorLogsEnabled() bool
	IsSeparateDebugLogsEnabled() bool
}

// resolveInternalLogFileRoleは、internal logのlevelとfile分離設定から
// 実際に書き込むFileLogger roleを決定します。
//
// ErrorLoggerとDebugLoggerは、それぞれ専用fileへの分離が有効な場合だけ使います。
// 分離が無効なlevelとwarn/infoは、通常ログと同じDefaultLoggerへ集約します。
func resolveInternalLogFileRole(
	layout internalLogFileLayout,
	level pkglogger.LogLevel,
) loggerType {
	switch level {
	case pkglogger.LogLevelError:
		if layout.IsSeparateErrorLogsEnabled() {
			return ErrorLogger
		}
	case pkglogger.LogLevelDebug:
		if layout.IsSeparateDebugLogsEnabled() {
			return DebugLogger
		}
	}
	return DefaultLogger
}

// fileLoggerInternalSourceは、同じfile backend内でもdefault/error/debugの
// どのroleで発生したinternal logかを識別できるsourceを返します。
func fileLoggerInternalSource(role loggerType) string {
	return common.SourceNameFileLogger + ":" + string(role)
}

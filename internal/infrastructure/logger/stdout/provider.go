package stdout

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/pkg/logger"
)

type StdoutLoggerConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() RuntimeConfigProvider
	LogBody() LogBodyConfigProvider
	Sampling() sampling.SamplingConfigProvider
	Handler() handler.ConfigProvider
	UseOpenTelemetryFormat() bool      // OpenTelemetry準拠のログ形式を使用する
	UseGoogleCloudLoggingFormat() bool // Google Cloud Loggingが読むログ形式を使用する
}

// outputFormatConfigProviderは、出力形式を決めるための設定です。通常のloggerとDelivery loggerの
// 設定のどちらも満たします。
type outputFormatConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	LogBody() LogBodyConfigProvider
	UseOpenTelemetryFormat() bool
	UseGoogleCloudLoggingFormat() bool
}

type RuntimeConfigProvider interface {
	IsDebug() bool
	Target() string
	LogLevel() logger.LogLevel
}

type LogBodyConfigProvider interface {
	LogLineDateFormat() string
	ErrorTraceDepth() int
	ErrorTraceDepthDebugMode() int
}

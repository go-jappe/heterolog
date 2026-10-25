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
	UseOpenTelemetryFormat() bool // OpenTelemetry準拠のログ形式を使用する
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

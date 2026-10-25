package slack

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/pkg/logger"
	"net/http"
	"time"
)

type SlackLoggerConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() RuntimeConfigProvider
	LogBody() LogBodyConfigProvider

	Timeout() time.Duration
	// HTTPClientはSlack API clientの基にする設定です。nilはnet/httpの既定transportを
	// 表します。loggerは戻り値をcopyしてTimeoutを適用し、戻り値自体は変更しません。
	HTTPClient() *http.Client
	Token() string
	Channel() string
	ErrorChannel() string
	WarnChannel() string
	InfoChannel() string
	DebugChannel() string
	MaxLogsPerNotification() int
	MaxRestThreshold() time.Duration
	Intervals() []time.Duration
	MaxMessageLength() int
	Sampling() sampling.SamplingConfigProvider
	Handler() handler.ConfigProvider

	SlackPostWarnThresholdDuration() time.Duration
	SlackPostErrorThresholdDuration() time.Duration
}

type RuntimeConfigProvider interface {
	AppName() string
	IsDebug() bool
	Target() string
	LogLevel() logger.LogLevel
}

type LogBodyConfigProvider interface {
	LogLineDateFormat() string
	ErrorTraceDepth() int
	ErrorTraceDepthDebugMode() int
}

type slackChannelConfigProvider interface {
	Channel() string
	ErrorChannel() string
	WarnChannel() string
	InfoChannel() string
	DebugChannel() string
}

func channelForLogLevel(config slackChannelConfigProvider, level logger.LogLevel) string {
	switch level {
	case logger.LogLevelDebug:
		return fallbackChannel(config.DebugChannel(), config.Channel())
	case logger.LogLevelInfo:
		return fallbackChannel(config.InfoChannel(), config.Channel())
	case logger.LogLevelWarn:
		return fallbackChannel(config.WarnChannel(), config.Channel())
	case logger.LogLevelError:
		return fallbackChannel(config.ErrorChannel(), config.Channel())
	default:
		return config.Channel()
	}
}

func fallbackChannel(channel, fallback string) string {
	if channel != "" {
		return channel
	}
	return fallback
}

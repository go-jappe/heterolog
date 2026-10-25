package stdout

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/pkg/logger"
)

type configAdapter struct {
	cfg            logger.StdoutLoggerConfig
	timeConfig     loggertime.TimeConfigProvider
	runtimeConfig  RuntimeConfigProvider
	logBodyConfig  LogBodyConfigProvider
	samplingConfig sampling.SamplingConfigProvider
	handlerConfig  handler.ConfigProvider
}

func NewConfigAdapter(cfg logger.StdoutLoggerConfig) configAdapter {
	return configAdapter{
		cfg:            cfg,
		timeConfig:     loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		runtimeConfig:  newRuntimeConfigAdapter(cfg.RuntimeConfig),
		logBodyConfig:  newLogBodyConfigAdapter(cfg.LogBodyConfig),
		samplingConfig: sampling.NewSamplingConfigAdapter(cfg.SamplingConfig),
		handlerConfig:  handler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

// NewDeliveryFallbackConfigAdapterはDeliveryの設定を、戻り値なしのinternal log用に変換します。
// fallbackでは全levelを対象にし、samplingは行いません。
func NewDeliveryFallbackConfigAdapter(cfg logger.DeliveryStdoutLoggerConfig) configAdapter {
	return NewConfigAdapter(logger.StdoutLoggerConfig{
		TimeConfig: cfg.TimeConfig,
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  cfg.RuntimeConfig.AppName,
			IsDebug:  cfg.RuntimeConfig.IsDebug,
			Target:   cfg.RuntimeConfig.Target,
			LogLevel: logger.LogLevelDebug,
		},
		LogBodyConfig:          cfg.LogBodyConfig,
		HandlerConfig:          cfg.HandlerConfig,
		UseOpenTelemetryFormat: cfg.UseOpenTelemetryFormat,
	})
}

func (a configAdapter) Time() loggertime.TimeConfigProvider {
	return a.timeConfig
}
func (a configAdapter) Runtime() RuntimeConfigProvider {
	return a.runtimeConfig
}
func (a configAdapter) LogBody() LogBodyConfigProvider {
	return a.logBodyConfig
}
func (a configAdapter) Sampling() sampling.SamplingConfigProvider {
	return a.samplingConfig
}

func (a configAdapter) Handler() handler.ConfigProvider {
	return a.handlerConfig
}

func (a configAdapter) UseOpenTelemetryFormat() bool {
	return a.cfg.UseOpenTelemetryFormat
}

type runtimeConfigAdapter struct {
	AppNameConfig  string
	IsDebugConfig  bool
	TargetConfig   string
	LogLevelConfig logger.LogLevel
}

func newRuntimeConfigAdapter(cfg logger.RuntimeConfig) RuntimeConfigProvider {
	return runtimeConfigAdapter{
		AppNameConfig:  cfg.AppName,
		IsDebugConfig:  cfg.IsDebug,
		TargetConfig:   cfg.Target,
		LogLevelConfig: cfg.LogLevel,
	}
}

func (a runtimeConfigAdapter) AppName() string {
	return a.AppNameConfig
}
func (a runtimeConfigAdapter) IsDebug() bool {
	return a.IsDebugConfig
}
func (a runtimeConfigAdapter) Target() string {
	return a.TargetConfig
}
func (a runtimeConfigAdapter) LogLevel() logger.LogLevel {
	return a.LogLevelConfig
}

type logBodyConfigAdapter struct {
	LogLineDateFormatConfig        string
	ErrorTraceDepthConfig          int
	ErrorTraceDepthDebugModeConfig int
}

func newLogBodyConfigAdapter(cfg logger.LogBodyConfig) LogBodyConfigProvider {
	return logBodyConfigAdapter{
		LogLineDateFormatConfig:        cfg.LogLineDateFormat,
		ErrorTraceDepthConfig:          cfg.ErrorTraceDepth,
		ErrorTraceDepthDebugModeConfig: cfg.ErrorTraceDepthDebugMode,
	}
}

func (a logBodyConfigAdapter) LogLineDateFormat() string {
	return a.LogLineDateFormatConfig
}
func (a logBodyConfigAdapter) ErrorTraceDepth() int {
	return a.ErrorTraceDepthConfig
}
func (a logBodyConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.ErrorTraceDepthDebugModeConfig
}

type stackTraceConfigAdapter struct {
	IsDebugConfig                  bool
	ErrorTraceDepthDebugModeConfig int
	ErrorTraceDepthConfig          int
}

func NewStackTraceConfigAdapter(cfg StdoutLoggerConfigProvider) logging.StackTraceConfigProvider {
	return &stackTraceConfigAdapter{
		IsDebugConfig:                  cfg.Runtime().IsDebug(),
		ErrorTraceDepthDebugModeConfig: cfg.LogBody().ErrorTraceDepthDebugMode(),
		ErrorTraceDepthConfig:          cfg.LogBody().ErrorTraceDepth(),
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

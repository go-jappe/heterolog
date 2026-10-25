package stdout

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/pkg/logger"
)

type deliveryConfigAdapter struct {
	cfg           logger.DeliveryStdoutLoggerConfig
	timeConfig    loggertime.TimeConfigProvider
	runtimeConfig DeliveryRuntimeConfigProvider
	logBodyConfig LogBodyConfigProvider
	handlerConfig handler.ConfigProvider
}

func NewDeliveryConfigAdapter(cfg logger.DeliveryStdoutLoggerConfig) deliveryConfigAdapter {
	return deliveryConfigAdapter{
		cfg:           cfg,
		timeConfig:    loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		runtimeConfig: newDeliveryRuntimeConfigAdapter(cfg.RuntimeConfig),
		logBodyConfig: newLogBodyConfigAdapter(cfg.LogBodyConfig),
		handlerConfig: handler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

func (a deliveryConfigAdapter) Time() loggertime.TimeConfigProvider {
	return a.timeConfig
}
func (a deliveryConfigAdapter) Runtime() DeliveryRuntimeConfigProvider {
	return a.runtimeConfig
}
func (a deliveryConfigAdapter) LogBody() LogBodyConfigProvider {
	return a.logBodyConfig
}
func (a deliveryConfigAdapter) Handler() handler.ConfigProvider {
	return a.handlerConfig
}

func (a deliveryConfigAdapter) UseOpenTelemetryFormat() bool {
	return a.cfg.UseOpenTelemetryFormat
}

type deliveryRuntimeConfigAdapter struct {
	AppNameConfig string
	IsDebugConfig bool
	TargetConfig  string
}

func newDeliveryRuntimeConfigAdapter(cfg logger.DeliveryRuntimeConfig) deliveryRuntimeConfigAdapter {
	return deliveryRuntimeConfigAdapter{
		AppNameConfig: cfg.AppName,
		IsDebugConfig: cfg.IsDebug,
		TargetConfig:  cfg.Target,
	}
}

func (a deliveryRuntimeConfigAdapter) AppName() string {
	return a.AppNameConfig
}
func (a deliveryRuntimeConfigAdapter) IsDebug() bool {
	return a.IsDebugConfig
}
func (a deliveryRuntimeConfigAdapter) Target() string {
	return a.TargetConfig
}

type DeliveryStdoutLoggerConfigProvider interface {
	Time() loggertime.TimeConfigProvider
	Runtime() DeliveryRuntimeConfigProvider
	LogBody() LogBodyConfigProvider
	Handler() handler.ConfigProvider
	UseOpenTelemetryFormat() bool // OpenTelemetry準拠のログ形式を使用する
}

type DeliveryRuntimeConfigProvider interface {
	IsDebug() bool
	Target() string
}

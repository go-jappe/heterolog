package slack

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	"github.com/go-jappe/heterolog/pkg/logger"
	"net/http"
	"slices"
	"time"
)

type configAdapter struct {
	cfg            logger.SlackLoggerConfig
	timeConfig     loggertime.TimeConfigProvider
	runtimeConfig  RuntimeConfigProvider
	logBodyConfig  LogBodyConfigProvider
	samplingConfig sampling.SamplingConfigProvider
	handlerConfig  handler.ConfigProvider
}

func NewConfigAdapter(cfg logger.SlackLoggerConfig) configAdapter {
	cfg.Intervals = slices.Clone(cfg.Intervals)
	// 遅延生成するfallback loggerも構築時点の設定を使うよう、HTTP clientの設定を
	// copyして所有します。呼び出し元のclientは変更せず、後からの変更も反映しません。
	cfg.HTTPClient = cloneHTTPClient(cfg.HTTPClient)
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
// Delivery本体の一件同期送信設定は変更せず、fallback専用のbuffer上限と通知間隔を使います。
// samplingは行いません。変換した設定は共有できますが、送信bufferを持つhandlerは共有しません。
func NewDeliveryFallbackConfigAdapter(
	cfg logger.DeliverySlackLoggerConfig,
	batchCfg logger.DeliveryInternalLogSlackBatchConfig,
) configAdapter {
	return NewConfigAdapter(logger.SlackLoggerConfig{
		TimeConfig:                      cfg.TimeConfig,
		RuntimeConfig:                   cfg.RuntimeConfig,
		LogBodyConfig:                   cfg.LogBodyConfig,
		Timeout:                         cfg.Timeout,
		HTTPClient:                      cfg.HTTPClient,
		Token:                           cfg.Token,
		Channel:                         cfg.Channel,
		ErrorChannel:                    cfg.ErrorChannel,
		WarnChannel:                     cfg.WarnChannel,
		InfoChannel:                     cfg.InfoChannel,
		DebugChannel:                    cfg.DebugChannel,
		Intervals:                       batchCfg.Intervals,
		MaxLogsPerNotification:          batchCfg.MaxLogsPerNotification,
		MaxRestThreshold:                batchCfg.MaxRestThreshold,
		MaxMessageLength:                cfg.MaxMessageLength,
		HandlerConfig:                   cfg.HandlerConfig,
		SlackPostWarnThresholdDuration:  cfg.SlackPostWarnThresholdDuration,
		SlackPostErrorThresholdDuration: cfg.SlackPostErrorThresholdDuration,
	})
}

type deliveryConfigAdapter struct {
	cfg           logger.DeliverySlackLoggerConfig
	timeConfig    loggertime.TimeConfigProvider
	runtimeConfig RuntimeConfigProvider
	logBodyConfig LogBodyConfigProvider
	handlerConfig handler.ConfigProvider
}

func NewDeliveryConfigAdapter(cfg logger.DeliverySlackLoggerConfig) deliveryConfigAdapter {
	// NewConfigAdapterと同じく、構築時点のHTTP client設定をcopyして所有します。
	cfg.HTTPClient = cloneHTTPClient(cfg.HTTPClient)
	return deliveryConfigAdapter{
		cfg:           cfg,
		timeConfig:    loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		runtimeConfig: newRuntimeConfigAdapter(cfg.RuntimeConfig),
		logBodyConfig: newLogBodyConfigAdapter(cfg.LogBodyConfig),
		handlerConfig: handler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

func (a deliveryConfigAdapter) Time() loggertime.TimeConfigProvider {
	return a.timeConfig
}

func (a deliveryConfigAdapter) Runtime() RuntimeConfigProvider {
	return a.runtimeConfig
}

func (a deliveryConfigAdapter) LogBody() LogBodyConfigProvider {
	return a.logBodyConfig
}
func (a deliveryConfigAdapter) Handler() handler.ConfigProvider {
	return a.handlerConfig
}

func (a deliveryConfigAdapter) Timeout() time.Duration {
	return a.cfg.Timeout
}

// HTTPClientは所有しているclient設定を、呼び出しごとに別のcopyとして返します。
func (a deliveryConfigAdapter) HTTPClient() *http.Client {
	return cloneHTTPClient(a.cfg.HTTPClient)
}

func (a deliveryConfigAdapter) Token() string {
	return a.cfg.Token
}

func (a deliveryConfigAdapter) Channel() string {
	return a.cfg.Channel
}

func (a deliveryConfigAdapter) ErrorChannel() string {
	return a.cfg.ErrorChannel
}

func (a deliveryConfigAdapter) WarnChannel() string {
	return a.cfg.WarnChannel
}

func (a deliveryConfigAdapter) InfoChannel() string {
	return a.cfg.InfoChannel
}

func (a deliveryConfigAdapter) DebugChannel() string {
	return a.cfg.DebugChannel
}

func (a deliveryConfigAdapter) MaxMessageLength() int {
	return a.cfg.MaxMessageLength
}

func (a deliveryConfigAdapter) SlackPostWarnThresholdDuration() time.Duration {
	return a.cfg.SlackPostWarnThresholdDuration
}

func (a deliveryConfigAdapter) SlackPostErrorThresholdDuration() time.Duration {
	return a.cfg.SlackPostErrorThresholdDuration
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

func (a configAdapter) Timeout() time.Duration {
	return a.cfg.Timeout
}

// HTTPClientは所有しているclient設定を、呼び出しごとに別のcopyとして返します。
func (a configAdapter) HTTPClient() *http.Client {
	return cloneHTTPClient(a.cfg.HTTPClient)
}

func (a configAdapter) Token() string {
	return a.cfg.Token
}

func (a configAdapter) Channel() string {
	return a.cfg.Channel
}

func (a configAdapter) ErrorChannel() string {
	return a.cfg.ErrorChannel
}

func (a configAdapter) WarnChannel() string {
	return a.cfg.WarnChannel
}

func (a configAdapter) InfoChannel() string {
	return a.cfg.InfoChannel
}

func (a configAdapter) DebugChannel() string {
	return a.cfg.DebugChannel
}

func (a configAdapter) MaxLogsPerNotification() int {
	return a.cfg.MaxLogsPerNotification
}

func (a configAdapter) MaxRestThreshold() time.Duration {
	return a.cfg.MaxRestThreshold
}

func (a configAdapter) Intervals() []time.Duration {
	return slices.Clone(a.cfg.Intervals)
}

func (a configAdapter) MaxMessageLength() int {
	return a.cfg.MaxMessageLength
}

func (a configAdapter) SlackPostWarnThresholdDuration() time.Duration {
	return a.cfg.SlackPostWarnThresholdDuration
}
func (a configAdapter) SlackPostErrorThresholdDuration() time.Duration {
	return a.cfg.SlackPostErrorThresholdDuration
}

func (a configAdapter) Handler() handler.ConfigProvider {
	return a.handlerConfig
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

func NewStackTraceConfigAdapter(cfg SlackLoggerConfigProvider) logging.StackTraceConfigProvider {
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

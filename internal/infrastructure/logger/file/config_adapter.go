package file

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	loggertime "github.com/go-jappe/heterolog/internal/infrastructure/logger/time"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

type configAdapter struct {
	FsstoreConfig             appendlog.ConfigProvider
	OverloadConfig            FileOverloadConfigProvider
	ObjectStorageUploadConfig objectstorage.LogFileUploadConfigProvider
	ArchiveCheckpointConfig   pkgfsstore.ArchiveCheckpointConfig
	LifecycleModeConfig       pkgfsstore.FileLifecycleMode
	TimeConfig                loggertime.TimeConfigProvider
	RuntimeConfig             RuntimeProvider
	LogBodyConfig             LogBodyConfigProvider
	SamplingConfig            sampling.SamplingConfigProvider
	HandlerConfig             handler.ConfigProvider
}

func NewConfigAdapter(cfg logger.FileLoggerConfig) configAdapter {
	return configAdapter{
		TimeConfig:                loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		RuntimeConfig:             newRuntimeConfigAdapter(cfg.RuntimeConfig),
		LogBodyConfig:             newLogBodyConfigAdapter(cfg.LogBodyConfig),
		FsstoreConfig:             appendlog.NewConfigAdapter(cfg.FsstoreConfig),
		OverloadConfig:            fileOverloadConfigAdapter{config: cfg.FsstoreConfig.Overload},
		ObjectStorageUploadConfig: objectstorage.NewLogFileUploadConfigAdapter(cfg.ObjectStorageUploadConfig),
		ArchiveCheckpointConfig:   cfg.ArchiveCheckpointConfig,
		LifecycleModeConfig:       cfg.LifecycleMode,
		SamplingConfig:            sampling.NewConfigProvider(cfg.SamplingConfig, cfg.RuntimeConfig.Live),
		HandlerConfig:             handler.NewConfigAdapter(cfg.HandlerConfig),
	}
}

type deliveryConfigAdapter struct {
	FsstoreConfig       appendlog.FileWriteStoreConfigProvider
	LifecycleModeConfig pkgfsstore.FileLifecycleMode
	TimeConfig          loggertime.TimeConfigProvider
	RuntimeConfig       DeliveryRuntimeProvider
	LogBodyConfig       LogBodyConfigProvider
	HandlerConfig       handler.ConfigProvider
	// ArchiveCheckpointConfigとObjectStorageUploadConfigは、runnerが同じログ系列の送信に使う設定です。
	ArchiveCheckpointConfig   pkgfsstore.ArchiveCheckpointConfig
	ObjectStorageUploadConfig objectstorage.LogFileUploadConfigProvider
}

func NewDeliveryFileLoggerConfigAdapter(cfg logger.DeliveryFileLoggerConfig) deliveryConfigAdapter {
	return deliveryConfigAdapter{
		TimeConfig:                loggertime.NewTimeConfigAdapter(cfg.TimeConfig),
		RuntimeConfig:             newDeliveryRuntimeConfigAdapter(cfg.RuntimeConfig),
		LogBodyConfig:             newLogBodyConfigAdapter(cfg.LogBodyConfig),
		FsstoreConfig:             appendlog.NewDeliveryFileWriteStoreConfigAdapter(cfg.FsstoreConfig),
		LifecycleModeConfig:       cfg.LifecycleMode,
		HandlerConfig:             handler.NewConfigAdapter(cfg.HandlerConfig),
		ArchiveCheckpointConfig:   cfg.LifecycleRunnerSettings.ArchiveCheckpointConfig,
		ObjectStorageUploadConfig: objectstorage.NewLogFileUploadConfigAdapter(cfg.LifecycleRunnerSettings.ObjectStorageUploadConfig),
	}
}

func (a deliveryConfigAdapter) Time() loggertime.TimeConfigProvider {
	return a.TimeConfig
}
func (a deliveryConfigAdapter) Runtime() DeliveryRuntimeProvider {
	return a.RuntimeConfig
}
func (a deliveryConfigAdapter) LogBody() LogBodyConfigProvider {
	return a.LogBodyConfig
}
func (a deliveryConfigAdapter) Fsstore() appendlog.FileWriteStoreConfigProvider {
	return a.FsstoreConfig
}
func (a deliveryConfigAdapter) LifecycleMode() pkgfsstore.FileLifecycleMode {
	// Deliveryの空値はそのまま返します。validationは空値を許可し、writer自身は
	// inline lifecycleを持たないため、専用runnerも明示されない限り保守処理を起動しません。
	return a.LifecycleModeConfig
}
func (a deliveryConfigAdapter) Handler() handler.ConfigProvider {
	return a.HandlerConfig
}
func (a deliveryConfigAdapter) ArchiveCheckpoint() pkgfsstore.ArchiveCheckpointConfig {
	return a.ArchiveCheckpointConfig
}
func (a deliveryConfigAdapter) ObjectStorageUpload() objectstorage.LogFileUploadConfigProvider {
	return a.ObjectStorageUploadConfig
}

func (a configAdapter) Time() loggertime.TimeConfigProvider {
	return a.TimeConfig
}
func (a configAdapter) Runtime() RuntimeProvider {
	return a.RuntimeConfig
}
func (a configAdapter) LogBody() LogBodyConfigProvider {
	return a.LogBodyConfig
}
func (a configAdapter) Fsstore() appendlog.ConfigProvider {
	return a.FsstoreConfig
}
func (a configAdapter) Overload() FileOverloadConfigProvider {
	return a.OverloadConfig
}
func (a configAdapter) ObjectStorageUpload() objectstorage.LogFileUploadConfigProvider {
	return a.ObjectStorageUploadConfig
}
func (a configAdapter) ArchiveCheckpoint() pkgfsstore.ArchiveCheckpointConfig {
	return a.ArchiveCheckpointConfig
}
func (a configAdapter) LifecycleMode() pkgfsstore.FileLifecycleMode {
	// 通常FileLoggerはmodeを省略しても利用できるよう、空値をinlineへ解決します。
	if a.LifecycleModeConfig == "" {
		return pkgfsstore.FileLifecycleModeInline
	}
	return a.LifecycleModeConfig
}
func (a configAdapter) Sampling() sampling.SamplingConfigProvider {
	return a.SamplingConfig
}
func (a configAdapter) Handler() handler.ConfigProvider {
	return a.HandlerConfig
}

type fileOverloadConfigAdapter struct {
	config pkgfsstore.FileArchiveOverloadPolicyConfig
}

func (a fileOverloadConfigAdapter) OverloadPolicy() pkgfsstore.OverloadPolicy {
	return a.config.OverloadPolicy
}
func (a fileOverloadConfigAdapter) SpillDestination() pkgfsstore.SpillDestination {
	return a.config.SpillDestination
}
func (a fileOverloadConfigAdapter) MinAvailableDiskSpace() uint64 {
	return a.config.MinAvailableDiskSpace
}

type runtimeConfigAdapter struct {
	AppNameConfig  string
	IsDebugConfig  bool
	TargetConfig   string
	LogLevelConfig logger.LogLevel
	// LiveConfigを設定した場合は、LogLevelConfigの代わりにその現在のlevelを使います。
	LiveConfig *logger.LiveSettings
}

func newRuntimeConfigAdapter(cfg logger.RuntimeConfig) RuntimeProvider {
	return &runtimeConfigAdapter{
		AppNameConfig:  cfg.AppName,
		IsDebugConfig:  cfg.IsDebug,
		TargetConfig:   cfg.Target,
		LogLevelConfig: cfg.LogLevel,
		LiveConfig:     cfg.Live,
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
	if a.LiveConfig != nil {
		return a.LiveConfig.Level()
	}
	return a.LogLevelConfig
}

type deliveryRuntimeConfigAdapter struct {
	AppNameConfig string
	IsDebugConfig bool
	TargetConfig  string
}

func newDeliveryRuntimeConfigAdapter(cfg logger.DeliveryRuntimeConfig) DeliveryRuntimeProvider {
	return &deliveryRuntimeConfigAdapter{
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

type logBodyConfigAdapter struct {
	LogLineDateFormatConfig        string
	ErrorTraceDepthConfig          int
	ErrorTraceDepthDebugModeConfig int
}

func newLogBodyConfigAdapter(cfg logger.LogBodyConfig) LogBodyConfigProvider {
	return &logBodyConfigAdapter{
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

func NewStackTraceConfigAdapter(isDebug bool, logBody LogBodyConfigProvider) logging.StackTraceConfigProvider {
	return &stackTraceConfigAdapter{
		IsDebugConfig:                  isDebug,
		ErrorTraceDepthDebugModeConfig: logBody.ErrorTraceDepthDebugMode(),
		ErrorTraceDepthConfig:          logBody.ErrorTraceDepth(),
	}
}

func (a stackTraceConfigAdapter) IsDebug() bool {
	return a.IsDebugConfig
}
func (a stackTraceConfigAdapter) ErrorTraceDepthDebugMode() int {
	return a.ErrorTraceDepthDebugModeConfig
}
func (a stackTraceConfigAdapter) ErrorTraceDepth() int {
	return a.ErrorTraceDepthConfig
}

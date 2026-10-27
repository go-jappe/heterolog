package appendlog

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"time"
)

type ConfigAdapter struct {
	FsstoreLayoutConfig    FsstoreLayoutConfigProvider
	LifecycleConfig        FileLifecycleConfigProvider
	PurgeConfig            FilePurgeConfigProvider
	ErrorPurgeConfig       FilePurgeConfigProvider
	DebugPurgeConfig       FilePurgeConfigProvider
	DurationConfig         DurationConfigProvider
	RetryConfig            RetryConfigProvider
	CompressionCodecConfig fsstore.CompressionCodec
}

var _ CompressionCodecProvider = (*ConfigAdapter)(nil)

func NewConfigAdapter(cfg fsstore.Config) ConfigProvider {
	adapter := &ConfigAdapter{
		FsstoreLayoutConfig:    NewFsstoreConfigAdapter(cfg.Layout),
		LifecycleConfig:        NewFileLifecycleConfigAdapter(cfg.Lifecycle),
		PurgeConfig:            NewFilePurgeConfigAdapter(cfg.Lifecycle),
		DurationConfig:         NewDurationConfigAdapter(cfg.Duration),
		RetryConfig:            NewRetryConfigAdapter(cfg.Retry),
		CompressionCodecConfig: cfg.CompressionCodec,
	}
	// error/debugの個別設定がnilなら通常logのPurgeConfigを使うことを、ここで確定します。
	adapter.ErrorPurgeConfig = resolveLogFileRolePurgeConfig(adapter.PurgeConfig, cfg.Lifecycle.ErrorPurge, adapter.Layout(), adapter.Layout().ErrorLogDir())
	adapter.DebugPurgeConfig = resolveLogFileRolePurgeConfig(adapter.PurgeConfig, cfg.Lifecycle.DebugPurge, adapter.Layout(), adapter.Layout().DebugLogDir())
	return adapter
}

func (a *ConfigAdapter) Layout() FsstoreLayoutConfigProvider {
	return a.FsstoreLayoutConfig
}
func (a *ConfigAdapter) Rotation() FileRotationConfigProvider {
	return a.LifecycleConfig
}
func (a *ConfigAdapter) Lifecycle() FileLifecycleConfigProvider {
	return a.LifecycleConfig
}
func (a *ConfigAdapter) Purge() FilePurgeConfigProvider {
	return a.PurgeConfig
}
func (a *ConfigAdapter) ErrorPurge() FilePurgeConfigProvider {
	return a.ErrorPurgeConfig
}
func (a *ConfigAdapter) DebugPurge() FilePurgeConfigProvider {
	return a.DebugPurgeConfig
}
func (a *ConfigAdapter) Duration() DurationConfigProvider {
	return a.DurationConfig
}
func (a *ConfigAdapter) Retry() RetryConfigProvider {
	return a.RetryConfig
}
func (a *ConfigAdapter) CompressionCodec() fsstore.CompressionCodec {
	return a.CompressionCodecConfig
}

type DeliveryFileLifecycleStoreConfigAdapter struct {
	FsstoreLayoutConfig    FsstoreLayoutConfigProvider
	LifecycleConfig        FileLifecycleConfigProvider
	PurgeConfig            FilePurgeConfigProvider
	ErrorPurgeConfig       FilePurgeConfigProvider
	DebugPurgeConfig       FilePurgeConfigProvider
	DurationConfig         DurationConfigProvider
	RetryConfig            RetryConfigProvider
	CompressionCodecConfig fsstore.CompressionCodec
}

var _ CompressionCodecProvider = (*DeliveryFileLifecycleStoreConfigAdapter)(nil)

func NewDeliveryFileLifecycleStoreConfigAdapter(cfg fsstore.DeliveryFileLifecycleStoreConfig) ConfigProvider {
	adapter := &DeliveryFileLifecycleStoreConfigAdapter{
		FsstoreLayoutConfig:    NewFsstoreConfigAdapter(cfg.Layout),
		LifecycleConfig:        NewDeliveryFileLifecycleConfigAdapter(cfg.Lifecycle),
		PurgeConfig:            NewDeliveryFilePurgeConfigAdapter(cfg.Lifecycle),
		DurationConfig:         NewDurationConfigAdapter(cfg.Duration),
		RetryConfig:            NewRetryConfigAdapter(cfg.Retry),
		CompressionCodecConfig: cfg.CompressionCodec,
	}
	// Deliveryも個別設定の省略をここで解決し、利用側へnilを渡しません。
	adapter.ErrorPurgeConfig = resolveLogFileRolePurgeConfig(adapter.PurgeConfig, cfg.Lifecycle.ErrorPurge, adapter.Layout(), adapter.Layout().ErrorLogDir())
	adapter.DebugPurgeConfig = resolveLogFileRolePurgeConfig(adapter.PurgeConfig, cfg.Lifecycle.DebugPurge, adapter.Layout(), adapter.Layout().DebugLogDir())
	return adapter
}

func (a *DeliveryFileLifecycleStoreConfigAdapter) Layout() FsstoreLayoutConfigProvider {
	return a.FsstoreLayoutConfig
}
func (a *DeliveryFileLifecycleStoreConfigAdapter) Rotation() FileRotationConfigProvider {
	return a.LifecycleConfig
}
func (a *DeliveryFileLifecycleStoreConfigAdapter) Lifecycle() FileLifecycleConfigProvider {
	return a.LifecycleConfig
}
func (a *DeliveryFileLifecycleStoreConfigAdapter) Purge() FilePurgeConfigProvider {
	return a.PurgeConfig
}
func (a *DeliveryFileLifecycleStoreConfigAdapter) ErrorPurge() FilePurgeConfigProvider {
	return a.ErrorPurgeConfig
}
func (a *DeliveryFileLifecycleStoreConfigAdapter) DebugPurge() FilePurgeConfigProvider {
	return a.DebugPurgeConfig
}
func (a *DeliveryFileLifecycleStoreConfigAdapter) Duration() DurationConfigProvider {
	return a.DurationConfig
}
func (a *DeliveryFileLifecycleStoreConfigAdapter) Retry() RetryConfigProvider {
	return a.RetryConfig
}
func (a *DeliveryFileLifecycleStoreConfigAdapter) CompressionCodec() fsstore.CompressionCodec {
	return a.CompressionCodecConfig
}

type DeliveryFileWriteStoreConfigAdapter struct {
	FsstoreLayoutConfig FsstoreLayoutConfigProvider
	RotationConfig      FileRotationConfigProvider
	DurationConfig      DurationConfigProvider
	RetryConfig         RetryConfigProvider
}

func NewDeliveryFileWriteStoreConfigAdapter(cfg fsstore.DeliveryFileWriteConfig) FileWriteStoreConfigProvider {
	return &DeliveryFileWriteStoreConfigAdapter{
		FsstoreLayoutConfig: NewFsstoreConfigAdapter(cfg.Layout),
		RotationConfig:      NewFileRotationConfigAdapter(cfg.Rotation),
		DurationConfig:      NewDurationConfigAdapter(cfg.Duration),
		RetryConfig:         NewRetryConfigAdapter(cfg.Retry),
	}
}

func (a *DeliveryFileWriteStoreConfigAdapter) Layout() FsstoreLayoutConfigProvider {
	return a.FsstoreLayoutConfig
}
func (a *DeliveryFileWriteStoreConfigAdapter) Rotation() FileRotationConfigProvider {
	return a.RotationConfig
}
func (a *DeliveryFileWriteStoreConfigAdapter) Duration() DurationConfigProvider {
	return a.DurationConfig
}
func (a *DeliveryFileWriteStoreConfigAdapter) Retry() RetryConfigProvider {
	return a.RetryConfig
}

type FsstoreConfigAdapter struct {
	TargetConfig                       string
	LogPathConfig                      string // ex.) /var/log/{target}/
	LogDirConfig                       string
	ErrorLogDirConfig                  string
	DebugLogDirConfig                  string
	IsSeparateDebugLogDirEnabledConfig bool
	IsSeparateErrorLogsEnabledConfig   bool
	SeparateErrorLogsPrefixConfig      string
	IsSeparateDebugLogsEnabledConfig   bool
	SeparateDebugLogsPrefixConfig      string
	FileNameFormatConfig               string
	FileDateFormatConfig               string
}

func NewFsstoreConfigAdapter(cfg fsstore.FsstoreConfig) FsstoreLayoutConfigProvider {
	// internal logの上書き後の設定から保存先を確定します。共有・分離は空文字の
	// 有無ではなく専用flagで決め、無効時に残っている個別pathは使用しません。
	debugLogDir := cfg.LogDir
	if cfg.IsSeparateDebugLogDirEnabled {
		debugLogDir = cfg.DebugLogDir
	}
	return &FsstoreConfigAdapter{
		TargetConfig:                       cfg.Target,
		LogPathConfig:                      cfg.LogPath,
		LogDirConfig:                       cfg.LogDir,
		ErrorLogDirConfig:                  cfg.ErrorLogDir,
		DebugLogDirConfig:                  debugLogDir,
		IsSeparateDebugLogDirEnabledConfig: cfg.IsSeparateDebugLogDirEnabled,
		IsSeparateErrorLogsEnabledConfig:   cfg.IsSeparateErrorLogsEnabled,
		SeparateErrorLogsPrefixConfig:      cfg.SeparateErrorLogsPrefix,
		IsSeparateDebugLogsEnabledConfig:   cfg.IsSeparateDebugLogsEnabled,
		SeparateDebugLogsPrefixConfig:      cfg.SeparateDebugLogsPrefix,
		FileNameFormatConfig:               cfg.FileNameFormat,
		FileDateFormatConfig:               cfg.FileDateFormat,
	}
}

func (a FsstoreConfigAdapter) Target() string {
	return a.TargetConfig
}

func (a FsstoreConfigAdapter) LogPath() string {
	return a.LogPathConfig
}
func (a FsstoreConfigAdapter) LogDir() string {
	return a.LogDirConfig
}
func (a FsstoreConfigAdapter) ErrorLogDir() string {
	return a.ErrorLogDirConfig
}
func (a FsstoreConfigAdapter) DebugLogDir() string {
	return a.DebugLogDirConfig
}
func (a FsstoreConfigAdapter) IsSeparateDebugLogDirEnabled() bool {
	return a.IsSeparateDebugLogDirEnabledConfig
}
func (a FsstoreConfigAdapter) IsSeparateErrorLogsEnabled() bool {
	return a.IsSeparateErrorLogsEnabledConfig
}
func (a FsstoreConfigAdapter) SeparateErrorLogsPrefix() string {
	return a.SeparateErrorLogsPrefixConfig
}
func (a FsstoreConfigAdapter) IsSeparateDebugLogsEnabled() bool {
	return a.IsSeparateDebugLogsEnabledConfig
}
func (a FsstoreConfigAdapter) SeparateDebugLogsPrefix() string {
	return a.SeparateDebugLogsPrefixConfig
}
func (a FsstoreConfigAdapter) FileNameFormat() string {
	return a.FileNameFormatConfig
}
func (a FsstoreConfigAdapter) FileDateFormat() string {
	return a.FileDateFormatConfig
}

type FileLifecycleConfigAdapter struct {
	EnableRotateConfig          bool
	FileRotationTypeConfig      fsstore.FileRotationType
	FileRotationDayOfWeekConfig time.Weekday
	MaxFileSizeConfig           uint64
	EnableFileCompressConfig    bool
	CompressFormatConfig        fsstore.CompressFormat
}

func NewFileLifecycleConfigAdapter(cfg fsstore.FileLifecycleConfig) FileLifecycleConfigProvider {
	return &FileLifecycleConfigAdapter{
		EnableRotateConfig:          cfg.EnableRotate,
		FileRotationTypeConfig:      cfg.RotationType,
		FileRotationDayOfWeekConfig: cfg.RotationDayOfWeek,
		MaxFileSizeConfig:           cfg.MaxFileSize,
		EnableFileCompressConfig:    cfg.EnableFileCompress,
		CompressFormatConfig:        cfg.CompressFormat,
	}
}

type FileRotationConfigAdapter struct {
	EnableRotateConfig          bool
	FileRotationTypeConfig      fsstore.FileRotationType
	FileRotationDayOfWeekConfig time.Weekday
	MaxFileSizeConfig           uint64
}

func NewFileRotationConfigAdapter(cfg fsstore.FileRotationConfig) FileRotationConfigProvider {
	return &FileRotationConfigAdapter{
		EnableRotateConfig:          cfg.EnableRotate,
		FileRotationTypeConfig:      cfg.RotationType,
		FileRotationDayOfWeekConfig: cfg.RotationDayOfWeek,
		MaxFileSizeConfig:           cfg.MaxFileSize,
	}
}

func (a FileRotationConfigAdapter) EnableRotate() bool {
	return a.EnableRotateConfig
}
func (a FileRotationConfigAdapter) FileRotationType() fsstore.FileRotationType {
	return a.FileRotationTypeConfig
}
func (a FileRotationConfigAdapter) FileRotationDayOfWeek() time.Weekday {
	return a.FileRotationDayOfWeekConfig
}
func (a FileRotationConfigAdapter) MaxFileSize() uint64 {
	return a.MaxFileSizeConfig
}

func (a FileLifecycleConfigAdapter) EnableRotate() bool {
	return a.EnableRotateConfig
}
func (a FileLifecycleConfigAdapter) FileRotationType() fsstore.FileRotationType {
	return a.FileRotationTypeConfig
}
func (a FileLifecycleConfigAdapter) FileRotationDayOfWeek() time.Weekday {
	return a.FileRotationDayOfWeekConfig
}
func (a FileLifecycleConfigAdapter) MaxFileSize() uint64 {
	return a.MaxFileSizeConfig
}
func (a FileLifecycleConfigAdapter) EnableFileCompress() bool {
	return a.EnableFileCompressConfig
}
func (a FileLifecycleConfigAdapter) CompressFormat() fsstore.CompressFormat {
	return a.CompressFormatConfig
}

func NewDeliveryFileLifecycleConfigAdapter(cfg fsstore.DeliveryFileLifecycleConfig) FileLifecycleConfigProvider {
	return &FileLifecycleConfigAdapter{
		EnableRotateConfig:          cfg.EnableRotate,
		FileRotationTypeConfig:      cfg.RotationType,
		FileRotationDayOfWeekConfig: cfg.RotationDayOfWeek,
		MaxFileSizeConfig:           cfg.MaxFileSize,
		EnableFileCompressConfig:    cfg.EnableFileCompress,
		CompressFormatConfig:        cfg.CompressFormat,
	}
}

func NewDeliveryFilePurgeConfigAdapter(cfg fsstore.DeliveryFileLifecycleConfig) FilePurgeConfigProvider {
	return &FilePurgeConfigAdapter{
		UsageCacheTTLConfig:  cfg.UsageCacheTTL,
		MaxFileBackupsConfig: cfg.MaxFileBackups,
		MaxFileAgeConfig:     cfg.MaxFileAge,
		MaxFileDirSizeConfig: cfg.MaxDirSize,
	}
}

type FilePurgeConfigAdapter struct {
	UsageCacheTTLConfig  time.Duration
	MaxFileBackupsConfig int
	MaxFileAgeConfig     int
	MaxFileDirSizeConfig uint64
}

func NewFilePurgeConfigAdapter(cfg fsstore.FileLifecycleConfig) FilePurgeConfigProvider {
	return &FilePurgeConfigAdapter{
		UsageCacheTTLConfig:  cfg.UsageCacheTTL,
		MaxFileBackupsConfig: cfg.MaxFileBackups,
		MaxFileAgeConfig:     cfg.MaxFileAge,
		MaxFileDirSizeConfig: cfg.MaxDirSize,
	}
}

func (a FilePurgeConfigAdapter) UsageCacheTTL() time.Duration {
	return a.UsageCacheTTLConfig
}

func (a FilePurgeConfigAdapter) MaxFileBackups() int {
	return a.MaxFileBackupsConfig
}
func (a FilePurgeConfigAdapter) MaxFileAge() int {
	return a.MaxFileAgeConfig
}
func (a FilePurgeConfigAdapter) MaxDirSize() uint64 {
	return a.MaxFileDirSizeConfig
}

type DurationConfigAdapter struct {
	CompressWarnThresholdDurationConfig   time.Duration
	CompressErrorThresholdDurationConfig  time.Duration
	WriteFileWarnThresholdDurationConfig  time.Duration
	WriteFileErrorThresholdDurationConfig time.Duration
	WriteFileLatencyEnabledConfig         bool
}

func NewDurationConfigAdapter(cfg fsstore.DurationConfig) DurationConfigProvider {
	return &DurationConfigAdapter{
		CompressWarnThresholdDurationConfig:   cfg.CompressWarnThresholdDuration,
		CompressErrorThresholdDurationConfig:  cfg.CompressErrorThresholdDuration,
		WriteFileWarnThresholdDurationConfig:  cfg.WriteFileWarnThresholdDuration,
		WriteFileErrorThresholdDurationConfig: cfg.WriteFileErrorThresholdDuration,
		WriteFileLatencyEnabledConfig:         cfg.WriteFileLatencyEnabled,
	}
}

func (a DurationConfigAdapter) CompressWarnThresholdDuration() time.Duration {
	return a.CompressWarnThresholdDurationConfig
}
func (a DurationConfigAdapter) CompressErrorThresholdDuration() time.Duration {
	return a.CompressErrorThresholdDurationConfig
}
func (a DurationConfigAdapter) WriteFileWarnThresholdDuration() time.Duration {
	return a.WriteFileWarnThresholdDurationConfig
}
func (a DurationConfigAdapter) WriteFileErrorThresholdDuration() time.Duration {
	return a.WriteFileErrorThresholdDurationConfig
}
func (a DurationConfigAdapter) WriteFileLatencyEnabled() bool {
	return a.WriteFileLatencyEnabledConfig
}

type RetryConfigAdapter struct {
	MaxWriteRetriesConfig            int
	FileOperationRetryWaitTimeConfig time.Duration
}

func NewRetryConfigAdapter(cfg fsstore.RetryConfig) RetryConfigProvider {
	return &RetryConfigAdapter{
		MaxWriteRetriesConfig:            cfg.MaxWriteRetries,
		FileOperationRetryWaitTimeConfig: cfg.FileOperationRetryWaitTime,
	}
}

func (a RetryConfigAdapter) MaxWriteRetries() int {
	return a.MaxWriteRetriesConfig
}

func (a RetryConfigAdapter) FileOperationRetryWaitTime() time.Duration {
	return a.FileOperationRetryWaitTimeConfig
}

type NameLogPatternConfigAdapter struct {
	cfg FileWriteStoreConfigProvider
}

func NewNameLogPatternConfigAdapter(p FileWriteStoreConfigProvider) logpattern.ConfigProvider {
	return &NameLogPatternConfigAdapter{
		cfg: p,
	}
}

func (p *NameLogPatternConfigAdapter) RotationType() fsstore.FileRotationType {
	return p.cfg.Rotation().FileRotationType()
}
func (p *NameLogPatternConfigAdapter) RotationDayOfWeek() time.Weekday {
	return p.cfg.Rotation().FileRotationDayOfWeek()
}
func (p *NameLogPatternConfigAdapter) FileDateFormat() string {
	return p.cfg.Layout().FileDateFormat()
}
func (p *NameLogPatternConfigAdapter) FileNameFormat() string {
	return p.cfg.Layout().FileNameFormat()
}
func (p *NameLogPatternConfigAdapter) MaxFileSize() uint64 {
	return p.cfg.Rotation().MaxFileSize()
}

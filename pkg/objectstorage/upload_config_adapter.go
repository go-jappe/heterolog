package objectstorage

import (
	"slices"
	"time"

	"github.com/go-jappe/heterolog/pkg/failurestore"
)

type logFileUploadConfigAdapter struct {
	enabled      bool
	defaultRoute UploadTargetConfigProvider
	errorRoute   UploadTargetConfigProvider
	debugRoute   UploadTargetConfigProvider
	spool        CompressedLogFileUploadSpoolConfigProvider
}

var _ LogFileUploadConfigProvider = logFileUploadConfigAdapter{}

// NewLogFileUploadConfigAdapterは、構築側の設定をproviderへ変換します。
// 値と宛先一覧をコピーし、設定の補完や検証は行いません。spoolは構築済みproviderを引き継ぎます。
func NewLogFileUploadConfigAdapter(config LogFileUploadConfig) LogFileUploadConfigProvider {
	return logFileUploadConfigAdapter{
		enabled:      config.Enabled,
		defaultRoute: NewUploadTargetConfigAdapter(config.Default),
		errorRoute:   NewUploadTargetConfigAdapter(config.Error),
		debugRoute:   NewUploadTargetConfigAdapter(config.Debug),
		spool:        config.CompressedLogFileUploadSpool,
	}
}

func (a logFileUploadConfigAdapter) Enabled() bool { return a.enabled }
func (a logFileUploadConfigAdapter) Default() UploadTargetConfigProvider {
	return a.defaultRoute
}
func (a logFileUploadConfigAdapter) Error() UploadTargetConfigProvider { return a.errorRoute }
func (a logFileUploadConfigAdapter) Debug() UploadTargetConfigProvider { return a.debugRoute }
func (a logFileUploadConfigAdapter) CompressedLogFileUploadSpool() CompressedLogFileUploadSpoolConfigProvider {
	return a.spool
}
func (a logFileUploadConfigAdapter) UsesCompressedLogFileUploadSpool(separateError, separateDebug bool) bool {
	if !a.enabled {
		return false
	}
	for _, target := range a.physicalRoutes(separateError, separateDebug) {
		if target.Enabled() && slices.Contains(target.CompressedLogFileUploadFailureStore().DurableType(), failurestore.DurableTypeSpool) {
			return true
		}
	}
	return false
}
func (a logFileUploadConfigAdapter) CompressedLogFileUploadSpoolServices(separateError, separateDebug bool) []UploadTarget {
	if !a.enabled {
		return nil
	}
	var services []UploadTarget
	for _, target := range a.physicalRoutes(separateError, separateDebug) {
		for _, service := range target.CompressedLogFileUploadSpoolServices() {
			if !slices.Contains(services, service) {
				services = append(services, service)
			}
		}
	}
	return services
}
func (a logFileUploadConfigAdapter) physicalRoutes(separateError, separateDebug bool) []UploadTargetConfigProvider {
	routes := []UploadTargetConfigProvider{a.defaultRoute}
	if separateError {
		routes = append(routes, a.errorRoute)
	}
	if separateDebug {
		routes = append(routes, a.debugRoute)
	}
	return routes
}

type uploadTargetConfigAdapter struct {
	enabled        bool
	targets        []UploadTarget
	s3Destinations []S3ConfigProvider
	failureStore   CompressedLogFileUploadFailureStoreConfigProvider
	spool          CompressedLogFileUploadSpoolConfigProvider
}

var _ UploadTargetConfigProvider = uploadTargetConfigAdapter{}

// NewUploadTargetConfigAdapterは、一つのログ系列の送信・退避設定をproviderへ変換します。
// S3の単一指定と複数指定の選択は元の設定と同じ規則に従い、宛先一覧と設定値をコピーします。
func NewUploadTargetConfigAdapter(config UploadTargetConfig) UploadTargetConfigProvider {
	configs := config.S3DestinationConfigs()
	destinations := make([]S3ConfigProvider, len(configs))
	for index, destination := range configs {
		destinations[index] = NewS3ConfigAdapter(destination)
	}
	return uploadTargetConfigAdapter{
		enabled:        config.Enabled,
		targets:        slices.Clone(config.Targets),
		s3Destinations: destinations,
		failureStore:   NewCompressedLogFileUploadFailureStoreConfigAdapter(config.CompressedLogFileUploadFailureStore),
		spool:          config.CompressedLogFileUploadSpool,
	}
}

func (a uploadTargetConfigAdapter) Enabled() bool { return a.enabled }
func (a uploadTargetConfigAdapter) Targets() []UploadTarget {
	return slices.Clone(a.targets)
}
func (a uploadTargetConfigAdapter) S3DestinationConfigs() []S3ConfigProvider {
	return slices.Clone(a.s3Destinations)
}
func (a uploadTargetConfigAdapter) CompressedLogFileUploadFailureStore() CompressedLogFileUploadFailureStoreConfigProvider {
	return a.failureStore
}
func (a uploadTargetConfigAdapter) CompressedLogFileUploadSpool() CompressedLogFileUploadSpoolConfigProvider {
	return a.spool
}
func (a uploadTargetConfigAdapter) Has(target UploadTarget) bool {
	return slices.Contains(a.targets, target)
}
func (a uploadTargetConfigAdapter) CompressedLogFileUploadSpoolServices() []UploadTarget {
	if !a.enabled || !slices.Contains(a.failureStore.DurableType(), failurestore.DurableTypeSpool) {
		return nil
	}
	var services []UploadTarget
	for _, target := range a.targets {
		if !slices.Contains(services, target) {
			services = append(services, target)
		}
	}
	return services
}

type s3ConfigAdapter struct{ config S3Config }

var _ S3ConfigProvider = s3ConfigAdapter{}

// NewS3ConfigAdapterは、一つのS3宛先の設定値をコピーしてproviderへ変換します。
func NewS3ConfigAdapter(config S3Config) S3ConfigProvider {
	return s3ConfigAdapter{config: config}
}

func (a s3ConfigAdapter) Region() string          { return a.config.Region }
func (a s3ConfigAdapter) Endpoint() string        { return a.config.Endpoint }
func (a s3ConfigAdapter) AccessKeyID() string     { return a.config.AccessKeyID }
func (a s3ConfigAdapter) SecretAccessKey() string { return a.config.SecretAccessKey }
func (a s3ConfigAdapter) SessionToken() string    { return a.config.SessionToken }
func (a s3ConfigAdapter) Bucket() string          { return a.config.Bucket }
func (a s3ConfigAdapter) Prefix() string          { return a.config.Prefix }
func (a s3ConfigAdapter) ForcePathStyle() bool    { return a.config.ForcePathStyle }
func (a s3ConfigAdapter) Timeout() time.Duration  { return a.config.Timeout }

// CompressedLogFileUploadFailureStoreConfigAdapterは、圧縮ログの退避方式と保存先の設定を保持します。
type CompressedLogFileUploadFailureStoreConfigAdapter struct {
	config CompressedLogFileUploadFailureStoreConfig
}

var _ CompressedLogFileUploadFailureStoreConfigProvider = CompressedLogFileUploadFailureStoreConfigAdapter{}

// NewCompressedLogFileUploadFailureStoreConfigAdapterは、退避設定と保存方式の一覧をコピーします。
// 値の補完や検証は行いません。
func NewCompressedLogFileUploadFailureStoreConfigAdapter(config CompressedLogFileUploadFailureStoreConfig) CompressedLogFileUploadFailureStoreConfigAdapter {
	config.DurableType = slices.Clone(config.DurableType)
	return CompressedLogFileUploadFailureStoreConfigAdapter{config: config}
}

func (a CompressedLogFileUploadFailureStoreConfigAdapter) DurableType() []failurestore.DurableType {
	return slices.Clone(a.config.DurableType)
}
func (a CompressedLogFileUploadFailureStoreConfigAdapter) FileArchiveConfig() CompressedLogFileUploadFailureArchiveConfigProvider {
	return NewCompressedLogFileUploadFailureArchiveConfigAdapter(a.config.DurableStoreConfig.FileArchiveConfig)
}

// CompressedLogFileUploadFailureArchiveConfigAdapterは、再送せず保管する圧縮ログの保存先と保持設定を保持します。
type CompressedLogFileUploadFailureArchiveConfigAdapter struct {
	config CompressedLogFileUploadFailureArchiveConfig
}

var _ CompressedLogFileUploadFailureArchiveConfigProvider = CompressedLogFileUploadFailureArchiveConfigAdapter{}

// NewCompressedLogFileUploadFailureArchiveConfigAdapterは、archiveの保存先・周期・保持設定をコピーします。
func NewCompressedLogFileUploadFailureArchiveConfigAdapter(config CompressedLogFileUploadFailureArchiveConfig) CompressedLogFileUploadFailureArchiveConfigAdapter {
	return CompressedLogFileUploadFailureArchiveConfigAdapter{config: config}
}

func (a CompressedLogFileUploadFailureArchiveConfigAdapter) RootPath() string {
	return a.config.RootPath
}
func (a CompressedLogFileUploadFailureArchiveConfigAdapter) LoggerContract() string {
	return a.config.LoggerContract
}
func (a CompressedLogFileUploadFailureArchiveConfigAdapter) LifecycleInterval() time.Duration {
	return a.config.LifecycleInterval
}
func (a CompressedLogFileUploadFailureArchiveConfigAdapter) RetryablePurge() CompressedLogFileUploadFailureArchivePurgeConfig {
	return a.config.Retryable
}
func (a CompressedLogFileUploadFailureArchiveConfigAdapter) PermanentPurge() CompressedLogFileUploadFailureArchivePurgeConfig {
	return a.config.Permanent
}

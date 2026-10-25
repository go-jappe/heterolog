package logger

import (
	"errors"
	"fmt"
	"slices"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// compressedLogSpoolRuntimesはtop-level loggerが終了時に解放するruntimeだけを保持します。
// 構築時の設定やrootの照合表は保持せず、各runtimeを一度だけCloseします。
type compressedLogSpoolRuntimes struct {
	runtimes []*compressedlog.RootRuntime
}

// newCompressedLogSpoolRuntimeBuilderは有効な送信経路が使うrootを構築時に確保します。
// 共有の判定はBuilderが解決済みrootと管理設定で行い、設定の個体には依存しません。
func newCompressedLogSpoolRuntimeBuilder(uploads ...objectstorage.LogFileUploadConfig) (*compressedlog.RootRuntimeBuilder, error) {
	builder := compressedlog.NewRootRuntimeBuilder()
	for _, upload := range uploads {
		if _, err := buildCompressedLogSpoolRuntimes(upload, builder); err != nil {
			return nil, errors.Join(err, builder.Close())
		}
	}
	return builder, nil
}

// buildCompressedLogSpoolRuntimesは構築中の共有Builderから、この送信経路が使うruntimeを取得します。
// 呼び出し元は返されたruntimeを利用先へ直接渡し、Builderはlogger構築後に保持しません。
func buildCompressedLogSpoolRuntimes(upload objectstorage.LogFileUploadConfig, builders ...*compressedlog.RootRuntimeBuilder) (compressedlog.RootRuntimes, error) {
	if len(builders) == 0 || builders[0] == nil || !upload.UsesCompressedLogFileUploadSpool(true, true) {
		return nil, nil
	}
	if upload.CompressedLogFileUploadSpool == nil {
		return nil, fmt.Errorf("%w: spool config provider is nil", compressedlog.ErrInvalidCompressedLogFileUploadSpoolConfig)
	}
	configs := compressedlog.NewConfigAdapters(upload.CompressedLogFileUploadSpool, upload.CompressedLogFileUploadSpoolServices(true, true))
	return builders[0].Build(configs...)
}

func (r *compressedLogSpoolRuntimes) Close() error {
	if r == nil {
		return nil
	}
	var errs []error
	for _, runtime := range r.runtimes {
		err := runtime.Close()
		// 強制終了の待機解除を資源の終了と扱わず、親の終了workerは実際の解放まで待ちます。
		// 通常のowner解放は、登録済みrouteが残っていれば従来どおり戻れます。
		loggercommon.WaitForForcedShutdown(runtime, err)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func newLoggerCompressedLogSpoolRuntimeBuilder(cfg pkglogger.LoggerConfig) (*compressedlog.RootRuntimeBuilder, error) {
	var uploads []objectstorage.LogFileUploadConfig
	if baseFileRuntimeEnabled(cfg) && cfg.File.LifecycleMode != pkgfsstore.FileLifecycleModeDisable {
		uploads = append(uploads, activeCompressedLogSpoolUpload(cfg.File.ObjectStorageUploadConfig, cfg.File.FsstoreConfig.Layout))
	}
	if cfg.InternalLogDestinations != nil && cfg.InternalLogDestinations.File != nil &&
		cfg.InternalLogDestinations.File.LifecycleMode != pkgfsstore.FileLifecycleModeDisable {
		uploads = append(uploads, activeCompressedLogSpoolUpload(cfg.InternalLogDestinations.File.ObjectStorageUploadConfig, cfg.InternalLogDestinations.File.FsstoreConfig.Layout))
	}
	for _, backend := range pkglogger.AllLogOutputDestinations() {
		if !logOutputEnabled(cfg.LogOutputDestinations, backend) {
			continue
		}
		failure, exists := spoolFailureStoreConfig(backend, cfg)
		if exists && slices.Contains(failure.DurableType, durable.DurableTypeFileArchive) {
			uploads = append(uploads, failureArchiveCompressedLogSpoolUpload(failure.DurableStoreConfig.FileArchiveConfig))
		}
	}
	return newCompressedLogSpoolRuntimeBuilder(uploads...)
}

func newDeliveryCompressedLogSpoolRuntimeBuilder(cfg pkglogger.DeliveryLoggerConfig) (*compressedlog.RootRuntimeBuilder, error) {
	var uploads []objectstorage.LogFileUploadConfig
	if baseDeliveryFileRuntimeEnabled(cfg) && cfg.File.LifecycleMode == pkgfsstore.FileLifecycleModeRunner {
		uploads = append(uploads, activeCompressedLogSpoolUpload(cfg.File.LifecycleRunnerSettings.ObjectStorageUploadConfig, cfg.File.FsstoreConfig.Layout))
	}
	if cfg.InternalLogDestinations != nil && cfg.InternalLogDestinations.File != nil &&
		cfg.InternalLogDestinations.File.LifecycleMode == pkgfsstore.FileLifecycleModeRunner {
		uploads = append(uploads, activeCompressedLogSpoolUpload(cfg.InternalLogDestinations.File.LifecycleRunnerSettings.ObjectStorageUploadConfig, cfg.InternalLogDestinations.File.FsstoreConfig.Layout))
	}
	for _, backend := range pkglogger.AllLogOutputDestinations() {
		if !logOutputEnabled(cfg.LogOutputDestinations, backend) {
			continue
		}
		failure, exists := deliveryFailureStoreForCheckpoint(backend, cfg)
		if exists && slices.Contains(failure.DurableType, durable.DurableTypeFileArchive) {
			archive := failure.FileArchiveConfig
			uploads = append(uploads, objectstorage.LogFileUploadConfig{
				Enabled:                      archive.ObjectStorageUploadConfig.Enabled && archive.LifecycleMode != pkgfsstore.FileLifecycleModeDisable,
				Default:                      archive.ObjectStorageUploadConfig,
				CompressedLogFileUploadSpool: archive.ObjectStorageUploadConfig.CompressedLogFileUploadSpool,
			})
		}
	}
	return newCompressedLogSpoolRuntimeBuilder(uploads...)
}

// failureArchiveCompressedLogSpoolUploadは、.jsonl系列の単一送信設定を共有rootの構築へ渡します。
func failureArchiveCompressedLogSpoolUpload(cfg durable.FileArchiveConfig) objectstorage.LogFileUploadConfig {
	upload := cfg.ObjectStorageUploadConfig
	return objectstorage.LogFileUploadConfig{
		Enabled:                      upload.Enabled && cfg.LifecycleMode != pkgfsstore.FileLifecycleModeDisable,
		Default:                      upload,
		CompressedLogFileUploadSpool: upload.CompressedLogFileUploadSpool,
	}
}

// 通常ログとbackend送信失敗archiveは、同じ解決済みrootに対応するruntimeを借用します。
func buildCompressedLogSpoolRuntimesForFailureStore(cfg loggerdurable.FailureStoreConfigProvider, builders ...*compressedlog.RootRuntimeBuilder) (compressedlog.RootRuntimes, error) {
	if cfg == nil || !slices.Contains(cfg.DurableType(), durable.DurableTypeFileArchive) || cfg.DurableStoreConfig() == nil || cfg.DurableStoreConfig().FileArchiveConfig() == nil {
		return nil, nil
	}
	archive := cfg.DurableStoreConfig().FileArchiveConfig()
	upload := archive.ObjectStorageUpload()
	return buildCompressedLogSpoolRuntimes(objectstorage.LogFileUploadConfig{
		Enabled: upload.Enabled && archive.LifecycleMode() != pkgfsstore.FileLifecycleModeDisable,
		Default: upload, CompressedLogFileUploadSpool: upload.CompressedLogFileUploadSpool,
	}, builders...)
}

func buildCompressedLogSpoolRuntimesForDeliveryFailureStore(cfg loggerdurable.DeliveryFailureStoreConfigProvider, builders ...*compressedlog.RootRuntimeBuilder) (compressedlog.RootRuntimes, error) {
	if cfg == nil || !slices.Contains(cfg.DurableType(), durable.DurableTypeFileArchive) || cfg.DeliveryDurableStoreConfig() == nil || cfg.DeliveryDurableStoreConfig().FileArchiveConfig() == nil {
		return nil, nil
	}
	archive := cfg.DeliveryDurableStoreConfig().FileArchiveConfig()
	upload := archive.ObjectStorageUpload()
	return buildCompressedLogSpoolRuntimes(objectstorage.LogFileUploadConfig{
		Enabled: upload.Enabled && archive.LifecycleMode() != pkgfsstore.FileLifecycleModeDisable,
		Default: upload, CompressedLogFileUploadSpool: upload.CompressedLogFileUploadSpool,
	}, builders...)
}

func activeCompressedLogSpoolUpload(upload objectstorage.LogFileUploadConfig, layout pkgfsstore.FsstoreConfig) objectstorage.LogFileUploadConfig {
	if !layout.IsSeparateErrorLogsEnabled {
		upload.Error.Enabled = false
	}
	if !layout.IsSeparateDebugLogsEnabled {
		upload.Debug.Enabled = false
	}
	return upload
}

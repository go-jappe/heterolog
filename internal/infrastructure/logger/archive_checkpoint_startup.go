package logger

import (
	"slices"

	commoncheckpoint "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	objectstoragevalidation "github.com/go-jappe/heterolog/pkg/objectstorage/validation"
)

// ArchiveCheckpointDirectoryErrorは、logger構築時に共通のcheckpoint保存先を準備できなかった
// ことを表します。errors.AsでDirectoryを取り出せ、Errは準備処理の原因です。Error()は
// 原因の文言をそのまま返します。
type ArchiveCheckpointDirectoryError struct {
	Directory string
	Err       error
}

func (e *ArchiveCheckpointDirectoryError) Error() string { return e.Err.Error() }

func (e *ArchiveCheckpointDirectoryError) Unwrap() error { return e.Err }

// prepareLoggerArchiveCheckpointDirectoryは、アプリケーションの起動時に
// 構築するloggerで圧縮ファイルをobjectstorageへuploadする機能を使う場合だけ、
// workerの開始前に共通のcheckpoint保存先のディレクトリを準備（存在しないディレクトリを作成、パスの精査）します。
// 全系列が同じ保存先を使うため、
// 最初の有効なupload設定から一件だけ準備し、directory一覧の収集や重複排除は行いません。
// config検証後に呼び出します。
// 有効なupload設定がなければ、checkpoint保存先ディレクトリの検査・作成は行いません。
func prepareLoggerArchiveCheckpointDirectory(cfg pkglogger.LoggerConfig) error {
	if baseFileRuntimeEnabled(cfg) && logFileUploadsEnabled(cfg.File) {
		return prepareLogFileCheckpointDirectory(cfg.File.ArchiveCheckpointConfig.Directory, cfg.File.ObjectStorageUploadConfig, cfg.File.FsstoreConfig.Layout)
	}
	if cfg.InternalLogDestinations != nil && logFileUploadsEnabled(cfg.InternalLogDestinations.File) {
		internal := cfg.InternalLogDestinations.File
		return prepareLogFileCheckpointDirectory(internal.ArchiveCheckpointConfig.Directory, internal.ObjectStorageUploadConfig, internal.FsstoreConfig.Layout)
	}
	for _, backend := range pkglogger.AllLogOutputDestinations() {
		if !logOutputEnabled(cfg.LogOutputDestinations, backend) {
			continue
		}
		failure, exists := spoolFailureStoreConfig(backend, cfg)
		if !exists || !slices.Contains(failure.DurableType, durable.DurableTypeFileArchive) {
			continue
		}
		archive := failure.DurableStoreConfig.FileArchiveConfig
		if archiveUploadLifecycleEnabled(archive.LifecycleMode) && archive.ObjectStorageUploadConfig.Enabled {
			if err := objectstoragevalidation.ValidateUploadTargetConfig("failure archive", archive.ObjectStorageUploadConfig); err != nil {
				return err
			}
			return prepareArchiveCheckpointDirectory(archive.ArchiveCheckpointConfig.Directory)
		}
	}
	return nil
}

// prepareDeliveryArchiveCheckpointDirectoryも保存先は一つです。Deliveryの通常・内部
// fileはrunnerの場合だけuploadし、backendのfailure archiveはinlineにも対応します。
func prepareDeliveryArchiveCheckpointDirectory(cfg pkglogger.DeliveryLoggerConfig) error {
	if baseDeliveryFileRuntimeEnabled(cfg) && deliveryLogFileUploadsEnabled(cfg.File) {
		return prepareLogFileCheckpointDirectory(cfg.File.LifecycleRunnerSettings.ArchiveCheckpointConfig.Directory, cfg.File.LifecycleRunnerSettings.ObjectStorageUploadConfig, cfg.File.FsstoreConfig.Layout)
	}
	if cfg.InternalLogDestinations != nil && deliveryLogFileUploadsEnabled(cfg.InternalLogDestinations.File) {
		internal := cfg.InternalLogDestinations.File
		return prepareLogFileCheckpointDirectory(internal.LifecycleRunnerSettings.ArchiveCheckpointConfig.Directory, internal.LifecycleRunnerSettings.ObjectStorageUploadConfig, internal.FsstoreConfig.Layout)
	}
	for _, backend := range pkglogger.AllLogOutputDestinations() {
		if !logOutputEnabled(cfg.LogOutputDestinations, backend) {
			continue
		}
		failure, exists := deliveryFailureStoreForCheckpoint(backend, cfg)
		if !exists || !slices.Contains(failure.DurableType, durable.DurableTypeFileArchive) {
			continue
		}
		archive := failure.FileArchiveConfig
		if archiveUploadLifecycleEnabled(archive.LifecycleMode) && archive.ObjectStorageUploadConfig.Enabled {
			if err := objectstoragevalidation.ValidateUploadTargetConfig("delivery failure archive", archive.ObjectStorageUploadConfig); err != nil {
				return err
			}
			return prepareArchiveCheckpointDirectory(archive.ArchiveCheckpointConfig.Directory)
		}
	}
	return nil
}

// prepareLogFileCheckpointDirectoryは、upload設定の誤りをfilesystem操作より先に返します。
func prepareLogFileCheckpointDirectory(directory string, upload objectstorage.LogFileUploadConfig, layout pkgfsstore.FsstoreConfig) error {
	if err := objectstoragevalidation.ValidateLogFileUploadConfig("file logger", upload, layout.IsSeparateErrorLogsEnabled, layout.IsSeparateDebugLogsEnabled); err != nil {
		return err
	}
	return prepareArchiveCheckpointDirectory(directory)
}

// prepareArchiveCheckpointDirectoryは、checkpoint保存先の準備の失敗を発生元のdirectoryと
// ともに返します。
func prepareArchiveCheckpointDirectory(directory string) error {
	if err := commoncheckpoint.PrepareDirectory(directory); err != nil {
		return &ArchiveCheckpointDirectoryError{Directory: directory, Err: err}
	}
	return nil
}

func logFileUploadsEnabled(cfg *pkglogger.FileLoggerConfig) bool {
	return cfg != nil && archiveUploadLifecycleEnabled(cfg.LifecycleMode) &&
		fileUploadRoleEnabled(cfg.ObjectStorageUploadConfig, cfg.FsstoreConfig.Layout)
}

func deliveryLogFileUploadsEnabled(cfg *pkglogger.DeliveryFileLoggerConfig) bool {
	return cfg != nil && cfg.LifecycleMode == pkgfsstore.FileLifecycleModeRunner &&
		fileUploadRoleEnabled(cfg.LifecycleRunnerSettings.ObjectStorageUploadConfig, cfg.FsstoreConfig.Layout)
}

func fileUploadRoleEnabled(upload objectstorage.LogFileUploadConfig, layout pkgfsstore.FsstoreConfig) bool {
	return upload.Enabled && (upload.Default.Enabled ||
		layout.IsSeparateErrorLogsEnabled && upload.Error.Enabled ||
		layout.IsSeparateDebugLogsEnabled && upload.Debug.Enabled)
}

func archiveUploadLifecycleEnabled(mode pkgfsstore.FileLifecycleMode) bool {
	// 直接DTOを渡す利用では、既存のFileLogger／failure archiveと同じく空値はinlineです。
	switch mode {
	case "", pkgfsstore.FileLifecycleModeInline, pkgfsstore.FileLifecycleModeRunner:
		return true
	default:
		return false
	}
}

func deliveryFailureStoreForCheckpoint(backend pkglogger.LogOutputDestination, cfg pkglogger.DeliveryLoggerConfig) (durable.DeliveryFailureStoreConfig, bool) {
	switch backend {
	case pkglogger.LogOutputOpenSearch:
		if cfg.OpenSearch != nil {
			return cfg.OpenSearch.FailureStoreConfig, true
		}
	case pkglogger.LogOutputLoki:
		if cfg.Loki != nil {
			return cfg.Loki.FailureStoreConfig, true
		}
	case pkglogger.LogOutputCloudWatchLogs:
		if cfg.CloudWatchLogs != nil {
			return cfg.CloudWatchLogs.FailureStoreConfig, true
		}
	case pkglogger.LogOutputClickHouse:
		if cfg.ClickHouse != nil {
			return cfg.ClickHouse.FailureStoreConfig, true
		}
	}
	return durable.DeliveryFailureStoreConfig{}, false
}

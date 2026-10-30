package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	configappendlogrunner "github.com/go-jappe/heterolog/internal/config/appendlogrunner"
	configclickhouse "github.com/go-jappe/heterolog/internal/config/clickhouse"
	configcloudwatchlogs "github.com/go-jappe/heterolog/internal/config/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	configloki "github.com/go-jappe/heterolog/internal/config/loki"
	"github.com/go-jappe/heterolog/internal/config/objectstoragecompressedlog"
	configopensearch "github.com/go-jappe/heterolog/internal/config/opensearch"
	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/otel"
	"github.com/go-jappe/heterolog/pkg/path/appstatepath"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	pkgspoollifecycle "github.com/go-jappe/heterolog/pkg/spool/spoollifecycle"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	pkgspoolsupervisor "github.com/go-jappe/heterolog/pkg/spool/spoolsupervisor"
)

type archiveCheckpointSourcePrefix string

const (
	checkpointSourceLoggerFile         archiveCheckpointSourcePrefix = "logger_file"
	checkpointSourceAsyncLoggerFile    archiveCheckpointSourcePrefix = "async_logger_file"
	checkpointSourceDeliveryLoggerFile archiveCheckpointSourcePrefix = "delivery_logger_file"

	checkpointSourceLoggerOpenSearch             archiveCheckpointSourcePrefix = "logger_opensearch_failure_archive"
	checkpointSourceAsyncLoggerOpenSearch        archiveCheckpointSourcePrefix = "async_logger_opensearch_failure_archive"
	checkpointSourceDeliveryLoggerOpenSearch     archiveCheckpointSourcePrefix = "delivery_logger_opensearch_failure_archive"
	checkpointSourceLoggerLoki                   archiveCheckpointSourcePrefix = "logger_loki_failure_archive"
	checkpointSourceTracesOTLP                   archiveCheckpointSourcePrefix = "telemetry_otlp_traces_failure_archive"
	checkpointSourceTracesOpenSearch             archiveCheckpointSourcePrefix = "telemetry_opensearch_traces_failure_archive"
	checkpointSourceTracesClickHouse             archiveCheckpointSourcePrefix = "telemetry_clickhouse_traces_failure_archive"
	checkpointSourceMetricsOTLP                  archiveCheckpointSourcePrefix = "telemetry_otlp_metrics_failure_archive"
	checkpointSourceMetricsVictoriaMetrics       archiveCheckpointSourcePrefix = "telemetry_victoriametrics_metrics_failure_archive"
	checkpointSourceAsyncLoggerLoki              archiveCheckpointSourcePrefix = "async_logger_loki_failure_archive"
	checkpointSourceDeliveryLoggerLoki           archiveCheckpointSourcePrefix = "delivery_logger_loki_failure_archive"
	checkpointSourceLoggerCloudWatchLogs         archiveCheckpointSourcePrefix = "logger_cloudwatchlogs_failure_archive"
	checkpointSourceAsyncLoggerCloudWatchLogs    archiveCheckpointSourcePrefix = "async_logger_cloudwatchlogs_failure_archive"
	checkpointSourceDeliveryLoggerCloudWatchLogs archiveCheckpointSourcePrefix = "delivery_logger_cloudwatchlogs_failure_archive"
	checkpointSourceLoggerClickHouse             archiveCheckpointSourcePrefix = "logger_clickhouse_failure_archive"
	checkpointSourceAsyncLoggerClickHouse        archiveCheckpointSourcePrefix = "async_logger_clickhouse_failure_archive"
	checkpointSourceDeliveryLoggerClickHouse     archiveCheckpointSourcePrefix = "delivery_logger_clickhouse_failure_archive"
)

func newArchiveCheckpointConfig(
	a *AppConfig,
	source archiveCheckpointSourcePrefix,
) fsstore.ArchiveCheckpointConfig {
	return fsstore.ArchiveCheckpointConfig{
		Directory: appstatepath.ArchiveCheckpointDirectory(
			a.FsstoreConfig.AppStatePath,
			a.RuntimeConfig.Target,
		),
		SourcePrefix:                 string(source),
		AutoRecoverMissingCheckpoint: a.ArchiveCheckpointAutoRecovery,
	}
}

// upload失敗時の保存先設定を組み込んで返却
func attachCompressedLogFileUploadFailureStoreForUploadTarget(
	a *AppConfig,
	upload objectstorage.UploadTargetConfig,
	contract logger.LoggerContractKind,
) objectstorage.UploadTargetConfig {
	if a == nil || !upload.Enabled {
		return upload
	}
	failureStoreConfig := a.CompressedLogFileUploadFailureStore.Standard
	if contract == logger.LoggerContractDelivery {
		failureStoreConfig = a.CompressedLogFileUploadFailureStore.Delivery
	}
	if len(failureStoreConfig.DurableType) == 0 {
		return upload
	}

	failureStore := objectstorage.CompressedLogFileUploadFailureStoreConfig{
		DurableType: slices.Clone(failureStoreConfig.DurableType),
	}
	if slices.Contains(failureStoreConfig.DurableType, failurestore.DurableTypeFileArchive) {
		archive := failureStoreConfig.DurableStoreConfig.FileArchiveConfig
		failureStore.DurableStoreConfig.FileArchiveConfig =
			objectstorage.CompressedLogFileUploadFailureArchiveConfig{
				RootPath: filepath.Join(
					a.FsstoreConfig.AppStatePath,
					a.RuntimeConfig.Target,
					archive.DirName,
				),
				LoggerContract:    contract.String(),
				LifecycleInterval: archive.LifecycleInterval,
				Retryable: mapObjectStorageCompressedLogFileUploadFailureArchivePurgeConfig(
					archive.Retryable,
				),
				Permanent: mapObjectStorageCompressedLogFileUploadFailureArchivePurgeConfig(
					archive.Permanent,
				),
			}
	}
	upload.CompressedLogFileUploadFailureStore = failureStore
	upload.CompressedLogFileUploadSpool = newCompressedLogFileUploadSpoolConfig(a, contract)
	return upload
}

// 通常・error・debugの3系統のupload設定へ、失敗時の保存先設定をまとめて組み込む関数です。
func attachCompressedLogFileUploadFailureStoreForLogFileTargets(
	a *AppConfig,
	upload objectstorage.LogFileUploadConfig,
	contract logger.LoggerContractKind,
) objectstorage.LogFileUploadConfig {
	upload.Default = attachCompressedLogFileUploadFailureStoreForUploadTarget(a, upload.Default, contract)
	upload.Error = attachCompressedLogFileUploadFailureStoreForUploadTarget(a, upload.Error, contract)
	upload.Debug = attachCompressedLogFileUploadFailureStoreForUploadTarget(a, upload.Debug, contract)
	upload.CompressedLogFileUploadSpool = newCompressedLogFileUploadSpoolConfig(a, contract)
	for _, target := range []*objectstorage.UploadTargetConfig{&upload.Default, &upload.Error, &upload.Debug} {
		if target.CompressedLogFileUploadSpool != nil {
			target.CompressedLogFileUploadSpool = upload.CompressedLogFileUploadSpool
		}
	}
	return upload
}

// newCompressedLogFileUploadSpoolConfigは、圧縮ログの再送用spoolの設定をproviderへ変換します。
// 設定の具体値はここで組み立て、利用側には読み取り用の契約を渡します。
func newCompressedLogFileUploadSpoolConfig(a *AppConfig, contract logger.LoggerContractKind) objectstorage.CompressedLogFileUploadSpoolConfigProvider {
	if a == nil {
		return nil
	}
	failureStore := a.CompressedLogFileUploadFailureStore.Standard
	if contract == logger.LoggerContractDelivery {
		failureStore = a.CompressedLogFileUploadFailureStore.Delivery
	}
	if !slices.Contains(failureStore.DurableType, failurestore.DurableTypeSpool) {
		return nil
	}
	spool := failureStore.DurableStoreConfig.SpoolConfig
	return objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(objectstorage.CompressedLogFileUploadSpoolConfig{
		SpoolPath:                  a.FsstoreConfig.SpoolPath,
		Target:                     a.RuntimeConfig.Target,
		LoggerContract:             contract.String(),
		RunnerInterval:             spool.Runner.Interval,
		LifecycleInterval:          spool.Lifecycle.Interval,
		CircuitBreakerOpenCooldown: spool.CircuitBreaker.OpenCooldown,
		MaxRetryAfter:              spool.Runner.MaxRetryAfter,
		Done:                       mapObjectStorageCompressedLogFileUploadSpoolPurgeConfig(spool.Lifecycle.Done),
		Dead:                       mapObjectStorageCompressedLogFileUploadSpoolPurgeConfig(spool.Lifecycle.Dead),
		Quarantine:                 mapObjectStorageCompressedLogFileUploadSpoolPurgeConfig(spool.Lifecycle.Quarantine),
	})
}

func mapObjectStorageCompressedLogFileUploadSpoolPurgeConfig(
	cfg objectstoragecompressedlog.PurgeConfig,
) objectstorage.CompressedLogFileUploadSpoolPurgeConfig {
	return objectstorage.CompressedLogFileUploadSpoolPurgeConfig{
		Enabled:        cfg.Enabled,
		MaxFileAgeDays: cfg.MaxFileAgeDays,
		MaxDirSize:     cfg.MaxDirSize,
	}
}

func mapObjectStorageCompressedLogFileUploadFailureArchivePurgeConfig(
	cfg objectstoragecompressedlog.PurgeConfig,
) objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig {
	return objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
		Enabled:        cfg.Enabled,
		MaxFileAgeDays: cfg.MaxFileAgeDays,
		MaxDirSize:     cfg.MaxDirSize,
	}
}

func newAppendLogRunnerConfig(cfg configappendlogrunner.Config) fsstore.AppendLogRunnerConfig {
	return fsstore.AppendLogRunnerConfig{
		Interval:    cfg.Interval,
		StopOnError: cfg.StopOnError,
		Supervisor: fsstore.AppendLogRunnerSupervisorConfig{
			Enabled:      cfg.Supervisor.Enabled,
			RestartDelay: cfg.Supervisor.RestartDelay,
			MaxRestarts:  cfg.Supervisor.MaxRestarts,
		},
	}
}

// MapLoggerConfig はsync logger用DTOへ変換し、変換後に選択されたbackendの
// 設定が受け取り先packageの契約を満たすことを検証します。
//
// 環境変数の構文検証やlogger間のtimeout・directory整合性はここでは扱いません。
func MapLoggerConfig(cfg AppConfig) (logger.LoggerConfig, error) {
	mapped := ToLoggerConfig(cfg)
	sourceLabel := logger.LoggerContractSync.DisplayName()
	if err := collapseResolvedInternalLogFileDestination(
		mapped.File, mapped.InternalLogDestinations, mapped.LogOutputDestinations.Has(logger.LogOutputFile),
	); err != nil {
		return logger.LoggerConfig{}, fmt.Errorf("%s directory config: %w", sourceLabel, err)
	}
	if err := logger.ValidateMaxLogEntryBytes(mapped.MaxLogEntryBytes); err != nil {
		return logger.LoggerConfig{}, &MaxLogEntryBytesError{
			Contract: logger.LoggerContractSync,
			Value:    mapped.MaxLogEntryBytes,
			Err:      err,
		}
	}
	if err := errors.Join(
		mappedvalidation.ValidateSyncLoggerConfig(sourceLabel, mapped),
		mappedvalidation.ValidateCloudWatchLogsStreamRoutingForLogger(
			sourceLabel,
			mapped,
			cfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		),
	); err != nil {
		return logger.LoggerConfig{}, err
	}
	return mapped, nil
}

// MapAsyncLoggerConfig はasync logger用DTOへ変換し、変換後に選択されたbackendの
// 設定が受け取り先packageの契約を満たすことを検証します。
func MapAsyncLoggerConfig(cfg AppConfig) (logger.AsyncBestEffortMultiLoggerConfig, error) {
	mapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	sourceLabel := logger.LoggerContractAsyncBestEffort.DisplayName()
	if err := collapseResolvedInternalLogFileDestination(
		mapped.File, mapped.InternalLogDestinations, mapped.LogOutputDestinations.Has(logger.LogOutputFile),
	); err != nil {
		return logger.AsyncBestEffortMultiLoggerConfig{}, fmt.Errorf("%s directory config: %w", sourceLabel, err)
	}
	mappedLoggerConfig := mapped.ToLoggerConfig()
	if err := logger.ValidateMaxLogEntryBytes(mapped.MaxLogEntryBytes); err != nil {
		return logger.AsyncBestEffortMultiLoggerConfig{}, &MaxLogEntryBytesError{
			Contract: logger.LoggerContractAsyncBestEffort,
			Value:    mapped.MaxLogEntryBytes,
			Err:      err,
		}
	}
	if err := errors.Join(
		mappedvalidation.ValidateSyncLoggerConfig(sourceLabel, mappedLoggerConfig),
		mappedvalidation.ValidateCloudWatchLogsStreamRoutingForLogger(
			sourceLabel,
			mappedLoggerConfig,
			cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamRoutingGranularity,
		),
	); err != nil {
		return logger.AsyncBestEffortMultiLoggerConfig{}, err
	}
	return mapped, nil
}

// MapDeliveryLoggerConfig はDelivery logger用DTOへ変換し、変換後に選択されたbackendの
// 設定が受け取り先packageの契約を満たすことを検証します。
func MapDeliveryLoggerConfig(cfg AppConfig) (logger.DeliveryLoggerConfig, error) {
	return validateMappedDeliveryLoggerConfig(cfg, ToDeliveryLoggerConfig(cfg))
}

// validateMappedDeliveryLoggerConfigは、cfgのDelivery loggerから変換したmappedが、受け取り先
// packageの契約を満たすことを検証します。管理用APIの監査用loggerも、変換後の設定を
// 監査用へ変えてから、この検証を使います。
func validateMappedDeliveryLoggerConfig(cfg AppConfig, mapped logger.DeliveryLoggerConfig) (logger.DeliveryLoggerConfig, error) {
	sourceLabel := logger.LoggerContractDelivery.DisplayName()
	if err := collapseResolvedDeliveryInternalLogFileDestination(
		mapped.File, mapped.InternalLogDestinations, mapped.LogOutputDestinations.Has(logger.LogOutputFile),
	); err != nil {
		return logger.DeliveryLoggerConfig{}, fmt.Errorf("%s directory config: %w", sourceLabel, err)
	}
	if err := logger.ValidateMaxLogEntryBytes(mapped.MaxLogEntryBytes); err != nil {
		return logger.DeliveryLoggerConfig{}, &MaxLogEntryBytesError{
			Contract: logger.LoggerContractDelivery,
			Value:    mapped.MaxLogEntryBytes,
			Err:      err,
		}
	}
	if err := errors.Join(
		mappedvalidation.ValidateDeliveryLoggerConfig(sourceLabel, mapped),
		mappedvalidation.ValidateCloudWatchLogsStreamRoutingForDeliveryLogger(
			sourceLabel,
			mapped,
			cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamRoutingGranularity,
		),
	); err != nil {
		return logger.DeliveryLoggerConfig{}, err
	}
	return mapped, nil
}

// ToLoggerConfig はsync loggerのfield変換だけを行います。
// application runtimeへ渡す設定には、検証を含むMapLoggerConfigを使用します。
func ToLoggerConfig(cfg AppConfig) logger.LoggerConfig {
	destinations := cfg.Logger.LogOutputDestinations
	handlerTargets := cfg.Logger.HandlerTargetDestinations

	loggerConfig := logger.LoggerConfig{
		LogOutputDestinations: cfg.Logger.LogOutputDestinations,
		Timeout:               cfg.Logger.Timeout,
		MaxLogEntryBytes:      cfg.MaxLogEntryBytes,
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputFile) {
		loggerConfig.File = NewFileLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputStdout) {
		loggerConfig.Stdout = NewStdoutLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputSlack) {
		loggerConfig.Slack = NewSlackLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputOpenSearch) {
		loggerConfig.OpenSearch = &logger.OpenSearchConfig{
			OpenSearchLoggerBaseConfig:   NewOpenSearchLoggerConfig(&cfg),
			OpenSearchFlushManagerConfig: NewOpenSearchFlushManagerConfig(&cfg),
		}

		loggerConfig.OpenSearchClient = NewOpenSearchClientConfig(&cfg)

		loggerConfig.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig =
			&logger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxDelay,
			}

	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputLoki) {
		loggerConfig.Loki = &logger.LokiConfig{
			LokiLoggerBaseConfig:   NewLokiLoggerConfig(&cfg),
			LokiFlushManagerConfig: NewLokiFlushManagerConfig(&cfg),
		}
		loggerConfig.LokiClient = NewLokiClientConfig(&cfg)
		loggerConfig.Loki.LokiRetryableClientCaseTransportErrorConfig =
			&logger.LokiRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.Logger.LokiConfig.LokiRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.Logger.LokiConfig.LokiRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.Logger.LokiConfig.LokiRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.Logger.LokiConfig.LokiRetryAbleClientConfig.MaxDelay,
			}
	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputCloudWatchLogs) {
		loggerConfig.CloudWatchLogs = &logger.CloudWatchLogsConfig{
			CloudWatchLogsLoggerBaseConfig:   NewCloudWatchLogsLoggerConfig(&cfg),
			CloudWatchLogsFlushManagerConfig: NewCloudWatchLogsFlushManagerConfig(&cfg),
		}
		loggerConfig.CloudWatchLogsClient = NewCloudWatchLogsClientConfig(&cfg)
		loggerConfig.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig =
			&logger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxDelay,
			}
	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputClickHouse) {
		loggerConfig.ClickHouse = &logger.ClickHouseConfig{
			ClickHouseLoggerBaseConfig:   NewClickHouseLoggerConfig(&cfg),
			ClickHouseFlushManagerConfig: NewClickHouseFlushManagerConfig(&cfg),
		}
		loggerConfig.ClickHouseClient = NewClickHouseClientConfig(&cfg)
		loggerConfig.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig =
			&logger.ClickHouseRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.Logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.Logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.Logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.Logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxDelay,
			}
	}
	loggerConfig.Resource = NewOtelResourceConfig(&cfg)
	loggerConfig.InternalLogDestinations = newInternalLogDestinations(
		cfg.Logger.InternalLogDestinationConfig,
		func(target logger.LogOutputDestination) bool {
			return loggerUsesInternalHandlerTarget(cfg.Logger, target)
		},
		loggerConfig,
	)

	return loggerConfig
}

// ToAsyncBestEffortMultiLoggerConfig はasync loggerのfield変換だけを行います。
// application runtimeへ渡す設定には、検証を含むMapAsyncLoggerConfigを使用します。
func ToAsyncBestEffortMultiLoggerConfig(cfg AppConfig) logger.AsyncBestEffortMultiLoggerConfig {
	destinations := cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations
	handlerTargets := cfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations

	loggerConfig := logger.AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations: cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations,
		Timeout:               cfg.AsyncBestEffortMultiLoggerConfig.Timeout,
		MaxLogEntryBytes:      cfg.MaxLogEntryBytes,
		MaxParallel:           cfg.AsyncBestEffortMultiLoggerConfig.MaxParallel,
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputFile) {
		loggerConfig.File = NewAsyncFileLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputStdout) {
		loggerConfig.Stdout = NewAsyncStdoutLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputSlack) {
		loggerConfig.Slack = NewAsyncSlackLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputOpenSearch) {
		loggerConfig.OpenSearch = &logger.OpenSearchConfig{
			OpenSearchLoggerBaseConfig:   NewAsyncOpenSearchLoggerConfig(&cfg),
			OpenSearchFlushManagerConfig: NewAsyncOpenSearchFlushManagerConfig(&cfg),
		}

		loggerConfig.OpenSearchClient = NewAsyncOpenSearchClientConfig(&cfg)

		loggerConfig.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig =
			&logger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxDelay,
			}

	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputLoki) {
		loggerConfig.Loki = &logger.LokiConfig{
			LokiLoggerBaseConfig:   NewAsyncLokiLoggerConfig(&cfg),
			LokiFlushManagerConfig: NewAsyncLokiFlushManagerConfig(&cfg),
		}
		loggerConfig.LokiClient = NewAsyncLokiClientConfig(&cfg)
		loggerConfig.Loki.LokiRetryableClientCaseTransportErrorConfig =
			&logger.LokiRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig.MaxDelay,
			}
	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputCloudWatchLogs) {
		loggerConfig.CloudWatchLogs = &logger.CloudWatchLogsConfig{
			CloudWatchLogsLoggerBaseConfig:   NewAsyncCloudWatchLogsLoggerConfig(&cfg),
			CloudWatchLogsFlushManagerConfig: NewAsyncCloudWatchLogsFlushManagerConfig(&cfg),
		}
		loggerConfig.CloudWatchLogsClient = NewAsyncCloudWatchLogsClientConfig(&cfg)
		loggerConfig.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig =
			&logger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxDelay,
			}
	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputClickHouse) {
		loggerConfig.ClickHouse = &logger.ClickHouseConfig{
			ClickHouseLoggerBaseConfig:   NewAsyncClickHouseLoggerConfig(&cfg),
			ClickHouseFlushManagerConfig: NewAsyncClickHouseFlushManagerConfig(&cfg),
		}
		loggerConfig.ClickHouseClient = NewAsyncClickHouseClientConfig(&cfg)
		loggerConfig.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig =
			&logger.ClickHouseRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxDelay,
			}
	}
	loggerConfig.Resource = NewOtelResourceConfig(&cfg)
	loggerConfig.InternalLogDestinations = newInternalLogDestinations(
		cfg.AsyncBestEffortMultiLoggerConfig.InternalLogDestinationConfig,
		func(target logger.LogOutputDestination) bool {
			return asyncLoggerUsesInternalHandlerTarget(
				cfg.AsyncBestEffortMultiLoggerConfig,
				target,
			)
		},
		loggerConfig.ToLoggerConfig(),
	)

	return loggerConfig
}

// ToDeliveryLoggerConfig はdelivery loggerのfield変換だけを行います。
// application runtimeへ渡す設定には、検証を含むMapDeliveryLoggerConfigを使用します。
func ToDeliveryLoggerConfig(cfg AppConfig) logger.DeliveryLoggerConfig {
	destinations := cfg.DeliveryLogger.LogOutputDestinations
	handlerTargets := cfg.DeliveryLogger.HandlerTargetDestinations

	loggerConfig := logger.DeliveryLoggerConfig{
		LogOutputDestinations: cfg.DeliveryLogger.LogOutputDestinations,
		InternalLogBatchConfig: newDeliveryInternalLogBatchConfig(
			cfg.DeliveryLogger.InternalLogBatchConfig,
		),
		Timeout:          cfg.DeliveryLogger.Timeout,
		MaxLogEntryBytes: cfg.MaxLogEntryBytes,
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputFile) {
		loggerConfig.File = NewDeliveryFileLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputStdout) {
		// stdoutはasyncを用意していない
		loggerConfig.Stdout = NewDeliveryStdoutLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputSlack) {
		loggerConfig.Slack = NewDeliverySlackLoggerConfig(&cfg)
	}

	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputOpenSearch) {
		loggerConfig.OpenSearch = &logger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchLoggerBaseConfig:   NewDeliveryOpenSearchLoggerConfig(&cfg),
			DeliveryOpenSearchFlushManagerConfig: NewDeliveryOpenSearchFlushManagerConfig(&cfg),
		}

		loggerConfig.OpenSearchClient = NewDeliveryOpenSearchClientConfig(&cfg)

		loggerConfig.OpenSearch.OpenSearchRetryableClientCaseTransportErrorConfig =
			&logger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxDelay,
			}

	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputLoki) {
		loggerConfig.Loki = &logger.DeliveryLokiConfig{
			DeliveryLokiLoggerBaseConfig:   NewDeliveryLokiLoggerConfig(&cfg),
			DeliveryLokiFlushManagerConfig: NewDeliveryLokiFlushManagerConfig(&cfg),
		}
		loggerConfig.LokiClient = NewDeliveryLokiClientConfig(&cfg)
		loggerConfig.Loki.LokiRetryableClientCaseTransportErrorConfig =
			&logger.LokiRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig.MaxDelay,
			}
	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputCloudWatchLogs) {
		loggerConfig.CloudWatchLogs = &logger.DeliveryCloudWatchLogsConfig{
			DeliveryCloudWatchLogsLoggerBaseConfig:   NewDeliveryCloudWatchLogsLoggerConfig(&cfg),
			DeliveryCloudWatchLogsFlushManagerConfig: NewDeliveryCloudWatchLogsFlushManagerConfig(&cfg),
		}
		loggerConfig.CloudWatchLogsClient = NewDeliveryCloudWatchLogsClientConfig(&cfg)
		loggerConfig.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig =
			&logger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxDelay,
			}
	}
	if requiresLoggerTargetConfig(destinations, handlerTargets, logconfig.LogOutputClickHouse) {
		loggerConfig.ClickHouse = &logger.DeliveryClickHouseConfig{
			DeliveryClickHouseLoggerBaseConfig:   NewDeliveryClickHouseLoggerConfig(&cfg),
			DeliveryClickHouseFlushManagerConfig: NewDeliveryClickHouseFlushManagerConfig(&cfg),
		}
		loggerConfig.ClickHouseClient = NewDeliveryClickHouseClientConfig(&cfg)
		loggerConfig.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig =
			&logger.ClickHouseRetryableClientCaseTransportErrorConfig{
				RetryClientMode: cfg.DeliveryLogger.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig.RetryClientMode,
				MaxRetries:      cfg.DeliveryLogger.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxRetries,
				BaseDelay:       cfg.DeliveryLogger.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig.BaseDelay,
				MaxDelay:        cfg.DeliveryLogger.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxDelay,
			}
	}
	loggerConfig.Resource = NewOtelResourceConfig(&cfg)
	loggerConfig.InternalLogDestinations = newDeliveryInternalLogDestinations(
		cfg.DeliveryLogger.InternalLogDestinationConfig,
		func(target logger.LogOutputDestination) bool {
			return deliveryLoggerUsesInternalHandlerTarget(cfg.DeliveryLogger, target)
		},
		loggerConfig,
	)

	return loggerConfig
}

func newDeliveryInternalLogBatchConfig(
	cfg logconfig.DeliveryInternalLogBatchConfig,
) logger.DeliveryInternalLogBatchConfig {
	return logger.DeliveryInternalLogBatchConfig{
		OpenSearch: logger.DeliveryInternalLogOpenSearchBatchConfig{
			FlushInterval:       cfg.OpenSearch.FlushInterval,
			MaxBulkDocument:     cfg.OpenSearch.MaxBulkDocument,
			BufferFlushBytes:    cfg.OpenSearch.BufferFlushBytes,
			MaxBulkRequestBytes: cfg.OpenSearch.MaxBulkRequestBytes,
		},
		Loki: logger.DeliveryInternalLogLokiBatchConfig{
			FlushInterval:           cfg.Loki.FlushInterval,
			MaxEntries:              cfg.Loki.MaxEntries,
			BufferFlushBytes:        cfg.Loki.BufferFlushBytes,
			FallbackQueueMaxEntries: cfg.Loki.FallbackQueueMaxEntries,
		},
		CloudWatchLogs: logger.DeliveryInternalLogCloudWatchLogsBatchConfig{
			FlushInterval:          cfg.CloudWatchLogs.FlushInterval,
			BufferFlushMaxEvents:   cfg.CloudWatchLogs.BufferFlushMaxEvents,
			BufferFlushBytes:       cfg.CloudWatchLogs.BufferFlushBytes,
			FallbackQueueMaxEvents: cfg.CloudWatchLogs.FallbackQueueMaxEvents,
		},
		Slack: logger.DeliveryInternalLogSlackBatchConfig{
			Intervals:              append([]time.Duration(nil), cfg.Slack.Intervals...),
			MaxLogsPerNotification: cfg.Slack.MaxLogsPerNotification,
			MaxRestThreshold:       cfg.Slack.MaxRestThreshold,
		},
	}
}

func NewFileLoggerConfig(a *AppConfig) *logger.FileLoggerConfig {
	return &logger.FileLoggerConfig{
		FsstoreConfig: fsstore.Config{
			Layout: fsstore.FsstoreConfig{
				Target:                       a.RuntimeConfig.Target,
				LogPath:                      a.FsstoreConfig.LogPath,
				LogDir:                       a.Logger.FileConfig.FileSystemStoreConfig.LogDir,
				ErrorLogDir:                  a.Logger.FileConfig.FileSystemStoreConfig.ErrorLogDir,
				DebugLogDir:                  a.Logger.FileConfig.FileSystemStoreConfig.DebugLogDir,
				IsSeparateDebugLogDirEnabled: a.Logger.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogDirEnabled,
				IsSeparateErrorLogsEnabled:   a.Logger.FileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled,
				SeparateErrorLogsPrefix:      a.Logger.FileConfig.FileSystemStoreConfig.SeparateErrorLogsPrefix,
				IsSeparateDebugLogsEnabled:   a.Logger.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
				SeparateDebugLogsPrefix:      a.Logger.FileConfig.FileSystemStoreConfig.SeparateDebugLogsPrefix,
				FileNameFormat:               a.FsstoreConfig.FileNameFormat,
				FileDateFormat:               a.FsstoreConfig.FileDateFormat,
			},
			Lifecycle: fsstore.FileLifecycleConfig{
				EnableRotate:       a.Logger.FileConfig.FileSystemStoreConfig.EnableRotate,
				RotationType:       a.Logger.FileConfig.FileSystemStoreConfig.RotationType,
				RotationDayOfWeek:  a.Logger.FileConfig.FileSystemStoreConfig.RotationDayOfWeek,
				UsageCacheTTL:      a.FsstoreConfig.UsageCacheTTL,
				MaxFileBackups:     a.Logger.FileConfig.FileSystemStoreConfig.MaxFileBackups,
				MaxFileAge:         a.Logger.FileConfig.FileSystemStoreConfig.MaxFileAge,
				MaxFileSize:        a.Logger.FileConfig.FileSystemStoreConfig.MaxFileSize,
				MaxDirSize:         a.Logger.FileConfig.FileSystemStoreConfig.MaxDirSize,
				ErrorPurge:         a.Logger.FileConfig.FileSystemStoreConfig.ErrorPurge,
				DebugPurge:         a.Logger.FileConfig.FileSystemStoreConfig.DebugPurge,
				EnableFileCompress: a.Logger.FileConfig.FileSystemStoreConfig.EnableFileCompress,
				CompressFormat:     a.Logger.FileConfig.FileSystemStoreConfig.CompressFormat,
			},
			Overload: fsstore.FileArchiveOverloadPolicyConfig{
				OverloadPolicy:        fsstore.OverloadPolicy(a.Logger.FileConfig.FileSystemStoreConfig.OverloadConfig.OverloadPolicy),
				SpillDestination:      fsstore.SpillDestination(a.Logger.FileConfig.FileSystemStoreConfig.OverloadConfig.SpillDestination),
				MinAvailableDiskSpace: a.FsstoreConfig.MinAvailableDiskSpace,
			},
			Duration: fsstore.DurationConfig{
				CompressWarnThresholdDuration:   a.Logger.FileConfig.FileSystemStoreConfig.CompressWarnThresholdDuration,
				CompressErrorThresholdDuration:  a.Logger.FileConfig.FileSystemStoreConfig.CompressErrorThresholdDuration,
				WriteFileWarnThresholdDuration:  a.Logger.FileConfig.FileSystemStoreConfig.WriteLogFileWarnThresholdDuration,
				WriteFileErrorThresholdDuration: a.Logger.FileConfig.FileSystemStoreConfig.WriteLogFileErrorThresholdDuration,
				WriteFileLatencyEnabled:         a.Logger.FileConfig.WriteFileLatencyEnabled,
			},
			Retry: fsstore.RetryConfig{
				MaxWriteRetries:            a.Logger.FileConfig.FileSystemStoreConfig.MaxWriteRetries,
				FileOperationRetryWaitTime: a.Logger.FileConfig.FileSystemStoreConfig.FileOperationRetryWaitTime,
			},
		},
		ObjectStorageUploadConfig: attachCompressedLogFileUploadFailureStoreForLogFileTargets(
			a,
			a.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
			logger.LoggerContractSync,
		),
		ArchiveCheckpointConfig: newArchiveCheckpointConfig(a, checkpointSourceLoggerFile),
		LifecycleRunnerSettings: newAppendLogRunnerConfig(a.AppendLogRunnerConfig),
		LifecycleMode:           a.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode,
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.Logger.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.Logger.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.Logger.FileConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.Logger.FileConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.Logger.FileConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.Logger.FileConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.Logger.FileConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.Logger.FileConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.Logger.FileConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.Logger.FileConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.Logger.FileConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.Logger.FileConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.Logger.FileConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.Logger.FileConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.Logger.FileConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.Logger.FileConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.Logger.FileConfig.HandlerConfig.OnDebugConfig,
		},
		LifecycleHandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.Logger.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.Logger.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.Logger.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnInfoConfig,
			OnDebugConfig: a.Logger.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnDebugConfig,
		},
	}
}

func NewAsyncFileLoggerConfig(a *AppConfig) *logger.FileLoggerConfig {
	return &logger.FileLoggerConfig{
		FsstoreConfig: fsstore.Config{
			Layout: fsstore.FsstoreConfig{
				Target:                       a.RuntimeConfig.Target,
				LogPath:                      a.FsstoreConfig.LogPath,
				LogDir:                       a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LogDir,
				ErrorLogDir:                  a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.ErrorLogDir,
				DebugLogDir:                  a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.DebugLogDir,
				IsSeparateDebugLogDirEnabled: a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogDirEnabled,
				IsSeparateErrorLogsEnabled:   a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled,
				SeparateErrorLogsPrefix:      a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.SeparateErrorLogsPrefix,
				IsSeparateDebugLogsEnabled:   a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
				SeparateDebugLogsPrefix:      a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.SeparateDebugLogsPrefix,
				FileNameFormat:               a.FsstoreConfig.FileNameFormat,
				FileDateFormat:               a.FsstoreConfig.FileDateFormat,
			},
			Lifecycle: fsstore.FileLifecycleConfig{
				EnableRotate:       a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.EnableRotate,
				RotationType:       a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.RotationType,
				RotationDayOfWeek:  a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.RotationDayOfWeek,
				UsageCacheTTL:      a.FsstoreConfig.UsageCacheTTL,
				MaxFileBackups:     a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.MaxFileBackups,
				MaxFileAge:         a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.MaxFileAge,
				MaxFileSize:        a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.MaxFileSize,
				MaxDirSize:         a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.MaxDirSize,
				ErrorPurge:         a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.ErrorPurge,
				DebugPurge:         a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.DebugPurge,
				EnableFileCompress: a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.EnableFileCompress,
				CompressFormat:     a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.CompressFormat,
			},
			Overload: fsstore.FileArchiveOverloadPolicyConfig{
				OverloadPolicy:        fsstore.OverloadPolicy(a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.OverloadConfig.OverloadPolicy),
				SpillDestination:      fsstore.SpillDestination(a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.OverloadConfig.SpillDestination),
				MinAvailableDiskSpace: a.FsstoreConfig.MinAvailableDiskSpace,
			},
			Duration: fsstore.DurationConfig{
				CompressWarnThresholdDuration:   a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.CompressWarnThresholdDuration,
				CompressErrorThresholdDuration:  a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.CompressErrorThresholdDuration,
				WriteFileWarnThresholdDuration:  a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.WriteLogFileWarnThresholdDuration,
				WriteFileErrorThresholdDuration: a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.WriteLogFileErrorThresholdDuration,
				WriteFileLatencyEnabled:         a.AsyncBestEffortMultiLoggerConfig.FileConfig.WriteFileLatencyEnabled,
			},
			Retry: fsstore.RetryConfig{
				MaxWriteRetries:            a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.MaxWriteRetries,
				FileOperationRetryWaitTime: a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.FileOperationRetryWaitTime,
			},
		},
		ObjectStorageUploadConfig: attachCompressedLogFileUploadFailureStoreForLogFileTargets(
			a,
			a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
			logger.LoggerContractAsyncBestEffort,
		),
		ArchiveCheckpointConfig: newArchiveCheckpointConfig(a, checkpointSourceAsyncLoggerFile),
		LifecycleRunnerSettings: newAppendLogRunnerConfig(a.AppendLogRunnerConfig),
		LifecycleMode:           a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LifecycleMode,
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.AsyncBestEffortMultiLoggerConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.AsyncBestEffortMultiLoggerConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.AsyncBestEffortMultiLoggerConfig.FileConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.AsyncBestEffortMultiLoggerConfig.FileConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.AsyncBestEffortMultiLoggerConfig.FileConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.AsyncBestEffortMultiLoggerConfig.FileConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.AsyncBestEffortMultiLoggerConfig.FileConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.AsyncBestEffortMultiLoggerConfig.FileConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.AsyncBestEffortMultiLoggerConfig.FileConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.AsyncBestEffortMultiLoggerConfig.FileConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.AsyncBestEffortMultiLoggerConfig.FileConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.AsyncBestEffortMultiLoggerConfig.FileConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.AsyncBestEffortMultiLoggerConfig.FileConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.AsyncBestEffortMultiLoggerConfig.FileConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.AsyncBestEffortMultiLoggerConfig.FileConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.AsyncBestEffortMultiLoggerConfig.FileConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.AsyncBestEffortMultiLoggerConfig.FileConfig.HandlerConfig.OnDebugConfig,
		},
		LifecycleHandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnInfoConfig,
			OnDebugConfig: a.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnDebugConfig,
		},
	}
}

func NewDeliveryFileLoggerConfig(a *AppConfig) *logger.DeliveryFileLoggerConfig {
	return &logger.DeliveryFileLoggerConfig{
		FsstoreConfig: fsstore.DeliveryFileWriteConfig{
			Layout: fsstore.FsstoreConfig{
				Target:                       a.RuntimeConfig.Target,
				LogPath:                      a.FsstoreConfig.LogPath,
				LogDir:                       a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LogDir,
				ErrorLogDir:                  a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.ErrorLogDir,
				DebugLogDir:                  a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.DebugLogDir,
				IsSeparateDebugLogDirEnabled: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.IsSeparateDebugLogDirEnabled,
				IsSeparateErrorLogsEnabled:   a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.IsSeparateErrorLogsEnabled,
				SeparateErrorLogsPrefix:      a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.SeparateErrorLogsPrefix,
				IsSeparateDebugLogsEnabled:   a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.IsSeparateDebugLogsEnabled,
				SeparateDebugLogsPrefix:      a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.SeparateDebugLogsPrefix,
				FileNameFormat:               a.FsstoreConfig.FileNameFormat,
				FileDateFormat:               a.FsstoreConfig.FileDateFormat,
			},
			Rotation: fsstore.FileRotationConfig{
				EnableRotate:      a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.EnableRotate,
				RotationType:      a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.RotationType,
				RotationDayOfWeek: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.RotationDayOfWeek,
				MaxFileSize:       a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.MaxFileSize,
			},
			Duration: fsstore.DurationConfig{
				WriteFileWarnThresholdDuration:  a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.WriteLogFileWarnThresholdDuration,
				WriteFileErrorThresholdDuration: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.WriteLogFileErrorThresholdDuration,
				WriteFileLatencyEnabled:         a.DeliveryLogger.DeliveryFileConfig.WriteFileLatencyEnabled,
			},
			Retry: fsstore.RetryConfig{
				MaxWriteRetries:            a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.MaxWriteRetries,
				FileOperationRetryWaitTime: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.FileOperationRetryWaitTime,
			},
		},
		LifecycleMode: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleMode,
		LifecycleRunnerSettings: logger.DeliveryFileLifecycleRunnerSettings{
			UsageCacheTTL:                  a.FsstoreConfig.UsageCacheTTL,
			MaxFileBackups:                 a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.MaxFileBackups,
			MaxFileAge:                     a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.MaxFileAge,
			MaxDirSize:                     a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.MaxDirSize,
			ErrorPurge:                     a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.ErrorPurge,
			DebugPurge:                     a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.DebugPurge,
			EnableFileCompress:             a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.EnableFileCompress,
			CompressFormat:                 a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.CompressFormat,
			CompressWarnThresholdDuration:  a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.CompressWarnThresholdDuration,
			CompressErrorThresholdDuration: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.CompressErrorThresholdDuration,
			ObjectStorageUploadConfig: attachCompressedLogFileUploadFailureStoreForLogFileTargets(
				a,
				a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig,
				logger.LoggerContractDelivery,
			),
			ArchiveCheckpointConfig: newArchiveCheckpointConfig(a, checkpointSourceDeliveryLoggerFile),
			RunnerConfig:            newAppendLogRunnerConfig(a.AppendLogRunnerConfig),
			LifecycleHandlerConfig: handler.HandlerConfig{
				OnErrorConfig: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnErrorConfig,
				OnWarnConfig:  a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnWarnConfig,
				OnInfoConfig:  a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnInfoConfig,
				OnDebugConfig: a.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleHandlerConfig.OnDebugConfig,
			},
		},
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.DeliveryLogger.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.DeliveryLogger.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.DeliveryRuntimeConfig{
			AppName: a.AppConfig.AppName,
			IsDebug: a.RuntimeConfig.IsDebug,
			Target:  a.RuntimeConfig.Target,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.DeliveryLogger.DeliveryFileConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.DeliveryLogger.DeliveryFileConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.DeliveryLogger.DeliveryFileConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.DeliveryLogger.DeliveryFileConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.DeliveryLogger.DeliveryFileConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.DeliveryLogger.DeliveryFileConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.DeliveryLogger.DeliveryFileConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewStdoutLoggerConfig(a *AppConfig) *logger.StdoutLoggerConfig {
	return &logger.StdoutLoggerConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.Logger.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.Logger.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.Logger.StdoutConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.Logger.StdoutConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.Logger.StdoutConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.Logger.StdoutConfig.ErrorTraceDepthDebugMode,
		},
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.Logger.StdoutConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.Logger.StdoutConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.Logger.StdoutConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.Logger.StdoutConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.Logger.StdoutConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.Logger.StdoutConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.Logger.StdoutConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.Logger.StdoutConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.Logger.StdoutConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.Logger.StdoutConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.Logger.StdoutConfig.HandlerConfig.OnDebugConfig,
		},
		UseOpenTelemetryFormat: a.Logger.StdoutConfig.OpenTelemetryFormatEnabled,
	}
}

// NewAsyncStdoutLoggerConfigは、LoggerContractAsyncBestEffort用に読み込んだstdout設定をmapします。
// stdoutの実装は同期loggerと同じですが、時刻と出力先の設定はあくまでこの契約種別に属します。
func NewAsyncStdoutLoggerConfig(a *AppConfig) *logger.StdoutLoggerConfig {
	return &logger.StdoutLoggerConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.AsyncBestEffortMultiLoggerConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.AsyncBestEffortMultiLoggerConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.ErrorTraceDepthDebugMode,
		},
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.HandlerConfig.OnDebugConfig,
		},
		UseOpenTelemetryFormat: a.AsyncBestEffortMultiLoggerConfig.StdoutConfig.OpenTelemetryFormatEnabled,
	}
}

func NewDeliveryStdoutLoggerConfig(a *AppConfig) *logger.DeliveryStdoutLoggerConfig {
	return &logger.DeliveryStdoutLoggerConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.DeliveryLogger.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.DeliveryLogger.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.DeliveryRuntimeConfig{
			AppName: a.AppConfig.AppName,
			IsDebug: a.RuntimeConfig.IsDebug,
			Target:  a.RuntimeConfig.Target,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.DeliveryLogger.DeliveryStdoutConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.DeliveryLogger.DeliveryStdoutConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.DeliveryLogger.DeliveryStdoutConfig.ErrorTraceDepthDebugMode,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.DeliveryLogger.DeliveryStdoutConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.DeliveryLogger.DeliveryStdoutConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.DeliveryLogger.DeliveryStdoutConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.DeliveryLogger.DeliveryStdoutConfig.HandlerConfig.OnDebugConfig,
		},
		UseOpenTelemetryFormat: a.DeliveryLogger.DeliveryStdoutConfig.OpenTelemetryFormatEnabled,
	}
}

func NewSlackLoggerConfig(a *AppConfig) *logger.SlackLoggerConfig {
	return &logger.SlackLoggerConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.Logger.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.Logger.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.Logger.SlackConfig.LogLevel, // requireLogLevelForSend
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.Logger.SlackConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.Logger.SlackConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.Logger.SlackConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		Timeout:                a.Logger.SlackConfig.Timeout,
		Token:                  a.Logger.SlackConfig.Token,
		Channel:                a.Logger.SlackConfig.Channel,
		ErrorChannel:           a.Logger.SlackConfig.ErrorChannel,
		WarnChannel:            a.Logger.SlackConfig.WarnChannel,
		InfoChannel:            a.Logger.SlackConfig.InfoChannel,
		DebugChannel:           a.Logger.SlackConfig.DebugChannel,
		MaxLogsPerNotification: a.Logger.SlackConfig.MaxLogsPerNotification,
		MaxRestThreshold:       a.Logger.SlackConfig.MaxRestThreshold,
		Intervals:              a.Logger.SlackConfig.Intervals,
		MaxMessageLength:       a.Logger.SlackConfig.MaxMessageLength,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.Logger.SlackConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.Logger.SlackConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.Logger.SlackConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.Logger.SlackConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.Logger.SlackConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.Logger.SlackConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.Logger.SlackConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.Logger.SlackConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.Logger.SlackConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.Logger.SlackConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.Logger.SlackConfig.HandlerConfig.OnDebugConfig,
		},
		SlackPostWarnThresholdDuration:  a.Slack.SlackPostWarnThresholdDuration,
		SlackPostErrorThresholdDuration: a.Slack.SlackPostErrorThresholdDuration,
	}
}

func NewAsyncSlackLoggerConfig(a *AppConfig) *logger.SlackLoggerConfig {
	return &logger.SlackLoggerConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.AsyncBestEffortMultiLoggerConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.AsyncBestEffortMultiLoggerConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.AsyncBestEffortMultiLoggerConfig.SlackConfig.LogLevel, // requireLogLevelForSend
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.AsyncBestEffortMultiLoggerConfig.SlackConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.AsyncBestEffortMultiLoggerConfig.SlackConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.AsyncBestEffortMultiLoggerConfig.SlackConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		Timeout:                a.AsyncBestEffortMultiLoggerConfig.SlackConfig.Timeout,
		Token:                  a.AsyncBestEffortMultiLoggerConfig.SlackConfig.Token,
		Channel:                a.AsyncBestEffortMultiLoggerConfig.SlackConfig.Channel,
		ErrorChannel:           a.AsyncBestEffortMultiLoggerConfig.SlackConfig.ErrorChannel,
		WarnChannel:            a.AsyncBestEffortMultiLoggerConfig.SlackConfig.WarnChannel,
		InfoChannel:            a.AsyncBestEffortMultiLoggerConfig.SlackConfig.InfoChannel,
		DebugChannel:           a.AsyncBestEffortMultiLoggerConfig.SlackConfig.DebugChannel,
		MaxLogsPerNotification: a.AsyncBestEffortMultiLoggerConfig.SlackConfig.MaxLogsPerNotification,
		MaxRestThreshold:       a.AsyncBestEffortMultiLoggerConfig.SlackConfig.MaxRestThreshold,
		Intervals:              a.AsyncBestEffortMultiLoggerConfig.SlackConfig.Intervals,
		MaxMessageLength:       a.AsyncBestEffortMultiLoggerConfig.SlackConfig.MaxMessageLength,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.AsyncBestEffortMultiLoggerConfig.SlackConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.AsyncBestEffortMultiLoggerConfig.SlackConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.AsyncBestEffortMultiLoggerConfig.SlackConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.AsyncBestEffortMultiLoggerConfig.SlackConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.AsyncBestEffortMultiLoggerConfig.SlackConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.AsyncBestEffortMultiLoggerConfig.SlackConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.AsyncBestEffortMultiLoggerConfig.SlackConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.AsyncBestEffortMultiLoggerConfig.SlackConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.AsyncBestEffortMultiLoggerConfig.SlackConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.AsyncBestEffortMultiLoggerConfig.SlackConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.AsyncBestEffortMultiLoggerConfig.SlackConfig.HandlerConfig.OnDebugConfig,
		},
		SlackPostWarnThresholdDuration:  a.Slack.SlackPostWarnThresholdDuration,
		SlackPostErrorThresholdDuration: a.Slack.SlackPostErrorThresholdDuration,
	}
}

func NewDeliverySlackLoggerConfig(a *AppConfig) *logger.DeliverySlackLoggerConfig {
	return &logger.DeliverySlackLoggerConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.DeliveryLogger.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.DeliveryLogger.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.DeliveryLogger.DeliverySlackConfig.LogLevel, // requireLogLevelForSend
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.DeliveryLogger.DeliverySlackConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.DeliveryLogger.DeliverySlackConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.DeliveryLogger.DeliverySlackConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		Timeout:          a.DeliveryLogger.DeliverySlackConfig.Timeout,
		Token:            a.DeliveryLogger.DeliverySlackConfig.Token,
		Channel:          a.DeliveryLogger.DeliverySlackConfig.Channel,
		ErrorChannel:     a.DeliveryLogger.DeliverySlackConfig.ErrorChannel,
		WarnChannel:      a.DeliveryLogger.DeliverySlackConfig.WarnChannel,
		InfoChannel:      a.DeliveryLogger.DeliverySlackConfig.InfoChannel,
		DebugChannel:     a.DeliveryLogger.DeliverySlackConfig.DebugChannel,
		MaxMessageLength: a.DeliveryLogger.DeliverySlackConfig.MaxMessageLength,
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.DeliveryLogger.DeliverySlackConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.DeliveryLogger.DeliverySlackConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.DeliveryLogger.DeliverySlackConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.DeliveryLogger.DeliverySlackConfig.HandlerConfig.OnDebugConfig,
		},
		SlackPostWarnThresholdDuration:  a.Slack.SlackPostWarnThresholdDuration,
		SlackPostErrorThresholdDuration: a.Slack.SlackPostErrorThresholdDuration,
	}
}

func NewOpenSearchFlushManagerConfig(a *AppConfig) logger.OpenSearchFlushManagerConfig {
	return logger.OpenSearchFlushManagerConfig{
		Index:                      a.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.Index,
		IsDebug:                    a.RuntimeConfig.IsDebug,
		UseOpenTelemetryFormat:     a.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.OpenTelemetryFormatEnabled,
		PostWarnThresholdDuration:  a.Logger.OpenSearchConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.Logger.OpenSearchConfig.PostErrorThresholdDuration,
		IndexLatencyEnabled:        a.Logger.OpenSearchConfig.IndexLatencyEnabled,
		MaxItemRetries:             a.Logger.OpenSearchConfig.MaxItemRetries,
		RetryWaitTime:              a.Logger.OpenSearchConfig.RetryWaitTime,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newFailureStoreConfig(
			a,
			checkpointSourceLoggerOpenSearch,
			logger.LoggerContractSync,
			a.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig,
			a.Logger.OpenSearchConfig.LogBodyConfig,
		),
	}
}

func NewAsyncOpenSearchFlushManagerConfig(a *AppConfig) logger.OpenSearchFlushManagerConfig {
	return logger.OpenSearchFlushManagerConfig{
		Index:                      a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.Index,
		IsDebug:                    a.RuntimeConfig.IsDebug,
		UseOpenTelemetryFormat:     a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.OpenTelemetryFormatEnabled,
		PostWarnThresholdDuration:  a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.PostErrorThresholdDuration,
		IndexLatencyEnabled:        a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.IndexLatencyEnabled,
		MaxItemRetries:             a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxItemRetries,
		RetryWaitTime:              a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.RetryWaitTime,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newFailureStoreConfig(
			a,
			checkpointSourceAsyncLoggerOpenSearch,
			logger.LoggerContractAsyncBestEffort,
			a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig,
			a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.LogBodyConfig,
		),
	}
}

func NewDeliveryOpenSearchFlushManagerConfig(a *AppConfig) logger.DeliveryOpenSearchFlushManagerConfig {
	return logger.DeliveryOpenSearchFlushManagerConfig{
		Index:                      a.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.Index,
		IsDebug:                    a.RuntimeConfig.IsDebug,
		UseOpenTelemetryFormat:     a.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.OpenTelemetryFormatEnabled,
		PostWarnThresholdDuration:  a.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.PostErrorThresholdDuration,
		IndexLatencyEnabled:        a.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.IndexLatencyEnabled,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newDeliveryFailureStoreConfig(
			a,
			checkpointSourceDeliveryLoggerOpenSearch,
			a.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig,
			a.DeliveryLogger.DeliveryOpenSearchConfig.LogBodyConfig,
		),
	}
}

func NewLokiFlushManagerConfig(a *AppConfig) logger.LokiFlushManagerConfig {
	return logger.LokiFlushManagerConfig{
		PostWarnThresholdDuration:  a.Logger.LokiConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.Logger.LokiConfig.PostErrorThresholdDuration,
		PushLatencyEnabled:         a.Logger.LokiConfig.PushLatencyEnabled,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newFailureStoreConfig(
			a,
			checkpointSourceLoggerLoki,
			logger.LoggerContractSync,
			a.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig,
			a.Logger.LokiConfig.LogBodyConfig,
		),
	}
}

func NewAsyncLokiFlushManagerConfig(a *AppConfig) logger.LokiFlushManagerConfig {
	return logger.LokiFlushManagerConfig{
		PostWarnThresholdDuration:  a.AsyncBestEffortMultiLoggerConfig.LokiConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.PostErrorThresholdDuration,
		PushLatencyEnabled:         a.AsyncBestEffortMultiLoggerConfig.LokiConfig.PushLatencyEnabled,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newFailureStoreConfig(
			a,
			checkpointSourceAsyncLoggerLoki,
			logger.LoggerContractAsyncBestEffort,
			a.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig,
			a.AsyncBestEffortMultiLoggerConfig.LokiConfig.LogBodyConfig,
		),
	}
}

func NewDeliveryLokiFlushManagerConfig(a *AppConfig) logger.DeliveryLokiFlushManagerConfig {
	fh := a.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.FailureHandlerConfig
	return logger.DeliveryLokiFlushManagerConfig{
		PostWarnThresholdDuration:  a.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.PostErrorThresholdDuration,
		PushLatencyEnabled:         a.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.PushLatencyEnabled,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newDeliveryFailureStoreConfig(
			a,
			checkpointSourceDeliveryLoggerLoki,
			fh,
			a.DeliveryLogger.DeliveryLokiConfig.LogBodyConfig,
		),
	}
}

func NewCloudWatchLogsFlushManagerConfig(a *AppConfig) logger.CloudWatchLogsFlushManagerConfig {
	return logger.CloudWatchLogsFlushManagerConfig{
		PostWarnThresholdDuration:  a.Logger.CloudWatchLogsConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.Logger.CloudWatchLogsConfig.PostErrorThresholdDuration,
		PutLatencyEnabled:          a.Logger.CloudWatchLogsConfig.PutLatencyEnabled,
		LogGroupName:               a.Logger.CloudWatchLogsConfig.LogGroupName,
		LogStreamName:              a.Logger.CloudWatchLogsConfig.LogStreamName,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newFailureStoreConfig(
			a,
			checkpointSourceLoggerCloudWatchLogs,
			logger.LoggerContractSync,
			a.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
			a.Logger.CloudWatchLogsConfig.LogBodyConfig,
		),
	}
}

func NewAsyncCloudWatchLogsFlushManagerConfig(a *AppConfig) logger.CloudWatchLogsFlushManagerConfig {
	return logger.CloudWatchLogsFlushManagerConfig{
		PostWarnThresholdDuration:  a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.PostErrorThresholdDuration,
		PutLatencyEnabled:          a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.PutLatencyEnabled,
		LogGroupName:               a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogGroupName,
		LogStreamName:              a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamName,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newFailureStoreConfig(
			a,
			checkpointSourceAsyncLoggerCloudWatchLogs,
			logger.LoggerContractAsyncBestEffort,
			a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
			a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogBodyConfig,
		),
	}
}

func NewDeliveryCloudWatchLogsFlushManagerConfig(a *AppConfig) logger.DeliveryCloudWatchLogsFlushManagerConfig {
	fm := a.DeliveryLogger.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig
	fh := fm.FailureHandlerConfig
	return logger.DeliveryCloudWatchLogsFlushManagerConfig{
		PostWarnThresholdDuration:  fm.PostWarnThresholdDuration,
		PostErrorThresholdDuration: fm.PostErrorThresholdDuration,
		PutLatencyEnabled:          fm.PutLatencyEnabled,
		LogGroupName:               fm.LogGroupName,
		LogStreamName:              fm.LogStreamName,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newDeliveryFailureStoreConfig(
			a,
			checkpointSourceDeliveryLoggerCloudWatchLogs,
			fh,
			a.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogBodyConfig,
		),
	}
}

func NewClickHouseFlushManagerConfig(a *AppConfig) logger.ClickHouseFlushManagerConfig {
	return logger.ClickHouseFlushManagerConfig{
		PostWarnThresholdDuration:  a.Logger.ClickHouseConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.Logger.ClickHouseConfig.PostErrorThresholdDuration,
		InsertLatencyEnabled:       a.Logger.ClickHouseConfig.InsertLatencyEnabled,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newFailureStoreConfig(
			a,
			checkpointSourceLoggerClickHouse,
			logger.LoggerContractSync,
			a.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig,
			a.Logger.ClickHouseConfig.LogBodyConfig,
		),
	}
}

func NewAsyncClickHouseFlushManagerConfig(a *AppConfig) logger.ClickHouseFlushManagerConfig {
	return logger.ClickHouseFlushManagerConfig{
		PostWarnThresholdDuration:  a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.PostErrorThresholdDuration,
		InsertLatencyEnabled:       a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.InsertLatencyEnabled,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newFailureStoreConfig(
			a,
			checkpointSourceAsyncLoggerClickHouse,
			logger.LoggerContractAsyncBestEffort,
			a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig,
			a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.LogBodyConfig,
		),
	}
}

func NewDeliveryClickHouseFlushManagerConfig(a *AppConfig) logger.DeliveryClickHouseFlushManagerConfig {
	fm := a.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig
	fh := fm.FailureHandlerConfig
	return logger.DeliveryClickHouseFlushManagerConfig{
		PostWarnThresholdDuration:  a.DeliveryLogger.DeliveryClickHouseConfig.PostWarnThresholdDuration,
		PostErrorThresholdDuration: a.DeliveryLogger.DeliveryClickHouseConfig.PostErrorThresholdDuration,
		InsertLatencyEnabled:       a.DeliveryLogger.DeliveryClickHouseConfig.InsertLatencyEnabled,
		Resource:                   newLoggerResource(a),
		FailureStoreConfig: newDeliveryFailureStoreConfig(
			a,
			checkpointSourceDeliveryLoggerClickHouse,
			fh,
			a.DeliveryLogger.DeliveryClickHouseConfig.LogBodyConfig,
		),
	}
}

func newFailureStoreConfig(
	a *AppConfig,
	checkpointSource archiveCheckpointSourcePrefix,
	contract logger.LoggerContractKind,
	fh failure.FailureHandlerConfig,
	logBody logconfig.LogBodyConfig,
) durable.FailureStoreConfig {
	return durable.FailureStoreConfig{
		EnableDegrade: fh.EnableDegrade,
		MaxConcurrent: fh.MaxConcurrent,
		DurableType:   fh.DurableType,
		SpillType:     fh.SpillType,
		DurableStoreConfig: durable.DurableStoreConfig{
			FileArchiveConfig: durable.FileArchiveConfig{
				Target:                     a.RuntimeConfig.Target,
				MaxFileSize:                fh.Fsstore.FileArchiveConfig.MaxFileSize,
				FailureFileArchivePath:     a.FsstoreConfig.AppStatePath,
				Dir:                        fh.Fsstore.FileArchiveConfig.Dir,
				DirCaseRetryable:           fh.Fsstore.FileArchiveConfig.DirCaseRetryable,
				DirCasePermanent:           fh.Fsstore.FileArchiveConfig.DirCasePermanent,
				FileNameFormat:             fsstore.FailureArchiveFileNameFormat,
				FileDateFormat:             a.FsstoreConfig.FileDateFormat,
				EnableRotate:               fh.Fsstore.FileArchiveConfig.EnableRotate,
				UsageCacheTTL:              a.FsstoreConfig.UsageCacheTTL,
				MaxFileBackups:             fh.Fsstore.FileArchiveConfig.MaxFileBackups,
				MaxFileAge:                 fh.Fsstore.FileArchiveConfig.MaxFileAge,
				MaxDirSize:                 fh.Fsstore.FileArchiveConfig.MaxDirSize,
				EnableFileCompress:         fh.Fsstore.FileArchiveConfig.EnableFileCompress,
				CompressFormat:             fh.Fsstore.FileArchiveConfig.CompressFormat,
				RotationType:               fh.Fsstore.FileArchiveConfig.FileRotation,
				RotationDayOfWeek:          fh.Fsstore.FileArchiveConfig.FileRotationDayOfWeek,
				MaxWriteRetries:            fh.Fsstore.FileArchiveConfig.MaxWriteRetries,
				FileOperationRetryWaitTime: fh.Fsstore.FileArchiveConfig.FileOperationRetryWaitTime,
				ObjectStorageUploadConfig: attachCompressedLogFileUploadFailureStoreForUploadTarget(
					a, fh.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig, contract,
				),
				ArchiveCheckpointConfig: newArchiveCheckpointConfig(a, checkpointSource),
				LifecycleRunnerConfig:   newAppendLogRunnerConfig(a.AppendLogRunnerConfig),
				LifecycleMode:           fh.Fsstore.FileArchiveConfig.LifecycleMode,
				OverloadPolicyConfig: durable.FileArchiveOverloadPolicyConfig{
					OverloadPolicy:        fh.Fsstore.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy,
					SpillDestination:      fh.Fsstore.FileArchiveConfig.OverloadPolicyConfig.SpillDestination,
					MinAvailableDiskSpace: a.FsstoreConfig.MinAvailableDiskSpace,
				},
				HandlerConfig: handler.HandlerConfig{
					OnErrorConfig: fh.Fsstore.FileArchiveConfig.HandlerConfig.OnErrorConfig,
					OnWarnConfig:  fh.Fsstore.FileArchiveConfig.HandlerConfig.OnWarnConfig,
					OnInfoConfig:  fh.Fsstore.FileArchiveConfig.HandlerConfig.OnInfoConfig,
					OnDebugConfig: fh.Fsstore.FileArchiveConfig.HandlerConfig.OnDebugConfig,
				},
				IsDebug:                  a.RuntimeConfig.IsDebug,
				ErrorTraceDepth:          logBody.ErrorTraceDepth,
				ErrorTraceDepthDebugMode: logBody.ErrorTraceDepthDebugMode,
			},
			SpoolConfig: durable.SpoolConfig{
				SpoolPath: a.FsstoreConfig.SpoolPath,
				Target:    a.RuntimeConfig.Target,
				Dir:       fh.Fsstore.SpoolConfig.Dir,
				OverloadPolicyConfig: durable.SpoolOverloadPolicyConfig{
					OverloadPolicy:                fh.Fsstore.SpoolConfig.OverloadPolicyConfig.OverloadPolicy,
					SpillDestination:              fh.Fsstore.SpoolConfig.OverloadPolicyConfig.SpillDestination,
					MaxCallsSinceScan:             fh.Fsstore.SpoolConfig.OverloadPolicyConfig.MaxCallsSinceScan,
					StatsScanInterval:             fh.Fsstore.SpoolConfig.OverloadPolicyConfig.StatsScanInterval,
					MaxDirSize:                    fh.Fsstore.SpoolConfig.OverloadPolicyConfig.MaxDirSize,
					ActiveBacklogWarnAfterDays:    fh.Fsstore.SpoolConfig.OverloadPolicyConfig.ActiveBacklogWarnAfterDays,
					MinAvailableDiskSpace:         a.FsstoreConfig.MinAvailableDiskSpace,
					UseMemoryPressureForOverload:  fh.Fsstore.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload,
					MemoryCriticalRatio:           a.RuntimeConfig.Health.Memory.CriticalRatio,
					MinAvailableMemoryBytes:       a.RuntimeConfig.Health.Memory.MinAvailableBytes,
					MemoryCriticalConsecutiveHits: a.RuntimeConfig.Health.Memory.CriticalConsecutiveHits,
				},
				HandlerConfig: handler.HandlerConfig{
					OnErrorConfig: fh.Fsstore.SpoolConfig.HandlerConfig.OnErrorConfig,
					OnWarnConfig:  fh.Fsstore.SpoolConfig.HandlerConfig.OnWarnConfig,
					OnInfoConfig:  fh.Fsstore.SpoolConfig.HandlerConfig.OnInfoConfig,
					OnDebugConfig: fh.Fsstore.SpoolConfig.HandlerConfig.OnDebugConfig,
				},
			},
		},
	}
}

func newDeliveryFailureStoreConfig(
	a *AppConfig,
	checkpointSource archiveCheckpointSourcePrefix,
	fh failure.DeliveryFailureHandlerConfig,
	logBody logconfig.LogBodyConfig,
) durable.DeliveryFailureStoreConfig {
	return durable.DeliveryFailureStoreConfig{
		EnableDegrade: fh.EnableDegrade,
		DurableType:   fh.DurableType,
		SpillType:     fh.SpillType,
		FileArchiveConfig: durable.DeliveryFileArchiveConfig{
			Target:                     a.RuntimeConfig.Target,
			MaxFileSize:                fh.FileArchiveConfig.MaxFileSize,
			FailureFileArchivePath:     a.FsstoreConfig.AppStatePath,
			Dir:                        fh.FileArchiveConfig.Dir,
			DirCaseRetryable:           fh.FileArchiveConfig.DirCaseRetryable,
			DirCasePermanent:           fh.FileArchiveConfig.DirCasePermanent,
			FileNameFormat:             fsstore.FailureArchiveFileNameFormat,
			FileDateFormat:             a.FsstoreConfig.FileDateFormat,
			EnableRotate:               fh.FileArchiveConfig.EnableRotate,
			UsageCacheTTL:              a.FsstoreConfig.UsageCacheTTL,
			MaxFileBackups:             fh.FileArchiveConfig.MaxFileBackups,
			MaxFileAge:                 fh.FileArchiveConfig.MaxFileAge,
			MaxDirSize:                 fh.FileArchiveConfig.MaxDirSize,
			EnableFileCompress:         fh.FileArchiveConfig.EnableFileCompress,
			CompressFormat:             fh.FileArchiveConfig.CompressFormat,
			RotationType:               fh.FileArchiveConfig.FileRotation,
			RotationDayOfWeek:          fh.FileArchiveConfig.FileRotationDayOfWeek,
			MaxWriteRetries:            fh.FileArchiveConfig.MaxWriteRetries,
			FileOperationRetryWaitTime: fh.FileArchiveConfig.FileOperationRetryWaitTime,
			ObjectStorageUploadConfig: attachCompressedLogFileUploadFailureStoreForUploadTarget(
				a, fh.FileArchiveConfig.ObjectStorageUploadConfig, logger.LoggerContractDelivery,
			),
			ArchiveCheckpointConfig: newArchiveCheckpointConfig(a, checkpointSource),
			LifecycleRunnerConfig:   newAppendLogRunnerConfig(a.AppendLogRunnerConfig),
			LifecycleMode:           fh.FileArchiveConfig.LifecycleMode,
			OverloadPolicyConfig: durable.FileArchiveOverloadPolicyConfig{
				OverloadPolicy:        fh.FileArchiveConfig.OverloadPolicyConfig.OverloadPolicy,
				SpillDestination:      fh.FileArchiveConfig.OverloadPolicyConfig.SpillDestination,
				MinAvailableDiskSpace: a.FsstoreConfig.MinAvailableDiskSpace,
			},
			HandlerConfig: handler.HandlerConfig{
				OnErrorConfig: fh.FileArchiveConfig.HandlerConfig.OnErrorConfig,
				OnWarnConfig:  fh.FileArchiveConfig.HandlerConfig.OnWarnConfig,
				OnInfoConfig:  fh.FileArchiveConfig.HandlerConfig.OnInfoConfig,
				OnDebugConfig: fh.FileArchiveConfig.HandlerConfig.OnDebugConfig,
			},
			IsDebug:                  a.RuntimeConfig.IsDebug,
			ErrorTraceDepth:          logBody.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: logBody.ErrorTraceDepthDebugMode,
		},
	}
}

// newLoggerResourceは、実行環境の設定からResourceを作ります。LoadRuntimeEnvは常にCloudを
// 設定しますが、zero値のEnvConfigのTelemetryConfigからも呼ばれるため、nilのCloudも扱います。
func newLoggerResource(a *AppConfig) logger.Resource {
	cloud := &logger.CloudResourceInfo{RuntimeInstanceID: a.RuntimeEnv.CoreInfo.RuntimeInstanceID}
	if a.RuntimeEnv.Cloud != nil {
		cloud.Provider = a.RuntimeEnv.Cloud.Provider
		cloud.Region = a.RuntimeEnv.Cloud.Region
	}
	return logger.Resource{
		ServiceName:    a.AppConfig.AppName,
		ServiceVersion: a.AppConfig.AppVersion,
		HostName:       a.RuntimeEnv.CoreInfo.HostName,
		OSType:         a.RuntimeEnv.CoreInfo.OSType,
		Environment:    a.RuntimeEnv.CoreInfo.Environment,
		RuntimeType:    a.RuntimeEnv.CoreInfo.RuntimeType,
		Cloud:          cloud,
	}
}

func NewOpenSearchLoggerConfig(a *AppConfig) logger.OpenSearchLoggerBaseConfig {
	return logger.OpenSearchLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.Logger.OpenSearchConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.Logger.OpenSearchConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.Logger.OpenSearchConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.Logger.OpenSearchConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.Logger.OpenSearchConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.Logger.OpenSearchConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:          a.Logger.OpenSearchConfig.FlushInterval,
		MaxBulkDocument:        a.Logger.OpenSearchConfig.MaxBulkDocument,
		BufferFlushBytes:       a.Logger.OpenSearchConfig.BufferFlushBytes,
		MaxBulkRequestBytes:    a.Logger.OpenSearchConfig.MaxBulkRequestBytes,
		NoBulkFlushModeEnabled: a.Logger.OpenSearchConfig.NoBulkFlushModeEnabled,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.Logger.OpenSearchConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.Logger.OpenSearchConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.Logger.OpenSearchConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.Logger.OpenSearchConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.Logger.OpenSearchConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.Logger.OpenSearchConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.Logger.OpenSearchConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.Logger.OpenSearchConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.Logger.OpenSearchConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.Logger.OpenSearchConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.Logger.OpenSearchConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewAsyncOpenSearchLoggerConfig(a *AppConfig) logger.OpenSearchLoggerBaseConfig {
	return logger.OpenSearchLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:       a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.FlushInterval,
		MaxBulkDocument:     a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxBulkDocument,
		BufferFlushBytes:    a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.BufferFlushBytes,
		MaxBulkRequestBytes: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxBulkRequestBytes,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewDeliveryOpenSearchLoggerConfig(a *AppConfig) logger.DeliveryOpenSearchLoggerBaseConfig {
	return logger.DeliveryOpenSearchLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.DeliveryLogger.DeliveryOpenSearchConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.DeliveryLogger.DeliveryOpenSearchConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.DeliveryRuntimeConfig{
			AppName: a.AppConfig.AppName,
			IsDebug: a.RuntimeConfig.IsDebug,
			Target:  a.RuntimeConfig.Target,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.DeliveryLogger.DeliveryOpenSearchConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.DeliveryLogger.DeliveryOpenSearchConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.DeliveryLogger.DeliveryOpenSearchConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.DeliveryLogger.DeliveryOpenSearchConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.DeliveryLogger.DeliveryOpenSearchConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.DeliveryLogger.DeliveryOpenSearchConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.DeliveryLogger.DeliveryOpenSearchConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewLokiLoggerConfig(a *AppConfig) logger.LokiLoggerBaseConfig {
	return logger.LokiLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.Logger.LokiConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.Logger.LokiConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.Logger.LokiConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.Logger.LokiConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.Logger.LokiConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.Logger.LokiConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:           a.Logger.LokiConfig.FlushInterval,
		MaxEntries:              a.Logger.LokiConfig.MaxEntries,
		BufferFlushBytes:        a.Logger.LokiConfig.BufferFlushBytes,
		FallbackQueueMaxEntries: a.Logger.LokiConfig.FallbackQueueMaxEntries,
		MaxLineBytes:            a.Logger.LokiConfig.MaxLineBytes,
		MaxPushRequestBytes:     a.Logger.LokiConfig.MaxPushRequestBytes,
		NoBulkFlushModeEnabled:  a.Logger.LokiConfig.NoBulkFlushModeEnabled,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.Logger.LokiConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.Logger.LokiConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.Logger.LokiConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.Logger.LokiConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.Logger.LokiConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.Logger.LokiConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.Logger.LokiConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.Logger.LokiConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.Logger.LokiConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.Logger.LokiConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.Logger.LokiConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewAsyncLokiLoggerConfig(a *AppConfig) logger.LokiLoggerBaseConfig {
	return logger.LokiLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.AsyncBestEffortMultiLoggerConfig.LokiConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.AsyncBestEffortMultiLoggerConfig.LokiConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.AsyncBestEffortMultiLoggerConfig.LokiConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:           a.AsyncBestEffortMultiLoggerConfig.LokiConfig.FlushInterval,
		MaxEntries:              a.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxEntries,
		BufferFlushBytes:        a.AsyncBestEffortMultiLoggerConfig.LokiConfig.BufferFlushBytes,
		FallbackQueueMaxEntries: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.FallbackQueueMaxEntries,
		MaxLineBytes:            a.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxLineBytes,
		MaxPushRequestBytes:     a.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxPushRequestBytes,
		NoBulkFlushModeEnabled:  false,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.AsyncBestEffortMultiLoggerConfig.LokiConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.AsyncBestEffortMultiLoggerConfig.LokiConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.AsyncBestEffortMultiLoggerConfig.LokiConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.AsyncBestEffortMultiLoggerConfig.LokiConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.AsyncBestEffortMultiLoggerConfig.LokiConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.AsyncBestEffortMultiLoggerConfig.LokiConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.AsyncBestEffortMultiLoggerConfig.LokiConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.AsyncBestEffortMultiLoggerConfig.LokiConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewDeliveryLokiLoggerConfig(a *AppConfig) logger.DeliveryLokiLoggerBaseConfig {
	return logger.DeliveryLokiLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.DeliveryLogger.DeliveryLokiConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.DeliveryLogger.DeliveryLokiConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.DeliveryRuntimeConfig{
			AppName: a.AppConfig.AppName,
			IsDebug: a.RuntimeConfig.IsDebug,
			Target:  a.RuntimeConfig.Target,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.DeliveryLogger.DeliveryLokiConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.DeliveryLogger.DeliveryLokiConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.DeliveryLogger.DeliveryLokiConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		MaxLineBytes:        a.DeliveryLogger.DeliveryLokiConfig.MaxLineBytes,
		MaxPushRequestBytes: a.DeliveryLogger.DeliveryLokiConfig.MaxPushRequestBytes,
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.DeliveryLogger.DeliveryLokiConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.DeliveryLogger.DeliveryLokiConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.DeliveryLogger.DeliveryLokiConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.DeliveryLogger.DeliveryLokiConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewCloudWatchLogsLoggerConfig(a *AppConfig) logger.CloudWatchLogsLoggerBaseConfig {
	return logger.CloudWatchLogsLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.Logger.CloudWatchLogsConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.Logger.CloudWatchLogsConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.Logger.CloudWatchLogsConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.Logger.CloudWatchLogsConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.Logger.CloudWatchLogsConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.Logger.CloudWatchLogsConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:          a.Logger.CloudWatchLogsConfig.FlushInterval,
		BufferFlushMaxEvents:   a.Logger.CloudWatchLogsConfig.BufferFlushMaxEvents,
		BufferFlushBytes:       a.Logger.CloudWatchLogsConfig.BufferFlushBytes,
		FallbackQueueMaxEvents: a.Logger.CloudWatchLogsConfig.FallbackQueueMaxEvents,
		NoBulkFlushModeEnabled: a.Logger.CloudWatchLogsConfig.NoBulkFlushModeEnabled,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.Logger.CloudWatchLogsConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.Logger.CloudWatchLogsConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.Logger.CloudWatchLogsConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.Logger.CloudWatchLogsConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.Logger.CloudWatchLogsConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.Logger.CloudWatchLogsConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.Logger.CloudWatchLogsConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.Logger.CloudWatchLogsConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.Logger.CloudWatchLogsConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.Logger.CloudWatchLogsConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.Logger.CloudWatchLogsConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewAsyncCloudWatchLogsLoggerConfig(a *AppConfig) logger.CloudWatchLogsLoggerBaseConfig {
	return logger.CloudWatchLogsLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:          a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.FlushInterval,
		BufferFlushMaxEvents:   a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.BufferFlushMaxEvents,
		BufferFlushBytes:       a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.BufferFlushBytes,
		FallbackQueueMaxEvents: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.FallbackQueueMaxEvents,
		NoBulkFlushModeEnabled: false,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewDeliveryCloudWatchLogsLoggerConfig(a *AppConfig) logger.DeliveryCloudWatchLogsLoggerBaseConfig {
	return logger.DeliveryCloudWatchLogsLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.DeliveryLogger.DeliveryCloudWatchLogsConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.DeliveryLogger.DeliveryCloudWatchLogsConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.DeliveryRuntimeConfig{
			AppName: a.AppConfig.AppName,
			IsDebug: a.RuntimeConfig.IsDebug,
			Target:  a.RuntimeConfig.Target,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.DeliveryLogger.DeliveryCloudWatchLogsConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.DeliveryLogger.DeliveryCloudWatchLogsConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.DeliveryLogger.DeliveryCloudWatchLogsConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.DeliveryLogger.DeliveryCloudWatchLogsConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewClickHouseLoggerConfig(a *AppConfig) logger.ClickHouseLoggerBaseConfig {
	return logger.ClickHouseLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.Logger.ClickHouseConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.Logger.ClickHouseConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.Logger.ClickHouseConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.Logger.ClickHouseConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.Logger.ClickHouseConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.Logger.ClickHouseConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:           a.Logger.ClickHouseConfig.FlushInterval,
		MaxEntries:              a.Logger.ClickHouseConfig.MaxEntries,
		BufferFlushBytes:        a.Logger.ClickHouseConfig.BufferFlushBytes,
		FallbackQueueMaxEntries: a.Logger.ClickHouseConfig.FallbackQueueMaxEntries,
		MaxInsertRequestBytes:   a.Logger.ClickHouseConfig.MaxInsertRequestBytes,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.Logger.ClickHouseConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.Logger.ClickHouseConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.Logger.ClickHouseConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.Logger.ClickHouseConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.Logger.ClickHouseConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.Logger.ClickHouseConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.Logger.ClickHouseConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.Logger.ClickHouseConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.Logger.ClickHouseConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.Logger.ClickHouseConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.Logger.ClickHouseConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewAsyncClickHouseLoggerConfig(a *AppConfig) logger.ClickHouseLoggerBaseConfig {
	return logger.ClickHouseLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.RuntimeConfig{
			AppName:  a.AppConfig.AppName,
			IsDebug:  a.RuntimeConfig.IsDebug,
			Target:   a.RuntimeConfig.Target,
			LogLevel: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.LogLevel,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:           a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.FlushInterval,
		MaxEntries:              a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.MaxEntries,
		BufferFlushBytes:        a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.BufferFlushBytes,
		FallbackQueueMaxEntries: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.FallbackQueueMaxEntries,
		MaxInsertRequestBytes:   a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.MaxInsertRequestBytes,
		SamplingConfig: logger.SamplingConfig{
			IsSamplingEnabled:        a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.SamplingConfig.IsSamplingEnabled,
			EnableLoggingByRequestID: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.SamplingConfig.EnableLoggingByRequestID,
			LoggingByRequestIDRate:   a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.SamplingConfig.LoggingByRequestIDRate,
			SamplingDebugRate:        a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.SamplingConfig.SamplingDebugRate,
			SamplingInfoRate:         a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.SamplingConfig.SamplingInfoRate,
			SamplingWarnRate:         a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.SamplingConfig.SamplingWarnRate,
			SamplingErrorRate:        a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.SamplingConfig.SamplingErrorRate,
		},
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewDeliveryClickHouseLoggerConfig(a *AppConfig) logger.DeliveryClickHouseLoggerBaseConfig {
	return logger.DeliveryClickHouseLoggerBaseConfig{
		TimeConfig: logger.TimeConfig{
			IsLocalTimeEnabled: a.DeliveryLogger.DeliveryClickHouseConfig.ClockConfig.IsLocalTimeEnabled,
			TimeZone:           a.DeliveryLogger.DeliveryClickHouseConfig.ClockConfig.TimeZone,
		},
		RuntimeConfig: logger.DeliveryRuntimeConfig{
			AppName: a.AppConfig.AppName,
			IsDebug: a.RuntimeConfig.IsDebug,
			Target:  a.RuntimeConfig.Target,
		},
		LogBodyConfig: logger.LogBodyConfig{
			LogLineDateFormat:        a.DeliveryLogger.DeliveryClickHouseConfig.LogBodyConfig.LogLineDateFormat,
			ErrorTraceDepth:          a.DeliveryLogger.DeliveryClickHouseConfig.LogBodyConfig.ErrorTraceDepth,
			ErrorTraceDepthDebugMode: a.DeliveryLogger.DeliveryClickHouseConfig.LogBodyConfig.ErrorTraceDepthDebugMode,
		},
		FlushInterval:           a.DeliveryLogger.DeliveryClickHouseConfig.FlushInterval,
		MaxEntries:              a.DeliveryLogger.DeliveryClickHouseConfig.MaxEntries,
		BufferFlushBytes:        a.DeliveryLogger.DeliveryClickHouseConfig.BufferFlushBytes,
		FallbackQueueMaxEntries: a.DeliveryLogger.DeliveryClickHouseConfig.FallbackQueueMaxEntries,
		MaxInsertRequestBytes:   a.DeliveryLogger.DeliveryClickHouseConfig.MaxInsertRequestBytes,
		HandlerConfig: handler.HandlerConfig{
			OnErrorConfig: a.DeliveryLogger.DeliveryClickHouseConfig.HandlerConfig.OnErrorConfig,
			OnWarnConfig:  a.DeliveryLogger.DeliveryClickHouseConfig.HandlerConfig.OnWarnConfig,
			OnInfoConfig:  a.DeliveryLogger.DeliveryClickHouseConfig.HandlerConfig.OnInfoConfig,
			OnDebugConfig: a.DeliveryLogger.DeliveryClickHouseConfig.HandlerConfig.OnDebugConfig,
		},
	}
}

func NewOpenSearchClientConfig(a *AppConfig) *opensearch.Config {
	return newOpenSearchClientConfig(a.OpenSearchClient)
}

func NewAsyncOpenSearchClientConfig(a *AppConfig) *opensearch.Config {
	return newOpenSearchClientConfig(a.AsyncOpenSearchClient)
}

func NewDeliveryOpenSearchClientConfig(a *AppConfig) *opensearch.Config {
	return newOpenSearchClientConfig(a.DeliveryOpenSearchClient)
}

func newOpenSearchClientConfig(cfg configopensearch.OpenSearchClientConfig) *opensearch.Config {
	var authCfg interface{}

	switch cfg.Auth.Type {
	case opensearch.AuthTypeBasic:
		if cfg.Auth.Basic != nil {
			authCfg = opensearch.BasicAuthConfig{
				Username: cfg.Auth.Basic.Username,
				Password: cfg.Auth.Basic.Password,
			}
		}
	case opensearch.AuthTypeAPIKey:
		if cfg.Auth.APIKey != nil {
			authCfg = opensearch.APIKeyAuthConfig{
				ID:     cfg.Auth.APIKey.ID,
				Secret: cfg.Auth.APIKey.Secret,
			}
		}
	case opensearch.AuthTypeAWSIAM:
		if cfg.Auth.AWSIAM != nil {
			authCfg = opensearch.AWSIAMAuthConfig{
				Region:          cfg.Auth.AWSIAM.Region,
				AccessKeyID:     cfg.Auth.AWSIAM.AccessKeyID,
				SecretAccessKey: cfg.Auth.AWSIAM.SecretAccessKey,
				SessionToken:    cfg.Auth.AWSIAM.SessionToken,
			}
		}
	case opensearch.AuthTypeSAML:
		if cfg.Auth.SAML != nil {
			authCfg = opensearch.SAMLAuthConfig{
				Token: cfg.Auth.SAML.Token,
			}
		}
	case opensearch.AuthTypeOIDC:
		if cfg.Auth.OIDC != nil {
			authCfg = opensearch.OIDCAuthConfig{
				Token: cfg.Auth.OIDC.Token,
			}
		}
	case opensearch.AuthTypeCertificate:
		if cfg.Auth.Certificate != nil {
			authCfg = opensearch.CertificateAuthConfig{
				CertFile: cfg.Auth.Certificate.CertFile,
				KeyFile:  cfg.Auth.Certificate.KeyFile,
				CAFile:   cfg.Auth.Certificate.CAFile,
			}
		}
	case opensearch.AuthTypeNone:
		authCfg = nil
	default:
		// unsupported typeは、DTO生成後にopensearch.ValidateConfigから設定エラーとして返す。
		authCfg = nil
	}

	return &opensearch.Config{
		Endpoint:      cfg.Endpoint,
		Timeout:       cfg.Timeout,
		SkipVerifySSL: cfg.SkipVerifySSL,
		AuthConfig: opensearch.AuthConfig{
			Type:   cfg.Auth.Type,
			Config: authCfg,
		},
		CompressRequest: cfg.CompressRequest,
	}
}

func NewLokiClientConfig(a *AppConfig) *pkgloki.Config {
	return newLokiClientConfig(a.LokiClient)
}

func NewAsyncLokiClientConfig(a *AppConfig) *pkgloki.Config {
	return newLokiClientConfig(a.AsyncLokiClient)
}

func NewDeliveryLokiClientConfig(a *AppConfig) *pkgloki.Config {
	return newLokiClientConfig(a.DeliveryLokiClient)
}

func newLokiClientConfig(cfg configloki.LokiClientConfig) *pkgloki.Config {
	var basicAuth *pkgloki.BasicAuthConfig
	if cfg.BasicAuth != nil {
		basicAuth = &pkgloki.BasicAuthConfig{
			Username: cfg.BasicAuth.Username,
			Password: cfg.BasicAuth.Password,
		}
	}

	return &pkgloki.Config{
		Endpoint:        cfg.Endpoint,
		Timeout:         cfg.Timeout,
		TenantID:        cfg.TenantID,
		BasicAuth:       basicAuth,
		BearerToken:     cfg.BearerToken,
		Headers:         cfg.Headers,
		CompressRequest: cfg.CompressRequest,
	}
}

func NewCloudWatchLogsClientConfig(a *AppConfig) *pkgcloudwatchlogs.Config {
	return newCloudWatchLogsClientConfig(a.CloudWatchLogsClient)
}

func NewAsyncCloudWatchLogsClientConfig(a *AppConfig) *pkgcloudwatchlogs.Config {
	return newCloudWatchLogsClientConfig(a.AsyncCloudWatchLogsClient)
}

func NewDeliveryCloudWatchLogsClientConfig(a *AppConfig) *pkgcloudwatchlogs.Config {
	return newCloudWatchLogsClientConfig(a.DeliveryCloudWatchLogsClient)
}

func newCloudWatchLogsClientConfig(cfg configcloudwatchlogs.CloudWatchLogsClientConfig) *pkgcloudwatchlogs.Config {
	return &pkgcloudwatchlogs.Config{
		Region:              cfg.Region,
		Endpoint:            cfg.Endpoint,
		LogGroupName:        cfg.LogGroupName,
		Timeout:             cfg.Timeout,
		AccessKeyID:         cfg.AccessKeyID,
		SecretAccessKey:     cfg.SecretAccessKey,
		SessionToken:        cfg.SessionToken,
		Headers:             cfg.Headers,
		AutoCreateLogStream: cfg.AutoCreateLogStream,
		CompressRequest:     cfg.CompressRequest,
	}
}

func NewClickHouseClientConfig(a *AppConfig) *pkgclickhouse.Config {
	return newClickHouseClientConfig(a.ClickHouseClient)
}

func NewAsyncClickHouseClientConfig(a *AppConfig) *pkgclickhouse.Config {
	return newClickHouseClientConfig(a.AsyncClickHouseClient)
}

func NewDeliveryClickHouseClientConfig(a *AppConfig) *pkgclickhouse.Config {
	return newClickHouseClientConfig(a.DeliveryClickHouseClient)
}

func newClickHouseClientConfig(cfg configclickhouse.ClientConfig) *pkgclickhouse.Config {
	return &pkgclickhouse.Config{
		Endpoint:        cfg.Endpoint,
		Database:        cfg.Database,
		Table:           cfg.Table,
		Timeout:         cfg.Timeout,
		Username:        cfg.Username,
		Password:        cfg.Password,
		Headers:         cfg.Headers,
		CompressRequest: cfg.CompressRequest,
	}
}

func NewOtelResourceConfig(a *AppConfig) *otel.Resource {
	return &otel.Resource{
		ServiceName:    a.AppConfig.AppName,
		ServiceVersion: a.AppConfig.AppVersion,
		CloudProvider:  a.RuntimeEnv.Cloud.Provider,
		HostName:       a.RuntimeEnv.CoreInfo.HostName,
		OSType:         a.RuntimeEnv.CoreInfo.OSType,
		Environment:    a.RuntimeEnv.CoreInfo.Environment,
	}
}

func NewSpoolConfig(a *AppConfig) *pkgspool.SpoolConfig {
	return &pkgspool.SpoolConfig{
		SupervisorConfig: pkgspoolsupervisor.RunnerSupervisorConfig{
			RestartDelay: a.SpoolConfig.SupervisorConfig.RestartDelay,
			MaxRestarts:  a.SpoolConfig.SupervisorConfig.MaxRestarts,
		},
		RunnerConfig: pkgspoolrunner.SpoolRunnerConfig{
			OpensearchConfig:     mapBackendSpoolRunnerConfig(a.SpoolConfig.RunnerConfig.OpensearchConfig),
			LokiConfig:           mapBackendSpoolRunnerConfig(a.SpoolConfig.RunnerConfig.LokiConfig),
			CloudWatchLogsConfig: mapBackendSpoolRunnerConfig(a.SpoolConfig.RunnerConfig.CloudWatchLogsConfig),
			ClickHouseConfig:     mapBackendSpoolRunnerConfig(a.SpoolConfig.RunnerConfig.ClickHouseConfig),
		},
	}
}

func mapBackendSpoolRunnerConfig(cfg pkgspoolrunner.BackendSpoolRunnerConfig) pkgspoolrunner.BackendSpoolRunnerConfig {
	return pkgspoolrunner.BackendSpoolRunnerConfig{
		Runner: pkgspoolrunner.RunnerConfig{
			Interval:    cfg.Runner.Interval,
			StopOnError: cfg.Runner.StopOnError,
		},
		LifeCycle: pkgspoollifecycle.LifecycleConfig{
			EnablePurgeDone:          cfg.LifeCycle.EnablePurgeDone,
			DoneMaxFileAge:           cfg.LifeCycle.DoneMaxFileAge,
			DoneMaxDirSize:           cfg.LifeCycle.DoneMaxDirSize,
			EnablePurgeDead:          cfg.LifeCycle.EnablePurgeDead,
			DeadMaxFileAge:           cfg.LifeCycle.DeadMaxFileAge,
			DeadMaxDirSize:           cfg.LifeCycle.DeadMaxDirSize,
			EnablePurgeQuarantine:    cfg.LifeCycle.EnablePurgeQuarantine,
			QuarantineMaxFileAge:     cfg.LifeCycle.QuarantineMaxFileAge,
			QuarantineMaxDirSize:     cfg.LifeCycle.QuarantineMaxDirSize,
			SizePurgeInterval:        cfg.LifeCycle.SizePurgeInterval,
			EnableSizeEstimateCache:  cfg.LifeCycle.EnableSizeEstimateCache,
			SizeFullScanEveryNChecks: cfg.LifeCycle.SizeFullScanEveryNChecks,
			SizeFullScanMaxInterval:  cfg.LifeCycle.SizeFullScanMaxInterval,
		},
		Worker: pkgspoolrunner.WorkerConfig{
			Finalize: pkgspoolrunner.FinalizeConfig{
				RetryDelay: cfg.Worker.Finalize.RetryDelay,
				MaxEntries: cfg.Worker.Finalize.MaxEntries,
			},
			MaxRetryAfter: cfg.Worker.MaxRetryAfter,
		},
	}
}

// MaxLogEntryBytesErrorは、Contractのloggerへ変換した一件のlogのbyte上限Valueが不正であることを
// 表します。Errはpkg/loggerの検証errorで、ErrInvalidMaxLogEntryBytesとerrors.Isで判別できます。
type MaxLogEntryBytesError struct {
	Contract logger.LoggerContractKind
	Value    int
	Err      error
}

func (e *MaxLogEntryBytesError) Error() string {
	return fmt.Sprintf("%s config is invalid: %v", e.Contract.DisplayName(), e.Err)
}

func (e *MaxLogEntryBytesError) Unwrap() error { return e.Err }

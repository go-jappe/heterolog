package mappedvalidation

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/filelifecycle"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

// mappedFailureStoreは、送信に失敗したレコードの保存設定と、エラー表示用の送信先名です。
type mappedFailureStore struct {
	sourceSuffix string
	config       durable.FailureStoreConfig
}

// mappedDeliveryFailureStoreは、Delivery logger用の失敗レコード保存設定と送信先名です。
type mappedDeliveryFailureStore struct {
	sourceSuffix string
	config       durable.DeliveryFailureStoreConfig
}

// containsDurableTypeは、指定した保存方式が選択一覧に含まれるかを返します。
func containsDurableType(values []durable.DurableType, target durable.DurableType) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// validateMappedFailureStoresは、同期・非同期loggerで送信できなかったレコードを
// .jsonlへ追記保存するfileArchive、または再送用spoolへ保存する設定を検証します。
// 通常の.logを圧縮して送信する際の、圧縮ファイル自体の失敗保存設定ではありません。
//
// 通常出力に使うOpenSearch・Loki・CloudWatch Logs・ClickHouseだけが対象です。
// 内部エラー等の通知先としてだけ使う送信先には失敗保存処理を作らないため、含めません。
// 保存方式の空指定・未知値・重複・noneとの併用を拒否し、選択した方式の設定を調べます。
// 保存処理または直接退避が有効なら、同時処理数MaxConcurrentは正の値を必須とします。
// fileArchive/spoolを選ばない場合は、その設定を検証せず、直接退避が有効な場合だけ
// 退避先を検証します。設定エラーをまとめて返し、ファイル保存や送信は行いません。
func validateMappedFailureStores(sourceLabel string, cfg pkglogger.LoggerConfig) error {
	var errs []error
	for _, store := range mappedFailureStores(cfg) {
		storeSource := sourceLabel + " " + store.sourceSuffix
		if err := durable.ValidateFailureStoreDurableTypes(store.config.DurableType); err != nil {
			errs = append(errs, &ConfigError{Source: storeSource, Section: ConfigSectionFailureStore, Err: err})
			continue
		}
		storeSelected := containsDurableType(store.config.DurableType, durable.DurableTypeFileArchive) ||
			containsDurableType(store.config.DurableType, durable.DurableTypeSpool)
		if storeSelected || store.config.EnableDegrade {
			if err := durable.ValidateFailureHandlerMaxConcurrent(store.config.MaxConcurrent); err != nil {
				errs = append(errs, &ConfigError{
					Source:  storeSource,
					Section: ConfigSectionFailureStore,
					Err:     err,
				})
			}
		}
		if !storeSelected {
			if store.config.EnableDegrade {
				errs = append(errs, validateMappedDirectDegradeSpillTypes(
					storeSource,
					store.config.SpillType,
				))
			}
			continue
		}
		if containsDurableType(store.config.DurableType, durable.DurableTypeSpool) {
			// 未送信spoolの滞留警告日数は負数を拒否します。0は警告無効です。
			// spoolの全設定や再送runnerの設定をここで検証するわけではありません。
			if err := durable.ValidateSpoolActiveBacklogWarnAfterDays(
				store.config.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.ActiveBacklogWarnAfterDays,
			); err != nil {
				errs = append(errs, &ConfigError{
					Source:  storeSource,
					Section: ConfigSectionFailureSpool,
					Err:     err,
				})
			}
		}
		if containsDurableType(store.config.DurableType, durable.DurableTypeFileArchive) {
			// .jsonlの命名値に':'がないことと、圧縮・削除等の処理方式を調べます。
			// 使用量cacheの有効期間はinline/runner時、周期実行設定はrunner時だけ検証します。
			archiveConfig := store.config.DurableStoreConfig.FileArchiveConfig
			failureArchiveSourceLabel := storeSource + " failure file archive"
			if err := errors.Join(
				pkgfsstore.ValidateLogFileNameValue("target", archiveConfig.Target),
				pkgfsstore.ValidateLogFileNameValue("file name format", archiveConfig.FileNameFormat),
				pkgfsstore.ValidateLogFileNameValue("file date format", archiveConfig.FileDateFormat),
			); err != nil {
				errs = append(errs, &ConfigError{
					Source:  failureArchiveSourceLabel,
					Section: ConfigSectionFileName,
					Err:     err,
				})
			}
			if filelifecycle.UsesUsageCache(archiveConfig.LifecycleMode) {
				errs = append(errs, validateMappedUsageCacheTTL(
					failureArchiveSourceLabel,
					archiveConfig.UsageCacheTTL,
				))
			}
			errs = append(errs, validateMappedFileLifecycleMode(
				failureArchiveSourceLabel,
				archiveConfig.LifecycleMode,
			))
			errs = append(errs, validateMappedAppendLogRunnerConfig(
				failureArchiveSourceLabel,
				archiveConfig.LifecycleMode,
				archiveConfig.LifecycleRunnerConfig,
			))
		}
		errs = append(errs, validateMappedFailureArchiveUpload(storeSource, store.config))
	}
	return errors.Join(errs...)
}

// validateMappedDeliveryFailureStoresは、Delivery loggerの送信失敗レコードを
// .jsonlへ保存する設定を検証します。対象の送信先はvalidateMappedFailureStoresと同じです。
// DeliveryではfileArchiveまたはnoneだけを認め、spool・空指定・未知値・重複は拒否します。
// fileArchiveを使う場合は命名値・圧縮や削除等の処理方式・送信先・checkpointを調べます。
// noneの場合はfileArchive設定を無視し、直接退避が有効ならその退避先だけを検証します。
// 設定エラーをまとめて返すだけで、レコードの保存・再送は行いません。
func validateMappedDeliveryFailureStores(
	sourceLabel string,
	cfg pkglogger.DeliveryLoggerConfig,
) error {
	var errs []error
	for _, store := range mappedDeliveryFailureStores(cfg) {
		storeSource := sourceLabel + " " + store.sourceSuffix
		if err := durable.ValidateDeliveryFailureStoreDurableTypes(
			store.config.DurableType,
		); err != nil {
			errs = append(errs, &ConfigError{Source: storeSource, Section: ConfigSectionFailureStore, Err: err})
			continue
		}
		storeSelected := containsDurableType(
			store.config.DurableType,
			durable.DurableTypeFileArchive,
		)
		if !storeSelected {
			if store.config.EnableDegrade {
				errs = append(errs, validateMappedDirectDegradeSpillTypes(
					storeSource,
					store.config.SpillType,
				))
			}
			continue
		}
		if containsDurableType(store.config.DurableType, durable.DurableTypeFileArchive) {
			archiveConfig := store.config.FileArchiveConfig
			failureArchiveSourceLabel := storeSource + " failure file archive"
			if err := errors.Join(
				pkgfsstore.ValidateLogFileNameValue("target", archiveConfig.Target),
				pkgfsstore.ValidateLogFileNameValue("file name format", archiveConfig.FileNameFormat),
				pkgfsstore.ValidateLogFileNameValue("file date format", archiveConfig.FileDateFormat),
			); err != nil {
				errs = append(errs, &ConfigError{
					Source:  failureArchiveSourceLabel,
					Section: ConfigSectionFileName,
					Err:     err,
				})
			}
			if filelifecycle.UsesUsageCache(archiveConfig.LifecycleMode) {
				errs = append(errs, validateMappedUsageCacheTTL(
					failureArchiveSourceLabel,
					archiveConfig.UsageCacheTTL,
				))
			}
			errs = append(errs, validateMappedFileLifecycleMode(
				failureArchiveSourceLabel,
				archiveConfig.LifecycleMode,
			))
			errs = append(errs, validateMappedAppendLogRunnerConfig(
				failureArchiveSourceLabel,
				archiveConfig.LifecycleMode,
				archiveConfig.LifecycleRunnerConfig,
			))
		}
		errs = append(errs, validateMappedDeliveryFailureArchiveUpload(storeSource, store.config))
	}
	return errors.Join(errs...)
}

// validateMappedDirectDegradeSpillTypesは、fileArchive/spoolを使わず直接退避する場合の
// 出力先を検証します。現在はstdout一件だけを認め、空・none・未対応値・重複を拒否します。
func validateMappedDirectDegradeSpillTypes(
	sourceLabel string,
	types []durable.SpillType,
) error {
	if err := durable.ValidateDirectDegradeSpillTypes(types); err != nil {
		return &ConfigError{Source: sourceLabel, Section: ConfigSectionDirectDegrade, Err: err}
	}
	return nil
}

// mappedFailureStoresは、通常出力に使う送信先から失敗レコードの保存設定を集めます。
// OpenSearch・Loki・CloudWatch Logs・ClickHouseのうち、設定が存在するものだけを返します。
func mappedFailureStores(cfg pkglogger.LoggerConfig) []mappedFailureStore {
	destinations := cfg.LogOutputDestinations
	stores := make([]mappedFailureStore, 0, 4)
	if mappedFailureStoreSourceEnabled(destinations, pkglogger.LogOutputOpenSearch) &&
		cfg.OpenSearch != nil {
		stores = append(stores, mappedFailureStore{
			sourceSuffix: "opensearch",
			config:       cfg.OpenSearch.FailureStoreConfig,
		})
	}
	if mappedFailureStoreSourceEnabled(destinations, pkglogger.LogOutputLoki) && cfg.Loki != nil {
		stores = append(stores, mappedFailureStore{
			sourceSuffix: "loki",
			config:       cfg.Loki.FailureStoreConfig,
		})
	}
	if mappedFailureStoreSourceEnabled(destinations, pkglogger.LogOutputCloudWatchLogs) &&
		cfg.CloudWatchLogs != nil {
		stores = append(stores, mappedFailureStore{
			sourceSuffix: "cloudwatch logs",
			config:       cfg.CloudWatchLogs.FailureStoreConfig,
		})
	}
	if mappedFailureStoreSourceEnabled(destinations, pkglogger.LogOutputClickHouse) &&
		cfg.ClickHouse != nil {
		stores = append(stores, mappedFailureStore{
			sourceSuffix: "clickhouse",
			config:       cfg.ClickHouse.FailureStoreConfig,
		})
	}
	return stores
}

// mappedDeliveryFailureStoresは、通常出力に使う送信先のDelivery用失敗保存設定を集めます。
// 対象の選び方はmappedFailureStoresと同じです。
func mappedDeliveryFailureStores(
	cfg pkglogger.DeliveryLoggerConfig,
) []mappedDeliveryFailureStore {
	destinations := cfg.LogOutputDestinations
	stores := make([]mappedDeliveryFailureStore, 0, 4)
	if mappedFailureStoreSourceEnabled(destinations, pkglogger.LogOutputOpenSearch) &&
		cfg.OpenSearch != nil {
		stores = append(stores, mappedDeliveryFailureStore{
			sourceSuffix: "opensearch",
			config:       cfg.OpenSearch.FailureStoreConfig,
		})
	}
	if mappedFailureStoreSourceEnabled(destinations, pkglogger.LogOutputLoki) && cfg.Loki != nil {
		stores = append(stores, mappedDeliveryFailureStore{
			sourceSuffix: "loki",
			config:       cfg.Loki.FailureStoreConfig,
		})
	}
	if mappedFailureStoreSourceEnabled(destinations, pkglogger.LogOutputCloudWatchLogs) &&
		cfg.CloudWatchLogs != nil {
		stores = append(stores, mappedDeliveryFailureStore{
			sourceSuffix: "cloudwatch logs",
			config:       cfg.CloudWatchLogs.FailureStoreConfig,
		})
	}
	if mappedFailureStoreSourceEnabled(destinations, pkglogger.LogOutputClickHouse) &&
		cfg.ClickHouse != nil {
		stores = append(stores, mappedDeliveryFailureStore{
			sourceSuffix: "clickhouse",
			config:       cfg.ClickHouse.FailureStoreConfig,
		})
	}
	return stores
}

// mappedFailureStoreSourceEnabledは、失敗レコードの保存設定を調べる送信先かを判定します。
// 内部通知先の一覧ではなく通常出力の選択だけを見て、通知失敗から保存処理を再帰的に
// 起動する対象を増やしません。通常出力の一覧が空なら除外せず、呼び出し側で設定の有無を調べます。
func mappedFailureStoreSourceEnabled(
	destinations pkglogger.LogOutputDestinations,
	target pkglogger.LogOutputDestination,
) bool {
	return len(destinations) == 0 || destinations.Has(target)
}

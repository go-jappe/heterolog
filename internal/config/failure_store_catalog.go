package config

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/go-jappe/heterolog/internal/config/configerror"
	"github.com/go-jappe/heterolog/internal/config/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// configuredFailureStoreは、syncまたはasync backendに付随するfailure store設定と、
// runtimeでそのbackendを通常出力として構築するかを結び付けます。
type configuredFailureStore struct {
	source   string
	selected bool
	config   failure.FailureHandlerConfig
}

// configuredDeliveryFailureStoreは、Delivery backendに付随するfile-archive専用の
// failure store設定を保持します。Deliveryはspoolを使用しないため、sync／async用の
// configuredFailureStoreへ変換しません。
type configuredDeliveryFailureStore struct {
	source   string
	selected bool
	config   failure.DeliveryFailureHandlerConfig
}

// failureStoreSupportは、destinationがbackend failure storeを持つかを表します。
// ゼロ値は未分類です。destination追加時に対応可否を明示しなければ、起動時validationで
// 検出されます。
type failureStoreSupport uint8

const (
	failureStoreSupportUnclassified failureStoreSupport = iota
	failureStoreSupportUnsupported
	failureStoreSupportSupported
)

// failureStoreCatalogEntryは、一つのdestinationについてfailure storeの対応可否と、
// 対応する場合のsync、async、delivery設定をまとめます。全destinationをこの一つの
// catalogへ列挙することで、対応可否とruntime wiringを別々に管理しません。
type failureStoreCatalogEntry struct {
	destination   pkglogger.LogOutputDestination
	support       failureStoreSupport
	syncStore     configuredFailureStore
	asyncStore    configuredFailureStore
	deliveryStore configuredDeliveryFailureStore
}

// failureStoreCatalogは、全LogOutputDestinationのfailure store対応可否を列挙します。
// 対応するbackendについては、sync、async、deliveryの設定も同じentryへ結び付けます。
// pkg/loggerへdestinationを追加してここを更新し忘れた場合は、起動時validationと
// failure_store_catalog_test.goの両方が検出します。
func (a AppConfig) failureStoreCatalog() []failureStoreCatalogEntry {
	return []failureStoreCatalogEntry{
		{
			destination: pkglogger.LogOutputFile,
			support:     failureStoreSupportUnsupported,
		},
		{
			destination: pkglogger.LogOutputStdout,
			support:     failureStoreSupportUnsupported,
		},
		{
			destination: pkglogger.LogOutputSlack,
			support:     failureStoreSupportUnsupported,
		},
		{
			destination: pkglogger.LogOutputOpenSearch,
			support:     failureStoreSupportSupported,
			syncStore: configuredFailureStore{
				source:   loggerOutputSource(pkglogger.LoggerContractSync, pkglogger.LogOutputOpenSearch),
				selected: a.Logger.LogOutputDestinations.Has(pkglogger.LogOutputOpenSearch),
				config: a.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
					FailureHandlerConfig,
			},
			asyncStore: configuredFailureStore{
				source: loggerOutputSource(pkglogger.LoggerContractAsyncBestEffort, pkglogger.LogOutputOpenSearch),
				selected: a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(
					pkglogger.LogOutputOpenSearch,
				),
				config: a.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.
					OpenSearchFlushManagerConfig.FailureHandlerConfig,
			},
			deliveryStore: configuredDeliveryFailureStore{
				source:   loggerOutputSource(pkglogger.LoggerContractDelivery, pkglogger.LogOutputOpenSearch),
				selected: a.DeliveryLogger.LogOutputDestinations.Has(pkglogger.LogOutputOpenSearch),
				config: a.DeliveryLogger.DeliveryOpenSearchConfig.
					DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig,
			},
		},
		{
			destination: pkglogger.LogOutputLoki,
			support:     failureStoreSupportSupported,
			syncStore: configuredFailureStore{
				source:   loggerOutputSource(pkglogger.LoggerContractSync, pkglogger.LogOutputLoki),
				selected: a.Logger.LogOutputDestinations.Has(pkglogger.LogOutputLoki),
				config:   a.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig,
			},
			asyncStore: configuredFailureStore{
				source: loggerOutputSource(pkglogger.LoggerContractAsyncBestEffort, pkglogger.LogOutputLoki),
				selected: a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(
					pkglogger.LogOutputLoki,
				),
				config: a.AsyncBestEffortMultiLoggerConfig.LokiConfig.
					LokiFlushManagerConfig.FailureHandlerConfig,
			},
			deliveryStore: configuredDeliveryFailureStore{
				source:   loggerOutputSource(pkglogger.LoggerContractDelivery, pkglogger.LogOutputLoki),
				selected: a.DeliveryLogger.LogOutputDestinations.Has(pkglogger.LogOutputLoki),
				config: a.DeliveryLogger.DeliveryLokiConfig.
					DeliveryLokiFlushManagerConfig.FailureHandlerConfig,
			},
		},
		{
			destination: pkglogger.LogOutputCloudWatchLogs,
			support:     failureStoreSupportSupported,
			syncStore: configuredFailureStore{
				source:   loggerOutputSource(pkglogger.LoggerContractSync, pkglogger.LogOutputCloudWatchLogs),
				selected: a.Logger.LogOutputDestinations.Has(pkglogger.LogOutputCloudWatchLogs),
				config: a.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.
					FailureHandlerConfig,
			},
			asyncStore: configuredFailureStore{
				source: loggerOutputSource(pkglogger.LoggerContractAsyncBestEffort, pkglogger.LogOutputCloudWatchLogs),
				selected: a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(
					pkglogger.LogOutputCloudWatchLogs,
				),
				config: a.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.
					CloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
			},
			deliveryStore: configuredDeliveryFailureStore{
				source: loggerOutputSource(pkglogger.LoggerContractDelivery, pkglogger.LogOutputCloudWatchLogs),
				selected: a.DeliveryLogger.LogOutputDestinations.Has(
					pkglogger.LogOutputCloudWatchLogs,
				),
				config: a.DeliveryLogger.DeliveryCloudWatchLogsConfig.
					DeliveryCloudWatchLogsFlushManagerConfig.FailureHandlerConfig,
			},
		},
		{
			destination: pkglogger.LogOutputClickHouse,
			support:     failureStoreSupportSupported,
			syncStore: configuredFailureStore{
				source:   loggerOutputSource(pkglogger.LoggerContractSync, pkglogger.LogOutputClickHouse),
				selected: a.Logger.LogOutputDestinations.Has(pkglogger.LogOutputClickHouse),
				config: a.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.
					FailureHandlerConfig,
			},
			asyncStore: configuredFailureStore{
				source: loggerOutputSource(pkglogger.LoggerContractAsyncBestEffort, pkglogger.LogOutputClickHouse),
				selected: a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(
					pkglogger.LogOutputClickHouse,
				),
				config: a.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.
					ClickHouseFlushManagerConfig.FailureHandlerConfig,
			},
			deliveryStore: configuredDeliveryFailureStore{
				source:   loggerOutputSource(pkglogger.LoggerContractDelivery, pkglogger.LogOutputClickHouse),
				selected: a.DeliveryLogger.LogOutputDestinations.Has(pkglogger.LogOutputClickHouse),
				config: a.DeliveryLogger.DeliveryClickHouseConfig.
					DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig,
			},
		},
	}
}

// validateFailureStoreCatalogは、全LogOutputDestinationについてfailure store対応可否が
// 明示され、対応backendがsync／async／deliveryを一組としてcatalogへ登録されていることを
// 検証します。これは`.env`値ではなくprogram wiringの不整合なので、application全体の
// 起動時validationで実行します。
func validateFailureStoreCatalog(a AppConfig) error {
	return validateFailureStoreCatalogEntries(
		a,
		pkglogger.AllLogOutputDestinations(),
		a.failureStoreCatalog(),
	)
}

// failure storeの対応表とlog出力先の一覧の不整合の種類です。対応表はapplication内部の
// 定義なので、利用者の設定ではなく実装の不備を表します。
var (
	errFailureStoreCatalogMissingDestination = errors.New("log output destination is missing from the failure store catalog")
	errFailureStoreCatalogUnclassified       = errors.New("log output destination has not been classified for failure store support")
	errFailureStoreCatalogUnsupportedWiring  = errors.New("failure-store-unsupported destination must not contain failure store wiring")
)

func validateFailureStoreCatalogEntries(
	a AppConfig,
	destinations pkglogger.LogOutputDestinations,
	entries []failureStoreCatalogEntry,
) error {
	registered := make(map[pkglogger.LogOutputDestination]failureStoreCatalogEntry, len(entries))
	expected := make(map[pkglogger.LogOutputDestination]struct{}, len(destinations))
	var errs []error

	for _, destination := range destinations {
		expected[destination] = struct{}{}
	}

	for index, entry := range entries {
		if !entry.destination.IsValid() {
			errs = append(errs, fmt.Errorf(
				"failure store catalog[%d] has invalid destination %q",
				index,
				entry.destination,
			))
			continue
		}
		if _, duplicated := registered[entry.destination]; duplicated {
			errs = append(errs, fmt.Errorf(
				"failure store catalog contains destination %q more than once",
				entry.destination,
			))
			continue
		}
		registered[entry.destination] = entry

		if _, exists := expected[entry.destination]; !exists {
			errs = append(errs, fmt.Errorf(
				"failure store catalog contains unknown destination %q",
				entry.destination,
			))
		}

		switch entry.support {
		case failureStoreSupportSupported:
			errs = append(errs, validateFailureStoreEntry(a, entry))
		case failureStoreSupportUnsupported:
			errs = append(errs, validateUnsupportedFailureStoreEntry(entry))
		case failureStoreSupportUnclassified:
			errs = append(errs, configerror.New(
				errFailureStoreCatalogUnclassified,
				"log output destination %q has not been classified for failure store support",
				entry.destination,
			))
		default:
			errs = append(errs, fmt.Errorf(
				"log output destination %q has invalid failure store support %d",
				entry.destination,
				entry.support,
			))
		}
	}

	for _, destination := range destinations {
		if _, exists := registered[destination]; !exists {
			errs = append(errs, configerror.New(
				errFailureStoreCatalogMissingDestination,
				"log output destination %q is missing from the failure store catalog",
				destination,
			))
		}
	}
	return errors.Join(errs...)
}

func validateFailureStoreEntry(a AppConfig, entry failureStoreCatalogEntry) error {
	var errs []error

	sources := []struct {
		got  string
		want string
	}{
		{got: entry.syncStore.source, want: loggerOutputSource(pkglogger.LoggerContractSync, entry.destination)},
		{got: entry.asyncStore.source, want: loggerOutputSource(pkglogger.LoggerContractAsyncBestEffort, entry.destination)},
		{got: entry.deliveryStore.source, want: loggerOutputSource(pkglogger.LoggerContractDelivery, entry.destination)},
	}
	for _, source := range sources {
		if source.got != source.want {
			errs = append(errs, fmt.Errorf(
				"failure store destination %q has source %q, want %q",
				entry.destination,
				source.got,
				source.want,
			))
		}
	}

	wantSyncSelected := a.Logger.LogOutputDestinations.Has(entry.destination)
	if entry.syncStore.selected != wantSyncSelected {
		errs = append(errs, fmt.Errorf(
			"failure store destination %q sync selection is %t, want %t",
			entry.destination,
			entry.syncStore.selected,
			wantSyncSelected,
		))
	}
	wantAsyncSelected := a.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations.Has(entry.destination)
	if entry.asyncStore.selected != wantAsyncSelected {
		errs = append(errs, fmt.Errorf(
			"failure store destination %q async selection is %t, want %t",
			entry.destination,
			entry.asyncStore.selected,
			wantAsyncSelected,
		))
	}
	wantDeliverySelected := a.DeliveryLogger.LogOutputDestinations.Has(entry.destination)
	if entry.deliveryStore.selected != wantDeliverySelected {
		errs = append(errs, fmt.Errorf(
			"failure store destination %q delivery selection is %t, want %t",
			entry.destination,
			entry.deliveryStore.selected,
			wantDeliverySelected,
		))
	}
	return errors.Join(errs...)
}

func validateUnsupportedFailureStoreEntry(entry failureStoreCatalogEntry) error {
	if entry.syncStore.source == "" &&
		!entry.syncStore.selected &&
		reflect.DeepEqual(entry.syncStore.config, failure.FailureHandlerConfig{}) &&
		entry.asyncStore.source == "" &&
		!entry.asyncStore.selected &&
		reflect.DeepEqual(entry.asyncStore.config, failure.FailureHandlerConfig{}) &&
		entry.deliveryStore.source == "" &&
		!entry.deliveryStore.selected &&
		reflect.DeepEqual(entry.deliveryStore.config, failure.DeliveryFailureHandlerConfig{}) {
		return nil
	}
	return configerror.New(
		errFailureStoreCatalogUnsupportedWiring,
		"failure-store-unsupported destination %q must not contain failure store wiring",
		entry.destination,
	)
}

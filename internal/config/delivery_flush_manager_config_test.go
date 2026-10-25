package config

import (
	"errors"
	"reflect"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestNonClickHouseDeliveryConfigsExcludeBatchAndFlushRetryControls(t *testing.T) {
	tests := []struct {
		name             string
		loggerBaseType   reflect.Type
		flushManagerType reflect.Type
	}{
		{
			name:             "opensearch",
			loggerBaseType:   reflect.TypeOf(logconfig.DeliveryOpenSearchLoggerBaseConfig{}),
			flushManagerType: reflect.TypeOf(pkglogger.DeliveryOpenSearchFlushManagerConfig{}),
		},
		{
			name:             "loki",
			loggerBaseType:   reflect.TypeOf(logconfig.DeliveryLokiLoggerBaseConfig{}),
			flushManagerType: reflect.TypeOf(pkglogger.DeliveryLokiFlushManagerConfig{}),
		},
		{
			name:             "cloudwatchlogs",
			loggerBaseType:   reflect.TypeOf(logconfig.DeliveryCloudWatchLogsLoggerBaseConfig{}),
			flushManagerType: reflect.TypeOf(pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{}),
		},
	}

	batchFields := []string{
		"FlushInterval",
		"MaxBulkDocument",
		"MaxEntries",
		"MaxEvents",
		"NoBulkFlushModeEnabled",
	}
	flushRetryFields := []string{"MaxRetryAttempts", "RetryWaitTime"}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, field := range batchFields {
				if _, ok := tt.loggerBaseType.FieldByName(field); ok {
					t.Fatalf("%s must not contain batch control %s", tt.loggerBaseType.Name(), field)
				}
			}
			for _, field := range flushRetryFields {
				if _, ok := tt.flushManagerType.FieldByName(field); ok {
					t.Fatalf("%s must not contain flush retry control %s", tt.flushManagerType.Name(), field)
				}
			}
		})
	}
}

func TestNonOpenSearchConfigsExcludeUnusedFlushRetryControls(t *testing.T) {
	// これらのbackendはtransport retry設定を使用します。OpenSearchのBulk item
	// 再送用フィールドをコピーしても使用されないため、設定契約へ持ち込まないようにします。
	configTypes := []reflect.Type{
		reflect.TypeOf(logconfig.LokiLoggerBaseConfig{}),
		reflect.TypeOf(pkglogger.LokiFlushManagerConfig{}),
		reflect.TypeOf(logconfig.CloudWatchLogsLoggerBaseConfig{}),
		reflect.TypeOf(pkglogger.CloudWatchLogsFlushManagerConfig{}),
		reflect.TypeOf(logconfig.ClickHouseLoggerBaseConfig{}),
		reflect.TypeOf(pkglogger.ClickHouseFlushManagerConfig{}),
		reflect.TypeOf(logconfig.DeliveryClickHouseLoggerBaseConfig{}),
		reflect.TypeOf(pkglogger.DeliveryClickHouseFlushManagerConfig{}),
	}
	for _, configType := range configTypes {
		t.Run(configType.PkgPath()+"/"+configType.Name(), func(t *testing.T) {
			for _, field := range []string{"MaxRetryAttempts", "RetryWaitTime"} {
				if _, exists := configType.FieldByName(field); exists {
					t.Fatalf("%s must not contain unused flush retry control %s", configType, field)
				}
			}
		})
	}
}

func TestDeliveryBackendFlushManagerConfigsPreserveInvalidDurableTypesForValidation(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_TARGET=delivery-dto-target",
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_DELIVERY_LOG_TYPE=loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_DELIVERY_LOKI_FAILURE_ARCHIVE_DIR=delivery-loki-archive",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_DELIVERY_CLOUDWATCH_FAILURE_ARCHIVE_DIR=delivery-cloudwatch-archive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR=delivery-clickhouse-archive",
		"DELIVERY_CLICKHOUSE_FAILURE_SPOOL_DIR=delivery-clickhouse-spool",
		"MAX_DELIVERY_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=7",
	})

	var lokiConfig logconfig.DeliveryLokiFlushManagerConfig = cfg.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig
	var cloudWatchConfig logconfig.DeliveryCloudWatchLogsFlushManagerConfig = cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig
	var clickHouseConfig logconfig.DeliveryClickHouseFlushManagerConfig = cfg.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig

	assertDurableTypes(t, lokiConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, cloudWatchConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, clickHouseConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")

	delivery := ToDeliveryLoggerConfig(cfg)
	var mappedLoki pkglogger.DeliveryLokiFlushManagerConfig = delivery.Loki.DeliveryLokiFlushManagerConfig
	var mappedCloudWatch pkglogger.DeliveryCloudWatchLogsFlushManagerConfig = delivery.CloudWatchLogs.DeliveryCloudWatchLogsFlushManagerConfig
	var mappedClickHouse pkglogger.DeliveryClickHouseFlushManagerConfig = delivery.ClickHouse.DeliveryClickHouseFlushManagerConfig

	assertDurableTypes(t, mappedLoki.FailureStoreConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, mappedCloudWatch.FailureStoreConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, mappedClickHouse.FailureStoreConfig.DurableType, "spool", "fileArchive")
	for _, durableTypes := range [][]pkgdurable.DurableType{
		mappedLoki.FailureStoreConfig.DurableType,
		mappedCloudWatch.FailureStoreConfig.DurableType,
		mappedClickHouse.FailureStoreConfig.DurableType,
	} {
		err := pkgdurable.ValidateDeliveryFailureStoreDurableTypes(durableTypes)
		var typeErr *pkgdurable.DurableTypeError
		if !errors.As(err, &typeErr) || typeErr.Type != pkgdurable.DurableTypeSpool ||
			!errors.Is(err, pkgdurable.ErrUnsupportedDurableType) {
			t.Fatalf("delivery durable type validation error = %v, want unsupported spool", err)
		}
	}
	if got, want := mappedLoki.FailureStoreConfig.FileArchiveConfig.Dir, "delivery-loki-archive"; got != want {
		t.Fatalf("mapped Loki archive dir = %q, want %q", got, want)
	}
	if got, want := mappedCloudWatch.FailureStoreConfig.FileArchiveConfig.Dir, "delivery-cloudwatch-archive"; got != want {
		t.Fatalf("mapped CloudWatch Logs archive dir = %q, want %q", got, want)
	}
	if got, want := mappedClickHouse.FailureStoreConfig.FileArchiveConfig.Dir, "delivery-clickhouse-archive"; got != want {
		t.Fatalf("mapped ClickHouse archive dir = %q, want %q", got, want)
	}
}

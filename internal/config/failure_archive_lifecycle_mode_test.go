package config

import (
	"strings"
	"testing"

	configfailure "github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestFailureArchiveLifecycleModeLoadersCoverEveryLoggerContract(t *testing.T) {
	tests := []struct {
		name string
		key  string
		load func() pkgfsstore.FileLifecycleMode
	}{
		{
			name: "opensearch sync",
			key:  "HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadOpenSearchFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "opensearch async",
			key:  "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadAsyncOpenSearchFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "opensearch delivery",
			key:  "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadDeliveryOpenSearchFailureHandlerConfig)).FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "loki sync",
			key:  "HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadLokiFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "loki async",
			key:  "HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadAsyncLokiFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "loki delivery",
			key:  "HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadDeliveryLokiFailureHandlerConfig)).FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "cloudwatch logs sync",
			key:  "HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadCloudWatchLogsFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "cloudwatch logs async",
			key:  "HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadAsyncCloudWatchLogsFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "cloudwatch logs delivery",
			key:  "HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadDeliveryCloudWatchLogsFailureHandlerConfig)).FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "clickhouse sync",
			key:  "HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadClickHouseFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "clickhouse async",
			key:  "HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadAsyncClickHouseFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
			},
		},
		{
			name: "clickhouse delivery",
			key:  "HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
			load: func() pkgfsstore.FileLifecycleMode {
				return mustLoadConfig(t, fromProcessEnvironment(loadDeliveryClickHouseFailureHandlerConfig)).FileArchiveConfig.LifecycleMode
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := strings.TrimSuffix(tt.key, "_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE")
			t.Setenv(prefix+"_DURABLE_TYPES", "fileArchive")
			t.Setenv(tt.key, string(pkgfsstore.FileLifecycleModeRunner))
			if got := tt.load(); got != pkgfsstore.FileLifecycleModeRunner {
				t.Fatalf("%s = %q, want %q", tt.key, got, pkgfsstore.FileLifecycleModeRunner)
			}
		})
	}
}

func TestFailureArchiveLifecycleModeEmptyUsesInlineCompatibilityDefault(t *testing.T) {
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_TYPES", "fileArchive")
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE", "")

	got := mustLoadConfig(t, fromProcessEnvironment(loadOpenSearchFailureHandlerConfig)).Fsstore.FileArchiveConfig.LifecycleMode
	if got != pkgfsstore.FileLifecycleModeInline {
		t.Fatalf(
			"empty failure archive lifecycle mode = %q, want compatibility default %q",
			got,
			pkgfsstore.FileLifecycleModeInline,
		)
	}
}

func TestLoadConfigMapsFailureArchiveLifecycleModesForEveryLoggerContract(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_ASYNC_LOG_TYPE=opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_DELIVERY_LOG_TYPE=opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_LOKI_DURABLE_TYPES=fileArchive",
		"HETEROLOG_CLOUDWATCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES=fileArchive",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=runner",
		"HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=runner",
		"HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=runner",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=runner",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=disable",
		"HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=disable",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=disable",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=disable",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=inline",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=inline",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=inline",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=inline",
	})

	syncMapped := ToLoggerConfig(cfg)
	for name, mode := range map[string]pkgfsstore.FileLifecycleMode{
		"opensearch": syncMapped.OpenSearch.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.LifecycleMode,
		"loki": syncMapped.Loki.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.LifecycleMode,
		"cloudwatch logs": syncMapped.CloudWatchLogs.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.LifecycleMode,
		"clickhouse": syncMapped.ClickHouse.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.LifecycleMode,
	} {
		if mode != pkgfsstore.FileLifecycleModeRunner {
			t.Fatalf("mapped sync %s failure archive lifecycle mode = %q, want runner", name, mode)
		}
	}

	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	for name, mode := range map[string]pkgfsstore.FileLifecycleMode{
		"opensearch": asyncMapped.OpenSearch.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.LifecycleMode,
		"loki": asyncMapped.Loki.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.LifecycleMode,
		"cloudwatch logs": asyncMapped.CloudWatchLogs.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.LifecycleMode,
		"clickhouse": asyncMapped.ClickHouse.FailureStoreConfig.
			DurableStoreConfig.FileArchiveConfig.LifecycleMode,
	} {
		if mode != pkgfsstore.FileLifecycleModeDisable {
			t.Fatalf("mapped async %s failure archive lifecycle mode = %q, want disable", name, mode)
		}
	}

	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	for name, mode := range map[string]pkgfsstore.FileLifecycleMode{
		"opensearch":      deliveryMapped.OpenSearch.FailureStoreConfig.FileArchiveConfig.LifecycleMode,
		"loki":            deliveryMapped.Loki.FailureStoreConfig.FileArchiveConfig.LifecycleMode,
		"cloudwatch logs": deliveryMapped.CloudWatchLogs.FailureStoreConfig.FileArchiveConfig.LifecycleMode,
		"clickhouse":      deliveryMapped.ClickHouse.FailureStoreConfig.FileArchiveConfig.LifecycleMode,
	} {
		if mode != pkgfsstore.FileLifecycleModeInline {
			t.Fatalf("mapped delivery %s failure archive lifecycle mode = %q, want inline", name, mode)
		}
	}
}

func TestFailureArchiveLifecycleModeMappingPreservesScopeValue(t *testing.T) {
	appConfig := &AppConfig{}
	syncMapped := newFailureStoreConfig(
		appConfig,
		checkpointSourceLoggerOpenSearch,
		pkglogger.LoggerContractSync,
		configfailure.FailureHandlerConfig{
			Fsstore: configfailure.FailureFileSystemStoreConfig{
				FileArchiveConfig: configfailure.FileArchiveConfig{
					LifecycleMode: pkgfsstore.FileLifecycleModeRunner,
				},
			},
		},
		logconfig.LogBodyConfig{},
	)
	if got := syncMapped.DurableStoreConfig.FileArchiveConfig.LifecycleMode; got != pkgfsstore.FileLifecycleModeRunner {
		t.Fatalf("mapped sync failure archive lifecycle mode = %q, want runner", got)
	}
	if got := syncMapped.DurableStoreConfig.FileArchiveConfig.FileNameFormat; got != pkgfsstore.FailureArchiveFileNameFormat {
		t.Fatalf(
			"mapped sync failure archive file name format = %q, want %q",
			got,
			pkgfsstore.FailureArchiveFileNameFormat,
		)
	}

	deliveryMapped := newDeliveryFailureStoreConfig(
		appConfig,
		checkpointSourceDeliveryLoggerOpenSearch,
		configfailure.DeliveryFailureHandlerConfig{
			FileArchiveConfig: configfailure.DeliveryFileArchiveConfig{
				LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
			},
		},
		logconfig.LogBodyConfig{},
	)
	if got := deliveryMapped.FileArchiveConfig.LifecycleMode; got != pkgfsstore.FileLifecycleModeDisable {
		t.Fatalf("mapped delivery failure archive lifecycle mode = %q, want disable", got)
	}
	if got := deliveryMapped.FileArchiveConfig.FileNameFormat; got != pkgfsstore.FailureArchiveFileNameFormat {
		t.Fatalf(
			"mapped delivery failure archive file name format = %q, want %q",
			got,
			pkgfsstore.FailureArchiveFileNameFormat,
		)
	}
}

func TestPublicEnvExampleExposesFailureArchiveLifecycleModeKeys(t *testing.T) {
	example := readPublicEnvExample(t)

	keys := []string{
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE",
	}
	for _, key := range keys {
		if got := example[key]; got != string(pkgfsstore.FileLifecycleModeInline) {
			t.Fatalf(".env.example %s = %q, want inline", key, got)
		}
	}
}

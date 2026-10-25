package config

import (
	"reflect"
	"testing"
	"time"

	configfailure "github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/pkg/env"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	loggerdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestFailureHandlerLoadersDoNotReadUnselectedDurableSubconfig(t *testing.T) {
	tests := []struct {
		name           string
		durableTypeKey string
		unusedKey      string
		load           func() (configfailure.FailureHandlerConfig, error)
	}{
		{
			name:           "opensearch sync",
			durableTypeKey: "HETEROLOG_OPENSEARCH_DURABLE_TYPES",
			unusedKey:      "HETEROLOG_MAX_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB",
			load:           fromProcessEnvironment(loadOpenSearchFailureHandlerConfig),
		},
		{
			name:           "opensearch async",
			durableTypeKey: "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES",
			unusedKey:      "HETEROLOG_MAX_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB",
			load:           fromProcessEnvironment(loadAsyncOpenSearchFailureHandlerConfig),
		},
		{
			name:           "loki sync",
			durableTypeKey: "HETEROLOG_LOKI_DURABLE_TYPES",
			unusedKey:      "HETEROLOG_MAX_LOKI_DURABLE_FILE_ARCHIVE_SIZE_MB",
			load:           fromProcessEnvironment(loadLokiFailureHandlerConfig),
		},
		{
			name:           "loki async",
			durableTypeKey: "HETEROLOG_ASYNC_LOKI_DURABLE_TYPES",
			unusedKey:      "HETEROLOG_MAX_ASYNC_LOKI_DURABLE_FILE_ARCHIVE_SIZE_MB",
			load:           fromProcessEnvironment(loadAsyncLokiFailureHandlerConfig),
		},
		{
			name:           "cloudwatchlogs sync",
			durableTypeKey: "HETEROLOG_CLOUDWATCH_DURABLE_TYPES",
			unusedKey:      "HETEROLOG_MAX_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SIZE_MB",
			load:           fromProcessEnvironment(loadCloudWatchLogsFailureHandlerConfig),
		},
		{
			name:           "cloudwatchlogs async",
			durableTypeKey: "HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES",
			unusedKey:      "HETEROLOG_MAX_ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SIZE_MB",
			load:           fromProcessEnvironment(loadAsyncCloudWatchLogsFailureHandlerConfig),
		},
		{
			name:           "clickhouse sync",
			durableTypeKey: "HETEROLOG_CLICKHOUSE_DURABLE_TYPES",
			unusedKey:      "HETEROLOG_MAX_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB",
			load:           fromProcessEnvironment(loadClickHouseFailureHandlerConfig),
		},
		{
			name:           "clickhouse async",
			durableTypeKey: "HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES",
			unusedKey:      "HETEROLOG_MAX_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB",
			load:           fromProcessEnvironment(loadAsyncClickHouseFailureHandlerConfig),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.durableTypeKey, string(loggerdurable.DurableTypeSpool))
			t.Setenv(test.unusedKey, "not-an-integer")

			cfg, err := test.load()
			if err != nil {
				t.Fatalf("unused file archive setting produced error: %v", err)
			}
			if !reflect.DeepEqual(cfg.Fsstore.FileArchiveConfig, configfailure.FileArchiveConfig{}) {
				t.Fatal("unselected file archive config was loaded")
			}
			if cfg.Fsstore.SpoolConfig.Dir == "" {
				t.Fatal("selected spool config was not loaded")
			}
		})
	}
}

func TestDeliveryFailureHandlerLoadersDoNotReadArchiveWhenDurableTypeIsNone(t *testing.T) {
	tests := []struct {
		name           string
		durableTypeKey string
		unusedKey      string
		load           func() (configfailure.DeliveryFailureHandlerConfig, error)
	}{
		{"opensearch", "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES", "HETEROLOG_MAX_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB", fromProcessEnvironment(loadDeliveryOpenSearchFailureHandlerConfig)},
		{"loki", "HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES", "HETEROLOG_MAX_DELIVERY_LOKI_DURABLE_FILE_ARCHIVE_SIZE_MB", fromProcessEnvironment(loadDeliveryLokiFailureHandlerConfig)},
		{"cloudwatchlogs", "HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES", "HETEROLOG_MAX_DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE_SIZE_MB", fromProcessEnvironment(loadDeliveryCloudWatchLogsFailureHandlerConfig)},
		{"clickhouse", "HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES", "HETEROLOG_MAX_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SIZE_MB", fromProcessEnvironment(loadDeliveryClickHouseFailureHandlerConfig)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.durableTypeKey, string(loggerdurable.DurableTypeNone))
			t.Setenv(test.unusedKey, "not-an-integer")

			cfg, err := test.load()
			if err != nil {
				t.Fatalf("unselected archive setting produced error: %v", err)
			}
			if !reflect.DeepEqual(cfg.FileArchiveConfig, configfailure.DeliveryFileArchiveConfig{}) {
				t.Fatal("unselected file archive config was loaded")
			}
		})
	}
}

func TestLoadSpoolConfigReadsOnlySelectedBackendRunners(t *testing.T) {
	t.Setenv("HETEROLOG_OPENSEARCH_SPOOL_RUNNER_INTERVAL_MILLISECOND", "25")
	t.Setenv("HETEROLOG_LOKI_SPOOL_RUNNER_INTERVAL_MILLISECOND", "not-an-integer")
	t.Setenv("HETEROLOG_MAX_RESTART_SPOOL_TIMES", "4")

	appCfg := AppConfig{}
	appCfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputOpenSearch}
	appCfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig = configfailure.FailureHandlerConfig{
		DurableType: []loggerdurable.DurableType{loggerdurable.DurableTypeSpool},
	}

	cfg, err := loadSpoolConfig(processEnvironmentForTest(), appCfg)
	if err != nil {
		t.Fatalf("unselected Loki runner setting produced error: %v", err)
	}
	if cfg.RunnerConfig.OpensearchConfig.Runner.Interval != 25*time.Millisecond {
		t.Fatalf("OpenSearch interval = %s, want 25ms", cfg.RunnerConfig.OpensearchConfig.Runner.Interval)
	}
	if cfg.RunnerConfig.LokiConfig.Runner.Interval != 0 {
		t.Fatalf("unselected Loki interval = %s, want zero", cfg.RunnerConfig.LokiConfig.Runner.Interval)
	}
	if cfg.SupervisorConfig.MaxRestarts != 4 {
		t.Fatalf("MaxRestarts = %d, want 4", cfg.SupervisorConfig.MaxRestarts)
	}
}

func TestLoadSpoolConfigDoesNotReadSupervisorWithoutSelectedSpool(t *testing.T) {
	t.Setenv("HETEROLOG_MAX_RESTART_SPOOL_TIMES", "not-an-integer")

	cfg, err := loadSpoolConfig(processEnvironmentForTest(), AppConfig{})
	if err != nil {
		t.Fatalf("unused supervisor setting produced error: %v", err)
	}
	if cfg.SupervisorConfig.MaxRestarts != 0 {
		t.Fatalf("MaxRestarts = %d, want zero", cfg.SupervisorConfig.MaxRestarts)
	}
}

func TestObjectStorageLoadersDoNotParseCommonS3SettingsWhenUploadIsDisabled(t *testing.T) {
	t.Setenv("HETEROLOG_OBJECT_STORAGE_S3_TIMEOUT_SECOND", "not-a-duration")

	tests := []struct {
		name       string
		enabledKey string
		bucketKey  string
		loadBucket func() (string, error)
	}{
		{
			name:       "sync log file",
			enabledKey: "HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD",
			bucketKey:  "HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_BUCKET",
			loadBucket: func() (string, error) {
				cfg, err := loadLogFileObjectStorageUploadConfig(processEnvironmentForTest())
				return cfg.Default.S3.Bucket, err
			},
		},
		{
			name:       "async log file",
			enabledKey: "HETEROLOG_ENABLE_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD",
			bucketKey:  "HETEROLOG_ASYNC_LOG_FILE_OBJECT_STORAGE_S3_BUCKET",
			loadBucket: func() (string, error) {
				cfg, err := loadAsyncLogFileObjectStorageUploadConfig(processEnvironmentForTest())
				return cfg.Default.S3.Bucket, err
			},
		},
		{
			name:       "delivery log file",
			enabledKey: "HETEROLOG_ENABLE_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD",
			bucketKey:  "HETEROLOG_DELIVERY_LOG_FILE_OBJECT_STORAGE_S3_BUCKET",
			loadBucket: func() (string, error) {
				cfg, err := loadDeliveryLogFileObjectStorageUploadConfig(processEnvironmentForTest())
				return cfg.Default.S3.Bucket, err
			},
		},
		{
			name:       "failure archive",
			enabledKey: "HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
			bucketKey:  "HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET",
			loadBucket: func() (string, error) {
				cfg, err := loadOpenSearchFailureArchiveObjectStorageUploadConfig(processEnvironmentForTest())
				return cfg.S3.Bucket, err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.enabledKey, "false")
			const wantBucket = "configured-but-disabled"
			t.Setenv(test.bucketKey, wantBucket)

			gotBucket, err := test.loadBucket()
			if err != nil {
				t.Fatalf("disabled object storage parsed unused common S3 settings: %v", err)
			}
			if gotBucket != wantBucket {
				t.Fatalf("S3 bucket = %q, want preserved value %q", gotBucket, wantBucket)
			}
		})
	}
}

func TestObjectStorageLoadersParseCommonS3SettingsWhenUploadIsEnabled(t *testing.T) {
	t.Setenv("HETEROLOG_OBJECT_STORAGE_S3_TIMEOUT_SECOND", "not-a-duration")

	tests := []struct {
		name       string
		enabledKey string
		load       func() error
	}{
		{
			name:       "sync log file",
			enabledKey: "HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD",
			load: func() error {
				_, err := loadLogFileObjectStorageUploadConfig(processEnvironmentForTest())
				return err
			},
		},
		{
			name:       "async log file",
			enabledKey: "HETEROLOG_ENABLE_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD",
			load: func() error {
				_, err := loadAsyncLogFileObjectStorageUploadConfig(processEnvironmentForTest())
				return err
			},
		},
		{
			name:       "delivery log file",
			enabledKey: "HETEROLOG_ENABLE_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD",
			load: func() error {
				_, err := loadDeliveryLogFileObjectStorageUploadConfig(processEnvironmentForTest())
				return err
			},
		},
		{
			name:       "failure archive",
			enabledKey: "HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD",
			load: func() error {
				_, err := loadOpenSearchFailureArchiveObjectStorageUploadConfig(processEnvironmentForTest())
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.enabledKey, "true")
			requireEnvironmentKeyError(t, test.load(), "HETEROLOG_OBJECT_STORAGE_S3_TIMEOUT_SECOND", env.ErrNotInteger)
		})
	}
}

func TestLoggerConfigLoadersParseInternalLogLokiLabelsOnlyWhenLokiIsRequired(t *testing.T) {
	tests := []struct {
		name       string
		typeKey    string
		handlerKey string
		labelsKey  string
		load       func() error
	}{
		{
			name:       "sync",
			typeKey:    "HETEROLOG_LOG_TYPE",
			handlerKey: "HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
			labelsKey:  "HETEROLOG_LOG_INTERNAL_LOG_LOKI_LABELS",
			load: func() error {
				_, err := loadLoggerConfig(processEnvironmentForTest())
				return err
			},
		},
		{
			name:       "async",
			typeKey:    "HETEROLOG_ASYNC_LOG_TYPE",
			handlerKey: "HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
			labelsKey:  "HETEROLOG_ASYNC_LOG_INTERNAL_LOG_LOKI_LABELS",
			load: func() error {
				_, err := loadAsyncBestEffortMultiLoggerConfig(processEnvironmentForTest())
				return err
			},
		},
		{
			name:       "delivery",
			typeKey:    "HETEROLOG_DELIVERY_LOG_TYPE",
			handlerKey: "HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			labelsKey:  "HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_LABELS",
			load: func() error {
				_, err := loadDeliveryLoggerConfig(processEnvironmentForTest())
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.typeKey, "stdout")
			t.Setenv(test.labelsKey, "not-json")
			t.Setenv(test.handlerKey, "")

			if err := test.load(); err != nil {
				t.Fatalf("unused internal-log Loki labels produced error: %v", err)
			}

			t.Setenv(test.handlerKey, "loki")
			requireEnvironmentKeyError(t, test.load(), test.labelsKey, env.ErrInvalidJSON)
		})
	}
}

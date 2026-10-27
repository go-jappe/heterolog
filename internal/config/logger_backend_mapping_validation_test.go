package config

import (
	"testing"
	"time"

	cloudwatchlogscfg "github.com/go-jappe/heterolog/internal/config/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestMapLoggerConfigValidatesOnlyMappedBackendClients(t *testing.T) {
	tests := []struct {
		name        string
		configure   func(*AppConfig)
		wantBackend pkglogger.LogOutputDestination
	}{
		{
			name: "opensearch",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
					pkglogger.LogOutputOpenSearch,
				}
				cfg.OpenSearchClient.Endpoint = "missing-scheme"
				cfg.OpenSearchClient.Auth.Type = opensearch.AuthTypeNone
			},
			wantBackend: pkglogger.LogOutputOpenSearch,
		},
		{
			name: "loki",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
					pkglogger.LogOutputLoki,
				}
				cfg.LokiClient.Endpoint = "missing-scheme"
			},
			wantBackend: pkglogger.LogOutputLoki,
		},
		{
			name: "cloudwatch logs",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
					pkglogger.LogOutputCloudWatchLogs,
				}
				cfg.CloudWatchLogsClient.LogGroupName = ""
				cfg.Logger.CloudWatchLogsConfig.LogStreamName = "stream"
			},
			wantBackend: pkglogger.LogOutputCloudWatchLogs,
		},
		{
			name: "clickhouse",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
					pkglogger.LogOutputClickHouse,
				}
				cfg.ClickHouseClient.Endpoint = ""
				cfg.ClickHouseClient.Table = "logs"
			},
			wantBackend: pkglogger.LogOutputClickHouse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mapperValidationTestConfig()
			tt.configure(&cfg)

			_, err := MapLoggerConfig(cfg)
			requireLoggerConfigFieldError(
				t,
				err,
				"logger",
				tt.wantBackend,
				pkglogger.ConfigFieldClient,
				pkglogger.ErrInvalidClientConfig,
			)
		})
	}
}

func TestMapAsyncAndDeliveryLoggerConfigsValidateBackendClients(t *testing.T) {
	tests := []struct {
		name        string
		configure   func(*AppConfig)
		mapConfig   func(AppConfig) error
		wantSource  string
		wantBackend pkglogger.LogOutputDestination
	}{
		{
			name: "async loki",
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
				cfg.AsyncLokiClient.Endpoint = "missing-scheme"
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
			wantSource:  "async logger",
			wantBackend: pkglogger.LogOutputLoki,
		},
		{
			name: "delivery clickhouse",
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputClickHouse}
				cfg.DeliveryClickHouseClient.Endpoint = "ftp://localhost:8123"
				cfg.DeliveryClickHouseClient.Table = "logs"
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
			wantSource:  "delivery logger",
			wantBackend: pkglogger.LogOutputClickHouse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mapperValidationTestConfig()
			tt.configure(&cfg)

			err := tt.mapConfig(cfg)
			requireLoggerConfigFieldError(
				t,
				err,
				tt.wantSource,
				tt.wantBackend,
				pkglogger.ConfigFieldClient,
				pkglogger.ErrInvalidClientConfig,
			)
		})
	}
}

func TestMapLoggerConfigsRejectNonPositiveCloudWatchLogsBufferFlushMaxEvents(t *testing.T) {
	tests := []struct {
		name       string
		configure  func(*AppConfig)
		mapConfig  func(AppConfig) error
		wantSource string
		wantValue  int
	}{
		{
			name: "sync zero",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}
				cfg.Logger.CloudWatchLogsConfig.BufferFlushMaxEvents = 0
				cfg.Logger.CloudWatchLogsConfig.LogGroupName = "sync-group"
				cfg.Logger.CloudWatchLogsConfig.LogStreamName = "sync-stream"
				cfg.CloudWatchLogsClient = cloudwatchlogscfg.CloudWatchLogsClientConfig{
					Region:          "ap-northeast-1",
					LogGroupName:    "sync-group",
					AccessKeyID:     "test-access-key",
					SecretAccessKey: "test-secret-key",
				}
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
			wantSource: "logger",
			wantValue:  0,
		},
		{
			name: "async negative",
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputCloudWatchLogs}
				cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.BufferFlushMaxEvents = -1
				cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogGroupName =
					"async-group"
				cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamName =
					"async-stream"
				cfg.AsyncCloudWatchLogsClient = cloudwatchlogscfg.CloudWatchLogsClientConfig{
					Region:          "ap-northeast-1",
					LogGroupName:    "async-group",
					AccessKeyID:     "test-access-key",
					SecretAccessKey: "test-secret-key",
				}
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
			wantSource: "async logger",
			wantValue:  -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mapperValidationTestConfig()
			tt.configure(&cfg)

			err := tt.mapConfig(cfg)
			fieldErr := requireLoggerConfigFieldError(
				t,
				err,
				tt.wantSource,
				pkglogger.LogOutputCloudWatchLogs,
				pkglogger.ConfigFieldBufferFlushMaxEvents,
				pkglogger.ErrNonPositiveSetting,
			)
			if fieldErr.Value != tt.wantValue {
				t.Fatalf("buffer flush max events error value = %v, want %d", fieldErr.Value, tt.wantValue)
			}
		})
	}
}

// handler-only backendもinternal logの送信時にclientを構築するため、通常出力先に
// 含まれていなくても、active sourceからbare selectorで参照されたretry設定を検証する。
func TestMapLoggerConfigsValidateHandlerOnlyBackendRetryConfig(t *testing.T) {
	invalidMode := pkglogger.RetryClientMode("unsupported")
	tests := []struct {
		name        string
		wantSource  string
		wantBackend pkglogger.LogOutputDestination
		validate    func() error
	}{
		{
			name:        "sync",
			wantSource:  "logger",
			wantBackend: pkglogger.LogOutputOpenSearch,
			validate: func() error {
				cfg := mapperValidationTestConfig()
				cfg.Logger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.Logger.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
				cfg.Logger.StdoutConfig.HandlerConfig.OnErrorConfig =
					[]string{pkglogger.LogOutputOpenSearch.String()}
				cfg.Logger.OpenSearchConfig.
					OpenSearchRetryAbleClientConfig.
					RetryClientMode = invalidMode
				_, err := MapLoggerConfig(cfg)
				return err
			},
		},
		{
			name:        "async",
			wantSource:  "async logger",
			wantBackend: pkglogger.LogOutputLoki,
			validate: func() error {
				cfg := mapperValidationTestConfig()
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
				cfg.AsyncBestEffortMultiLoggerConfig.StdoutConfig.HandlerConfig.OnErrorConfig =
					[]string{pkglogger.LogOutputLoki.String()}
				cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.
					LokiRetryAbleClientConfig.
					RetryClientMode = invalidMode
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
		},
		{
			name:        "delivery",
			wantSource:  "delivery logger",
			wantBackend: pkglogger.LogOutputClickHouse,
			validate: func() error {
				cfg := mapperValidationTestConfig()
				cfg.DeliveryLogger.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
				cfg.DeliveryLogger.HandlerTargetDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputClickHouse}
				cfg.DeliveryLogger.DeliveryStdoutConfig.HandlerConfig.OnErrorConfig =
					[]string{pkglogger.LogOutputClickHouse.String()}
				cfg.DeliveryLogger.DeliveryClickHouseConfig.
					ClickHouseRetryAbleClientConfig.
					RetryClientMode = invalidMode
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.validate()
			fieldErr := requireLoggerConfigFieldError(
				t,
				err,
				tt.wantSource,
				tt.wantBackend,
				pkglogger.ConfigFieldRetryClientMode,
				pkglogger.ErrUnsupportedRetryClientMode,
			)
			if fieldErr.Value != invalidMode {
				t.Fatalf("retry client mode error value = %v, want %q", fieldErr.Value, invalidMode)
			}
		})
	}
}

func mapperValidationTestConfig() AppConfig {
	cfg := AppConfig{
		RuntimeEnv: configruntime.RuntimeEnv{
			Cloud: &configruntime.CloudInfo{},
		},
		MaxLogEntryBytes: pkglogger.DefaultMaxLogEntryBytes,
	}
	cfg.FsstoreConfig.UsageCacheTTL = time.Minute
	// mapperへ渡す使用対象のfile設定は、loaderでmode解決済みの状態を再現します。
	cfg.Logger.FileConfig.FileSystemStoreConfig.LifecycleMode = logconfig.DefaultLogFileLifecycleMode
	cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LifecycleMode = logconfig.DefaultAsyncLogFileLifecycleMode
	cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LifecycleMode = logconfig.DefaultDeliveryLogFileLifecycleMode
	// loader適用済みの使用対象LogDirを再現し、未指定の既定値は空にしません。
	cfg.Logger.FileConfig.FileSystemStoreConfig.LogDir = logconfig.LogDirCaseFile
	cfg.AsyncBestEffortMultiLoggerConfig.FileConfig.FileSystemStoreConfig.LogDir = logconfig.AsyncLogDirCaseFile
	cfg.DeliveryLogger.DeliveryFileConfig.FileSystemStoreConfig.LogDir = logconfig.DeliveryLogDirCaseFile

	openSearchRetry := logconfig.OpenSearchRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	lokiRetry := logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	cloudWatchLogsRetry := logconfig.CloudWatchLogsRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	clickHouseRetry := logconfig.ClickHouseRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}

	cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig = openSearchRetry
	cfg.Logger.OpenSearchConfig.BufferFlushBytes = logconfig.DefaultOpenSearchBufferFlushBytes
	cfg.Logger.OpenSearchConfig.MaxBulkRequestBytes = logconfig.DefaultOpenSearchMaxBulkRequestBytes
	cfg.Logger.LokiConfig.LokiRetryAbleClientConfig = lokiRetry
	cfg.Logger.LokiConfig.MaxLineBytes = logconfig.DefaultLokiMaxLineBytes
	cfg.Logger.LokiConfig.MaxPushRequestBytes = logconfig.DefaultLokiMaxPushRequestBytes
	cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig = cloudWatchLogsRetry
	cfg.Logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig = clickHouseRetry

	cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchRetryAbleClientConfig =
		openSearchRetry
	cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.BufferFlushBytes =
		logconfig.DefaultOpenSearchBufferFlushBytes
	cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxBulkRequestBytes =
		logconfig.DefaultOpenSearchMaxBulkRequestBytes
	cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig = lokiRetry
	cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxLineBytes = logconfig.DefaultLokiMaxLineBytes
	cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxPushRequestBytes = logconfig.DefaultLokiMaxPushRequestBytes
	cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig =
		cloudWatchLogsRetry
	cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseRetryAbleClientConfig =
		clickHouseRetry

	cfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig =
		openSearchRetry
	cfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig = lokiRetry
	cfg.DeliveryLogger.DeliveryLokiConfig.MaxLineBytes = logconfig.DefaultLokiMaxLineBytes
	cfg.DeliveryLogger.DeliveryLokiConfig.MaxPushRequestBytes = logconfig.DefaultLokiMaxPushRequestBytes
	cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig =
		cloudWatchLogsRetry
	cfg.DeliveryLogger.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig =
		clickHouseRetry

	return cfg
}

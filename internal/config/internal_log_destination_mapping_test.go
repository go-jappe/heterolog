package config

import (
	"errors"
	"testing"

	configcloudwatchlogs "github.com/go-jappe/heterolog/internal/config/cloudwatchlogs"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestMapLoggerConfigsValidateInternalLogDestinationClients(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AppConfig)
		mapConfig func(AppConfig) error
		// wantSourceは不正な内部ログ用設定を示す設定元です。wantStreamNameがtrueなら、
		// 時間区分のsuffixを付けたCloudWatch Logs stream名の不正を期待し、falseなら
		// wantBackendのclient設定の不正を期待します。
		wantSource     string
		wantBackend    pkglogger.LogOutputDestination
		wantStreamName bool
	}{
		{
			name: "sync cloudwatch logs",
			configure: func(cfg *AppConfig) {
				configureSyncInternalLogTarget(cfg, pkglogger.LogOutputCloudWatchLogs)
				configureValidCloudWatchLogsClient(&cfg.CloudWatchLogsClient)
				cfg.Logger.CloudWatchLogsConfig.LogGroupName = "base-group"
				cfg.Logger.CloudWatchLogsConfig.LogStreamName = "base-stream"
				cfg.Logger.InternalLogDestinationConfig.
					CloudWatchLogsLogGroupName = " "
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
			wantSource:  "logger internal log destination",
			wantBackend: pkglogger.LogOutputCloudWatchLogs,
		},
		{
			name: "sync clickhouse",
			configure: func(cfg *AppConfig) {
				configureSyncInternalLogTarget(cfg, pkglogger.LogOutputClickHouse)
				configureValidClickHouseClient(
					&cfg.ClickHouseClient.Endpoint,
					&cfg.ClickHouseClient.Database,
					&cfg.ClickHouseClient.Table,
				)
				cfg.Logger.InternalLogDestinationConfig.ClickHouseTable = " "
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
			wantSource:  "logger internal log destination",
			wantBackend: pkglogger.LogOutputClickHouse,
		},
		{
			name: "async cloudwatch logs",
			configure: func(cfg *AppConfig) {
				configureAsyncInternalLogTarget(cfg, pkglogger.LogOutputCloudWatchLogs)
				configureValidCloudWatchLogsClient(&cfg.AsyncCloudWatchLogsClient)
				cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogGroupName =
					"base-group"
				cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamName =
					"base-stream"
				cfg.AsyncBestEffortMultiLoggerConfig.InternalLogDestinationConfig.
					CloudWatchLogsLogStreamName = " "
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
			wantSource:     "async logger internal log destination",
			wantStreamName: true,
		},
		{
			name: "async clickhouse",
			configure: func(cfg *AppConfig) {
				configureAsyncInternalLogTarget(cfg, pkglogger.LogOutputClickHouse)
				configureValidClickHouseClient(
					&cfg.AsyncClickHouseClient.Endpoint,
					&cfg.AsyncClickHouseClient.Database,
					&cfg.AsyncClickHouseClient.Table,
				)
				cfg.AsyncBestEffortMultiLoggerConfig.InternalLogDestinationConfig.
					ClickHouseTable = " "
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
			wantSource:  "async logger internal log destination",
			wantBackend: pkglogger.LogOutputClickHouse,
		},
		{
			name: "delivery cloudwatch logs",
			configure: func(cfg *AppConfig) {
				configureDeliveryInternalLogTarget(
					cfg,
					pkglogger.LogOutputCloudWatchLogs,
				)
				configureValidCloudWatchLogsClient(&cfg.DeliveryCloudWatchLogsClient)
				cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogGroupName =
					"base-group"
				cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamName =
					"base-stream"
				cfg.DeliveryLogger.InternalLogDestinationConfig.
					CloudWatchLogsLogGroupName = " "
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
			wantSource:  "delivery logger internal log destination",
			wantBackend: pkglogger.LogOutputCloudWatchLogs,
		},
		{
			name: "delivery clickhouse",
			configure: func(cfg *AppConfig) {
				configureDeliveryInternalLogTarget(cfg, pkglogger.LogOutputClickHouse)
				configureValidClickHouseClient(
					&cfg.DeliveryClickHouseClient.Endpoint,
					&cfg.DeliveryClickHouseClient.Database,
					&cfg.DeliveryClickHouseClient.Table,
				)
				cfg.DeliveryLogger.InternalLogDestinationConfig.ClickHouseTable = " "
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
			wantSource:  "delivery logger internal log destination",
			wantBackend: pkglogger.LogOutputClickHouse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mapperValidationTestConfig()
			tt.configure(&cfg)

			err := tt.mapConfig(cfg)
			if tt.wantStreamName {
				requireCloudWatchLogsStreamNameError(t, err, tt.wantSource, pkgcloudwatchlogs.ErrLogStreamNameRequired)
				return
			}
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

func TestMapLoggerConfigsValidateInternalLogLokiLabelsFromEnv(t *testing.T) {
	contracts := []struct {
		name      string
		prefix    string
		contract  pkglogger.LoggerContractKind
		mapConfig func(AppConfig) error
	}{
		{
			name:     "sync",
			contract: pkglogger.LoggerContractSync,
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
		},
		{
			name:     "async",
			prefix:   "ASYNC_",
			contract: pkglogger.LoggerContractAsyncBestEffort,
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
		},
		{
			name:     "delivery",
			prefix:   "DELIVERY_",
			contract: pkglogger.LoggerContractDelivery,
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
		},
	}
	cases := []struct {
		name    string
		output  string
		handler string
		labels  string
		wantErr bool
	}{
		{name: "invalid label", output: "stdout", handler: "loki", labels: `{"bad-key":"internal"}`, wantErr: true},
		{name: "valid labels", output: "stdout", handler: "loki", labels: `{"event_scope":"internal"}`},
		{name: "empty labels", output: "stdout", handler: "loki", labels: `{}`},
		{name: "inherit labels", output: "stdout", handler: "loki", labels: `null`},
		{name: "unused override with normal Loki output", output: "loki", labels: `{"bad-key":"internal"}`},
	}
	for _, contract := range contracts {
		for _, tt := range cases {
			t.Run(contract.name+"/"+tt.name, func(t *testing.T) {
				cfg := loadConfigFromTempEnv(t, []string{
					"HETEROLOG_ENABLE_LOGGING=true",
					"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
					"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
					"HETEROLOG_LOG_TYPE=" + tt.output,
					"HETEROLOG_ASYNC_LOG_TYPE=" + tt.output,
					"HETEROLOG_DELIVERY_LOG_TYPE=" + tt.output,
					"HETEROLOG_" + contract.prefix + "STDOUT_LOGGER_ERROR_HANDLER=" + tt.handler,
					"HETEROLOG_" + contract.prefix + "LOG_INTERNAL_LOG_LOKI_LABELS=" + tt.labels,
				})
				err := contract.mapConfig(cfg)
				if !tt.wantErr {
					if err != nil {
						t.Fatalf("mapped logger config error = %v, want nil", err)
					}
					return
				}
				fieldErr := requireLoggerConfigFieldError(
					t,
					err,
					contract.contract.DisplayName()+" internal log destination",
					pkglogger.LogOutputLoki,
					pkglogger.ConfigFieldStaticLabels,
					pkglogger.ErrInvalidSetting,
				)
				var labelErr *pkgloki.InvalidLabelNameError
				if !errors.As(fieldErr, &labelErr) || labelErr.Name != "bad-key" {
					t.Fatalf("internal-log Loki label error = %v, want invalid label name %q", fieldErr, "bad-key")
				}
			})
		}
	}
}

func configureSyncInternalLogTarget(
	cfg *AppConfig,
	target pkglogger.LogOutputDestination,
) {
	cfg.Logger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	cfg.Logger.HandlerTargetDestinations = pkglogger.LogOutputDestinations{target}
	cfg.Logger.StdoutConfig.HandlerConfig.OnErrorConfig = []string{target.String()}
}

func configureAsyncInternalLogTarget(
	cfg *AppConfig,
	target pkglogger.LogOutputDestination,
) {
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	cfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations =
		pkglogger.LogOutputDestinations{target}
	cfg.AsyncBestEffortMultiLoggerConfig.StdoutConfig.HandlerConfig.OnErrorConfig =
		[]string{target.String()}
}

func configureDeliveryInternalLogTarget(
	cfg *AppConfig,
	target pkglogger.LogOutputDestination,
) {
	cfg.DeliveryLogger.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputStdout}
	cfg.DeliveryLogger.HandlerTargetDestinations = pkglogger.LogOutputDestinations{target}
	cfg.DeliveryLogger.DeliveryStdoutConfig.HandlerConfig.OnErrorConfig =
		[]string{target.String()}
}

func configureValidCloudWatchLogsClient(
	cfg *configcloudwatchlogs.CloudWatchLogsClientConfig,
) {
	cfg.Region = "ap-northeast-1"
	cfg.LogGroupName = "base-group"
	cfg.AccessKeyID = "test-access-key"
	cfg.SecretAccessKey = "test-secret-key"
}

func configureValidClickHouseClient(endpoint, database, table *string) {
	*endpoint = "http://localhost:8123"
	*database = "base-database"
	*table = "base-table"
}

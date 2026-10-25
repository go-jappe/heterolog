package logger

import (
	"errors"
	"testing"
	"time"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
	pkgslack "github.com/go-jappe/heterolog/pkg/slack"
)

func TestValidateLoggerBackendConfigsRejectsNonPositiveFlushInterval(t *testing.T) {
	tests := []struct {
		name string
		cfg  LoggerConfig
	}{
		{
			name: "opensearch",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
				OpenSearch: &OpenSearchConfig{
					OpenSearchLoggerBaseConfig: OpenSearchLoggerBaseConfig{
						FlushInterval:       -time.Nanosecond,
						MaxBulkDocument:     1,
						BufferFlushBytes:    512,
						MaxBulkRequestBytes: 1024,
					},
					OpenSearchFlushManagerConfig: OpenSearchFlushManagerConfig{Index: "logs"},
				},
				OpenSearchClient: validBackendValidationOpenSearchClient(),
			},
		},
		{
			name: "loki",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputLoki},
				Loki: &LokiConfig{LokiLoggerBaseConfig: LokiLoggerBaseConfig{
					FlushInterval:           -time.Nanosecond,
					MaxEntries:              1,
					BufferFlushBytes:        1,
					FallbackQueueMaxEntries: 1,
					MaxLineBytes:            1024,
					MaxPushRequestBytes:     4096,
				}},
				LokiClient: &pkgloki.Config{Endpoint: "http://localhost:3100"},
			},
		},
		{
			name: "cloudwatch logs",
			cfg: func() LoggerConfig {
				client := validBackendValidationCloudWatchLogsClient()
				return LoggerConfig{
					LogOutputDestinations: LogOutputDestinations{LogOutputCloudWatchLogs},
					CloudWatchLogs: &CloudWatchLogsConfig{
						CloudWatchLogsLoggerBaseConfig: CloudWatchLogsLoggerBaseConfig{
							FlushInterval:          -time.Nanosecond,
							BufferFlushMaxEvents:   1,
							BufferFlushBytes:       1,
							FallbackQueueMaxEvents: 1,
						},
						CloudWatchLogsFlushManagerConfig: CloudWatchLogsFlushManagerConfig{
							LogGroupName:  client.LogGroupName,
							LogStreamName: "logs",
						},
					},
					CloudWatchLogsClient: client,
				}
			}(),
		},
		{
			name: "clickhouse",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputClickHouse},
				ClickHouse: &ClickHouseConfig{ClickHouseLoggerBaseConfig: ClickHouseLoggerBaseConfig{
					FlushInterval:           -time.Nanosecond,
					MaxEntries:              1,
					BufferFlushBytes:        1,
					FallbackQueueMaxEntries: 1,
					MaxInsertRequestBytes:   1,
				}},
				ClickHouseClient: &pkgclickhouse.Config{
					Endpoint: "http://localhost:8123",
					Table:    "logs",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLoggerBackendConfigs("logger", tt.cfg)
			backend := tt.cfg.LogOutputDestinations[0]
			fieldErr := findBackendFieldError(err, "logger", backend, ConfigFieldFlushInterval, ErrNonPositiveSetting)
			if fieldErr == nil || fieldErr.Value != -time.Nanosecond {
				t.Fatalf("ValidateLoggerBackendConfigs() error = %v, want non-positive %s flush interval", err, backend)
			}
		})
	}
}

func TestValidateLoggerBackendConfigsRejectsMissingExplicitConfig(t *testing.T) {
	for _, destination := range AllLogOutputDestinations() {
		t.Run(destination.String(), func(t *testing.T) {
			err := ValidateLoggerBackendConfigs("logger", LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{destination},
			})
			if findBackendFieldError(err, "logger", destination, ConfigFieldBackendConfig, ErrBackendConfigRequired) == nil {
				t.Fatalf("validation error = %v, want missing logger %s config", err, destination)
			}
		})
	}
}

func TestValidateDeliveryLoggerBackendConfigsRejectsMissingExplicitConfig(t *testing.T) {
	for _, destination := range AllLogOutputDestinations() {
		t.Run(destination.String(), func(t *testing.T) {
			err := ValidateDeliveryLoggerBackendConfigs(
				"delivery logger",
				DeliveryLoggerConfig{
					LogOutputDestinations: LogOutputDestinations{destination},
				},
			)
			if findBackendFieldError(
				err, "delivery logger", destination, ConfigFieldBackendConfig, ErrBackendConfigRequired,
			) == nil {
				t.Fatalf("validation error = %v, want missing delivery logger %s config", err, destination)
			}
		})
	}
}

// 出力先のない設定ではlogが黙って失われ、Delivery loggerも送信先のない結果しか
// 返せないため、構築前に設定errorにします。通常出力はruntimeと同じ規則で数え、
// destinationが空ならnon-nilのbase設定すべて、明示した場合は設定のある指定先だけです。
func TestBackendConfigValidationRequiresActiveLogOutput(t *testing.T) {
	tests := []struct {
		name     string
		sync     LoggerConfig
		delivery DeliveryLoggerConfig
		wantErr  bool
	}{
		{name: "empty config", wantErr: true},
		{
			name: "only internal-log destination",
			sync: LoggerConfig{InternalLogDestinations: &InternalLogDestinationConfig{
				Slack: &SlackLoggerConfig{},
			}},
			delivery: DeliveryLoggerConfig{InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
				Slack: &DeliverySlackLoggerConfig{},
			}},
			wantErr: true,
		},
		{
			name:     "only client config",
			sync:     LoggerConfig{OpenSearchClient: validBackendValidationOpenSearchClient()},
			delivery: DeliveryLoggerConfig{OpenSearchClient: validBackendValidationOpenSearchClient()},
			wantErr:  true,
		},
		{
			name: "configured backend outside explicit destinations",
			sync: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
				Stdout:                &StdoutLoggerConfig{},
			},
			delivery: DeliveryLoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
				Stdout:                &DeliveryStdoutLoggerConfig{},
			},
			wantErr: true,
		},
		{
			name:     "implicit output from non-nil config",
			sync:     LoggerConfig{Stdout: &StdoutLoggerConfig{}},
			delivery: DeliveryLoggerConfig{Stdout: &DeliveryStdoutLoggerConfig{}},
		},
		{
			name: "explicit output",
			sync: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
				Stdout:                &StdoutLoggerConfig{},
			},
			delivery: DeliveryLoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
				Stdout:                &DeliveryStdoutLoggerConfig{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for label, err := range map[string]error{
				"sync":     ValidateLoggerBackendConfigs("logger", tt.sync),
				"delivery": ValidateDeliveryLoggerBackendConfigs("delivery logger", tt.delivery),
			} {
				if tt.wantErr {
					if !errors.Is(err, ErrNoActiveLogOutput) {
						t.Fatalf("%s validation error = %v, want ErrNoActiveLogOutput", label, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s validation error = %v, want nil", label, err)
				}
			}
		})
	}
}

func TestBackendConfigValidationRejectsUnknownTypedDestination(t *testing.T) {
	const unknown LogOutputDestination = "future-backend"
	err := ValidateLoggerBackendConfigs("logger", LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{unknown},
	})
	fieldErr := findBackendFieldError(
		err, "logger", unknown, ConfigFieldLogOutputDestinations, ErrUnsupportedLogOutputDestination,
	)
	if fieldErr == nil || fieldErr.Value != unknown {
		t.Fatalf("validation error = %v, want unsupported log output destination %q", err, unknown)
	}
}

func TestBareHandlerTargetMissingIsOwnedByRoutingValidation(t *testing.T) {
	syncCfg := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
		Stdout: &StdoutLoggerConfig{
			HandlerConfig: loggerhandler.HandlerConfig{
				OnErrorConfig: []string{LogOutputOpenSearch.String()},
			},
		},
	}
	if err := ValidateLoggerBackendConfigs("logger", syncCfg); err != nil {
		t.Fatalf("backend value validation error = %v, want nil", err)
	}

	err := ValidateLoggerHandlerRouting("logger", syncCfg)
	if !hasHandlerSelectorError(err, "error", LogOutputOpenSearch.String()) {
		t.Fatalf("routing validation error = %v, want unresolved opensearch selector", err)
	}

	deliveryCfg := DeliveryLoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
		Stdout: &DeliveryStdoutLoggerConfig{
			HandlerConfig: loggerhandler.HandlerConfig{
				OnErrorConfig: []string{LogOutputOpenSearch.String()},
			},
		},
	}
	if err := ValidateDeliveryLoggerBackendConfigs("delivery logger", deliveryCfg); err != nil {
		t.Fatalf("delivery backend value validation error = %v, want nil", err)
	}

	err = ValidateDeliveryLoggerHandlerRouting("delivery logger", deliveryCfg)
	if !hasHandlerSelectorError(err, "error", LogOutputOpenSearch.String()) {
		t.Fatalf("delivery routing validation error = %v, want unresolved opensearch selector", err)
	}
}

func TestRequiresBaseBackendDistinguishesImplicitPresenceFromExplicitRequirement(
	t *testing.T,
) {
	if LoggerConfigRequiresBaseBackend(LoggerConfig{}, LogOutputOpenSearch) {
		t.Fatal("empty sync config unexpectedly requires OpenSearch")
	}
	if DeliveryLoggerConfigRequiresBaseBackend(
		DeliveryLoggerConfig{},
		LogOutputOpenSearch,
	) {
		t.Fatal("empty delivery config unexpectedly requires OpenSearch")
	}

	if !LoggerConfigRequiresBaseBackend(
		LoggerConfig{OpenSearch: &OpenSearchConfig{}},
		LogOutputOpenSearch,
	) {
		t.Fatal("implicit non-nil sync OpenSearch config was not active")
	}
	if !DeliveryLoggerConfigRequiresBaseBackend(
		DeliveryLoggerConfig{OpenSearch: &DeliveryOpenSearchConfig{}},
		LogOutputOpenSearch,
	) {
		t.Fatal("implicit non-nil delivery OpenSearch config was not active")
	}

	if !LoggerConfigRequiresBaseBackend(
		LoggerConfig{
			LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
		},
		LogOutputOpenSearch,
	) {
		t.Fatal("explicit sync OpenSearch destination was not required")
	}
	if !DeliveryLoggerConfigRequiresBaseBackend(
		DeliveryLoggerConfig{
			LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
		},
		LogOutputOpenSearch,
	) {
		t.Fatal("explicit delivery OpenSearch destination was not required")
	}

	syncWithOverride := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
		Stdout: &StdoutLoggerConfig{
			HandlerConfig: loggerhandler.HandlerConfig{
				OnErrorConfig: []string{LogOutputOpenSearch.String()},
			},
		},
		OpenSearch: &OpenSearchConfig{},
		InternalLogDestinations: &InternalLogDestinationConfig{
			OpenSearch: &OpenSearchConfig{},
		},
	}
	if LoggerConfigRequiresBaseBackend(syncWithOverride, LogOutputOpenSearch) {
		t.Fatal("dedicated sync internal-log OpenSearch unexpectedly required base OpenSearch")
	}

	deliveryWithOverride := DeliveryLoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
		Stdout: &DeliveryStdoutLoggerConfig{
			HandlerConfig: loggerhandler.HandlerConfig{
				OnErrorConfig: []string{LogOutputOpenSearch.String()},
			},
		},
		OpenSearch: &DeliveryOpenSearchConfig{},
		InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
			OpenSearch: &DeliveryOpenSearchConfig{},
		},
	}
	if DeliveryLoggerConfigRequiresBaseBackend(
		deliveryWithOverride,
		LogOutputOpenSearch,
	) {
		t.Fatal("dedicated delivery internal-log OpenSearch unexpectedly required base OpenSearch")
	}

	// 専用internal-log設定はhandler-only経路だけを置き換えます。通常出力として
	// 明示したbase backend、または空destination時のnon-nil base設定は引き続き必要です。
	syncWithOverride.LogOutputDestinations = LogOutputDestinations{LogOutputOpenSearch}
	if !LoggerConfigRequiresBaseBackend(syncWithOverride, LogOutputOpenSearch) {
		t.Fatal("explicit sync OpenSearch destination was hidden by internal-log override")
	}
	deliveryWithOverride.LogOutputDestinations = nil
	if !DeliveryLoggerConfigRequiresBaseBackend(
		deliveryWithOverride,
		LogOutputOpenSearch,
	) {
		t.Fatal("implicit delivery OpenSearch destination was hidden by internal-log override")
	}
}

func TestInternalLogOverrideResolvesHandlerTargetAwayFromBaseBackend(t *testing.T) {
	targets := []LogOutputDestination{
		LogOutputFile,
		LogOutputSlack,
		LogOutputOpenSearch,
		LogOutputLoki,
		LogOutputCloudWatchLogs,
		LogOutputClickHouse,
	}

	syncCfg := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
		Stdout:                &StdoutLoggerConfig{},
		File:                  &FileLoggerConfig{},
		Slack:                 &SlackLoggerConfig{},
		OpenSearch:            &OpenSearchConfig{},
		Loki:                  &LokiConfig{},
		CloudWatchLogs:        &CloudWatchLogsConfig{},
		ClickHouse:            &ClickHouseConfig{},
		InternalLogDestinations: &InternalLogDestinationConfig{
			File:           &FileLoggerConfig{},
			Slack:          &SlackLoggerConfig{},
			OpenSearch:     &OpenSearchConfig{},
			Loki:           &LokiConfig{},
			CloudWatchLogs: &CloudWatchLogsConfig{},
			ClickHouse:     &ClickHouseConfig{},
		},
	}
	deliveryCfg := DeliveryLoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
		Stdout:                &DeliveryStdoutLoggerConfig{},
		File:                  &DeliveryFileLoggerConfig{},
		Slack:                 &DeliverySlackLoggerConfig{},
		OpenSearch:            &DeliveryOpenSearchConfig{},
		Loki:                  &DeliveryLokiConfig{},
		CloudWatchLogs:        &DeliveryCloudWatchLogsConfig{},
		ClickHouse:            &DeliveryClickHouseConfig{},
		InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
			File:           &DeliveryFileLoggerConfig{},
			Slack:          &DeliverySlackLoggerConfig{},
			OpenSearch:     &DeliveryOpenSearchConfig{},
			Loki:           &DeliveryLokiConfig{},
			CloudWatchLogs: &DeliveryCloudWatchLogsConfig{},
			ClickHouse:     &DeliveryClickHouseConfig{},
		},
	}

	for _, target := range targets {
		t.Run(target.String(), func(t *testing.T) {
			syncCfg.Stdout.HandlerConfig.OnErrorConfig = []string{target.String()}
			if LoggerConfigRequiresBaseBackend(syncCfg, target) {
				t.Fatalf("sync handler target %q unexpectedly required base backend", target)
			}

			deliveryCfg.Stdout.HandlerConfig.OnErrorConfig = []string{target.String()}
			if DeliveryLoggerConfigRequiresBaseBackend(deliveryCfg, target) {
				t.Fatalf("delivery handler target %q unexpectedly required base backend", target)
			}
		})
	}
}

func TestValidateLoggerBackendConfigsRejectsActiveBackendValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  LoggerConfig
		// wantFieldとwantKindは、"logger"の不正として報告されるbackend設定の項目と種類です。
		wantField ConfigField
		wantKind  error
		// wantIsは、呼び出し側がerrors.Isで判別するbackend packageの設定errorです。
		wantIs error
	}{
		{
			name: "slack",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputSlack},
				Slack:                 &SlackLoggerConfig{Channel: "logs"},
			},
			wantField: ConfigFieldClient,
			wantKind:  ErrInvalidClientConfig,
			wantIs:    pkgslack.ErrTokenRequired,
		},
		{
			name: "opensearch index",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
				OpenSearch:            &OpenSearchConfig{},
				OpenSearchClient:      validBackendValidationOpenSearchClient(),
			},
			wantField: ConfigFieldIndex,
			wantKind:  ErrInvalidSetting,
			wantIs:    pkgopensearch.ErrIndexRequired,
		},
		{
			name: "opensearch client",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
				OpenSearch: &OpenSearchConfig{
					OpenSearchFlushManagerConfig: OpenSearchFlushManagerConfig{Index: "logs"},
				},
				OpenSearchClient: &pkgopensearch.Config{},
			},
			wantField: ConfigFieldClient,
			wantKind:  ErrInvalidClientConfig,
			wantIs:    pkgopensearch.ErrEndpointRequired,
		},
		{
			name: "loki client",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputLoki},
				Loki:                  &LokiConfig{},
				LokiClient:            &pkgloki.Config{},
			},
			wantField: ConfigFieldClient,
			wantKind:  ErrInvalidClientConfig,
			wantIs:    pkgloki.ErrEndpointRequired,
		},
		{
			name: "cloudwatch logs client",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputCloudWatchLogs},
				CloudWatchLogs:        &CloudWatchLogsConfig{},
				CloudWatchLogsClient:  &pkgcloudwatchlogs.Config{},
			},
			wantField: ConfigFieldClient,
			wantKind:  ErrInvalidClientConfig,
			wantIs:    pkgcloudwatchlogs.ErrLogGroupNameRequired,
		},
		{
			name: "clickhouse client",
			cfg: LoggerConfig{
				LogOutputDestinations: LogOutputDestinations{LogOutputClickHouse},
				ClickHouse:            &ClickHouseConfig{},
				ClickHouseClient:      &pkgclickhouse.Config{},
			},
			wantField: ConfigFieldClient,
			wantKind:  ErrInvalidClientConfig,
			wantIs:    pkgclickhouse.ErrEndpointRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLoggerBackendConfigs("logger", tt.cfg)
			backend := tt.cfg.LogOutputDestinations[0]
			fieldErr := findBackendFieldError(err, "logger", backend, tt.wantField, tt.wantKind)
			if fieldErr == nil || !errors.Is(fieldErr, tt.wantIs) {
				t.Fatalf("ValidateLoggerBackendConfigs() error = %v, want logger %s %s %v caused by %v",
					err, backend, tt.wantField, tt.wantKind, tt.wantIs)
			}
		})
	}
}

func TestValidateLoggerBackendConfigsRejectsNonPositiveBufferThresholds(t *testing.T) {
	tests := []struct {
		name  string
		build func(int) LoggerConfig
		// wantFieldは、0以下として報告されるbackend設定の項目です。
		wantField ConfigField
	}{
		{
			name: "slack max logs per notification",
			build: func(value int) LoggerConfig {
				return LoggerConfig{
					LogOutputDestinations: LogOutputDestinations{LogOutputSlack},
					Slack: &SlackLoggerConfig{
						Token:                  "xoxb-test",
						Channel:                "logs",
						MaxLogsPerNotification: value,
					},
				}
			},
			wantField: ConfigFieldMaxLogsPerNotification,
		},
		{
			name: "opensearch max bulk document",
			build: func(value int) LoggerConfig {
				return LoggerConfig{
					LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
					OpenSearch: &OpenSearchConfig{
						OpenSearchLoggerBaseConfig: OpenSearchLoggerBaseConfig{
							MaxBulkDocument:     value,
							BufferFlushBytes:    512,
							MaxBulkRequestBytes: 1024,
						},
						OpenSearchFlushManagerConfig: OpenSearchFlushManagerConfig{Index: "logs"},
					},
					OpenSearchClient: validBackendValidationOpenSearchClient(),
				}
			},
			wantField: ConfigFieldMaxBulkDocument,
		},
		{
			name: "loki max entries",
			build: func(value int) LoggerConfig {
				return LoggerConfig{
					LogOutputDestinations: LogOutputDestinations{LogOutputLoki},
					Loki: &LokiConfig{LokiLoggerBaseConfig: LokiLoggerBaseConfig{
						MaxEntries:              value,
						BufferFlushBytes:        1,
						FallbackQueueMaxEntries: 1,
						MaxLineBytes:            1024,
						MaxPushRequestBytes:     4096,
					}},
					LokiClient: &pkgloki.Config{Endpoint: "http://localhost:3100"},
				}
			},
			wantField: ConfigFieldMaxEntries,
		},
		{
			name: "clickhouse max entries",
			build: func(value int) LoggerConfig {
				return LoggerConfig{
					LogOutputDestinations: LogOutputDestinations{LogOutputClickHouse},
					ClickHouse: &ClickHouseConfig{ClickHouseLoggerBaseConfig: ClickHouseLoggerBaseConfig{
						MaxEntries:              value,
						BufferFlushBytes:        1,
						FallbackQueueMaxEntries: 1,
						MaxInsertRequestBytes:   1,
					}},
					ClickHouseClient: &pkgclickhouse.Config{
						Endpoint: "http://localhost:8123",
						Table:    "logs",
					},
				}
			},
			wantField: ConfigFieldMaxEntries,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, value := range []int{-1, 0} {
				cfg := tt.build(value)
				err := ValidateLoggerBackendConfigs("logger", cfg)
				backend := cfg.LogOutputDestinations[0]
				fieldErr := findBackendFieldError(err, "logger", backend, tt.wantField, ErrNonPositiveSetting)
				if fieldErr == nil || fieldErr.Value != value {
					t.Fatalf(
						"value=%d ValidateLoggerBackendConfigs() error = %v, want non-positive logger %s %s",
						value,
						err,
						backend,
						tt.wantField,
					)
				}
			}
		})
	}
}

func TestValidateLoggerBackendConfigsValidatesEveryInternalLogDestinationBackend(t *testing.T) {
	cfg := LoggerConfig{
		InternalLogDestinations: &InternalLogDestinationConfig{
			Slack:                &SlackLoggerConfig{},
			OpenSearch:           &OpenSearchConfig{},
			OpenSearchClient:     &pkgopensearch.Config{},
			Loki:                 &LokiConfig{},
			LokiClient:           &pkgloki.Config{},
			CloudWatchLogs:       &CloudWatchLogsConfig{},
			CloudWatchLogsClient: &pkgcloudwatchlogs.Config{},
			ClickHouse:           &ClickHouseConfig{},
			ClickHouseClient:     &pkgclickhouse.Config{},
		},
	}

	err := ValidateLoggerBackendConfigs("logger", cfg)
	const source = "logger internal log destination"
	for _, want := range []struct {
		backend LogOutputDestination
		field   ConfigField
		kind    error
	}{
		{backend: LogOutputSlack, field: ConfigFieldClient, kind: ErrInvalidClientConfig},
		{backend: LogOutputOpenSearch, field: ConfigFieldClient, kind: ErrInvalidClientConfig},
		{backend: LogOutputOpenSearch, field: ConfigFieldFlushInterval, kind: ErrNonPositiveSetting},
		{backend: LogOutputLoki, field: ConfigFieldClient, kind: ErrInvalidClientConfig},
		{backend: LogOutputCloudWatchLogs, field: ConfigFieldClient, kind: ErrInvalidClientConfig},
		{backend: LogOutputClickHouse, field: ConfigFieldClient, kind: ErrInvalidClientConfig},
	} {
		if findBackendFieldError(err, source, want.backend, want.field, want.kind) == nil {
			t.Fatalf("ValidateLoggerBackendConfigs() error = %v, want %s %s %s %v",
				err, source, want.backend, want.field, want.kind)
		}
	}
}

func validBackendValidationOpenSearchClient() *pkgopensearch.Config {
	return &pkgopensearch.Config{
		Endpoint: "http://localhost:9200",
		AuthConfig: pkgopensearch.AuthConfig{
			Type: pkgopensearch.AuthTypeNone,
		},
	}
}

func validBackendValidationCloudWatchLogsClient() *pkgcloudwatchlogs.Config {
	return &pkgcloudwatchlogs.Config{
		Region:          "ap-northeast-1",
		LogGroupName:    "client-group",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
	}
}

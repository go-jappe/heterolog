package logger

import (
	"errors"
	"testing"

	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

// runtimeはlogger設定がnon-nilのbackendだけをinternal-log専用保存先へ置き換えます。
// client設定だけを指定すると、有効なclientでも黙って通常設定の保存先へ送られるため、
// logger設定だけでclient設定がない逆の組と同じく設定errorにします。
func TestBackendConfigValidationRejectsUnpairedInternalLogClient(t *testing.T) {
	lokiClient := &pkgloki.Config{Endpoint: "http://localhost:3100"}
	clickHouseClient := &pkgclickhouse.Config{Endpoint: "http://localhost:8123", Table: "logs"}
	tests := []struct {
		backend        LogOutputDestination
		sync           InternalLogDestinationConfig
		delivery       DeliveryInternalLogDestinationConfig
		pairedSync     InternalLogDestinationConfig
		pairedDelivery DeliveryInternalLogDestinationConfig
	}{
		{
			backend: LogOutputOpenSearch,
			sync: InternalLogDestinationConfig{
				OpenSearchClient: validBackendValidationOpenSearchClient(),
			},
			delivery: DeliveryInternalLogDestinationConfig{
				OpenSearchClient: validBackendValidationOpenSearchClient(),
			},
			pairedSync: InternalLogDestinationConfig{
				OpenSearch:       &OpenSearchConfig{},
				OpenSearchClient: validBackendValidationOpenSearchClient(),
			},
			pairedDelivery: DeliveryInternalLogDestinationConfig{
				OpenSearch:       &DeliveryOpenSearchConfig{},
				OpenSearchClient: validBackendValidationOpenSearchClient(),
			},
		},
		{
			backend:        LogOutputLoki,
			sync:           InternalLogDestinationConfig{LokiClient: lokiClient},
			delivery:       DeliveryInternalLogDestinationConfig{LokiClient: lokiClient},
			pairedSync:     InternalLogDestinationConfig{Loki: &LokiConfig{}, LokiClient: lokiClient},
			pairedDelivery: DeliveryInternalLogDestinationConfig{Loki: &DeliveryLokiConfig{}, LokiClient: lokiClient},
		},
		{
			backend: LogOutputCloudWatchLogs,
			sync: InternalLogDestinationConfig{
				CloudWatchLogsClient: validBackendValidationCloudWatchLogsClient(),
			},
			delivery: DeliveryInternalLogDestinationConfig{
				CloudWatchLogsClient: validBackendValidationCloudWatchLogsClient(),
			},
			pairedSync: InternalLogDestinationConfig{
				CloudWatchLogs:       &CloudWatchLogsConfig{},
				CloudWatchLogsClient: validBackendValidationCloudWatchLogsClient(),
			},
			pairedDelivery: DeliveryInternalLogDestinationConfig{
				CloudWatchLogs:       &DeliveryCloudWatchLogsConfig{},
				CloudWatchLogsClient: validBackendValidationCloudWatchLogsClient(),
			},
		},
		{
			backend:  LogOutputClickHouse,
			sync:     InternalLogDestinationConfig{ClickHouseClient: clickHouseClient},
			delivery: DeliveryInternalLogDestinationConfig{ClickHouseClient: clickHouseClient},
			pairedSync: InternalLogDestinationConfig{
				ClickHouse:       &ClickHouseConfig{},
				ClickHouseClient: clickHouseClient,
			},
			pairedDelivery: DeliveryInternalLogDestinationConfig{
				ClickHouse:       &DeliveryClickHouseConfig{},
				ClickHouseClient: clickHouseClient,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.backend.String(), func(t *testing.T) {
			assertUnpairedClient := func(label string, source string, err error) {
				t.Helper()
				if !errors.Is(err, ErrUnpairedInternalLogClient) {
					t.Fatalf("%s error = %v, want ErrUnpairedInternalLogClient", label, err)
				}
				for _, fieldErr := range ConfigFieldErrors(err) {
					if errors.Is(fieldErr, ErrUnpairedInternalLogClient) {
						want := ConfigFieldError{Source: source, Backend: tt.backend, Field: ConfigFieldClient}
						got := ConfigFieldError{Source: fieldErr.Source, Backend: fieldErr.Backend, Field: fieldErr.Field}
						if got != want {
							t.Fatalf("%s unpaired client = %+v, want %+v", label, got, want)
						}
					}
				}
			}
			assertUnpairedClient("sync/async", "logger internal log destination", ValidateLoggerBackendConfigs(
				"logger",
				LoggerConfig{InternalLogDestinations: &tt.sync},
			))
			assertUnpairedClient("delivery", "delivery logger internal log destination", ValidateDeliveryLoggerBackendConfigs(
				"delivery logger",
				DeliveryLoggerConfig{InternalLogDestinations: &tt.delivery},
			))

			// 組で指定した設定は、backend別validatorによる値の検証だけを受けます。
			if err := ValidateLoggerBackendConfigs(
				"logger",
				LoggerConfig{InternalLogDestinations: &tt.pairedSync},
			); errors.Is(err, ErrUnpairedInternalLogClient) {
				t.Fatalf("sync/async paired internal-log config error = %v, want no pairing error", err)
			}
			if err := ValidateDeliveryLoggerBackendConfigs(
				"delivery logger",
				DeliveryLoggerConfig{InternalLogDestinations: &tt.pairedDelivery},
			); errors.Is(err, ErrUnpairedInternalLogClient) {
				t.Fatalf("delivery paired internal-log config error = %v, want no pairing error", err)
			}
		})
	}
}

package logger

import (
	"reflect"
	"testing"

	"github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestDeliveryBackendConfigsEmbedDedicatedFlushManagerTypes(t *testing.T) {
	tests := []struct {
		name                 string
		configType           reflect.Type
		embeddedField        string
		legacyEmbeddedField  string
		flushManagerType     reflect.Type
		failureStoreTypeWant reflect.Type
	}{
		{
			name:                 "opensearch",
			configType:           reflect.TypeOf(DeliveryOpenSearchConfig{}),
			embeddedField:        "DeliveryOpenSearchFlushManagerConfig",
			legacyEmbeddedField:  "OpenSearchFlushManagerConfig",
			flushManagerType:     reflect.TypeOf(DeliveryOpenSearchFlushManagerConfig{}),
			failureStoreTypeWant: reflect.TypeOf(durable.DeliveryFailureStoreConfig{}),
		},
		{
			name:                 "loki",
			configType:           reflect.TypeOf(DeliveryLokiConfig{}),
			embeddedField:        "DeliveryLokiFlushManagerConfig",
			legacyEmbeddedField:  "LokiFlushManagerConfig",
			flushManagerType:     reflect.TypeOf(DeliveryLokiFlushManagerConfig{}),
			failureStoreTypeWant: reflect.TypeOf(durable.DeliveryFailureStoreConfig{}),
		},
		{
			name:                 "cloudwatchlogs",
			configType:           reflect.TypeOf(DeliveryCloudWatchLogsConfig{}),
			embeddedField:        "DeliveryCloudWatchLogsFlushManagerConfig",
			legacyEmbeddedField:  "CloudWatchLogsFlushManagerConfig",
			flushManagerType:     reflect.TypeOf(DeliveryCloudWatchLogsFlushManagerConfig{}),
			failureStoreTypeWant: reflect.TypeOf(durable.DeliveryFailureStoreConfig{}),
		},
		{
			name:                 "clickhouse",
			configType:           reflect.TypeOf(DeliveryClickHouseConfig{}),
			embeddedField:        "DeliveryClickHouseFlushManagerConfig",
			legacyEmbeddedField:  "ClickHouseFlushManagerConfig",
			flushManagerType:     reflect.TypeOf(DeliveryClickHouseFlushManagerConfig{}),
			failureStoreTypeWant: reflect.TypeOf(durable.DeliveryFailureStoreConfig{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedded, ok := tt.configType.FieldByName(tt.embeddedField)
			if !ok || !embedded.Anonymous {
				t.Fatalf("%s is not embedded in %s", tt.embeddedField, tt.configType.Name())
			}
			if _, ok := tt.configType.FieldByName(tt.legacyEmbeddedField); ok {
				t.Fatalf("legacy %s is still embedded in %s", tt.legacyEmbeddedField, tt.configType.Name())
			}
			failureStore, ok := tt.flushManagerType.FieldByName("FailureStoreConfig")
			if !ok {
				t.Fatalf("FailureStoreConfig is missing from %s", tt.flushManagerType.Name())
			}
			if failureStore.Type != tt.failureStoreTypeWant {
				t.Fatalf("FailureStoreConfig type = %s, want %s", failureStore.Type, tt.failureStoreTypeWant)
			}
		})
	}
}

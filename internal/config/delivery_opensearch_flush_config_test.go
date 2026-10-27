package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestDeliveryOpenSearchConfigKeepsSendSettingsOutOfLoggerBase(t *testing.T) {
	baseType := reflect.TypeOf(logconfig.DeliveryOpenSearchLoggerBaseConfig{})
	for _, field := range []string{
		"PostWarnThresholdDuration",
		"PostErrorThresholdDuration",
		"IndexLatencyEnabled",
		"MaxItemRetries",
		"RetryWaitTime",
		"MaxRequestBytes",
	} {
		if _, ok := baseType.FieldByName(field); ok {
			t.Fatalf("DeliveryOpenSearchLoggerBaseConfig unexpectedly contains %s", field)
		}
	}

	flushType := reflect.TypeOf(logconfig.DeliveryOpenSearchFlushManagerConfig{})
	for _, field := range []string{
		"Index",
		"OpenTelemetryFormatEnabled",
		"FailureHandlerConfig",
		"PostWarnThresholdDuration",
		"PostErrorThresholdDuration",
		"IndexLatencyEnabled",
	} {
		if _, ok := flushType.FieldByName(field); !ok {
			t.Fatalf("DeliveryOpenSearchFlushManagerConfig does not contain %s", field)
		}
	}
	for _, field := range []string{"MaxItemRetries", "RetryWaitTime"} {
		if _, ok := flushType.FieldByName(field); ok {
			t.Fatalf("DeliveryOpenSearchFlushManagerConfig unexpectedly contains bulk-only %s", field)
		}
	}

	mappedType := reflect.TypeOf(pkglogger.DeliveryOpenSearchFlushManagerConfig{})
	for _, field := range []string{"MaxItemRetries", "RetryWaitTime"} {
		if _, ok := mappedType.FieldByName(field); ok {
			t.Fatalf("pkg DeliveryOpenSearchFlushManagerConfig unexpectedly contains bulk-only %s", field)
		}
	}
}

func TestDeliveryOpenSearchFlushConfigLoaderAndMapperPreserveImmediateSendSettings(t *testing.T) {
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_INDEX", "delivery-direct-index")
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_OTEL_FORMAT_LOG", "true")
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_INDEX_OPENSEARCH_LATENCY", "false")

	loaded := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryOpenSearchFlushManagerConfig))
	if got, want := loaded.Index, "delivery-direct-index"; got != want {
		t.Fatalf("loaded Index = %q, want %q", got, want)
	}
	if !loaded.OpenTelemetryFormatEnabled {
		t.Fatal("loaded OpenTelemetryFormatEnabled = false, want true")
	}
	if loaded.IndexLatencyEnabled {
		t.Fatal("loaded IndexLatencyEnabled = true, want false")
	}
	if got, want := loaded.PostWarnThresholdDuration, logconfig.PostLogWarnThresholdDuration; got != want {
		t.Fatalf("loaded PostWarnThresholdDuration = %s, want %s", got, want)
	}
	if got, want := loaded.PostErrorThresholdDuration, logconfig.PostLogErrorThresholdDuration; got != want {
		t.Fatalf("loaded PostErrorThresholdDuration = %s, want %s", got, want)
	}

	appCfg := AppConfig{
		RuntimeEnv: configruntime.RuntimeEnv{Cloud: &configruntime.CloudInfo{}},
	}
	appCfg.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig = loaded
	mapped := NewDeliveryOpenSearchFlushManagerConfig(&appCfg)
	if got, want := mapped.Index, loaded.Index; got != want {
		t.Fatalf("mapped Index = %q, want %q", got, want)
	}
	if got, want := mapped.PostWarnThresholdDuration, loaded.PostWarnThresholdDuration; got != want {
		t.Fatalf("mapped PostWarnThresholdDuration = %s, want %s", got, want)
	}
	if got, want := mapped.PostErrorThresholdDuration, loaded.PostErrorThresholdDuration; got != want {
		t.Fatalf("mapped PostErrorThresholdDuration = %s, want %s", got, want)
	}
	if got, want := mapped.IndexLatencyEnabled, loaded.IndexLatencyEnabled; got != want {
		t.Fatalf("mapped IndexLatencyEnabled = %t, want %t", got, want)
	}
}

func TestValidateLoggerTimeoutHierarchyCountsOnlyClientWrapperAttemptsForDeliveryOpenSearch(t *testing.T) {
	newConfig := func(parentTimeout time.Duration) AppConfig {
		var cfg AppConfig
		cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputOpenSearch}
		cfg.DeliveryLogger.Timeout = parentTimeout
		cfg.DeliveryLogger.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig = logconfig.OpenSearchRetryAbleClientConfig{
			RetryClientMode: pkglogger.RetryClientModeRetry,
			MaxRetries:      1,
			BaseDelay:       100 * time.Millisecond,
			MaxDelay:        100 * time.Millisecond,
		}
		cfg.DeliveryOpenSearchClient.Timeout = time.Second
		return cfg
	}

	// 1秒のclient呼び出し2回と100msのbackoffはDeliveryでもclient wrapperが消費します。
	if err := validateLoggerTimeoutHierarchy(newConfig(2 * time.Second)); err == nil {
		t.Fatal("validateLoggerTimeoutHierarchy() error = nil, want client wrapper retry budget error")
	}

	// 必要な予算は2.1秒だけです。Deliveryは1件を直接Indexするため、ここへbulk item再送回数を掛けません。
	if err := validateLoggerTimeoutHierarchy(newConfig(2200 * time.Millisecond)); err != nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() error = %v, want nil for direct Index retry budget", err)
	}
}

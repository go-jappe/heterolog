package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// Delivery Lokiは1件ずつPushするため、flush manager DTOに再試行設定を重複して持たせません。
// 再試行設定がclient wrapper用の設定としてロード・mappingされることも併せて確認します。
func TestDeliveryLokiRetryConfigBelongsToClientWrapper(t *testing.T) {
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_LOKI_PUSH_LATENCY", "false")
	t.Setenv("HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE", "retry_policy")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOKI_RETRYABLE_CLIENT_ATTEMPTS", "7")
	t.Setenv("HETEROLOG_DELIVERY_LOKI_RETRYABLE_CLIENT_MILLISECOND", "11")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND", "19")

	retryCfg, err := loadDeliveryLokiRetryableClientConfig(processEnvironmentForTest())
	if err != nil {
		t.Fatalf("load Delivery Loki retry client config: %v", err)
	}
	appCfg := AppConfig{
		RuntimeEnv: configruntime.RuntimeEnv{Cloud: &configruntime.CloudInfo{}},
		DeliveryLogger: logconfig.DeliveryLoggerConfig{
			LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki},
			DeliveryLokiConfig: logconfig.DeliveryLokiConfig{
				LokiRetryAbleClientConfig: retryCfg,
			},
		},
	}

	mapped := ToDeliveryLoggerConfig(appCfg)
	if mapped.Loki == nil || mapped.Loki.LokiRetryableClientCaseTransportErrorConfig == nil {
		t.Fatal("Delivery Loki client wrapper retry config is nil")
	}
	got := mapped.Loki.LokiRetryableClientCaseTransportErrorConfig
	if got.RetryClientMode != pkglogger.RetryClientModeRetryPolicy {
		t.Fatalf("RetryClientMode = %q, want %q", got.RetryClientMode, pkglogger.RetryClientModeRetryPolicy)
	}
	if got.MaxAttempts != 7 || got.BaseDelay != 11*time.Millisecond || got.MaxDelay != 19*time.Millisecond {
		t.Fatalf("retry config = attempts:%d base:%s max:%s", got.MaxAttempts, got.BaseDelay, got.MaxDelay)
	}

	flushConfigType := reflect.TypeOf(pkglogger.DeliveryLokiFlushManagerConfig{})
	for _, field := range []string{"MaxRetryAttempts", "RetryWaitTime"} {
		if _, exists := flushConfigType.FieldByName(field); exists {
			t.Fatalf("DeliveryLokiFlushManagerConfig must not own client retry field %s", field)
		}
	}

	// 遅延監視はPushを実行するflush manager側の責務であり、ログ生成用base configには置きません。
	baseConfigType := reflect.TypeOf(logconfig.DeliveryLokiLoggerBaseConfig{})
	loadedFlushConfig := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryLokiFlushManagerConfig))
	if loadedFlushConfig.PushLatencyEnabled {
		t.Fatal("Delivery Loki flush manager PushLatencyEnabled = true, want false")
	}
	for _, field := range []string{"PostWarnThresholdDuration", "PostErrorThresholdDuration", "PushLatencyEnabled"} {
		if _, exists := baseConfigType.FieldByName(field); exists {
			t.Fatalf("DeliveryLokiLoggerBaseConfig must not own flush field %s", field)
		}
		if _, exists := reflect.TypeOf(loadedFlushConfig).FieldByName(field); !exists {
			t.Fatalf("DeliveryLokiFlushManagerConfig is missing send field %s", field)
		}
	}
}

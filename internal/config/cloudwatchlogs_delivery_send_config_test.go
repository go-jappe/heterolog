package config

import (
	"reflect"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestDeliveryCloudWatchLogsSendSettingsBelongToFlushManagerConfig(t *testing.T) {
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_PUT_LATENCY", "true")

	baseType := reflect.TypeOf(logconfig.DeliveryCloudWatchLogsLoggerBaseConfig{})
	for _, field := range []string{
		"PostWarnThresholdDuration",
		"PostErrorThresholdDuration",
		"PutLatencyEnabled",
		"MaxItemRetries",
		"RetryWaitTime",
	} {
		if _, ok := baseType.FieldByName(field); ok {
			t.Fatalf("%s must not belong to DeliveryCloudWatchLogsLoggerBaseConfig", field)
		}
	}

	fm := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryCloudWatchLogsFlushManagerConfig))
	if !fm.PutLatencyEnabled {
		t.Fatal("PutLatencyEnabled = false, want true from HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_PUT_LATENCY")
	}
	if fm.PostWarnThresholdDuration != logconfig.PostLogWarnThresholdDuration {
		t.Fatalf("PostWarnThresholdDuration = %s, want %s", fm.PostWarnThresholdDuration, logconfig.PostLogWarnThresholdDuration)
	}
	if fm.PostErrorThresholdDuration != logconfig.PostLogErrorThresholdDuration {
		t.Fatalf("PostErrorThresholdDuration = %s, want %s", fm.PostErrorThresholdDuration, logconfig.PostLogErrorThresholdDuration)
	}

	appCfg := AppConfig{RuntimeEnv: configruntime.RuntimeEnv{Cloud: &configruntime.CloudInfo{}}}
	appCfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig = fm
	mapped := NewDeliveryCloudWatchLogsFlushManagerConfig(&appCfg)
	if !mapped.PutLatencyEnabled {
		t.Fatal("mapped PutLatencyEnabled = false, want true")
	}
	if mapped.PostWarnThresholdDuration != fm.PostWarnThresholdDuration {
		t.Fatalf("mapped PostWarnThresholdDuration = %s, want %s", mapped.PostWarnThresholdDuration, fm.PostWarnThresholdDuration)
	}
	if mapped.PostErrorThresholdDuration != fm.PostErrorThresholdDuration {
		t.Fatalf("mapped PostErrorThresholdDuration = %s, want %s", mapped.PostErrorThresholdDuration, fm.PostErrorThresholdDuration)
	}
}

func TestDeliveryCloudWatchLogsFlushManagerConfigExcludesUnusedRetrySettings(t *testing.T) {
	typeOf := reflect.TypeOf(pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{})
	for _, field := range []string{"MaxItemRetries", "RetryWaitTime"} {
		if _, ok := typeOf.FieldByName(field); ok {
			t.Fatalf("%s is unused; retry client config owns transport retry", field)
		}
	}

	if _, ok := typeOf.FieldByName("PutLatencyEnabled"); !ok {
		t.Fatal("PutLatencyEnabled must remain because immediate PutLogEvents measures latency")
	}
	if _, ok := typeOf.FieldByName("LogStreamRotation"); ok {
		t.Fatal("LogStreamRotation must not leak application stream routing policy into pkg/logger DTO")
	}
	if _, ok := typeOf.FieldByName("LogStreamRoutingGranularity"); ok {
		t.Fatal("LogStreamRoutingGranularity must not leak application stream routing policy into pkg/logger DTO")
	}
	if got := typeOf.NumField(); got != 7 {
		t.Fatalf("DeliveryCloudWatchLogsFlushManagerConfig field count = %d, want 7", got)
	}

	internalType := reflect.TypeOf(logconfig.DeliveryCloudWatchLogsFlushManagerConfig{})
	if _, ok := internalType.FieldByName("LogStreamRoutingGranularity"); !ok {
		t.Fatal("internal DeliveryCloudWatchLogsFlushManagerConfig must retain stream routing granularity")
	}
}

func TestMappedCloudWatchLogsFlushManagerConfigsExcludeStreamRoutingPolicy(t *testing.T) {
	for _, typeOf := range []reflect.Type{
		reflect.TypeOf(pkglogger.CloudWatchLogsFlushManagerConfig{}),
		reflect.TypeOf(pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{}),
	} {
		for _, field := range []string{"LogStreamRotation", "LogStreamRoutingGranularity"} {
			if _, ok := typeOf.FieldByName(field); ok {
				t.Fatalf("%s.%s must remain outside pkg/logger DTO", typeOf.Name(), field)
			}
		}
	}
}

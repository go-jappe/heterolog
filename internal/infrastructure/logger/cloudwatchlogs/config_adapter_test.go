package cloudwatchlogs

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/wrapper"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestRetryConfigProviderPreservesInvalidModeForChainValidation(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{"", "typo"} {
		t.Run(string(mode), func(t *testing.T) {
			provider := NewRetryConfigProvider(&pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{
				RetryClientMode: mode,
				MaxAttempts:     2,
				BaseDelay:       time.Millisecond,
				MaxDelay:        time.Second,
			})
			if provider == nil {
				t.Fatal("invalid mode must reach client chain validation, not become nil")
			}
			if got := provider.RetryClientSelection().Mode; got != mode {
				t.Fatalf("mode = %q, want %q", got, mode)
			}
			client, err := wrapper.BuildClientChain(&pkgcloudwatchlogs.HTTPClient{}, provider, nil, nil)
			// 未対応のmodeは、どのbackendのretry client設定かとともに報告します。
			var invalid *retryable.InvalidRetryClientConfigError
			if !errors.As(err, &invalid) || invalid.Backend != "cloudwatch logs" ||
				!errors.Is(err, pkglogger.ErrUnsupportedRetryClientMode) {
				t.Fatalf("BuildClientChain() error = %v, want unsupported cloudwatch logs retry client mode", err)
			}
			if fieldErrs := pkglogger.ConfigFieldErrors(err); len(fieldErrs) != 1 ||
				fieldErrs[0].Field != pkglogger.ConfigFieldRetryClientMode || fieldErrs[0].Value != mode {
				t.Fatalf("config field errors = %+v, want %s %q", fieldErrs, pkglogger.ConfigFieldRetryClientMode, mode)
			}
			if client != nil {
				t.Fatal("invalid config must not return a usable client")
			}
		})
	}
}

func TestNewRetryConfigProviderMapsSelectedClientChain(t *testing.T) {
	t.Parallel()

	cfg := &pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicyWithBreaker,
		MaxAttempts:     4,
		BaseDelay:       50 * time.Millisecond,
		MaxDelay:        2 * time.Second,
	}
	provider := NewRetryConfigProvider(cfg)
	if provider == nil {
		t.Fatal("NewRetryConfigProvider() returned nil")
	}
	if got := provider.RetryClientSelection(); !got.UseRetryPolicyClient || !got.UseCircuitBreakerClient {
		t.Fatalf("RetryClientSelection() = %+v, want retry policy with breaker", got)
	}
	if provider.MaxAttempts() != cfg.MaxAttempts || provider.BaseDelay() != cfg.BaseDelay || provider.MaxDelay() != cfg.MaxDelay {
		t.Fatalf("retry delay config was not preserved")
	}
}

func TestNewRetryConfigProviderReturnsNilWithoutWrapper(t *testing.T) {
	t.Parallel()

	if got := NewRetryConfigProvider(nil); got != nil {
		t.Fatalf("NewRetryConfigProvider(nil) = %T, want nil", got)
	}
	if got := NewRetryConfigProvider(&pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig{RetryClientMode: pkglogger.RetryClientModeNone}); got != nil {
		t.Fatalf("NewRetryConfigProvider(none) = %T, want nil", got)
	}
}

func TestDeliveryFlushManagerConfigAdapterUsesDeliveryFailureStoreConfig(t *testing.T) {
	t.Parallel()

	cfg := pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
		FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
			DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
			FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{
				Target:                 "delivery-cloudwatchlogs",
				FailureFileArchivePath: "/tmp/delivery-cloudwatchlogs",
			},
		},
		LogGroupName:               "/app/delivery",
		LogStreamName:              "delivery-stream",
		PostWarnThresholdDuration:  2 * time.Second,
		PostErrorThresholdDuration: 3 * time.Second,
		PutLatencyEnabled:          true,
	}

	routingGranularity := streamrouting.Hourly
	adapter := NewDeliveryFlushManagerConfigAdapter(cfg, routingGranularity)

	var _ DeliveryCloudWatchLogsFlushManagerConfigProvider = adapter
	var _ CloudWatchLogsFlushManagerRuntimeConfigProvider = adapter
	if _, ok := any(adapter).(CloudWatchLogsFlushManagerConfigProvider); ok {
		t.Fatal("delivery flush manager adapter must not expose normal failure store config")
	}

	if got := adapter.LogGroupName(); got != cfg.LogGroupName {
		t.Fatalf("LogGroupName() = %q, want %q", got, cfg.LogGroupName)
	}
	if got := adapter.LogStreamName(); got != cfg.LogStreamName {
		t.Fatalf("LogStreamName() = %q, want %q", got, cfg.LogStreamName)
	}
	if got := adapter.LogStreamRoutingGranularity(); got != routingGranularity {
		t.Fatalf(
			"LogStreamRoutingGranularity() = %q, want %q",
			got,
			routingGranularity,
		)
	}
	if got := adapter.PostWarnThresholdDuration(); got != cfg.PostWarnThresholdDuration {
		t.Fatalf("PostWarnThresholdDuration() = %s, want %s", got, cfg.PostWarnThresholdDuration)
	}
	if got := adapter.PostErrorThresholdDuration(); got != cfg.PostErrorThresholdDuration {
		t.Fatalf("PostErrorThresholdDuration() = %s, want %s", got, cfg.PostErrorThresholdDuration)
	}
	if !adapter.PutLatencyEnabled() {
		t.Fatal("PutLatencyEnabled() = false, want true")
	}
	failureStore := adapter.DeliveryFailureStoreConfig()
	if failureStore == nil {
		t.Fatal("DeliveryFailureStoreConfig() is nil")
	}
	if got := failureStore.DurableType(); len(got) != 1 || got[0] != pkgdurable.DurableTypeFileArchive {
		t.Fatalf("delivery DurableType() = %v, want [fileArchive]", got)
	}
	archiveCfg := failureStore.DeliveryDurableStoreConfig().FileArchiveConfig()
	if got := archiveCfg.Target(); got != "delivery-cloudwatchlogs" {
		t.Fatalf("file archive Target() = %q, want %q", got, "delivery-cloudwatchlogs")
	}
}

func TestCloudWatchLogsLoggerConfigAdapterExposesTimeConfig(t *testing.T) {
	t.Parallel()

	adapter := NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		TimeConfig: pkglogger.TimeConfig{
			IsLocalTimeEnabled: true,
			TimeZone:           "Asia/Tokyo",
		},
	})

	var _ CloudWatchLogsConfigProvider = adapter
	if !adapter.Time().EnableLocalTime() {
		t.Fatal("Time().EnableLocalTime() = false, want true")
	}
	if got := adapter.Time().TimeZone(); got != "Asia/Tokyo" {
		t.Fatalf("Time().TimeZone() = %q, want Asia/Tokyo", got)
	}
}

func TestCloudWatchLogsLoggerConfigAdapterExposesSeparateBufferContracts(t *testing.T) {
	t.Parallel()

	adapter := NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		BufferFlushMaxEvents:   123,
		BufferFlushBytes:       456789,
		FallbackQueueMaxEvents: 321,
	})

	if got := adapter.BufferFlushMaxEvents(); got != 123 {
		t.Fatalf("BufferFlushMaxEvents() = %d, want 123", got)
	}
	if got := adapter.BufferFlushBytes(); got != 456789 {
		t.Fatalf("BufferFlushBytes() = %d, want 456789", got)
	}
	if got := adapter.FallbackQueueMaxEvents(); got != 321 {
		t.Fatalf("FallbackQueueMaxEvents() = %d, want 321", got)
	}
}

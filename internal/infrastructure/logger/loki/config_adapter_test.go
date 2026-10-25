package loki

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/loki/wrapper"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
)

func TestRetryConfigProviderPreservesInvalidModeForChainValidation(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{"", "typo"} {
		t.Run(string(mode), func(t *testing.T) {
			provider := NewRetryConfigProvider(&pkglogger.LokiRetryableClientCaseTransportErrorConfig{
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
			client, err := wrapper.BuildClientChain(&pkgloki.Client{}, provider, nil, nil)
			// 未対応のmodeは、どのbackendのretry client設定かとともに報告します。
			var invalid *retryable.InvalidRetryClientConfigError
			if !errors.As(err, &invalid) || invalid.Backend != "loki" ||
				!errors.Is(err, pkglogger.ErrUnsupportedRetryClientMode) {
				t.Fatalf("BuildClientChain() error = %v, want unsupported loki retry client mode", err)
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

	cfg := &pkglogger.LokiRetryableClientCaseTransportErrorConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicyWithBreaker,
		MaxAttempts:     4,
		BaseDelay:       25 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	provider := NewRetryConfigProvider(cfg)
	if provider == nil {
		t.Fatal("NewRetryConfigProvider() returned nil")
	}
	if got := provider.RetryClientSelection(); !got.UseRetryPolicyClient || !got.UseCircuitBreakerClient {
		t.Fatalf("RetryClientSelection() = %+v, want retry policy with circuit breaker", got)
	}
	if got := provider.MaxAttempts(); got != cfg.MaxAttempts {
		t.Fatalf("MaxAttempts() = %d, want %d", got, cfg.MaxAttempts)
	}
}

func TestNewRetryConfigProviderReturnsNilWithoutWrapper(t *testing.T) {
	t.Parallel()

	if got := NewRetryConfigProvider(nil); got != nil {
		t.Fatalf("NewRetryConfigProvider(nil) = %T, want nil", got)
	}
	if got := NewRetryConfigProvider(&pkglogger.LokiRetryableClientCaseTransportErrorConfig{RetryClientMode: pkglogger.RetryClientModeNone}); got != nil {
		t.Fatalf("NewRetryConfigProvider(none) = %T, want nil", got)
	}
}

func TestLokiLoggerConfigAdapterMapsBufferThresholdAndFallbackQueueCapacity(t *testing.T) {
	t.Parallel()

	adapter := NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		BufferFlushBytes:        456789,
		FallbackQueueMaxEntries: 321,
	})
	if got := adapter.BufferFlushBytes(); got != 456789 {
		t.Fatalf("BufferFlushBytes() = %d, want 456789", got)
	}
	if got := adapter.FallbackQueueMaxEntries(); got != 321 {
		t.Fatalf("FallbackQueueMaxEntries() = %d, want 321", got)
	}
}

func TestDeliveryFlushManagerConfigAdapterUsesDeliveryFailureStoreConfig(t *testing.T) {
	t.Parallel()

	cfg := pkglogger.DeliveryLokiFlushManagerConfig{
		StaticLabels: map[string]string{"event_scope": "delivery_lifecycle"},
		FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
			DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
			FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{
				Target:                 "delivery-loki",
				FailureFileArchivePath: "/tmp/delivery-loki",
			},
		},
		PostWarnThresholdDuration:  2 * time.Second,
		PostErrorThresholdDuration: 3 * time.Second,
		PushLatencyEnabled:         true,
	}

	adapter := NewDeliveryFlushManagerConfigAdapter(cfg)
	cfg.StaticLabels["event_scope"] = "mutated-after-adapter-construction"

	var _ DeliveryLokiFlushManagerConfigProvider = adapter
	var _ LokiFlushManagerRuntimeConfigProvider = adapter
	if _, ok := any(adapter).(LokiFlushManagerConfigProvider); ok {
		t.Fatal("delivery flush manager adapter must not expose normal failure store config")
	}

	if got := adapter.PostWarnThresholdDuration(); got != cfg.PostWarnThresholdDuration {
		t.Fatalf("PostWarnThresholdDuration() = %s, want %s", got, cfg.PostWarnThresholdDuration)
	}
	if got := adapter.PostErrorThresholdDuration(); got != cfg.PostErrorThresholdDuration {
		t.Fatalf("PostErrorThresholdDuration() = %s, want %s", got, cfg.PostErrorThresholdDuration)
	}
	if !adapter.PushLatencyEnabled() {
		t.Fatal("PushLatencyEnabled() = false, want true")
	}
	labels := adapter.StaticLabels()
	if got := labels["event_scope"]; got != "delivery_lifecycle" {
		t.Fatalf("StaticLabels()[event_scope] = %q", got)
	}
	labels["event_scope"] = "mutated"
	if got := adapter.StaticLabels()["event_scope"]; got != "delivery_lifecycle" {
		t.Fatalf("StaticLabels() leaked mutable map: %q", got)
	}
	failureStore := adapter.DeliveryFailureStoreConfig()
	if failureStore == nil {
		t.Fatal("DeliveryFailureStoreConfig() is nil")
	}
	if got := failureStore.DurableType(); len(got) != 1 || got[0] != pkgdurable.DurableTypeFileArchive {
		t.Fatalf("delivery DurableType() = %v, want [fileArchive]", got)
	}
	archiveCfg := failureStore.DeliveryDurableStoreConfig().FileArchiveConfig()
	if got := archiveCfg.Target(); got != "delivery-loki" {
		t.Fatalf("file archive Target() = %q, want %q", got, "delivery-loki")
	}
}

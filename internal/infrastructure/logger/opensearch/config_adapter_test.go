package opensearch

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch/wrapper"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestRetryConfigProviderPreservesInvalidModeForChainValidation(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{"", "typo"} {
		t.Run(string(mode), func(t *testing.T) {
			provider := NewRetryConfigProvider(&pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
				RetryClientMode: mode,
				MaxRetries:      1,
				BaseDelay:       time.Millisecond,
				MaxDelay:        time.Second,
			})
			if provider == nil {
				t.Fatal("invalid mode must reach client chain validation, not become nil")
			}
			if got := provider.RetryClientSelection().Mode; got != mode {
				t.Fatalf("mode = %q, want %q", got, mode)
			}
			client, err := wrapper.BuildClientChain(NewMockOpenSearchClient(), provider, nil, nil)
			// 未対応のmodeは、どのbackendのretry client設定かとともに報告します。
			var invalid *retryable.InvalidRetryClientConfigError
			if !errors.As(err, &invalid) || invalid.Backend != "opensearch" ||
				!errors.Is(err, pkglogger.ErrUnsupportedRetryClientMode) {
				t.Fatalf("BuildClientChain() error = %v, want unsupported opensearch retry client mode", err)
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

func TestRetryConfigProvider(t *testing.T) {
	cfg := &pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicyWithBreaker,
		MaxRetries:      3,
		BaseDelay:       50 * time.Millisecond,
		MaxDelay:        2 * time.Second,
	}

	adapter := NewRetryConfigProvider(cfg)

	if adapter == nil {
		t.Fatal("NewRetryConfigProvider() = nil, want provider")
	}
	selection := adapter.RetryClientSelection()
	if !selection.UseRetryPolicyClient || selection.UseRetryingClient || !selection.UseCircuitBreakerClient {
		t.Fatalf("RetryClientSelection() = %+v, want policy + circuit breaker", selection)
	}
	// wrapperへは、再試行の回数に初回を足した試行回数を渡します。
	if got := adapter.MaxAttempts(); got != cfg.MaxRetries+1 {
		t.Fatalf("MaxAttempts() = %d, want %d", got, cfg.MaxRetries+1)
	}
	if got := adapter.BaseDelay(); got != cfg.BaseDelay {
		t.Fatalf("BaseDelay() = %s, want %s", got, cfg.BaseDelay)
	}
	if got := adapter.MaxDelay(); got != cfg.MaxDelay {
		t.Fatalf("MaxDelay() = %s, want %s", got, cfg.MaxDelay)
	}
}

func TestRetryConfigProviderReturnsNilWhenNoWrapperIsSelected(t *testing.T) {
	if got := NewRetryConfigProvider(nil); got != nil {
		t.Fatalf("nil config adapter = %T, want nil", got)
	}
	if got := NewRetryConfigProvider(
		&pkglogger.OpenSearchRetryableClientCaseTransportErrorConfig{
			RetryClientMode: pkglogger.RetryClientModeNone,
		},
	); got != nil {
		t.Fatalf("none mode adapter = %T, want nil", got)
	}
}

func TestDeliveryFlushManagerAdapterDoesNotExposeNormalFailureStoreConfig(t *testing.T) {
	cfg := pkglogger.DeliveryOpenSearchFlushManagerConfig{
		Resource: pkglogger.Resource{
			Cloud: &pkglogger.CloudResourceInfo{},
		},
		FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
			DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
		},
	}

	adapter := NewDeliveryFlushManagerConfigAdapter(cfg)

	var _ DeliveryOpenSearchFlushManagerConfigProvider = adapter
	if _, ok := any(adapter).(OpenSearchBulkFlushManagerConfigProvider); ok {
		t.Fatal("delivery flush manager adapter must not expose bulk retry config")
	}
	if _, ok := any(adapter).(OpenSearchFlushManagerConfigProvider); ok {
		t.Fatal("delivery flush manager adapter must not expose sync failure store config")
	}
}

func TestNormalFlushManagerAdapterExposesBulkRetryConfig(t *testing.T) {
	adapter := NewFlushManagerConfigAdapter(pkglogger.OpenSearchFlushManagerConfig{
		MaxItemRetries: 4,
		Resource: pkglogger.Resource{
			Cloud: &pkglogger.CloudResourceInfo{},
		},
	})

	var bulkConfig OpenSearchBulkFlushManagerConfigProvider = adapter
	if got, want := bulkConfig.MaxItemRetries(), 4; got != want {
		t.Fatalf("MaxItemRetries() = %d, want %d", got, want)
	}
}

func TestOpenSearchLoggerConfigAdapterExposesRequestLimit(t *testing.T) {
	adapter := NewOpenSearchLoggerConfigAdapter(pkglogger.OpenSearchLoggerBaseConfig{
		BufferFlushBytes:    5 * 1024 * 1024,
		MaxBulkRequestBytes: 15 * 1024 * 1024,
	})

	if got, want := adapter.BufferFlushBytes(), 5*1024*1024; got != want {
		t.Fatalf("BufferFlushBytes() = %d, want %d", got, want)
	}
	if got, want := adapter.MaxBulkRequestBytes(), 15*1024*1024; got != want {
		t.Fatalf("MaxBulkRequestBytes() = %d, want %d", got, want)
	}
}

package clickhouse

import (
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/clickhouse/wrapper"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"

	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryConfigProviderPreservesInvalidModeForChainValidation(t *testing.T) {
	for _, mode := range []pkglogger.RetryClientMode{"", "typo"} {
		t.Run(string(mode), func(t *testing.T) {
			provider := NewRetryConfigProvider(&pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig{
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
			client, err := wrapper.BuildClientChain(&pkgclickhouse.Client{}, provider, nil, nil)
			// 未対応のmodeは、どのbackendのretry client設定かとともに報告します。
			var invalid *retryable.InvalidRetryClientConfigError
			if !errors.As(err, &invalid) || invalid.Backend != "clickhouse" ||
				!errors.Is(err, pkglogger.ErrUnsupportedRetryClientMode) {
				t.Fatalf("BuildClientChain() error = %v, want unsupported clickhouse retry client mode", err)
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

	cfg := &pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicyWithBreaker,
		MaxRetries:      3,
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
	if provider.MaxAttempts() != cfg.MaxRetries+1 || provider.BaseDelay() != cfg.BaseDelay || provider.MaxDelay() != cfg.MaxDelay {
		t.Fatalf("retry delay config was not preserved")
	}
}

func TestNewRetryConfigProviderReturnsNilWithoutWrapper(t *testing.T) {
	t.Parallel()

	if got := NewRetryConfigProvider(nil); got != nil {
		t.Fatalf("NewRetryConfigProvider(nil) = %T, want nil", got)
	}
	if got := NewRetryConfigProvider(&pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig{RetryClientMode: pkglogger.RetryClientModeNone}); got != nil {
		t.Fatalf("NewRetryConfigProvider(none) = %T, want nil", got)
	}
}

func TestClickHouseLoggerConfigAdaptersPreserveRequestByteLimits(t *testing.T) {
	t.Parallel()

	const (
		bufferFlushBytes        = 3 * 1024 * 1024
		fallbackQueueMaxEntries = 2000
		maxInsertRequestBytes   = 8 * 1024 * 1024
	)

	normal := NewClickHouseLoggerConfigAdapter(pkglogger.ClickHouseLoggerBaseConfig{
		BufferFlushBytes:        bufferFlushBytes,
		FallbackQueueMaxEntries: fallbackQueueMaxEntries,
		MaxInsertRequestBytes:   maxInsertRequestBytes,
	})
	assert.Equal(t, bufferFlushBytes, normal.BufferFlushBytes())
	assert.Equal(t, fallbackQueueMaxEntries, normal.FallbackQueueMaxEntries())
	assert.Equal(t, maxInsertRequestBytes, normal.MaxInsertRequestBytes())

	delivery := NewDeliveryClickHouseLoggerConfigAdapter(pkglogger.DeliveryClickHouseLoggerBaseConfig{
		BufferFlushBytes:        bufferFlushBytes,
		FallbackQueueMaxEntries: fallbackQueueMaxEntries,
		MaxInsertRequestBytes:   maxInsertRequestBytes,
	})
	assert.Equal(t, bufferFlushBytes, delivery.BufferFlushBytes())
	assert.Equal(t, fallbackQueueMaxEntries, delivery.FallbackQueueMaxEntries())
	assert.Equal(t, maxInsertRequestBytes, delivery.MaxInsertRequestBytes())
}

func TestDeliveryFlushManagerConfigAdapterExposesFileArchiveOnlyFailureStore(t *testing.T) {
	warnThreshold := 150 * time.Millisecond
	errorThreshold := 500 * time.Millisecond
	cfg := pkglogger.DeliveryClickHouseFlushManagerConfig{
		FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
			DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive},
			FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{
				FailureFileArchivePath: "/var/lib/test",
				Dir:                    "clickhouse-failure-archive",
			},
		},
		PostWarnThresholdDuration:  warnThreshold,
		PostErrorThresholdDuration: errorThreshold,
		InsertLatencyEnabled:       true,
	}

	adapter := NewDeliveryFlushManagerConfigAdapter(cfg)

	var _ DeliveryClickHouseFlushManagerConfigProvider = adapter
	var _ ClickHouseFlushManagerRuntimeConfigProvider = adapter
	if _, ok := any(adapter).(ClickHouseFlushManagerConfigProvider); ok {
		t.Fatal("delivery flush manager adapter must not expose normal failure store config")
	}

	assert.Equal(t, warnThreshold, adapter.PostWarnThresholdDuration())
	assert.Equal(t, errorThreshold, adapter.PostErrorThresholdDuration())
	assert.True(t, adapter.InsertLatencyEnabled())

	failureStore := adapter.DeliveryFailureStoreConfig()
	require.NotNil(t, failureStore)
	assert.Equal(t, cfg.FailureStoreConfig.DurableType, failureStore.DurableType())
	require.NotNil(t, failureStore.DeliveryDurableStoreConfig())
	assert.Equal(t, "clickhouse-failure-archive", failureStore.DeliveryDurableStoreConfig().FileArchiveConfig().Dir())
}

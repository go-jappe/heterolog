package config

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/fsstoreconfig"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/pkg/env"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

const (
	usageCacheTTLEnvKey = "HETEROLOG_INLINE_FILESYSTEM_USAGE_CACHE_TTL_SECOND"
)

func TestLoadFsstoreConfigUsesDefaultUsageCacheTTL(t *testing.T) {
	t.Setenv(usageCacheTTLEnvKey, "")

	cfg, err := loadFsstoreConfig(processEnvironmentForTest())
	if err != nil {
		t.Fatalf("loadFsstoreConfig() error = %v", err)
	}
	if got, want := cfg.UsageCacheTTL, fsstoreconfig.DefaultUsageCacheTTL; got != want {
		t.Fatalf("UsageCacheTTL = %s, want %s", got, want)
	}
}

func TestLoadFsstoreConfigReadsUsageCacheTTL(t *testing.T) {
	t.Setenv(usageCacheTTLEnvKey, "1200")

	cfg, err := loadFsstoreConfig(processEnvironmentForTest())
	if err != nil {
		t.Fatalf("loadFsstoreConfig() error = %v", err)
	}
	if got, want := cfg.UsageCacheTTL, 20*time.Minute; got != want {
		t.Fatalf("UsageCacheTTL = %s, want %s", got, want)
	}
}

func TestLoadFsstoreConfigLeavesUsageCacheTTLSemanticsToMappedValidation(t *testing.T) {
	for _, testCase := range []struct {
		raw  string
		want time.Duration
	}{
		{raw: "0", want: 0},
		{raw: "-1", want: -time.Second},
	} {
		t.Run(testCase.raw, func(t *testing.T) {
			t.Setenv(usageCacheTTLEnvKey, testCase.raw)

			cfg, err := loadFsstoreConfig(processEnvironmentForTest())
			if err != nil {
				t.Fatalf("loadFsstoreConfig() error = %v", err)
			}
			if cfg.UsageCacheTTL != testCase.want {
				t.Fatalf("UsageCacheTTL = %s, want %s", cfg.UsageCacheTTL, testCase.want)
			}
		})
	}
}

func TestLoadFsstoreConfigRejectsMalformedUsageCacheTTL(t *testing.T) {
	t.Setenv(usageCacheTTLEnvKey, "not-a-number")

	_, err := loadFsstoreConfig(processEnvironmentForTest())
	requireEnvironmentKeyError(t, err, usageCacheTTLEnvKey, env.ErrNotInteger)
}

func TestUsageCacheTTLIsMappedToEveryInlineLifecycleConfig(t *testing.T) {
	const usageCacheTTL = 23 * time.Minute
	a := &AppConfig{
		FsstoreConfig: fsstoreconfig.CommonFileSystemStoreConfig{
			UsageCacheTTL: usageCacheTTL,
		},
	}

	if got := NewFileLoggerConfig(a).FsstoreConfig.Lifecycle.UsageCacheTTL; got != usageCacheTTL {
		t.Fatalf("sync file usage cache TTL = %s, want %s", got, usageCacheTTL)
	}
	if got := NewAsyncFileLoggerConfig(a).FsstoreConfig.Lifecycle.UsageCacheTTL; got != usageCacheTTL {
		t.Fatalf("async file usage cache TTL = %s, want %s", got, usageCacheTTL)
	}
	if got := NewDeliveryFileLoggerConfig(a).LifecycleRunnerSettings.UsageCacheTTL; got != usageCacheTTL {
		t.Fatalf("delivery file lifecycle usage cache TTL = %s, want %s", got, usageCacheTTL)
	}

	failureStore := newFailureStoreConfig(a, checkpointSourceLoggerOpenSearch, pkglogger.LoggerContractSync, failure.FailureHandlerConfig{}, logconfig.LogBodyConfig{})
	if got := failureStore.DurableStoreConfig.FileArchiveConfig.UsageCacheTTL; got != usageCacheTTL {
		t.Fatalf("failure archive usage cache TTL = %s, want %s", got, usageCacheTTL)
	}
	deliveryFailureStore := newDeliveryFailureStoreConfig(a, checkpointSourceDeliveryLoggerOpenSearch, failure.DeliveryFailureHandlerConfig{}, logconfig.LogBodyConfig{})
	if got := deliveryFailureStore.FileArchiveConfig.UsageCacheTTL; got != usageCacheTTL {
		t.Fatalf("delivery failure archive usage cache TTL = %s, want %s", got, usageCacheTTL)
	}
}

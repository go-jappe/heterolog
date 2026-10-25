package config

import (
	"testing"

	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestLoadConfigThenMapLoggerConfigsDefaultsBlankFileLifecycleMode(t *testing.T) {
	for _, value := range []string{"", `"   "`} {
		t.Run("value="+value, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=file",
				"HETEROLOG_ASYNC_LOG_TYPE=file",
				"HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_LOG_FILE_LIFECYCLE_MODE=" + value,
				"HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_MODE=" + value,
				"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_MODE=" + value,
			})

			if got := cfg.Logger.FileSystemStoreConfig.LifecycleMode; got != pkgfsstore.FileLifecycleModeInline {
				t.Fatalf("loaded sync lifecycle mode = %q, want inline", got)
			}
			if got := cfg.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.LifecycleMode; got != pkgfsstore.FileLifecycleModeInline {
				t.Fatalf("loaded async lifecycle mode = %q, want inline", got)
			}
			if got := cfg.DeliveryLogger.FileSystemStoreConfig.LifecycleMode; got != pkgfsstore.FileLifecycleModeRunner {
				t.Fatalf("loaded delivery lifecycle mode = %q, want runner", got)
			}

			syncMapped, err := MapLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapLoggerConfig() after env defaulting: %v", err)
			}
			if syncMapped.File == nil || syncMapped.File.LifecycleMode != pkgfsstore.FileLifecycleModeInline {
				t.Fatalf("mapped sync file config = %+v, want inline lifecycle mode", syncMapped.File)
			}
			asyncMapped, err := MapAsyncLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapAsyncLoggerConfig() after env defaulting: %v", err)
			}
			if asyncMapped.File == nil || asyncMapped.File.LifecycleMode != pkgfsstore.FileLifecycleModeInline {
				t.Fatalf("mapped async file config = %+v, want inline lifecycle mode", asyncMapped.File)
			}
			deliveryMapped, err := MapDeliveryLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapDeliveryLoggerConfig() after env defaulting: %v", err)
			}
			if deliveryMapped.File == nil || deliveryMapped.File.LifecycleMode != pkgfsstore.FileLifecycleModeRunner {
				t.Fatalf("mapped delivery file config = %+v, want runner lifecycle mode", deliveryMapped.File)
			}
		})
	}
}

func TestLoadConfigThenMapLoggerConfigsDefaultsBlankFailureArchiveLifecycleMode(t *testing.T) {
	for _, value := range []string{"", `"   "`} {
		t.Run("value="+value, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_USERNAME=test-user",
				"HETEROLOG_OPENSEARCH_PASSWORD=test-password",
				"HETEROLOG_ASYNC_OPENSEARCH_USERNAME=test-user",
				"HETEROLOG_ASYNC_OPENSEARCH_PASSWORD=test-password",
				"HETEROLOG_DELIVERY_OPENSEARCH_USERNAME=test-user",
				"HETEROLOG_DELIVERY_OPENSEARCH_PASSWORD=test-password",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=" + value,
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=" + value,
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_LIFECYCLE_MODE=" + value,
			})

			for name, mode := range map[string]pkgfsstore.FileLifecycleMode{
				"sync": cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
					FailureHandlerConfig.Fsstore.FileArchiveConfig.LifecycleMode,
				"async": cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.
					FailureHandlerConfig.Fsstore.FileArchiveConfig.LifecycleMode,
				"delivery": cfg.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.
					FailureHandlerConfig.FileArchiveConfig.LifecycleMode,
			} {
				if mode != pkgfsstore.FileLifecycleModeInline {
					t.Fatalf("loaded %s failure archive lifecycle mode = %q, want inline", name, mode)
				}
			}

			syncMapped, err := MapLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapLoggerConfig() after failure archive env defaulting: %v", err)
			}
			asyncMapped, err := MapAsyncLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapAsyncLoggerConfig() after failure archive env defaulting: %v", err)
			}
			deliveryMapped, err := MapDeliveryLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapDeliveryLoggerConfig() after failure archive env defaulting: %v", err)
			}
			for name, mode := range map[string]pkgfsstore.FileLifecycleMode{
				"sync":     syncMapped.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.LifecycleMode,
				"async":    asyncMapped.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.LifecycleMode,
				"delivery": deliveryMapped.OpenSearch.FailureStoreConfig.FileArchiveConfig.LifecycleMode,
			} {
				if mode != pkgfsstore.FileLifecycleModeInline {
					t.Fatalf("mapped %s failure archive lifecycle mode = %q, want inline", name, mode)
				}
			}
		})
	}
}

func TestMapLoggerConfigsRejectFileLifecycleModeClearedAfterLoading(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_ASYNC_LOG_TYPE=file",
		"HETEROLOG_DELIVERY_LOG_TYPE=file",
	})

	t.Run("sync", func(t *testing.T) {
		invalid := cfg
		invalid.Logger.FileSystemStoreConfig.LifecycleMode = ""
		mapped, err := MapLoggerConfig(invalid)
		requireMappedConfigError(
			t,
			err,
			"logger file",
			mappedvalidation.ConfigSectionLifecycle,
			mappedvalidation.ErrEmptyFileLifecycleMode,
		)
		if mapped.File != nil {
			t.Fatal("MapLoggerConfig() returned usable file config after validation failure")
		}
	})
	t.Run("async", func(t *testing.T) {
		invalid := cfg
		invalid.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.LifecycleMode = ""
		mapped, err := MapAsyncLoggerConfig(invalid)
		requireMappedConfigError(
			t,
			err,
			"async logger file",
			mappedvalidation.ConfigSectionLifecycle,
			mappedvalidation.ErrEmptyFileLifecycleMode,
		)
		if mapped.File != nil {
			t.Fatal("MapAsyncLoggerConfig() returned usable file config after validation failure")
		}
	})
	t.Run("delivery", func(t *testing.T) {
		invalid := cfg
		invalid.DeliveryLogger.FileSystemStoreConfig.LifecycleMode = ""
		mapped, err := MapDeliveryLoggerConfig(invalid)
		requireMappedConfigError(
			t,
			err,
			"delivery logger file",
			mappedvalidation.ConfigSectionLifecycle,
			mappedvalidation.ErrEmptyDeliveryFileLifecycleMode,
		)
		if mapped.File != nil {
			t.Fatal("MapDeliveryLoggerConfig() returned usable file config after validation failure")
		}
	})
}

func TestLoadConfigThenMapLoggerConfigsRejectUnsupportedFileLifecycleMode(t *testing.T) {
	tests := []struct {
		name       string
		env        []string
		wantSource string
		wantKind   error
		mapError   func(AppConfig) error
	}{
		{
			name: "sync",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_LOG_TYPE=file",
				"HETEROLOG_LOG_FILE_LIFECYCLE_MODE=unknown",
			},
			wantSource: "logger file",
			wantKind:   pkgfsstore.ErrUnsupportedFileLifecycleMode,
			mapError: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "async",
			env: []string{
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ASYNC_LOG_TYPE=file",
				"HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_MODE=unknown",
			},
			wantSource: "async logger file",
			wantKind:   pkgfsstore.ErrUnsupportedFileLifecycleMode,
			mapError: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "delivery",
			env: []string{
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_MODE=unknown",
			},
			wantSource: "delivery logger file",
			wantKind:   pkgfsstore.ErrUnsupportedDeliveryFileLifecycleMode,
			mapError: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "delivery inline logger is unsupported",
			env: []string{
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_MODE=inline",
			},
			wantSource: "delivery logger file",
			wantKind:   pkgfsstore.ErrUnsupportedDeliveryFileLifecycleMode,
			mapError: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.mapError(loadConfigFromTempEnv(t, tt.env))
			requireMappedConfigError(t, err, tt.wantSource, mappedvalidation.ConfigSectionLifecycle, tt.wantKind)
		})
	}
}

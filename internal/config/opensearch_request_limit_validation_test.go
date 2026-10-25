package config

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestLoadOpenSearchConfigsUsesDefaultBulkByteLimits(t *testing.T) {
	for _, key := range []string{
		"HETEROLOG_OPENSEARCH_BUFFER_FLUSH_BYTES",
		"HETEROLOG_MAX_OPENSEARCH_BULK_REQUEST_BYTES",
		"HETEROLOG_ASYNC_OPENSEARCH_BUFFER_FLUSH_BYTES",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_BULK_REQUEST_BYTES",
	} {
		t.Setenv(key, "")
	}
	syncConfig := mustLoadConfig(t, fromProcessEnvironment(loadOpenSearchConfig))
	asyncConfig := mustLoadConfig(t, fromProcessEnvironment(loadAsyncOpenSearchConfig))

	tests := []struct {
		name                string
		bufferFlushBytes    int
		maxBulkRequestBytes int
	}{
		{
			name:                "sync",
			bufferFlushBytes:    syncConfig.BufferFlushBytes,
			maxBulkRequestBytes: syncConfig.MaxBulkRequestBytes,
		},
		{
			name:                "async",
			bufferFlushBytes:    asyncConfig.BufferFlushBytes,
			maxBulkRequestBytes: asyncConfig.MaxBulkRequestBytes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.bufferFlushBytes != logconfig.DefaultOpenSearchBufferFlushBytes {
				t.Fatalf(
					"BufferFlushBytes = %d, want %d",
					tt.bufferFlushBytes,
					logconfig.DefaultOpenSearchBufferFlushBytes,
				)
			}
			if tt.maxBulkRequestBytes != logconfig.DefaultOpenSearchMaxBulkRequestBytes {
				t.Fatalf(
					"MaxBulkRequestBytes = %d, want %d",
					tt.maxBulkRequestBytes,
					logconfig.DefaultOpenSearchMaxBulkRequestBytes,
				)
			}
		})
	}
}

func TestMapLoggerConfigsRejectsInvalidOpenSearchBulkByteLimits(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AppConfig)
		mapConfig func(AppConfig) error
		want      loggerSettingError
	}{
		{
			name: "sync non-positive buffer flush threshold",
			configure: func(cfg *AppConfig) {
				configureValidSyncOpenSearchMapping(cfg)
				cfg.Logger.OpenSearchConfig.BufferFlushBytes = 0
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
			want: loggerSettingError{
				source: "logger", backend: pkglogger.LogOutputOpenSearch, field: pkglogger.ConfigFieldBufferFlushBytes,
				kind: pkglogger.ErrNonPositiveSetting, value: 0,
			},
		},
		{
			name: "async non-positive bulk request limit",
			configure: func(cfg *AppConfig) {
				configureValidAsyncOpenSearchMapping(cfg)
				cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxBulkRequestBytes = -1
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
			want: loggerSettingError{
				source: "async logger", backend: pkglogger.LogOutputOpenSearch, field: pkglogger.ConfigFieldMaxBulkRequestBytes,
				kind: pkglogger.ErrNonPositiveSetting, value: -1,
			},
		},
		{
			name: "buffer threshold is not below bulk request limit",
			configure: func(cfg *AppConfig) {
				configureValidSyncOpenSearchMapping(cfg)
				cfg.Logger.OpenSearchConfig.BufferFlushBytes =
					cfg.Logger.OpenSearchConfig.MaxBulkRequestBytes
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
			want: loggerSettingError{
				source: "logger", backend: pkglogger.LogOutputOpenSearch, field: pkglogger.ConfigFieldBufferFlushBytes,
				kind: pkglogger.ErrInconsistentSetting,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mapperValidationTestConfig()
			tt.configure(&cfg)
			requireLoggerSettingError(t, tt.mapConfig(cfg), tt.want)
		})
	}
}

func configureValidSyncOpenSearchMapping(cfg *AppConfig) {
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.Index = "logs"
	cfg.OpenSearchClient.Endpoint = "http://localhost:9200"
	cfg.OpenSearchClient.Auth.Type = pkgopensearch.AuthTypeNone
}

func configureValidAsyncOpenSearchMapping(cfg *AppConfig) {
	cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
		pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch}
	cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.Index = "logs"
	cfg.AsyncOpenSearchClient.Endpoint = "http://localhost:9200"
	cfg.AsyncOpenSearchClient.Auth.Type = pkgopensearch.AuthTypeNone
}

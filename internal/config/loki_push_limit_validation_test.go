package config

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLoadLokiConfigsUsesDefaultPushLimits(t *testing.T) {
	for _, key := range []string{
		"HETEROLOG_MAX_LOKI_LINE_BYTES",
		"HETEROLOG_MAX_LOKI_PUSH_REQUEST_BYTES",
		"HETEROLOG_MAX_ASYNC_LOKI_LINE_BYTES",
		"HETEROLOG_MAX_ASYNC_LOKI_PUSH_REQUEST_BYTES",
		"HETEROLOG_MAX_DELIVERY_LOKI_LINE_BYTES",
		"HETEROLOG_MAX_DELIVERY_LOKI_PUSH_REQUEST_BYTES",
	} {
		t.Setenv(key, "")
	}
	syncConfig := mustLoadConfig(t, fromProcessEnvironment(loadLokiConfig))
	asyncConfig := mustLoadConfig(t, fromProcessEnvironment(loadAsyncLokiConfig))
	deliveryConfig := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryLokiConfig))

	for name, limits := range map[string]struct {
		line int
		push int
	}{
		"sync": {
			line: syncConfig.MaxLineBytes,
			push: syncConfig.MaxPushRequestBytes,
		},
		"async": {
			line: asyncConfig.MaxLineBytes,
			push: asyncConfig.MaxPushRequestBytes,
		},
		"delivery": {
			line: deliveryConfig.MaxLineBytes,
			push: deliveryConfig.MaxPushRequestBytes,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if limits.line != logconfig.DefaultLokiMaxLineBytes {
				t.Fatalf("MaxLineBytes = %d, want %d", limits.line, logconfig.DefaultLokiMaxLineBytes)
			}
			if limits.push != logconfig.DefaultLokiMaxPushRequestBytes {
				t.Fatalf("MaxPushRequestBytes = %d, want %d", limits.push, logconfig.DefaultLokiMaxPushRequestBytes)
			}
		})
	}
}

func TestMapLoggerConfigsRejectsInvalidLokiPushLimits(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AppConfig)
		mapConfig func(AppConfig) error
		want      loggerSettingError
	}{
		{
			name: "sync non-positive line limit",
			configure: func(cfg *AppConfig) {
				cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
				cfg.Logger.LokiConfig.MaxLineBytes = 0
				cfg.LokiClient.Endpoint = "http://localhost:3100"
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
			want: loggerSettingError{
				source: "logger", backend: pkglogger.LogOutputLoki, field: pkglogger.ConfigFieldMaxLineBytes,
				kind: pkglogger.ErrNonPositiveSetting, value: 0,
			},
		},
		{
			name: "async non-positive request limit",
			configure: func(cfg *AppConfig) {
				cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations =
					pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
				cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxPushRequestBytes = -1
				cfg.AsyncLokiClient.Endpoint = "http://localhost:3100"
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
			want: loggerSettingError{
				source: "async logger", backend: pkglogger.LogOutputLoki, field: pkglogger.ConfigFieldMaxPushRequestBytes,
				kind: pkglogger.ErrNonPositiveSetting, value: -1,
			},
		},
		{
			name: "delivery line limit equals request limit",
			configure: func(cfg *AppConfig) {
				cfg.DeliveryLogger.LogOutputDestinations = pkglogger.LogOutputDestinations{pkglogger.LogOutputLoki}
				cfg.DeliveryLogger.DeliveryLokiConfig.MaxLineBytes = 1024
				cfg.DeliveryLogger.DeliveryLokiConfig.MaxPushRequestBytes = 1024
				cfg.DeliveryLokiClient.Endpoint = "http://localhost:3100"
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
			want: loggerSettingError{
				source: "delivery logger", backend: pkglogger.LogOutputLoki, field: pkglogger.ConfigFieldMaxLineBytes,
				kind: pkglogger.ErrInconsistentSetting, value: 1024, limit: 1024,
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

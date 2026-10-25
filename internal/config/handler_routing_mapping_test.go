package config

import (
	"errors"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// TestMapLoggerConfigsRejectUnknownHandlerSelectorLoadedFromEnvは、外部入力から
// 読み込んだhandler selectorが各loggerのmapping境界で検証されることを確認します。
// selector自体の網羅的な契約はpkg/loggerで検証します。
func TestMapLoggerConfigsRejectUnknownHandlerSelectorLoadedFromEnv(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
		mapIt func(AppConfig) error
	}{
		{
			name: "sync",
			lines: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=stdout",
				"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER=unknown",
			},
			mapIt: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "async",
			lines: []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_ASYNC_LOG_TYPE=stdout",
				"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				"HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER=unknown",
			},
			mapIt: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "delivery",
			lines: []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
				"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				"HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER=unknown",
			},
			mapIt: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, tt.lines)
			err := tt.mapIt(cfg)
			var selectorErr *pkglogger.HandlerSelectorError
			if !errors.As(err, &selectorErr) || selectorErr.Selector != "unknown" ||
				!errors.Is(err, pkglogger.ErrHandlerSelectorNotConfigured) {
				t.Fatalf("mapping error = %v, want unconfigured handler selector %q", err, "unknown")
			}
		})
	}
}

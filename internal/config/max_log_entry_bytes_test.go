package config

import (
	"errors"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLoadConfigMapsMaxLogEntryBytesToAllLoggerContracts(t *testing.T) {
	tests := []struct {
		name          string
		configuredMax string
		want          int
	}{
		{
			name: "default",
			want: pkglogger.DefaultMaxLogEntryBytes,
		},
		{
			name:          "explicit",
			configuredMax: "2048",
			want:          2048,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 全LoggerContractKindへの伝搬を確認するため、3つともstdoutで有効にします。
			// 出力先のない契約はmapperがErrNoActiveLogOutputで拒否します。
			lines := []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_LOG_TYPE=stdout",
				"HETEROLOG_ASYNC_LOG_TYPE=stdout",
				"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
			}
			if tt.configuredMax != "" {
				lines = append(lines, "HETEROLOG_MAX_LOG_ENTRY_BYTES="+tt.configuredMax)
			}

			cfg := loadConfigFromTempEnv(t, lines)
			if cfg.MaxLogEntryBytes != tt.want {
				t.Fatalf("AppConfig.MaxLogEntryBytes = %d, want %d", cfg.MaxLogEntryBytes, tt.want)
			}

			syncConfig, err := MapLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapLoggerConfig() error = %v", err)
			}
			asyncConfig, err := MapAsyncLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapAsyncLoggerConfig() error = %v", err)
			}
			deliveryConfig, err := MapDeliveryLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapDeliveryLoggerConfig() error = %v", err)
			}

			for contract, got := range map[string]int{
				"sync":     syncConfig.MaxLogEntryBytes,
				"async":    asyncConfig.MaxLogEntryBytes,
				"delivery": deliveryConfig.MaxLogEntryBytes,
			} {
				if got != tt.want {
					t.Errorf("%s MaxLogEntryBytes = %d, want %d", contract, got, tt.want)
				}
			}
		})
	}
}

func TestMapActiveLoggerConfigRejectsNonPositiveMaxLogEntryBytes(t *testing.T) {
	tests := []struct {
		name         string
		env          []string
		mapConfig    func(AppConfig) error
		wantContract pkglogger.LoggerContractKind
		wantValue    int
	}{
		{
			name: "sync zero",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=stdout",
				"HETEROLOG_MAX_LOG_ENTRY_BYTES=0",
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
			wantContract: pkglogger.LoggerContractSync,
			wantValue:    0,
		},
		{
			name: "async negative",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_ASYNC_LOG_TYPE=stdout",
				"HETEROLOG_MAX_LOG_ENTRY_BYTES=-1",
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
			wantContract: pkglogger.LoggerContractAsyncBestEffort,
			wantValue:    -1,
		},
		{
			name: "delivery zero",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
				"HETEROLOG_MAX_LOG_ENTRY_BYTES=0",
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
			wantContract: pkglogger.LoggerContractDelivery,
			wantValue:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, tt.env)
			err := tt.mapConfig(cfg)
			var entryErr *MaxLogEntryBytesError
			if !errors.As(err, &entryErr) || entryErr.Contract != tt.wantContract ||
				entryErr.Value != tt.wantValue || !errors.Is(err, pkglogger.ErrInvalidMaxLogEntryBytes) {
				t.Fatalf("mapped logger config error = %v, want %s max log entry bytes %d rejected", err, tt.wantContract, tt.wantValue)
			}
		})
	}
}

func TestLoadConfigAllLoggerContractsDisabledKeepsDefaultMaxLogEntryBytes(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_MAX_LOG_ENTRY_BYTES=not-an-integer",
	})

	if cfg.MaxLogEntryBytes != pkglogger.DefaultMaxLogEntryBytes {
		t.Fatalf(
			"MaxLogEntryBytes = %d, want default %d when all logger contracts are disabled",
			cfg.MaxLogEntryBytes,
			pkglogger.DefaultMaxLogEntryBytes,
		)
	}
}

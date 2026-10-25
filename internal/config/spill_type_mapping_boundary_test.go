package config

import (
	"errors"
	"reflect"
	"testing"

	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestLoadThenMapRejectsInvalidDirectDegradeSpillTypes(t *testing.T) {
	tests := []struct {
		name       string
		env        []string
		loaded     func(AppConfig) []pkgdurable.SpillType
		wantLoaded []pkgdurable.SpillType
		mapError   func(AppConfig) error
		wantSource string
		wantKind   error
		wantType   pkgdurable.SpillType
	}{
		{
			name: "sync unknown",
			env: storagePathValidationEnv(t,
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=none",
				"HETEROLOG_ENABLE_OPENSEARCH_DEGRADE=true",
				"HETEROLOG_OPENSEARCH_DEGRADE_TYPES= Future-Store ",
			),
			loaded: func(cfg AppConfig) []pkgdurable.SpillType {
				return cfg.Logger.OpenSearchConfig.
					OpenSearchFlushManagerConfig.
					FailureHandlerConfig.
					SpillType
			},
			wantLoaded: []pkgdurable.SpillType{"future-store"},
			mapError: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
			wantSource: "logger opensearch",
			wantKind:   pkgdurable.ErrUnsupportedSpillType,
			wantType:   "future-store",
		},
		{
			name: "async duplicate",
			env: storagePathValidationEnv(t,
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=none",
				"HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DEGRADE=true",
				"HETEROLOG_ASYNC_OPENSEARCH_DEGRADE_TYPES=stdout,STDOUT",
			),
			loaded: func(cfg AppConfig) []pkgdurable.SpillType {
				return cfg.AsyncBestEffortMultiLoggerConfig.
					OpenSearchConfig.
					OpenSearchFlushManagerConfig.
					FailureHandlerConfig.
					SpillType
			},
			wantLoaded: []pkgdurable.SpillType{"stdout", "stdout"},
			mapError: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
			wantSource: "async logger opensearch",
			wantKind:   pkgdurable.ErrDuplicateSpillType,
			wantType:   "stdout",
		},
		{
			name: "delivery unknown",
			env: storagePathValidationEnv(t,
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES=none",
				"HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DEGRADE=true",
				"HETEROLOG_DELIVERY_OPENSEARCH_DEGRADE_TYPES= Future-Store ",
			),
			loaded: func(cfg AppConfig) []pkgdurable.SpillType {
				return cfg.DeliveryLogger.
					DeliveryOpenSearchConfig.
					DeliveryOpenSearchFlushManagerConfig.
					FailureHandlerConfig.
					SpillType
			},
			wantLoaded: []pkgdurable.SpillType{"future-store"},
			mapError: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
			wantSource: "delivery logger opensearch",
			wantKind:   pkgdurable.ErrUnsupportedSpillType,
			wantType:   "future-store",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, tt.env)
			if got := tt.loaded(cfg); !reflect.DeepEqual(got, tt.wantLoaded) {
				t.Fatalf("loaded spill types = %v, want %v", got, tt.wantLoaded)
			}

			err := tt.mapError(cfg)
			configErr := requireMappedConfigError(
				t,
				err,
				tt.wantSource,
				mappedvalidation.ConfigSectionDirectDegrade,
				tt.wantKind,
			)
			var spillErr *pkgdurable.SpillTypeError
			if !errors.As(configErr, &spillErr) || spillErr.Type != tt.wantType {
				t.Fatalf("LoadConfig() -> Map error = %v, want spill type %q rejected", configErr, tt.wantType)
			}
		})
	}
}

func TestLoadThenMapUsesDefaultDirectDegradeSpillTypeForBlankEnvironment(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
		"HETEROLOG_DELIVERY_OPENSEARCH_USERNAME=test-user",
		"HETEROLOG_DELIVERY_OPENSEARCH_PASSWORD=test-password",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES=none",
		"HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DEGRADE=true",
		"HETEROLOG_DELIVERY_OPENSEARCH_DEGRADE_TYPES=",
	))

	got := cfg.DeliveryLogger.
		DeliveryOpenSearchConfig.
		DeliveryOpenSearchFlushManagerConfig.
		FailureHandlerConfig.
		SpillType
	want := []pkgdurable.SpillType{pkgdurable.SpillType("stdout")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("blank environment spill types = %v, want default %v", got, want)
	}
	if _, err := MapDeliveryLoggerConfig(cfg); err != nil {
		t.Fatalf("MapDeliveryLoggerConfig() error = %v, want nil", err)
	}
}

package config

import (
	"errors"
	"reflect"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/failure"
	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	"github.com/go-jappe/heterolog/pkg/env"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestLoadFailureStoreDurableTypesUsesDefaultWhenEnvironmentIsMissingOrBlank(t *testing.T) {
	defaults := []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}
	if got := loadDurableTypes(
		env.GetEnvSliceString("MISSING_FAILURE_STORE_DURABLE_TYPES_TEST", nil),
		defaults,
	); !reflect.DeepEqual(got, defaults) {
		t.Fatalf("missing environment durable types = %v, want %v", got, defaults)
	}

	key := "EXPLICIT_FAILURE_STORE_DURABLE_TYPES_TEST"
	t.Setenv(key, "")
	if got := loadDurableTypes(env.GetEnvSliceString(key, nil), defaults); !reflect.DeepEqual(got, defaults) {
		t.Fatalf("explicit empty durable types = %v, want defaults %v", got, defaults)
	}

	t.Setenv(key, " \t ")
	if got := loadDurableTypes(env.GetEnvSliceString(key, nil), defaults); !reflect.DeepEqual(got, defaults) {
		t.Fatalf("whitespace durable types = %v, want defaults %v", got, defaults)
	}

	t.Setenv(key, " SPOOL,Unknown,spool ")
	want := []pkgdurable.DurableType{
		pkgdurable.DurableTypeSpool,
		pkgdurable.DurableType("unknown"),
		pkgdurable.DurableTypeSpool,
	}
	if got := loadDurableTypes(env.GetEnvSliceString(key, nil), defaults); !reflect.DeepEqual(got, want) {
		t.Fatalf("explicit durable types = %v, want %v", got, want)
	}
}

func TestLoadFailureStoreDurableTypesPreservesExplicitNone(t *testing.T) {
	key := "EXPLICIT_NONE_FAILURE_STORE_DURABLE_TYPES_TEST"
	t.Setenv(key, " NONE ")

	got := loadDurableTypes(env.GetEnvSliceString(key, nil), []pkgdurable.DurableType{pkgdurable.DurableTypeSpool})
	want := []pkgdurable.DurableType{pkgdurable.DurableTypeNone}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("explicit none durable types = %v, want %v", got, want)
	}
}

func TestDurableTypesSelectNoneRequiresExplicitStandaloneNone(t *testing.T) {
	tests := []struct {
		name   string
		types  []pkgdurable.DurableType
		wanted bool
	}{
		{name: "standalone none", types: []pkgdurable.DurableType{pkgdurable.DurableTypeNone}, wanted: true},
		{name: "empty", types: nil, wanted: false},
		{name: "spool", types: []pkgdurable.DurableType{pkgdurable.DurableTypeSpool}, wanted: false},
		{name: "mixed none", types: []pkgdurable.DurableType{pkgdurable.DurableTypeNone, pkgdurable.DurableTypeSpool}, wanted: false},
		{name: "unknown", types: []pkgdurable.DurableType{"unknown"}, wanted: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := durableTypesSelectNone(tt.types); got != tt.wanted {
				t.Fatalf("durableTypesSelectNone(%v) = %t, want %t", tt.types, got, tt.wanted)
			}
		})
	}
}

func TestConfiguredFailureStoreDurableTypesPreserveInvalidValuesUntilValidation(t *testing.T) {
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_TYPES", " SPOOL,Unknown,spool ")
	got := mustLoadConfig(t, fromProcessEnvironment(loadOpenSearchFailureHandlerConfig)).DurableType
	want := []pkgdurable.DurableType{
		pkgdurable.DurableTypeSpool,
		pkgdurable.DurableType("unknown"),
		pkgdurable.DurableTypeSpool,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OpenSearch DurableType = %v, want %v", got, want)
	}
}

func TestConfiguredDeliveryDurableTypesDoNotReplaceUnsupportedValueWithDefault(t *testing.T) {
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES", " spool ")
	got := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryOpenSearchFailureHandlerConfig)).DurableType
	if want := []pkgdurable.DurableType{failure.DurableTypeSpool}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Delivery OpenSearch DurableType = %v, want %v", got, want)
	}
}

func TestExplicitNoneFailureSelectionIsAcceptedWithoutDirectDegrade(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=opensearch",
		"HETEROLOG_OPENSEARCH_USERNAME=test-user",
		"HETEROLOG_OPENSEARCH_PASSWORD=test-password",
		"HETEROLOG_ENABLE_OPENSEARCH_DEGRADE=false",
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES=none",
		"HETEROLOG_OPENSEARCH_DEGRADE_TYPES=none",
	))

	failureCfg := cfg.Logger.
		OpenSearchConfig.
		OpenSearchFlushManagerConfig.
		FailureHandlerConfig
	if want := []pkgdurable.DurableType{pkgdurable.DurableTypeNone}; !reflect.DeepEqual(
		failureCfg.DurableType,
		want,
	) {
		t.Fatalf("loaded durable types = %v, want %v", failureCfg.DurableType, want)
	}
	if want := []pkgdurable.SpillType(nil); !reflect.DeepEqual(
		failureCfg.SpillType,
		want,
	) {
		t.Fatalf("loaded spill types = %v, want %v", failureCfg.SpillType, want)
	}
	if _, err := MapLoggerConfig(cfg); err != nil {
		t.Fatalf("MapLoggerConfig() error = %v, want nil", err)
	}
}

func TestExplicitNoneDirectDegradeSelectionRejectsNoneSpillWhenEnabled(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_LOG_TYPE=opensearch",
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES=none",
		"HETEROLOG_ENABLE_OPENSEARCH_DEGRADE=true",
		"HETEROLOG_OPENSEARCH_DEGRADE_TYPES=none",
	))

	_, err := MapLoggerConfig(cfg)
	configErr := requireMappedConfigError(
		t,
		err,
		"logger opensearch",
		mappedvalidation.ConfigSectionDirectDegrade,
		pkgdurable.ErrSpillTypeNoneWithDegrade,
	)
	var spillErr *pkgdurable.SpillTypeError
	if !errors.As(configErr, &spillErr) || spillErr.Type != pkgdurable.SpillTypeNone {
		t.Fatalf("direct degrade config error = %v, want spill type %q rejected", configErr, pkgdurable.SpillTypeNone)
	}
}

package mappedvalidation

import (
	"errors"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestValidateMappedFailureStoresValidatesSelectedDirectDegradeSpillTypes(t *testing.T) {
	tests := []struct {
		name     string
		types    []pkgdurable.SpillType
		wantKind error
		wantType pkgdurable.SpillType
	}{
		{
			name:     "unknown",
			types:    []pkgdurable.SpillType{"future-store"},
			wantKind: pkgdurable.ErrUnsupportedSpillType,
			wantType: "future-store",
		},
		{
			name:     "empty value",
			types:    []pkgdurable.SpillType{""},
			wantKind: pkgdurable.ErrUnsupportedSpillType,
			wantType: "",
		},
		{
			name:     "duplicate",
			types:    []pkgdurable.SpillType{"stdout", "stdout"},
			wantKind: pkgdurable.ErrDuplicateSpillType,
			wantType: "stdout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{
					pkglogger.LogOutputOpenSearch,
				},
				OpenSearch: &pkglogger.OpenSearchConfig{
					OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
						FailureStoreConfig: pkgdurable.FailureStoreConfig{
							EnableDegrade: true,
							DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
							SpillType:     tt.types,
						},
					},
				},
			}

			err := validateMappedFailureStores("logger", cfg)
			requireSpillTypeError(t, err, "logger opensearch", tt.wantType, tt.wantKind)
		})
	}
}

func TestValidateMappedDeliveryFailureStoresValidatesSelectedDirectDegradeSpillTypes(
	t *testing.T,
) {
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputOpenSearch,
		},
		OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
			DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
				FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
					EnableDegrade: true,
					DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
					SpillType:     []pkgdurable.SpillType{"stdout", "stdout"},
				},
			},
		},
	}

	err := validateMappedDeliveryFailureStores("delivery logger", cfg)
	requireSpillTypeError(t, err, "delivery logger opensearch", "stdout", pkgdurable.ErrDuplicateSpillType)
}

func TestValidateMappedFailureStoresIgnoresInactiveDirectDegradeSpillTypes(t *testing.T) {
	tests := []struct {
		name  string
		store pkgdurable.FailureStoreConfig
	}{
		{
			name: "no durable store and direct degrade disabled",
			store: pkgdurable.FailureStoreConfig{
				DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
				SpillType:   []pkgdurable.SpillType{"future-store"},
			},
		},
		{
			name: "durable store selected",
			store: pkgdurable.FailureStoreConfig{
				EnableDegrade: true,
				MaxConcurrent: 1,
				DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
				SpillType:     []pkgdurable.SpillType{"future-store"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{
					pkglogger.LogOutputOpenSearch,
				},
				OpenSearch: &pkglogger.OpenSearchConfig{
					OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
						FailureStoreConfig: tt.store,
					},
				},
			}

			if err := validateMappedFailureStores("logger", cfg); err != nil {
				t.Fatalf("validateMappedFailureStores() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateMappedDeliveryFailureStoresIgnoresInactiveDirectDegradeSpillTypes(
	t *testing.T,
) {
	tests := []struct {
		name  string
		store pkgdurable.DeliveryFailureStoreConfig
	}{
		{
			name: "no durable store and direct degrade disabled",
			store: pkgdurable.DeliveryFailureStoreConfig{
				DurableType: []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
				SpillType:   []pkgdurable.SpillType{"future-store"},
			},
		},
		{
			name: "durable store selected",
			store: pkgdurable.DeliveryFailureStoreConfig{
				EnableDegrade: true,
				DurableType: []pkgdurable.DurableType{
					pkgdurable.DurableTypeFileArchive,
				},
				SpillType: []pkgdurable.SpillType{"future-store"},
				FileArchiveConfig: pkgdurable.DeliveryFileArchiveConfig{
					LifecycleMode: pkgfsstore.FileLifecycleModeInline,
					UsageCacheTTL: time.Minute,
					ArchiveCheckpointConfig: pkgfsstore.ArchiveCheckpointConfig{
						Directory:    "/var/lib/heterolog/archive-checkpoints",
						SourcePrefix: "delivery_failure_store_validation",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := pkglogger.DeliveryLoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{
					pkglogger.LogOutputOpenSearch,
				},
				OpenSearch: &pkglogger.DeliveryOpenSearchConfig{
					DeliveryOpenSearchFlushManagerConfig: pkglogger.DeliveryOpenSearchFlushManagerConfig{
						FailureStoreConfig: tt.store,
					},
				},
			}

			if err := validateMappedDeliveryFailureStores("delivery logger", cfg); err != nil {
				t.Fatalf("validateMappedDeliveryFailureStores() error = %v, want nil", err)
			}
		})
	}
}

func TestValidateMappedFailureStoresRejectsNoneMixedWithDestinationWhenDegradeEnabled(
	t *testing.T,
) {
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputOpenSearch,
		},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				FailureStoreConfig: pkgdurable.FailureStoreConfig{
					EnableDegrade: true,
					MaxConcurrent: 1,
					DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
					SpillType: []pkgdurable.SpillType{
						pkgdurable.SpillTypeNone,
						pkgdurable.SpillType(pkgdurable.SpillDestinationStdout),
					},
				},
			},
		},
	}

	err := validateMappedFailureStores("logger", cfg)
	requireSpillTypeError(t, err, "logger opensearch", pkgdurable.SpillTypeNone, pkgdurable.ErrSpillTypeNoneNotAlone)
}

// requireSpillTypeErrorは、errがsourceの直接退避先の選択について、wantTypeをkindの理由で
// 拒否したerrorを含むことを確認します。
func requireSpillTypeError(t *testing.T, err error, source string, wantType pkgdurable.SpillType, kind error) {
	t.Helper()
	configErr := requireConfigError(t, err, source, ConfigSectionDirectDegrade, kind)
	var spillErr *pkgdurable.SpillTypeError
	if !errors.As(configErr, &spillErr) || spillErr.Type != wantType {
		t.Fatalf("%s direct degrade error = %v, want spill type %q rejected", source, configErr, wantType)
	}
}

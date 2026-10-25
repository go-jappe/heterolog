package mappedvalidation

import (
	"errors"
	"testing"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestValidateMappedFailureStoresUsesSharedDurableTypeContract(t *testing.T) {
	tests := []struct {
		name       string
		cfg        pkglogger.LoggerConfig
		wantSource string
		wantKind   error
		wantType   pkgdurable.DurableType
	}{
		{
			name: "selected store requires a type",
			cfg: pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{
					pkglogger.LogOutputOpenSearch,
				},
				OpenSearch: &pkglogger.OpenSearchConfig{
					OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
						FailureStoreConfig: pkgdurable.FailureStoreConfig{},
					},
				},
			},
			wantSource: "logger opensearch",
			wantKind:   pkgdurable.ErrDurableTypesRequired,
		},
		{
			name: "selected store rejects duplicate types",
			cfg: pkglogger.LoggerConfig{
				LogOutputDestinations: pkglogger.LogOutputDestinations{
					pkglogger.LogOutputLoki,
				},
				Loki: &pkglogger.LokiConfig{
					LokiFlushManagerConfig: pkglogger.LokiFlushManagerConfig{
						FailureStoreConfig: pkgdurable.FailureStoreConfig{
							DurableType: []pkgdurable.DurableType{
								pkgdurable.DurableTypeSpool,
								pkgdurable.DurableTypeSpool,
							},
						},
					},
				},
			},
			wantSource: "logger loki",
			wantKind:   pkgdurable.ErrDuplicateDurableType,
			wantType:   pkgdurable.DurableTypeSpool,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMappedFailureStores("logger", tt.cfg)
			requireDurableTypeError(t, err, tt.wantSource, "failure store", tt.wantType, tt.wantKind)
		})
	}
}

func TestValidateMappedFailureStoresRejectsNonPositiveActiveHandlerConcurrency(t *testing.T) {
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				FailureStoreConfig: pkgdurable.FailureStoreConfig{
					MaxConcurrent: 0,
					DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
				},
			},
		},
	}

	err := validateMappedFailureStores("logger", cfg)
	requireConfigError(
		t,
		err,
		"logger opensearch",
		ConfigSectionFailureStore,
		pkgdurable.ErrInvalidFailureHandlerMaxConcurrent,
	)
}

func TestValidateMappedFailureStoresRejectsNegativeSpoolActiveBacklogWarnAfterDays(t *testing.T) {
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				FailureStoreConfig: pkgdurable.FailureStoreConfig{
					MaxConcurrent: 1,
					DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeSpool},
					DurableStoreConfig: pkgdurable.DurableStoreConfig{
						SpoolConfig: pkgdurable.SpoolConfig{
							OverloadPolicyConfig: pkgdurable.SpoolOverloadPolicyConfig{
								ActiveBacklogWarnAfterDays: -1,
							},
						},
					},
				},
			},
		},
	}

	err := validateMappedFailureStores("logger", cfg)
	requireConfigError(
		t,
		err,
		"logger opensearch",
		ConfigSectionFailureSpool,
		pkgdurable.ErrNegativeSpoolActiveBacklogWarnAfterDays,
	)
}

func TestValidateMappedDeliveryFailureStoresRejectsSpool(t *testing.T) {
	cfg := pkglogger.DeliveryLoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputClickHouse,
		},
		ClickHouse: &pkglogger.DeliveryClickHouseConfig{
			DeliveryClickHouseFlushManagerConfig: pkglogger.DeliveryClickHouseFlushManagerConfig{
				FailureStoreConfig: pkgdurable.DeliveryFailureStoreConfig{
					DurableType: []pkgdurable.DurableType{
						pkgdurable.DurableTypeSpool,
					},
				},
			},
		},
	}

	err := validateMappedDeliveryFailureStores("delivery logger", cfg)
	requireDurableTypeError(
		t,
		err,
		"delivery logger clickhouse",
		"delivery failure store",
		pkgdurable.DurableTypeSpool,
		pkgdurable.ErrUnsupportedDurableType,
	)
}

func TestValidateMappedFailureStoresIgnoresUnconstructedOrUnselectedStore(t *testing.T) {
	invalidStore := pkgdurable.FailureStoreConfig{
		DurableType: []pkgdurable.DurableType{"unknown"},
	}

	handlerOnly := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputStdout,
		},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				FailureStoreConfig: invalidStore,
			},
		},
	}
	if err := validateMappedFailureStores("logger", handlerOnly); err != nil {
		t.Fatalf("handler-only failure store validation error = %v, want nil", err)
	}

	unselected := handlerOnly
	unselected.LogOutputDestinations = pkglogger.LogOutputDestinations{
		pkglogger.LogOutputOpenSearch,
	}
	unselected.OpenSearch.FailureStoreConfig.DurableType = []pkgdurable.DurableType{pkgdurable.DurableTypeNone}
	if err := validateMappedFailureStores("logger", unselected); err != nil {
		t.Fatalf("unselected failure store validation error = %v, want nil", err)
	}
}

func TestValidateMappedFailureStoresRejectsNoneMixedWithRealType(t *testing.T) {
	cfg := pkglogger.LoggerConfig{
		LogOutputDestinations: pkglogger.LogOutputDestinations{
			pkglogger.LogOutputOpenSearch,
		},
		OpenSearch: &pkglogger.OpenSearchConfig{
			OpenSearchFlushManagerConfig: pkglogger.OpenSearchFlushManagerConfig{
				FailureStoreConfig: pkgdurable.FailureStoreConfig{
					DurableType: []pkgdurable.DurableType{
						pkgdurable.DurableTypeNone,
						pkgdurable.DurableTypeSpool,
					},
				},
			},
		},
	}

	err := validateMappedFailureStores("logger", cfg)
	requireDurableTypeError(
		t,
		err,
		"logger opensearch",
		"failure store",
		pkgdurable.DurableTypeNone,
		pkgdurable.ErrDurableTypeNoneNotAlone,
	)
}

// requireDurableTypeErrorは、errがsourceの送信失敗レコード保存設定について、scopeの保存方式の
// 選択をkindの理由で拒否したerrorを含むことを確認します。wantTypeは拒否した保存方式です。
func requireDurableTypeError(
	t *testing.T,
	err error,
	source string,
	scope string,
	wantType pkgdurable.DurableType,
	kind error,
) {
	t.Helper()
	configErr := requireConfigError(t, err, source, ConfigSectionFailureStore, kind)
	var typeErr *pkgdurable.DurableTypeError
	if !errors.As(configErr, &typeErr) || typeErr.Scope != scope || typeErr.Type != wantType {
		t.Fatalf("%s failure store error = %v, want %s durable type %q rejected", source, configErr, scope, wantType)
	}
}

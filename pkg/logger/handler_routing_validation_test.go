package logger

import (
	"errors"
	"testing"

	"github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	loggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestValidateLoggerHandlerRoutingRejectsUnresolvableSelectors(t *testing.T) {
	tests := []struct {
		name         string
		cfg          LoggerConfig
		wantSelector string
	}{
		{
			name: "unknown selector",
			cfg: LoggerConfig{
				Stdout: &StdoutLoggerConfig{
					HandlerConfig: loggerhandler.HandlerConfig{
						OnErrorConfig: []string{"unknown"},
					},
				},
			},
			wantSelector: "unknown",
		},
		{
			name: "qualified selector",
			cfg: LoggerConfig{
				Stdout: &StdoutLoggerConfig{
					HandlerConfig: loggerhandler.HandlerConfig{
						OnErrorConfig: []string{"file:maintenance"},
					},
				},
				File: &FileLoggerConfig{},
				InternalLogDestinations: &InternalLogDestinationConfig{
					File: &FileLoggerConfig{},
				},
			},
			wantSelector: "file:maintenance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLoggerHandlerRouting("logger", tt.cfg)
			if !hasHandlerSelectorError(err, "error", tt.wantSelector) {
				t.Fatalf("ValidateLoggerHandlerRouting() error = %v, want unresolved selector %q", err, tt.wantSelector)
			}
		})
	}
}

func TestHandlerRoutingResolvesRemoteSelectorStructurally(t *testing.T) {
	t.Run("sync and async", func(t *testing.T) {
		cfg := LoggerConfig{
			Stdout: &StdoutLoggerConfig{
				HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{"opensearch"},
				},
			},
			OpenSearch: &OpenSearchConfig{},
		}

		if err := ValidateLoggerHandlerRouting("logger", cfg); err != nil {
			t.Fatalf("ValidateLoggerHandlerRouting() error = %v, want structural match", err)
		}
	})

	t.Run("delivery", func(t *testing.T) {
		cfg := DeliveryLoggerConfig{
			Stdout: &DeliveryStdoutLoggerConfig{
				HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{"opensearch"},
				},
			},
			OpenSearch: &DeliveryOpenSearchConfig{},
		}

		if err := ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg); err != nil {
			t.Fatalf(
				"ValidateDeliveryLoggerHandlerRouting() error = %v, want structural match",
				err,
			)
		}
	})
}

func TestValidateLoggerHandlerRoutingNormalizesBareSelector(t *testing.T) {
	t.Run("sync internal-log target", func(t *testing.T) {
		cfg := LoggerConfig{
			Stdout: &StdoutLoggerConfig{
				HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{" FILE "},
				},
			},
			InternalLogDestinations: &InternalLogDestinationConfig{
				File: &FileLoggerConfig{},
			},
		}

		if err := ValidateLoggerHandlerRouting("logger", cfg); err != nil {
			t.Fatalf("ValidateLoggerHandlerRouting() error = %v, want nil", err)
		}
	})

	t.Run("delivery internal-log target", func(t *testing.T) {
		cfg := DeliveryLoggerConfig{
			Stdout: &DeliveryStdoutLoggerConfig{
				HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{" FILE "},
				},
			},
			InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
				File: &DeliveryFileLoggerConfig{},
			},
		}

		if err := ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg); err != nil {
			t.Fatalf("ValidateDeliveryLoggerHandlerRouting() error = %v, want nil", err)
		}
	})
}

func TestValidateLoggerHandlerRoutingIgnoresInactiveSourceButAcceptsHandlerOnlyTarget(
	t *testing.T,
) {
	cfg := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
		Stdout: &StdoutLoggerConfig{
			HandlerConfig: loggerhandler.HandlerConfig{
				OnErrorConfig: []string{"opensearch"},
			},
		},
		Slack: &SlackLoggerConfig{
			HandlerConfig: loggerhandler.HandlerConfig{
				OnErrorConfig: []string{"unknown"},
			},
		},
		OpenSearch: &OpenSearchConfig{
			OpenSearchFlushManagerConfig: OpenSearchFlushManagerConfig{Index: "logs"},
		},
		OpenSearchClient: validHandlerRoutingOpenSearchClient(),
	}

	if err := ValidateLoggerHandlerRouting("logger", cfg); err != nil {
		t.Fatalf("ValidateLoggerHandlerRouting() error = %v, want nil", err)
	}

	t.Run("handler-only target routing is not active", func(t *testing.T) {
		cfg := LoggerConfig{
			LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
			Stdout: &StdoutLoggerConfig{HandlerConfig: loggerhandler.HandlerConfig{
				OnErrorConfig: []string{LogOutputFile.String()},
			}},
			File: &FileLoggerConfig{HandlerConfig: loggerhandler.HandlerConfig{
				OnErrorConfig: []string{"unknown"},
			}},
		}

		if err := ValidateLoggerHandlerRouting("logger", cfg); err != nil {
			t.Fatalf(
				"handler-only target routing error = %v, target routing must not be evaluated",
				err,
			)
		}
	})
}

func validHandlerRoutingOpenSearchClient() *opensearch.Config {
	return &opensearch.Config{
		Endpoint: "http://localhost:9200",
		AuthConfig: opensearch.AuthConfig{
			Type: opensearch.AuthTypeNone,
		},
	}
}

func TestValidateHandlerRoutingTreatsEmptyDestinationsAsActive(t *testing.T) {
	t.Run("sync", func(t *testing.T) {
		cfg := LoggerConfig{
			Stdout: &StdoutLoggerConfig{
				HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{"unknown"},
				},
			},
		}

		err := ValidateLoggerHandlerRouting("logger", cfg)
		requireUnknownHandlerSelectorError(t, err)
	})

	t.Run("delivery", func(t *testing.T) {
		cfg := DeliveryLoggerConfig{
			Stdout: &DeliveryStdoutLoggerConfig{
				HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{"unknown"},
				},
			},
		}

		err := ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg)
		requireUnknownHandlerSelectorError(t, err)
	})
}

func TestHandlerRoutingIgnoresTerminalInternalLogFileLifecycleSource(t *testing.T) {
	t.Run("sync and async", func(t *testing.T) {
		cfg := LoggerConfig{
			LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
			Stdout:                &StdoutLoggerConfig{},
			InternalLogDestinations: &InternalLogDestinationConfig{
				File: &FileLoggerConfig{
					LifecycleMode: fsstore.FileLifecycleModeRunner,
					LifecycleHandlerConfig: loggerhandler.HandlerConfig{
						OnErrorConfig: []string{LogOutputOpenSearch.String()},
					},
				},
			},
		}

		if err := ValidateLoggerHandlerRouting("logger", cfg); err != nil {
			t.Fatalf(
				"ValidateLoggerHandlerRouting() error = %v, terminal internal file routing must be ignored",
				err,
			)
		}
		if LoggerConfigRequiresBaseBackend(cfg, LogOutputOpenSearch) {
			t.Fatal("terminal internal-log lifecycle unexpectedly activated OpenSearch")
		}
	})

	t.Run("delivery", func(t *testing.T) {
		cfg := DeliveryLoggerConfig{
			LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
			Stdout:                &DeliveryStdoutLoggerConfig{},
			InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
				File: &DeliveryFileLoggerConfig{
					LifecycleMode: fsstore.FileLifecycleModeRunner,
					LifecycleRunnerSettings: DeliveryFileLifecycleRunnerSettings{
						LifecycleHandlerConfig: loggerhandler.HandlerConfig{
							OnErrorConfig: []string{LogOutputOpenSearch.String()},
						},
					},
				},
			},
		}

		if err := ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg); err != nil {
			t.Fatalf(
				"ValidateDeliveryLoggerHandlerRouting() error = %v, terminal internal file routing must be ignored",
				err,
			)
		}
		if DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputOpenSearch) {
			t.Fatal("delivery terminal internal-log lifecycle unexpectedly activated OpenSearch")
		}
	})
}

func TestValidateLoggerHandlerRoutingAcceptsAppendLogMaintenanceRunnerWarnRouting(t *testing.T) {
	cfg := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputFile},
		File: &FileLoggerConfig{
			LifecycleMode: fsstore.FileLifecycleModeRunner,
			LifecycleHandlerConfig: loggerhandler.HandlerConfig{
				OnWarnConfig: []string{LogOutputStdout.String()},
			},
		},
		Stdout: &StdoutLoggerConfig{},
	}

	if err := ValidateLoggerHandlerRouting("logger", cfg); err != nil {
		t.Fatalf("ValidateLoggerHandlerRouting() error = %v, want nil", err)
	}
}

func TestValidateDeliveryLoggerHandlerRoutingAcceptsAppendLogMaintenanceRunnerWarnRouting(t *testing.T) {
	cfg := DeliveryLoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputFile},
		File: &DeliveryFileLoggerConfig{
			LifecycleMode: fsstore.FileLifecycleModeRunner,
			LifecycleRunnerSettings: DeliveryFileLifecycleRunnerSettings{
				LifecycleHandlerConfig: loggerhandler.HandlerConfig{
					OnWarnConfig: []string{LogOutputStdout.String()},
				},
			},
		},
		Stdout: &DeliveryStdoutLoggerConfig{},
	}

	if err := ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg); err != nil {
		t.Fatalf("ValidateDeliveryLoggerHandlerRouting() error = %v, want nil", err)
	}
}

func TestDedicatedInternalLogFileExcludesUnusedBaseFileLifecycleSource(t *testing.T) {
	t.Run("sync and async", func(t *testing.T) {
		cfg := LoggerConfig{
			LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
			Stdout: &StdoutLoggerConfig{
				HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{LogOutputFile.String()},
				},
			},
			File: &FileLoggerConfig{
				LifecycleMode: fsstore.FileLifecycleModeRunner,
				LifecycleHandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{LogOutputOpenSearch.String()},
				},
			},
			InternalLogDestinations: &InternalLogDestinationConfig{
				File: &FileLoggerConfig{},
			},
		}

		if err := ValidateLoggerHandlerRouting("logger", cfg); err != nil {
			t.Fatalf(
				"ValidateLoggerHandlerRouting() error = %v, unused base lifecycle must be ignored",
				err,
			)
		}
		if LoggerConfigRequiresBaseBackend(cfg, LogOutputOpenSearch) {
			t.Fatal("unused base lifecycle unexpectedly activated OpenSearch")
		}
	})

	t.Run("delivery", func(t *testing.T) {
		cfg := DeliveryLoggerConfig{
			LogOutputDestinations: LogOutputDestinations{LogOutputStdout},
			Stdout: &DeliveryStdoutLoggerConfig{
				HandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{LogOutputFile.String()},
				},
			},
			File: &DeliveryFileLoggerConfig{
				LifecycleMode: fsstore.FileLifecycleModeRunner,
				LifecycleRunnerSettings: DeliveryFileLifecycleRunnerSettings{
					LifecycleHandlerConfig: loggerhandler.HandlerConfig{
						OnErrorConfig: []string{LogOutputOpenSearch.String()},
					},
				},
			},
			InternalLogDestinations: &DeliveryInternalLogDestinationConfig{
				File: &DeliveryFileLoggerConfig{},
			},
		}

		if err := ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg); err != nil {
			t.Fatalf(
				"ValidateDeliveryLoggerHandlerRouting() error = %v, unused base lifecycle must be ignored",
				err,
			)
		}
		if DeliveryLoggerConfigRequiresBaseBackend(cfg, LogOutputOpenSearch) {
			t.Fatal("unused delivery base lifecycle unexpectedly activated OpenSearch")
		}
	})
}

func TestValidateLoggerHandlerRoutingUsesOnlyActiveFailureStoreRouting(t *testing.T) {
	cfg := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
		OpenSearch: &OpenSearchConfig{
			OpenSearchFlushManagerConfig: OpenSearchFlushManagerConfig{
				FailureStoreConfig: durable.FailureStoreConfig{
					DurableType: []durable.DurableType{durable.DurableTypeSpool},
					DurableStoreConfig: durable.DurableStoreConfig{
						FileArchiveConfig: durable.FileArchiveConfig{
							HandlerConfig: loggerhandler.HandlerConfig{
								OnErrorConfig: []string{"ignored"},
							},
						},
						SpoolConfig: durable.SpoolConfig{
							HandlerConfig: loggerhandler.HandlerConfig{
								OnErrorConfig: []string{"unknown"},
							},
						},
					},
				},
			},
		},
	}

	err := ValidateLoggerHandlerRouting("logger", cfg)
	var sources []string
	for _, selectorErr := range errorsOfType[*HandlerSelectorError](err) {
		if selectorErr.Selector == "ignored" {
			t.Fatalf("ValidateLoggerHandlerRouting() error = %v, unselected archive must be ignored", err)
		}
		sources = append(sources, selectorErr.Source)
	}
	if len(sources) != 1 || sources[0] != "logger opensearch failure spool" {
		t.Fatalf("ValidateLoggerHandlerRouting() error = %v, want only the active spool selector error", err)
	}
}

func TestValidateLoggerHandlerRoutingIgnoresFailureStoreRoutingWhenNoStoreIsSelected(
	t *testing.T,
) {
	cfg := LoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
		OpenSearch: &OpenSearchConfig{
			OpenSearchFlushManagerConfig: OpenSearchFlushManagerConfig{
				FailureStoreConfig: durable.FailureStoreConfig{
					DurableType: []durable.DurableType{durable.DurableTypeNone},
					DurableStoreConfig: durable.DurableStoreConfig{
						FileArchiveConfig: durable.FileArchiveConfig{
							HandlerConfig: loggerhandler.HandlerConfig{
								OnErrorConfig: []string{"unknown-archive"},
							},
						},
						SpoolConfig: durable.SpoolConfig{
							HandlerConfig: loggerhandler.HandlerConfig{
								OnErrorConfig: []string{"unknown-spool"},
							},
						},
					},
				},
			},
		},
	}

	if err := ValidateLoggerHandlerRouting("logger", cfg); err != nil {
		t.Fatalf(
			"unselected failure store routing error = %v, archive and spool must be ignored",
			err,
		)
	}
}

func TestValidateDeliveryLoggerHandlerRoutingIncludesLifecycleAndArchiveSources(t *testing.T) {
	cfg := DeliveryLoggerConfig{
		LogOutputDestinations: LogOutputDestinations{
			LogOutputFile,
			LogOutputOpenSearch,
		},
		File: &DeliveryFileLoggerConfig{
			LifecycleMode: fsstore.FileLifecycleModeRunner,
			LifecycleRunnerSettings: DeliveryFileLifecycleRunnerSettings{
				LifecycleHandlerConfig: loggerhandler.HandlerConfig{
					OnErrorConfig: []string{"lifecycle-unknown"},
				},
			},
		},
		OpenSearch: &DeliveryOpenSearchConfig{
			DeliveryOpenSearchFlushManagerConfig: DeliveryOpenSearchFlushManagerConfig{
				FailureStoreConfig: durable.DeliveryFailureStoreConfig{
					DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
					FileArchiveConfig: durable.DeliveryFileArchiveConfig{
						HandlerConfig: loggerhandler.HandlerConfig{
							OnErrorConfig: []string{"archive-unknown"},
						},
					},
				},
			},
		},
	}

	err := ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg)
	if err == nil {
		t.Fatal("ValidateDeliveryLoggerHandlerRouting() error = nil, want source errors")
	}
	for _, selector := range []string{"archive-unknown", "lifecycle-unknown"} {
		if !hasHandlerSelectorError(err, "error", selector) {
			t.Fatalf("ValidateDeliveryLoggerHandlerRouting() error = %v, want unresolved selector %q", err, selector)
		}
	}
}

// TestHandlerRoutingRejectsUnknownSelectorForEverySource guards the source
// enumerations separately from the selector validation itself. Adding a new output
// destination without wiring either logger contract must fail this table.
func TestHandlerRoutingRejectsUnknownSelectorForEverySource(t *testing.T) {
	for _, source := range AllLogOutputDestinations() {
		t.Run("sync/"+source.String(), func(t *testing.T) {
			cfg := handlerRoutingTestLoggerConfig(source)
			handlerRoutingTestSetLoggerRouting(
				&cfg,
				source,
				loggerhandler.HandlerConfig{OnErrorConfig: []string{"unknown"}},
			)

			requireUnknownHandlerSelectorError(
				t,
				ValidateLoggerHandlerRouting("logger", cfg),
			)
		})

		t.Run("delivery/"+source.String(), func(t *testing.T) {
			cfg := handlerRoutingTestDeliveryConfig(source)
			handlerRoutingTestSetDeliveryRouting(
				&cfg,
				source,
				loggerhandler.HandlerConfig{OnErrorConfig: []string{"unknown"}},
			)

			requireUnknownHandlerSelectorError(
				t,
				ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg),
			)
		})
	}
}

func TestValidateHandlerRoutingRejectsUnknownSelectorForEveryLevel(t *testing.T) {
	err := ValidateLoggerHandlerRouting("logger", LoggerConfig{
		Stdout: &StdoutLoggerConfig{HandlerConfig: loggerhandler.HandlerConfig{
			OnErrorConfig: []string{"unknown-error"},
			OnWarnConfig:  []string{"unknown-warn"},
			OnInfoConfig:  []string{"unknown-info"},
			OnDebugConfig: []string{"unknown-debug"},
		}},
	})
	if err == nil {
		t.Fatal("ValidateLoggerHandlerRouting() error = nil, want every level to fail")
	}
	for _, level := range []string{"error", "warn", "info", "debug"} {
		if !hasHandlerSelectorError(err, level, "unknown-"+level) {
			t.Fatalf("ValidateLoggerHandlerRouting() error = %v, want %s selector %q", err, level, "unknown-"+level)
		}
	}
}

func TestValidateLoggerHandlerRoutingChecksEverySelectedFailureStore(t *testing.T) {
	backends := []LogOutputDestination{
		LogOutputOpenSearch,
		LogOutputLoki,
		LogOutputCloudWatchLogs,
		LogOutputClickHouse,
	}
	stores := []struct {
		name       string
		durable    durable.DurableType
		setRouting func(*durable.FailureStoreConfig, loggerhandler.HandlerConfig)
	}{
		{
			name:    "file archive",
			durable: durable.DurableTypeFileArchive,
			setRouting: func(cfg *durable.FailureStoreConfig, routing loggerhandler.HandlerConfig) {
				cfg.DurableStoreConfig.FileArchiveConfig.HandlerConfig = routing
			},
		},
		{
			name:    "spool",
			durable: durable.DurableTypeSpool,
			setRouting: func(cfg *durable.FailureStoreConfig, routing loggerhandler.HandlerConfig) {
				cfg.DurableStoreConfig.SpoolConfig.HandlerConfig = routing
			},
		},
	}

	for _, backend := range backends {
		for _, store := range stores {
			t.Run(backend.String()+"/"+store.name, func(t *testing.T) {
				cfg := handlerRoutingTestLoggerConfig(backend)
				failureStore := durable.FailureStoreConfig{
					DurableType: []durable.DurableType{store.durable},
				}
				store.setRouting(
					&failureStore,
					loggerhandler.HandlerConfig{OnErrorConfig: []string{"unknown"}},
				)
				handlerRoutingTestSetFailureStore(&cfg, backend, failureStore)

				requireUnknownHandlerSelectorError(
					t,
					ValidateLoggerHandlerRouting("logger", cfg),
				)
			})
		}
	}
}

func TestValidateDeliveryLoggerHandlerRoutingChecksEverySelectedFailureArchive(t *testing.T) {
	backends := []LogOutputDestination{
		LogOutputOpenSearch,
		LogOutputLoki,
		LogOutputCloudWatchLogs,
		LogOutputClickHouse,
	}

	for _, backend := range backends {
		t.Run(backend.String(), func(t *testing.T) {
			cfg := handlerRoutingTestDeliveryConfig(backend)
			handlerRoutingTestSetDeliveryFailureStore(
				&cfg,
				backend,
				durable.DeliveryFailureStoreConfig{
					DurableType: []durable.DurableType{durable.DurableTypeFileArchive},
					FileArchiveConfig: durable.DeliveryFileArchiveConfig{
						HandlerConfig: loggerhandler.HandlerConfig{
							OnErrorConfig: []string{"unknown"},
						},
					},
				},
			)

			requireUnknownHandlerSelectorError(
				t,
				ValidateDeliveryLoggerHandlerRouting("delivery logger", cfg),
			)
		})
	}
}

func TestValidateLoggerHandlerRoutingChecksAsyncFailureStoreAfterConversion(t *testing.T) {
	cfg := AsyncBestEffortMultiLoggerConfig{
		LogOutputDestinations: LogOutputDestinations{LogOutputOpenSearch},
		OpenSearch: &OpenSearchConfig{
			OpenSearchFlushManagerConfig: OpenSearchFlushManagerConfig{
				FailureStoreConfig: durable.FailureStoreConfig{
					DurableType: []durable.DurableType{durable.DurableTypeSpool},
					DurableStoreConfig: durable.DurableStoreConfig{
						SpoolConfig: durable.SpoolConfig{HandlerConfig: loggerhandler.HandlerConfig{
							OnErrorConfig: []string{"unknown"},
						}},
					},
				},
			},
		},
	}

	requireUnknownHandlerSelectorError(
		t,
		ValidateLoggerHandlerRouting("async logger", cfg.ToLoggerConfig()),
	)
}

func handlerRoutingTestLoggerConfig(source LogOutputDestination) LoggerConfig {
	cfg := LoggerConfig{LogOutputDestinations: LogOutputDestinations{source}}
	switch source {
	case LogOutputFile:
		cfg.File = &FileLoggerConfig{}
	case LogOutputStdout:
		cfg.Stdout = &StdoutLoggerConfig{}
	case LogOutputSlack:
		cfg.Slack = &SlackLoggerConfig{}
	case LogOutputOpenSearch:
		cfg.OpenSearch = &OpenSearchConfig{}
	case LogOutputLoki:
		cfg.Loki = &LokiConfig{}
	case LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs = &CloudWatchLogsConfig{}
	case LogOutputClickHouse:
		cfg.ClickHouse = &ClickHouseConfig{}
	}
	return cfg
}

func handlerRoutingTestDeliveryConfig(source LogOutputDestination) DeliveryLoggerConfig {
	cfg := DeliveryLoggerConfig{LogOutputDestinations: LogOutputDestinations{source}}
	switch source {
	case LogOutputFile:
		cfg.File = &DeliveryFileLoggerConfig{}
	case LogOutputStdout:
		cfg.Stdout = &DeliveryStdoutLoggerConfig{}
	case LogOutputSlack:
		cfg.Slack = &DeliverySlackLoggerConfig{}
	case LogOutputOpenSearch:
		cfg.OpenSearch = &DeliveryOpenSearchConfig{}
	case LogOutputLoki:
		cfg.Loki = &DeliveryLokiConfig{}
	case LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs = &DeliveryCloudWatchLogsConfig{}
	case LogOutputClickHouse:
		cfg.ClickHouse = &DeliveryClickHouseConfig{}
	}
	return cfg
}

func handlerRoutingTestSetLoggerRouting(
	cfg *LoggerConfig,
	source LogOutputDestination,
	routing loggerhandler.HandlerConfig,
) {
	switch source {
	case LogOutputFile:
		cfg.File.HandlerConfig = routing
	case LogOutputStdout:
		cfg.Stdout.HandlerConfig = routing
	case LogOutputSlack:
		cfg.Slack.HandlerConfig = routing
	case LogOutputOpenSearch:
		cfg.OpenSearch.HandlerConfig = routing
	case LogOutputLoki:
		cfg.Loki.HandlerConfig = routing
	case LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs.HandlerConfig = routing
	case LogOutputClickHouse:
		cfg.ClickHouse.HandlerConfig = routing
	}
}

func handlerRoutingTestSetDeliveryRouting(
	cfg *DeliveryLoggerConfig,
	source LogOutputDestination,
	routing loggerhandler.HandlerConfig,
) {
	switch source {
	case LogOutputFile:
		cfg.File.HandlerConfig = routing
	case LogOutputStdout:
		cfg.Stdout.HandlerConfig = routing
	case LogOutputSlack:
		cfg.Slack.HandlerConfig = routing
	case LogOutputOpenSearch:
		cfg.OpenSearch.HandlerConfig = routing
	case LogOutputLoki:
		cfg.Loki.HandlerConfig = routing
	case LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs.HandlerConfig = routing
	case LogOutputClickHouse:
		cfg.ClickHouse.HandlerConfig = routing
	}
}

func handlerRoutingTestSetFailureStore(
	cfg *LoggerConfig,
	backend LogOutputDestination,
	failureStore durable.FailureStoreConfig,
) {
	switch backend {
	case LogOutputOpenSearch:
		cfg.OpenSearch.FailureStoreConfig = failureStore
	case LogOutputLoki:
		cfg.Loki.FailureStoreConfig = failureStore
	case LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs.FailureStoreConfig = failureStore
	case LogOutputClickHouse:
		cfg.ClickHouse.FailureStoreConfig = failureStore
	}
}

func handlerRoutingTestSetDeliveryFailureStore(
	cfg *DeliveryLoggerConfig,
	backend LogOutputDestination,
	failureStore durable.DeliveryFailureStoreConfig,
) {
	switch backend {
	case LogOutputOpenSearch:
		cfg.OpenSearch.FailureStoreConfig = failureStore
	case LogOutputLoki:
		cfg.Loki.FailureStoreConfig = failureStore
	case LogOutputCloudWatchLogs:
		cfg.CloudWatchLogs.FailureStoreConfig = failureStore
	case LogOutputClickHouse:
		cfg.ClickHouse.FailureStoreConfig = failureStore
	}
}

func requireUnknownHandlerSelectorError(t *testing.T, err error) {
	t.Helper()
	if !hasHandlerSelectorError(err, "error", "unknown") {
		t.Fatalf("handler routing error = %v, want unknown selector error", err)
	}
}

// hasHandlerSelectorErrorは、errに、levelのselectorを解決できないことを示す
// HandlerSelectorErrorが含まれるかを返します。errors.Joinでまとめた複数の不正からも探します。
func hasHandlerSelectorError(err error, level, selector string) bool {
	if !errors.Is(err, ErrHandlerSelectorNotConfigured) {
		return false
	}
	for _, selectorErr := range errorsOfType[*HandlerSelectorError](err) {
		if selectorErr.Level == level && selectorErr.Selector == selector {
			return true
		}
	}
	return false
}

// errorsOfTypeは、errのerror treeに含まれるT型のerrorを出現順に返します。
func errorsOfType[T error](err error) []T {
	var found []T
	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}
		if typed, ok := err.(T); ok {
			found = append(found, typed)
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			for _, child := range wrapped.Unwrap() {
				visit(child)
			}
		case interface{ Unwrap() error }:
			visit(wrapped.Unwrap())
		}
	}
	visit(err)
	return found
}

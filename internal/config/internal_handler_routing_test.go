package config

import (
	"errors"
	"slices"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/logger/handler"
)

// TestLoadConfigKeepsInternalHandlerRoutingIndependentForEveryLoggerSource は、
// sync / async / deliveryの全sourceについて、source固有のenv keyからroutingを読み、
// 通常出力先を増やさずにhandler target用configだけをmapperへ残すことを検証します。
func TestLoadConfigKeepsInternalHandlerRoutingIndependentForEveryLoggerSource(t *testing.T) {
	sourceCases := []struct {
		source pkglogger.LogOutputDestination
		target pkglogger.LogOutputDestination
		keys   [3]string
	}{
		{
			source: pkglogger.LogOutputFile,
			target: pkglogger.LogOutputStdout,
			keys: [3]string{
				"HETEROLOG_FILE_LOGGER_ERROR_HANDLER",
				"HETEROLOG_ASYNC_FILE_LOGGER_ERROR_HANDLER",
				"HETEROLOG_DELIVERY_FILE_LOGGER_ERROR_HANDLER",
			},
		},
		{
			source: pkglogger.LogOutputStdout,
			target: pkglogger.LogOutputSlack,
			keys: [3]string{
				"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER",
				"HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER",
				"HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER",
			},
		},
		{
			source: pkglogger.LogOutputSlack,
			target: pkglogger.LogOutputOpenSearch,
			keys: [3]string{
				"HETEROLOG_SLACK_LOGGER_ERROR_HANDLER",
				"HETEROLOG_ASYNC_SLACK_LOGGER_ERROR_HANDLER",
				"HETEROLOG_DELIVERY_SLACK_LOGGER_ERROR_HANDLER",
			},
		},
		{
			source: pkglogger.LogOutputOpenSearch,
			target: pkglogger.LogOutputLoki,
			keys: [3]string{
				"HETEROLOG_OPENSEARCH_LOGGER_ERROR_HANDLER",
				"HETEROLOG_ASYNC_OPENSEARCH_LOGGER_ERROR_HANDLER",
				"HETEROLOG_DELIVERY_OPENSEARCH_LOGGER_ERROR_HANDLER",
			},
		},
		{
			source: pkglogger.LogOutputLoki,
			target: pkglogger.LogOutputCloudWatchLogs,
			keys: [3]string{
				"HETEROLOG_LOKI_LOGGER_ERROR_HANDLER",
				"HETEROLOG_ASYNC_LOKI_LOGGER_ERROR_HANDLER",
				"HETEROLOG_DELIVERY_LOKI_LOGGER_ERROR_HANDLER",
			},
		},
		{
			source: pkglogger.LogOutputCloudWatchLogs,
			target: pkglogger.LogOutputClickHouse,
			keys: [3]string{
				"HETEROLOG_CLOUDWATCH_LOGGER_ERROR_HANDLER",
				"HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_ERROR_HANDLER",
				"HETEROLOG_DELIVERY_CLOUDWATCH_LOGGER_ERROR_HANDLER",
			},
		},
		{
			source: pkglogger.LogOutputClickHouse,
			target: pkglogger.LogOutputFile,
			keys: [3]string{
				"HETEROLOG_CLICKHOUSE_LOGGER_ERROR_HANDLER",
				"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_ERROR_HANDLER",
				"HETEROLOG_DELIVERY_CLICKHOUSE_LOGGER_ERROR_HANDLER",
			},
		},
	}
	allSources := pkglogger.AllLogOutputDestinations()
	if len(sourceCases) != len(allSources) {
		t.Fatalf(
			"internal handler routing source cases = %d, want every canonical destination (%d): %v",
			len(sourceCases),
			len(allSources),
			allSources,
		)
	}
	coveredSources := make(map[pkglogger.LogOutputDestination]struct{}, len(sourceCases))
	for _, candidate := range sourceCases {
		coveredSources[candidate.source] = struct{}{}
	}
	for _, source := range allSources {
		if _, covered := coveredSources[source]; !covered {
			t.Fatalf("internal handler routing source case is missing for %q", source)
		}
	}

	scopes := []struct {
		name     string
		keyIndex int
		env      func(pkglogger.LogOutputDestination, string, pkglogger.LogOutputDestination) []string
		loaded   func(AppConfig) (pkglogger.LogOutputDestinations, pkglogger.LogOutputDestinations)
		mapped   func(AppConfig) any
	}{
		{
			name:     "sync",
			keyIndex: 0,
			env: func(source pkglogger.LogOutputDestination, key string, target pkglogger.LogOutputDestination) []string {
				return []string{
					"HETEROLOG_ENABLE_LOGGING=true",
					"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
					"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
					"HETEROLOG_LOG_TYPE=" + source.String(),
					"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
					key + "=" + target.String(),
				}
			},
			loaded: func(cfg AppConfig) (pkglogger.LogOutputDestinations, pkglogger.LogOutputDestinations) {
				return cfg.Logger.LogOutputDestinations, cfg.Logger.HandlerTargetDestinations
			},
			mapped: func(cfg AppConfig) any {
				return ToLoggerConfig(cfg)
			},
		},
		{
			name:     "async",
			keyIndex: 1,
			env: func(source pkglogger.LogOutputDestination, key string, target pkglogger.LogOutputDestination) []string {
				return []string{
					"HETEROLOG_ENABLE_LOGGING=false",
					"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
					"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
					"HETEROLOG_ASYNC_LOG_TYPE=" + source.String(),
					"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
					key + "=" + target.String(),
				}
			},
			loaded: func(cfg AppConfig) (pkglogger.LogOutputDestinations, pkglogger.LogOutputDestinations) {
				asyncCfg := cfg.AsyncBestEffortMultiLoggerConfig
				return asyncCfg.LogOutputDestinations, asyncCfg.HandlerTargetDestinations
			},
			mapped: func(cfg AppConfig) any {
				return ToAsyncBestEffortMultiLoggerConfig(cfg)
			},
		},
		{
			name:     "delivery",
			keyIndex: 2,
			env: func(source pkglogger.LogOutputDestination, key string, target pkglogger.LogOutputDestination) []string {
				return []string{
					"HETEROLOG_ENABLE_LOGGING=false",
					"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
					"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
					"HETEROLOG_DELIVERY_LOG_TYPE=" + source.String(),
					"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
					key + "=" + target.String(),
				}
			},
			loaded: func(cfg AppConfig) (pkglogger.LogOutputDestinations, pkglogger.LogOutputDestinations) {
				return cfg.DeliveryLogger.LogOutputDestinations,
					cfg.DeliveryLogger.HandlerTargetDestinations
			},
			mapped: func(cfg AppConfig) any {
				return ToDeliveryLoggerConfig(cfg)
			},
		},
	}

	for _, scope := range scopes {
		for _, sourceCase := range sourceCases {
			t.Run(scope.name+"/"+sourceCase.source.String(), func(t *testing.T) {
				key := sourceCase.keys[scope.keyIndex]
				cfg := loadConfigFromTempEnv(
					t,
					scope.env(sourceCase.source, key, sourceCase.target),
				)

				destinations, handlerTargets := scope.loaded(cfg)
				if !slices.Equal(destinations, pkglogger.LogOutputDestinations{sourceCase.source}) {
					t.Fatalf("normal destinations = %v, want only %q", destinations, sourceCase.source)
				}
				if !slices.Equal(handlerTargets, pkglogger.LogOutputDestinations{sourceCase.target}) {
					t.Fatalf("handler targets = %v, want only %q", handlerTargets, sourceCase.target)
				}

				mapped := scope.mapped(cfg)
				assertMappedRouting(t, mapped, sourceCase.source, sourceCase.target)
				assertMappedTargetConfig(t, mapped, sourceCase.target)
				assertMappedDestinations(t, mapped, sourceCase.source, sourceCase.target)
			})
		}
	}
}

func TestDeliveryFailureArchiveRoutingLoadsAndMapsForEveryBackend(t *testing.T) {
	tests := []struct {
		backend       pkglogger.LogOutputDestination
		durableKey    string
		handlerKey    string
		mappedRouting func(pkglogger.DeliveryLoggerConfig) []string
	}{
		{
			backend:    pkglogger.LogOutputOpenSearch,
			durableKey: "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES",
			handlerKey: "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER",
			mappedRouting: func(cfg pkglogger.DeliveryLoggerConfig) []string {
				return cfg.OpenSearch.FailureStoreConfig.FileArchiveConfig.HandlerConfig.OnErrorConfig
			},
		},
		{
			backend:    pkglogger.LogOutputLoki,
			durableKey: "HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES",
			handlerKey: "HETEROLOG_DELIVERY_LOKI_DURABLE_FILE_ERROR_HANDLER",
			mappedRouting: func(cfg pkglogger.DeliveryLoggerConfig) []string {
				return cfg.Loki.FailureStoreConfig.FileArchiveConfig.HandlerConfig.OnErrorConfig
			},
		},
		{
			backend:    pkglogger.LogOutputCloudWatchLogs,
			durableKey: "HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES",
			handlerKey: "HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_FILE_ERROR_HANDLER",
			mappedRouting: func(cfg pkglogger.DeliveryLoggerConfig) []string {
				return cfg.CloudWatchLogs.FailureStoreConfig.FileArchiveConfig.HandlerConfig.OnErrorConfig
			},
		},
		{
			backend:    pkglogger.LogOutputClickHouse,
			durableKey: "HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES",
			handlerKey: "HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ERROR_HANDLER",
			mappedRouting: func(cfg pkglogger.DeliveryLoggerConfig) []string {
				return cfg.ClickHouse.FailureStoreConfig.FileArchiveConfig.HandlerConfig.OnErrorConfig
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.backend.String(), func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=" + tt.backend.String(),
				"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				tt.durableKey + "=fileArchive",
				tt.handlerKey + "=file",
			})

			if got := cfg.DeliveryLogger.HandlerTargetDestinations; !slices.Equal(
				got,
				pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
			) {
				t.Fatalf("handler targets = %v, want [file]", got)
			}

			mapped := ToDeliveryLoggerConfig(cfg)
			if got := tt.mappedRouting(mapped); !slices.Equal(got, []string{"file"}) {
				t.Fatalf("mapped archive OnErrorConfig = %v, want [file]", got)
			}
			if mapped.File == nil {
				t.Fatal("handler-only Delivery file config = nil")
			}
			if mapped.LogOutputDestinations.Has(pkglogger.LogOutputFile) {
				t.Fatalf(
					"mapped normal destinations = %v, must not enable file",
					mapped.LogOutputDestinations,
				)
			}
		})
	}
}

func TestAsyncStdoutAndDeliveryArchiveReadAllRoutingLevelsFromIndependentKeys(t *testing.T) {
	asyncCfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_ASYNC_LOG_TYPE=stdout",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER=clickhouse",
		"HETEROLOG_STDOUT_LOGGER_WARN_HANDLER=clickhouse",
		"HETEROLOG_STDOUT_LOGGER_INFO_HANDLER=clickhouse",
		"HETEROLOG_STDOUT_LOGGER_DEBUG_HANDLER=clickhouse",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER=file",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_WARN_HANDLER=slack",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_INFO_HANDLER=opensearch",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_DEBUG_HANDLER=loki",
	})
	asyncRouting := ToAsyncBestEffortMultiLoggerConfig(asyncCfg).Stdout.HandlerConfig
	assertRoutingLevels(
		t,
		"async stdout",
		asyncRouting,
		[]string{"file"},
		[]string{"slack"},
		[]string{"opensearch"},
		[]string{"loki"},
	)
	if asyncCfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations.Has(
		pkglogger.LogOutputClickHouse,
	) {
		t.Fatal("async stdout used sync stdout routing keys")
	}

	deliveryCfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER=file",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_WARN_HANDLER=stdout",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_INFO_HANDLER=slack",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_DEBUG_HANDLER=loki",
	})
	deliveryRouting := ToDeliveryLoggerConfig(deliveryCfg).
		OpenSearch.
		FailureStoreConfig.
		FileArchiveConfig.
		HandlerConfig
	assertRoutingLevels(
		t,
		"delivery OpenSearch failure archive",
		deliveryRouting,
		[]string{"file"},
		[]string{"stdout"},
		[]string{"slack"},
		[]string{"loki"},
	)
}

// failure archiveのroutingも通常logger本体と同じhandler target可用性へ寄与します。
// OpenSearch自体は通常出力先なのでhandler-only集合から除外し、それ以外の全targetは
// config/clientをmapperへ残します。
func TestFailureArchiveRoutingKeepsEveryHandlerTargetConfigForEachScope(t *testing.T) {
	allHandlerTargets := pkglogger.AllLogOutputDestinations()
	allTargets := allHandlerTargets.String()
	wantHandlerTargets := make(
		pkglogger.LogOutputDestinations,
		0,
		len(allHandlerTargets)-1,
	)
	for _, target := range allHandlerTargets {
		if target != pkglogger.LogOutputOpenSearch {
			wantHandlerTargets = append(wantHandlerTargets, target)
		}
	}

	tests := []struct {
		name            string
		env             []string
		loadedTargets   func(AppConfig) pkglogger.LogOutputDestinations
		mapped          func(AppConfig) any
		archiveRouting  func(any) []string
		mappedNormalOut func(any) pkglogger.LogOutputDestinations
	}{
		{
			name: "sync",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=opensearch",
				"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER=" + allTargets,
			},
			loadedTargets: func(cfg AppConfig) pkglogger.LogOutputDestinations {
				return cfg.Logger.HandlerTargetDestinations
			},
			mapped: func(cfg AppConfig) any {
				return ToLoggerConfig(cfg)
			},
			archiveRouting: func(mapped any) []string {
				cfg := mapped.(pkglogger.LoggerConfig)
				return cfg.OpenSearch.
					FailureStoreConfig.
					DurableStoreConfig.
					FileArchiveConfig.
					HandlerConfig.
					OnErrorConfig
			},
			mappedNormalOut: func(mapped any) pkglogger.LogOutputDestinations {
				return mapped.(pkglogger.LoggerConfig).LogOutputDestinations
			},
		},
		{
			name: "async",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
				"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER=" + allTargets,
			},
			loadedTargets: func(cfg AppConfig) pkglogger.LogOutputDestinations {
				return cfg.AsyncBestEffortMultiLoggerConfig.HandlerTargetDestinations
			},
			mapped: func(cfg AppConfig) any {
				return ToAsyncBestEffortMultiLoggerConfig(cfg)
			},
			archiveRouting: func(mapped any) []string {
				cfg := mapped.(pkglogger.AsyncBestEffortMultiLoggerConfig)
				return cfg.OpenSearch.
					FailureStoreConfig.
					DurableStoreConfig.
					FileArchiveConfig.
					HandlerConfig.
					OnErrorConfig
			},
			mappedNormalOut: func(mapped any) pkglogger.LogOutputDestinations {
				return mapped.(pkglogger.AsyncBestEffortMultiLoggerConfig).
					LogOutputDestinations
			},
		},
		{
			name: "delivery",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
				"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES=fileArchive",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER=" + allTargets,
			},
			loadedTargets: func(cfg AppConfig) pkglogger.LogOutputDestinations {
				return cfg.DeliveryLogger.HandlerTargetDestinations
			},
			mapped: func(cfg AppConfig) any {
				return ToDeliveryLoggerConfig(cfg)
			},
			archiveRouting: func(mapped any) []string {
				cfg := mapped.(pkglogger.DeliveryLoggerConfig)
				return cfg.OpenSearch.
					FailureStoreConfig.
					FileArchiveConfig.
					HandlerConfig.
					OnErrorConfig
			},
			mappedNormalOut: func(mapped any) pkglogger.LogOutputDestinations {
				return mapped.(pkglogger.DeliveryLoggerConfig).LogOutputDestinations
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, tt.env)
			if got := tt.loadedTargets(cfg); !slices.Equal(got, wantHandlerTargets) {
				t.Fatalf("handler targets = %v, want %v", got, wantHandlerTargets)
			}

			mapped := tt.mapped(cfg)
			if got := tt.mappedNormalOut(mapped); !slices.Equal(
				got,
				pkglogger.LogOutputDestinations{pkglogger.LogOutputOpenSearch},
			) {
				t.Fatalf("normal destinations = %v, want only [opensearch]", got)
			}
			if got := tt.archiveRouting(mapped); !slices.Equal(got, allHandlerTargets.Strings()) {
				t.Fatalf("mapped failure archive routing = %v, want every target", got)
			}

			for _, target := range allHandlerTargets {
				assertMappedTargetConfig(t, mapped, target)
			}
		})
	}
}

// 選択されていないsourceのrouting値と、handler専用target自身のrouting値は、
// target configの可用性を再帰的に増やしてはなりません。
func TestInternalHandlerTargetCollectionScansOnlySelectedSources(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER=file",
		"HETEROLOG_DELIVERY_FILE_LOGGER_ERROR_HANDLER=clickhouse",
		"HETEROLOG_DELIVERY_LOKI_LOGGER_ERROR_HANDLER=opensearch",
	})

	if got := cfg.DeliveryLogger.HandlerTargetDestinations; !slices.Equal(
		got,
		pkglogger.LogOutputDestinations{pkglogger.LogOutputFile},
	) {
		t.Fatalf("handler targets = %v, want only [file]", got)
	}

	mapped := ToDeliveryLoggerConfig(cfg)
	if mapped.File == nil {
		t.Fatal("selected stdout target file config = nil")
	}
	if mapped.ClickHouse != nil || mapped.Loki != nil || mapped.OpenSearch != nil {
		t.Fatalf(
			"unselected/recursive target configs were mapped: clickhouse=%v loki=%v opensearch=%v",
			mapped.ClickHouse != nil,
			mapped.Loki != nil,
			mapped.OpenSearch != nil,
		)
	}
}

func TestDeliveryHandlerOnlyFileConfigParticipatesInDirectoryClaims(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, storagePathValidationEnv(t,
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER=file",
		"HETEROLOG_DELIVERY_LOG_DIR=delivery-handler-only",
		"HETEROLOG_DELIVERY_ERROR_LOG_DIR=delivery-handler-only-error",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD=false",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_ERROR_LOGS=false",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_SEPARATE_DEBUG_LOGS=false",
	))

	if !cfg.DeliveryLogger.HandlerTargetDestinations.Has(pkglogger.LogOutputFile) {
		t.Fatalf(
			"handler targets = %v, want file",
			cfg.DeliveryLogger.HandlerTargetDestinations,
		)
	}
	if mapped := ToDeliveryLoggerConfig(cfg); mapped.File == nil {
		t.Fatal("mapped Delivery handler-only file config = nil")
	}

	claims, err := cfg.collectStorageDirectoryClaims()
	if err != nil {
		t.Fatalf("collectStorageDirectoryClaims() error = %v", err)
	}
	if !hasStorageDirectoryClaimSource(claims, "delivery_logger.file.default") {
		t.Fatalf(
			"storage claims = %+v, want delivery handler-only file claim",
			claims,
		)
	}
}

func assertRoutingLevels(
	t *testing.T,
	name string,
	got handler.HandlerConfig,
	wantError []string,
	wantWarn []string,
	wantInfo []string,
	wantDebug []string,
) {
	t.Helper()

	if !slices.Equal(got.OnErrorConfig, wantError) ||
		!slices.Equal(got.OnWarnConfig, wantWarn) ||
		!slices.Equal(got.OnInfoConfig, wantInfo) ||
		!slices.Equal(got.OnDebugConfig, wantDebug) {
		t.Fatalf(
			"%s routing = error:%v warn:%v info:%v debug:%v, want error:%v warn:%v info:%v debug:%v",
			name,
			got.OnErrorConfig,
			got.OnWarnConfig,
			got.OnInfoConfig,
			got.OnDebugConfig,
			wantError,
			wantWarn,
			wantInfo,
			wantDebug,
		)
	}
}

func assertMappedDestinations(
	t *testing.T,
	mapped any,
	source pkglogger.LogOutputDestination,
	handlerTarget pkglogger.LogOutputDestination,
) {
	t.Helper()

	var destinations pkglogger.LogOutputDestinations
	switch cfg := mapped.(type) {
	case pkglogger.LoggerConfig:
		destinations = cfg.LogOutputDestinations
	case pkglogger.AsyncBestEffortMultiLoggerConfig:
		destinations = cfg.LogOutputDestinations
	case pkglogger.DeliveryLoggerConfig:
		destinations = cfg.LogOutputDestinations
	default:
		t.Fatalf("unsupported mapped config type %T", mapped)
	}

	if !destinations.Has(source) || destinations.Has(handlerTarget) {
		t.Fatalf(
			"mapped destinations = %v, want source %q and no handler-only target %q",
			destinations,
			source,
			handlerTarget,
		)
	}
}

func assertMappedRouting(
	t *testing.T,
	mapped any,
	source pkglogger.LogOutputDestination,
	want pkglogger.LogOutputDestination,
) {
	t.Helper()

	var got []string
	switch cfg := mapped.(type) {
	case pkglogger.LoggerConfig:
		got = mappedSyncRouting(cfg, source)
	case pkglogger.AsyncBestEffortMultiLoggerConfig:
		got = mappedSyncRouting(cfg.ToLoggerConfig(), source)
	case pkglogger.DeliveryLoggerConfig:
		got = mappedDeliveryRouting(cfg, source)
	default:
		t.Fatalf("unsupported mapped config type %T", mapped)
	}
	if !slices.Equal(got, []string{want.String()}) {
		t.Fatalf("mapped %s OnErrorConfig = %v, want [%s]", source, got, want)
	}
}

func mappedSyncRouting(
	cfg pkglogger.LoggerConfig,
	source pkglogger.LogOutputDestination,
) []string {
	switch source {
	case pkglogger.LogOutputFile:
		return cfg.File.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputStdout:
		return cfg.Stdout.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputSlack:
		return cfg.Slack.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputOpenSearch:
		return cfg.OpenSearch.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputLoki:
		return cfg.Loki.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputCloudWatchLogs:
		return cfg.CloudWatchLogs.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputClickHouse:
		return cfg.ClickHouse.HandlerConfig.OnErrorConfig
	default:
		return nil
	}
}

func mappedDeliveryRouting(
	cfg pkglogger.DeliveryLoggerConfig,
	source pkglogger.LogOutputDestination,
) []string {
	switch source {
	case pkglogger.LogOutputFile:
		return cfg.File.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputStdout:
		return cfg.Stdout.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputSlack:
		return cfg.Slack.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputOpenSearch:
		return cfg.OpenSearch.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputLoki:
		return cfg.Loki.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputCloudWatchLogs:
		return cfg.CloudWatchLogs.HandlerConfig.OnErrorConfig
	case pkglogger.LogOutputClickHouse:
		return cfg.ClickHouse.HandlerConfig.OnErrorConfig
	default:
		return nil
	}
}

func assertMappedTargetConfig(
	t *testing.T,
	mapped any,
	target pkglogger.LogOutputDestination,
) {
	t.Helper()

	switch cfg := mapped.(type) {
	case pkglogger.LoggerConfig:
		assertMappedSyncTargetConfig(t, cfg, target)
	case pkglogger.AsyncBestEffortMultiLoggerConfig:
		assertMappedSyncTargetConfig(t, cfg.ToLoggerConfig(), target)
	case pkglogger.DeliveryLoggerConfig:
		assertMappedDeliveryTargetConfig(t, cfg, target)
	default:
		t.Fatalf("unsupported mapped config type %T", mapped)
	}
}

func assertMappedSyncTargetConfig(
	t *testing.T,
	cfg pkglogger.LoggerConfig,
	target pkglogger.LogOutputDestination,
) {
	t.Helper()

	switch target {
	case pkglogger.LogOutputFile:
		if cfg.File == nil {
			t.Fatal("mapped file config = nil")
		}
	case pkglogger.LogOutputStdout:
		if cfg.Stdout == nil {
			t.Fatal("mapped stdout config = nil")
		}
	case pkglogger.LogOutputSlack:
		if cfg.Slack == nil {
			t.Fatal("mapped slack config = nil")
		}
	case pkglogger.LogOutputOpenSearch:
		if cfg.OpenSearch == nil || cfg.OpenSearchClient == nil {
			t.Fatal("mapped OpenSearch config/client = nil")
		}
	case pkglogger.LogOutputLoki:
		if cfg.Loki == nil || cfg.LokiClient == nil {
			t.Fatal("mapped Loki config/client = nil")
		}
	case pkglogger.LogOutputCloudWatchLogs:
		if cfg.CloudWatchLogs == nil || cfg.CloudWatchLogsClient == nil {
			t.Fatal("mapped CloudWatch Logs config/client = nil")
		}
	case pkglogger.LogOutputClickHouse:
		if cfg.ClickHouse == nil || cfg.ClickHouseClient == nil {
			t.Fatal("mapped ClickHouse config/client = nil")
		}
	default:
		t.Fatalf("mapped sync target assertion is not implemented for %q", target)
	}
}

func assertMappedDeliveryTargetConfig(
	t *testing.T,
	cfg pkglogger.DeliveryLoggerConfig,
	target pkglogger.LogOutputDestination,
) {
	t.Helper()

	switch target {
	case pkglogger.LogOutputFile:
		if cfg.File == nil {
			t.Fatal("mapped Delivery file config = nil")
		}
	case pkglogger.LogOutputStdout:
		if cfg.Stdout == nil {
			t.Fatal("mapped Delivery stdout config = nil")
		}
	case pkglogger.LogOutputSlack:
		if cfg.Slack == nil {
			t.Fatal("mapped Delivery slack config = nil")
		}
	case pkglogger.LogOutputOpenSearch:
		if cfg.OpenSearch == nil || cfg.OpenSearchClient == nil {
			t.Fatal("mapped Delivery OpenSearch config/client = nil")
		}
	case pkglogger.LogOutputLoki:
		if cfg.Loki == nil || cfg.LokiClient == nil {
			t.Fatal("mapped Delivery Loki config/client = nil")
		}
	case pkglogger.LogOutputCloudWatchLogs:
		if cfg.CloudWatchLogs == nil || cfg.CloudWatchLogsClient == nil {
			t.Fatal("mapped Delivery CloudWatch Logs config/client = nil")
		}
	case pkglogger.LogOutputClickHouse:
		if cfg.ClickHouse == nil || cfg.ClickHouseClient == nil {
			t.Fatal("mapped Delivery ClickHouse config/client = nil")
		}
	default:
		t.Fatalf("mapped Delivery target assertion is not implemented for %q", target)
	}
}

func TestCollectInternalHandlerTargetsUsesCanonicalLoggerDestinationOrder(t *testing.T) {
	want := pkglogger.AllLogOutputDestinations()
	got := collectInternalHandlerTargets(
		nil,
		func(pkglogger.LogOutputDestination) bool {
			return true
		},
	)
	if !slices.Equal(got, want) {
		t.Fatalf("collected internal handler targets = %v, want %v", got, want)
	}
}

func TestHandlerTargetConfigLoadersRejectUnimplementedTarget(t *testing.T) {
	const unimplemented pkglogger.LogOutputDestination = "future-output"

	tests := []struct {
		name     string
		contract pkglogger.LoggerContractKind
		load     func() error
	}{
		{
			name:     "sync",
			contract: pkglogger.LoggerContractSync,
			load: func() error {
				cfg := logconfig.LoggerConfig{
					HandlerTargetDestinations: pkglogger.LogOutputDestinations{unimplemented},
				}
				return loadLoggerHandlerTargetConfigs(processEnvironmentForTest(), &cfg)
			},
		},
		{
			name:     "async",
			contract: pkglogger.LoggerContractAsyncBestEffort,
			load: func() error {
				cfg := logconfig.AsyncBestEffortMultiLoggerConfig{
					HandlerTargetDestinations: pkglogger.LogOutputDestinations{unimplemented},
				}
				return loadAsyncLoggerHandlerTargetConfigs(processEnvironmentForTest(), &cfg)
			},
		},
		{
			name:     "delivery",
			contract: pkglogger.LoggerContractDelivery,
			load: func() error {
				cfg := logconfig.DeliveryLoggerConfig{
					HandlerTargetDestinations: pkglogger.LogOutputDestinations{unimplemented},
				}
				return loadDeliveryLoggerHandlerTargetConfigs(processEnvironmentForTest(), &cfg)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.load()
			var targetErr *UnimplementedHandlerTargetError
			if !errors.As(err, &targetErr) || targetErr.Contract != tt.contract || targetErr.Target != unimplemented {
				t.Fatalf("handler target config loader error = %v, want unimplemented %s target %q", err, tt.contract, unimplemented)
			}
		})
	}
}

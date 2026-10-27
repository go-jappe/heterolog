package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	configfailure "github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

var spoolHandlerRoutingSuffixes = []string{
	"_SPOOL_FILE_ERROR_HANDLER",
	"_SPOOL_FILE_WARN_HANDLER",
	"_SPOOL_FILE_INFO_HANDLER",
	"_SPOOL_FILE_DEBUG_HANDLER",
}

// spoolを実際に構築するLoggerContractKindだけが、spool自身の内部エラー通知先を公開する。
// Delivery loggerはbackendを問わずfileArchive専用なのでspool用キーを持たない。
func TestSpoolHandlerRoutingPublicEnvExampleContract(t *testing.T) {
	enabledScopes := []string{
		"OPENSEARCH", "ASYNC_OPENSEARCH",
		"LOKI", "ASYNC_LOKI",
		"CLOUDWATCH", "ASYNC_CLOUDWATCH",
		"CLICKHOUSE", "ASYNC_CLICKHOUSE",
	}
	fileArchiveOnlyScopes := []string{
		"DELIVERY_OPENSEARCH",
		"DELIVERY_LOKI",
		"DELIVERY_CLOUDWATCH",
		"DELIVERY_CLICKHOUSE",
	}

	values := readPublicEnvExample(t)
	for _, scope := range enabledScopes {
		for _, suffix := range spoolHandlerRoutingSuffixes {
			key := "HETEROLOG_" + scope + suffix
			if got, want := values[key], "stdout"; got != want {
				t.Errorf("%s = %q, want %q", key, got, want)
			}
		}
	}
	for _, scope := range fileArchiveOnlyScopes {
		for _, suffix := range spoolHandlerRoutingSuffixes {
			key := "HETEROLOG_" + scope + suffix
			if _, exists := values[key]; exists {
				t.Errorf("fileArchive-only scope exposes unused key %s", key)
			}
		}
	}
}

func TestSpoolHandlerRoutingLoadAndMapperPreserveEachScope(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		load   func() (configfailure.FailureHandlerConfig, error)
	}{
		{name: "opensearch", prefix: "OPENSEARCH", load: fromProcessEnvironment(loadOpenSearchFailureHandlerConfig)},
		{name: "async opensearch", prefix: "ASYNC_OPENSEARCH", load: fromProcessEnvironment(loadAsyncOpenSearchFailureHandlerConfig)},
		{name: "loki", prefix: "LOKI", load: fromProcessEnvironment(loadLokiFailureHandlerConfig)},
		{name: "async loki", prefix: "ASYNC_LOKI", load: fromProcessEnvironment(loadAsyncLokiFailureHandlerConfig)},
		{name: "cloudwatch", prefix: "CLOUDWATCH", load: fromProcessEnvironment(loadCloudWatchLogsFailureHandlerConfig)},
		{name: "async cloudwatch", prefix: "ASYNC_CLOUDWATCH", load: fromProcessEnvironment(loadAsyncCloudWatchLogsFailureHandlerConfig)},
		{name: "clickhouse", prefix: "CLICKHOUSE", load: fromProcessEnvironment(loadClickHouseFailureHandlerConfig)},
		{name: "async clickhouse", prefix: "ASYNC_CLICKHOUSE", load: fromProcessEnvironment(loadAsyncClickHouseFailureHandlerConfig)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := []string{tt.prefix + "-error", tt.prefix + "-warn", tt.prefix + "-info", tt.prefix + "-debug"}
			t.Setenv("HETEROLOG_"+tt.prefix+spoolHandlerRoutingSuffixes[0], want[0])
			t.Setenv("HETEROLOG_"+tt.prefix+spoolHandlerRoutingSuffixes[1], want[1])
			t.Setenv("HETEROLOG_"+tt.prefix+spoolHandlerRoutingSuffixes[2], want[2])
			t.Setenv("HETEROLOG_"+tt.prefix+spoolHandlerRoutingSuffixes[3], want[3])

			loaded, err := tt.load()
			if err != nil {
				t.Fatalf("load failure handler config: %v", err)
			}
			loadedRouting := loaded.Fsstore.SpoolConfig.HandlerConfig
			assertSpoolHandlerRouting(
				t,
				"loaded",
				loadedRouting.OnErrorConfig,
				loadedRouting.OnWarnConfig,
				loadedRouting.OnInfoConfig,
				loadedRouting.OnDebugConfig,
				want,
			)

			mapped := newFailureStoreConfig(
				&AppConfig{},
				checkpointSourceLoggerOpenSearch,
				pkglogger.LoggerContractSync,
				loaded,
				logconfig.LogBodyConfig{},
			)
			mappedRouting := mapped.DurableStoreConfig.SpoolConfig.HandlerConfig
			assertSpoolHandlerRouting(
				t,
				"mapped",
				mappedRouting.OnErrorConfig,
				mappedRouting.OnWarnConfig,
				mappedRouting.OnInfoConfig,
				mappedRouting.OnDebugConfig,
				want,
			)

			adapted := loggerdurable.NewFailureStoreConfigAdapter(mapped).
				DurableStoreConfig().SpoolConfig().Handler()
			got := [][]string{adapted.OnError(), adapted.OnWarn(), adapted.OnInfo(), adapted.OnDebug()}
			for i, level := range []string{"error", "warn", "info", "debug"} {
				if !reflect.DeepEqual(got[i], []string{want[i]}) {
					t.Errorf("adapted %s routing = %v, want [%s]", level, got[i], want[i])
				}
			}
		})
	}
}

// 各levelに異なる有効なroutingを設定し、loader、検証付きtop-level mapper、
// runtime用durable adapterの境界を越えても欠落・混線しないことを確認する。
func TestSpoolHandlerRoutesRemainDistinctThroughTopLevelMappersAndAdapters(t *testing.T) {
	repoRoot := configFixtureRootForTest(t)
	envPath := filepath.Join(repoRoot, ".env")
	restoreProcessEnvAfterTest(t)

	tests := []struct {
		name       string
		prefix     string
		outputKey  string
		backend    string
		loaded     func(AppConfig) configfailure.FailureHandlerConfig
		mapFailure func(*testing.T, AppConfig) (pkgdurable.FailureStoreConfig, bool)
	}{
		{
			name: "opensearch", prefix: "OPENSEARCH", outputKey: "HETEROLOG_LOG_TYPE", backend: "opensearch",
			loaded: func(cfg AppConfig) configfailure.FailureHandlerConfig {
				return cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig
			},
			mapFailure: func(t *testing.T, cfg AppConfig) (pkgdurable.FailureStoreConfig, bool) {
				t.Helper()
				mapped, err := MapLoggerConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if mapped.OpenSearch == nil {
					return pkgdurable.FailureStoreConfig{}, false
				}
				return mapped.OpenSearch.FailureStoreConfig, true
			},
		},
		{
			name: "async opensearch", prefix: "ASYNC_OPENSEARCH", outputKey: "HETEROLOG_ASYNC_LOG_TYPE", backend: "opensearch",
			loaded: func(cfg AppConfig) configfailure.FailureHandlerConfig {
				return cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig
			},
			mapFailure: func(t *testing.T, cfg AppConfig) (pkgdurable.FailureStoreConfig, bool) {
				t.Helper()
				mapped, err := MapAsyncLoggerConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if mapped.OpenSearch == nil {
					return pkgdurable.FailureStoreConfig{}, false
				}
				return mapped.OpenSearch.FailureStoreConfig, true
			},
		},
		{
			name: "loki", prefix: "LOKI", outputKey: "HETEROLOG_LOG_TYPE", backend: "loki",
			loaded: func(cfg AppConfig) configfailure.FailureHandlerConfig {
				return cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig
			},
			mapFailure: func(t *testing.T, cfg AppConfig) (pkgdurable.FailureStoreConfig, bool) {
				t.Helper()
				mapped, err := MapLoggerConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if mapped.Loki == nil {
					return pkgdurable.FailureStoreConfig{}, false
				}
				return mapped.Loki.FailureStoreConfig, true
			},
		},
		{
			name: "async loki", prefix: "ASYNC_LOKI", outputKey: "HETEROLOG_ASYNC_LOG_TYPE", backend: "loki",
			loaded: func(cfg AppConfig) configfailure.FailureHandlerConfig {
				return cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig
			},
			mapFailure: func(t *testing.T, cfg AppConfig) (pkgdurable.FailureStoreConfig, bool) {
				t.Helper()
				mapped, err := MapAsyncLoggerConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if mapped.Loki == nil {
					return pkgdurable.FailureStoreConfig{}, false
				}
				return mapped.Loki.FailureStoreConfig, true
			},
		},
		{
			name: "cloudwatch", prefix: "CLOUDWATCH", outputKey: "HETEROLOG_LOG_TYPE", backend: "cloudwatchlogs",
			loaded: func(cfg AppConfig) configfailure.FailureHandlerConfig {
				return cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig
			},
			mapFailure: func(t *testing.T, cfg AppConfig) (pkgdurable.FailureStoreConfig, bool) {
				t.Helper()
				mapped, err := MapLoggerConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if mapped.CloudWatchLogs == nil {
					return pkgdurable.FailureStoreConfig{}, false
				}
				return mapped.CloudWatchLogs.FailureStoreConfig, true
			},
		},
		{
			name: "async cloudwatch", prefix: "ASYNC_CLOUDWATCH", outputKey: "HETEROLOG_ASYNC_LOG_TYPE", backend: "cloudwatchlogs",
			loaded: func(cfg AppConfig) configfailure.FailureHandlerConfig {
				return cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig
			},
			mapFailure: func(t *testing.T, cfg AppConfig) (pkgdurable.FailureStoreConfig, bool) {
				t.Helper()
				mapped, err := MapAsyncLoggerConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if mapped.CloudWatchLogs == nil {
					return pkgdurable.FailureStoreConfig{}, false
				}
				return mapped.CloudWatchLogs.FailureStoreConfig, true
			},
		},
		{
			name: "clickhouse", prefix: "CLICKHOUSE", outputKey: "HETEROLOG_LOG_TYPE", backend: "clickhouse",
			loaded: func(cfg AppConfig) configfailure.FailureHandlerConfig {
				return cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig
			},
			mapFailure: func(t *testing.T, cfg AppConfig) (pkgdurable.FailureStoreConfig, bool) {
				t.Helper()
				mapped, err := MapLoggerConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if mapped.ClickHouse == nil {
					return pkgdurable.FailureStoreConfig{}, false
				}
				return mapped.ClickHouse.FailureStoreConfig, true
			},
		},
		{
			name: "async clickhouse", prefix: "ASYNC_CLICKHOUSE", outputKey: "HETEROLOG_ASYNC_LOG_TYPE", backend: "clickhouse",
			loaded: func(cfg AppConfig) configfailure.FailureHandlerConfig {
				return cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig
			},
			mapFailure: func(t *testing.T, cfg AppConfig) (pkgdurable.FailureStoreConfig, bool) {
				t.Helper()
				mapped, err := MapAsyncLoggerConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if mapped.ClickHouse == nil {
					return pkgdurable.FailureStoreConfig{}, false
				}
				return mapped.ClickHouse.FailureStoreConfig, true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearProcessEnvForTest(t)
			for key, value := range map[string]string{
				"HETEROLOG_ENV_PATH":          envPath,
				"HETEROLOG_LOG_TYPE":          "stdout",
				"HETEROLOG_ASYNC_LOG_TYPE":    "stdout",
				"HETEROLOG_DELIVERY_LOG_TYPE": "stdout",
			} {
				if err := os.Setenv(key, value); err != nil {
					t.Fatalf("set %s: %v", key, err)
				}
			}
			if err := os.Setenv(tt.outputKey, tt.backend); err != nil {
				t.Fatalf("set %s: %v", tt.outputKey, err)
			}
			if tt.backend == "cloudwatchlogs" {
				for _, suffix := range []string{"_AWS_ACCESS_KEY_ID", "_AWS_SECRET_ACCESS_KEY"} {
					if err := os.Setenv("HETEROLOG_"+tt.prefix+suffix, "fixture-spool-routing"); err != nil {
						t.Fatal(err)
					}
				}
			}

			// levelごとに異なる有効な一覧を与えるため、公開例が全levelをstdoutへ振り分けていても
			// levelの取り違えを検出できます。stdoutとfileのどちらのtargetもhandler出力先として読み込まれます。
			want := [][]string{{"stdout"}, {"file"}, {"stdout", "file"}, {"file", "stdout"}}
			for i, suffix := range spoolHandlerRoutingSuffixes {
				if err := os.Setenv("HETEROLOG_"+tt.prefix+suffix, strings.Join(want[i], ",")); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig() with synthetic fixture: %v", err)
			}
			loadedFailure := tt.loaded(cfg)
			mappedFailure, ok := tt.mapFailure(t, cfg)
			if !ok {
				t.Fatalf("top-level mapper did not construct %s", tt.name)
			}

			loaded := loadedFailure.Fsstore.SpoolConfig.HandlerConfig
			assertSpoolHandlerRoutingSlices(
				t,
				"loaded",
				[][]string{loaded.OnErrorConfig, loaded.OnWarnConfig, loaded.OnInfoConfig, loaded.OnDebugConfig},
				want,
			)

			mapped := mappedFailure.DurableStoreConfig.SpoolConfig.HandlerConfig
			assertSpoolHandlerRoutingSlices(
				t,
				"mapped",
				[][]string{mapped.OnErrorConfig, mapped.OnWarnConfig, mapped.OnInfoConfig, mapped.OnDebugConfig},
				want,
			)

			adapted := loggerdurable.NewFailureStoreConfigAdapter(mappedFailure).
				DurableStoreConfig().SpoolConfig().Handler()
			assertSpoolHandlerRoutingSlices(
				t,
				"adapted",
				[][]string{adapted.OnError(), adapted.OnWarn(), adapted.OnInfo(), adapted.OnDebug()},
				want,
			)
		})
	}
}

func assertSpoolHandlerRouting(
	t *testing.T,
	stage string,
	onError []string,
	onWarn []string,
	onInfo []string,
	onDebug []string,
	want []string,
) {
	t.Helper()

	routes := [][]string{onError, onWarn, onInfo, onDebug}
	wantRoutes := [][]string{{want[0]}, {want[1]}, {want[2]}, {want[3]}}
	assertSpoolHandlerRoutingSlices(t, stage, routes, wantRoutes)
}

func assertSpoolHandlerRoutingSlices(t *testing.T, stage string, got, want [][]string) {
	t.Helper()

	for i, level := range []string{"error", "warn", "info", "debug"} {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Errorf("%s %s routing = %v, want %v", stage, level, got[i], want[i])
		}
	}
}

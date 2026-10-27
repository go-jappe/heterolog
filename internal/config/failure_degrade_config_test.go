package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/joho/godotenv"

	configfailure "github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

type degradeConfigValues struct {
	enabled    bool
	spillTypes []pkgdurable.SpillType
}

type degradeLoaderCase struct {
	name      string
	enableKey string
	typesKey  string
	load      func() (degradeConfigValues, error)
}

// TestFailureDegradeEnvKeysPerBackendAndContractは、全てのbackendと各LoggerContractKindが
// それぞれ専用のdirect degrade keyを明示的に持つことを検証します。これらのkeyは
// DURABLE_TYPES=noneの場合だけ使われます。overload policyは、これとは別にstoreごとに決めます。
// HETEROLOG_ENABLE_*_DEGRADEの既定値はtrueなので、無効化した場合も検証してkeyの読み取りを確認します。
func TestFailureDegradeEnvKeysPerBackendAndContract(t *testing.T) {
	for _, tt := range failureDegradeLoaderCases() {
		for _, enabled := range []bool{true, false} {
			t.Run(tt.name+"/enabled="+strconv.FormatBool(enabled), func(t *testing.T) {
				t.Setenv(tt.enableKey, strconv.FormatBool(enabled))
				t.Setenv(tt.typesKey, "s3,nfs")
				durableTypesKey := strings.Replace(tt.typesKey, "DEGRADE_TYPES", "DURABLE_TYPES", 1)
				t.Setenv(durableTypesKey, "none")
				if !strings.Contains(tt.name, "delivery") {
					prefix := strings.TrimSuffix(tt.typesKey, "_DEGRADE_TYPES")
					for _, level := range []string{"ERROR", "WARN", "INFO", "DEBUG"} {
						t.Setenv(prefix+"_DURABLE_FILE_"+level+"_HANDLER", "stdout")
					}
				}

				got, err := tt.load()
				if err != nil {
					t.Fatalf("load failure degrade config: %v", err)
				}
				if got.enabled != enabled {
					t.Fatalf("%s = %t, want %t", tt.enableKey, got.enabled, enabled)
				}
				// DEGRADE_TYPESはdirect degradeが有効な場合だけ読み込みます。
				var want []pkgdurable.SpillType
				if enabled {
					want = []pkgdurable.SpillType{
						configfailure.DegradeDestinationS3,
						configfailure.DegradeDestinationNfs,
					}
				}
				assertDegradeSpillTypes(t, got.spillTypes, want)
			})
		}
	}
}

// nonDefaultDegradeSpillTypesは、backendとLoggerContractKindごとに異なる既定以外の
// DEGRADE_TYPESです。この試験のmapperとadapterは値を検証しないため、未実装の予約値も
// 伝搬の確認に使えます。
var nonDefaultDegradeSpillTypes = []string{
	"s3", "nfs", "s3,nfs", "nfs,s3", "stdout,s3", "s3,stdout",
	"stdout,nfs", "nfs,stdout", "s3,nfs,stdout", "nfs,stdout,s3", "stdout,s3,nfs", "stdout,nfs,s3",
}

// TestFixtureEnvLoadsFailureDegradeSettingsは、公開例から生成したfixtureの.envを
// LoadConfig、mapper、provider adapterの境界まで通し、全backendとLoggerContractKindの
// direct degrade設定がruntimeへ届くことを検証します。公開例の値（有効・stdout）は
// code既定値と同じなので、無効化と既定以外の退避先をそれぞれ上書きし、keyを読まない
// loaderでは失敗するようにします。
func TestFixtureEnvLoadsFailureDegradeSettings(t *testing.T) {
	cases := failureDegradeLoaderCases()
	if len(cases) != len(nonDefaultDegradeSpillTypes) {
		t.Fatalf("degrade cases = %d, spill type overrides = %d", len(cases), len(nonDefaultDegradeSpillTypes))
	}
	for _, scenario := range []struct {
		name      string
		overrides func(index int, tt degradeLoaderCase) map[string]string
	}{
		{
			name: "disabled direct degrade",
			overrides: func(_ int, tt degradeLoaderCase) map[string]string {
				return map[string]string{tt.enableKey: "false"}
			},
		},
		{
			name: "non-default spill types",
			overrides: func(index int, tt degradeLoaderCase) map[string]string {
				return map[string]string{tt.typesKey: nonDefaultDegradeSpillTypes[index]}
			},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			repoRoot := configFixtureRootForTest(t)
			envPath := filepath.Join(repoRoot, ".env")
			envFile, err := godotenv.Read(envPath)
			if err != nil {
				t.Fatalf("read fixture .env: %v", err)
			}

			restoreProcessEnvAfterTest(t)
			clearProcessEnvForTest(t)
			if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
				t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
			}
			for key, value := range map[string]string{
				"HETEROLOG_ENABLE_LOGGING":                     "true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING":   "true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING":            "true",
				"HETEROLOG_LOG_TYPE":                           "opensearch,loki,cloudwatchlogs,clickhouse",
				"HETEROLOG_ASYNC_LOG_TYPE":                     "opensearch,loki,cloudwatchlogs,clickhouse",
				"HETEROLOG_DELIVERY_LOG_TYPE":                  "opensearch,loki,cloudwatchlogs,clickhouse",
				"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY":          "unbounded",
				"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY":    "unbounded",
				"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY": "unbounded",
				// direct degradeは、durableな保存先を一つも選択しない場合だけ有効です。
				"HETEROLOG_OPENSEARCH_DURABLE_TYPES":          "none",
				"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES":    "none",
				"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES": "none",
				"HETEROLOG_LOKI_DURABLE_TYPES":                "none",
				"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES":          "none",
				"HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES":       "none",
				"HETEROLOG_CLOUDWATCH_DURABLE_TYPES":          "none",
				"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES":    "none",
				"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES": "none",
				"HETEROLOG_CLICKHOUSE_DURABLE_TYPES":          "none",
				"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES":    "none",
				"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES": "none",
			} {
				if err := os.Setenv(key, value); err != nil {
					t.Fatalf("set %s: %v", key, err)
				}
			}
			for index, tt := range cases {
				overrideFixtureEnvValues(t, envFile, scenario.overrides(index, tt))
			}

			cfg, err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig() with fixture .env and all backend destinations: %v", err)
			}
			loaded := loadedDegradeValues(cfg)
			mapped, adapted := mappedAndAdaptedDegradeValues(t, cfg)

			for _, tt := range cases {
				t.Run(tt.name, func(t *testing.T) {
					wantEnabled := requiredDotEnvBool(t, envFile, tt.enableKey)
					// DEGRADE_TYPESはdirect degradeが有効な場合だけ読み込みます。
					var wantTypes []pkgdurable.SpillType
					if wantEnabled {
						wantTypes = configfailure.ParseSpillTypes(
							strings.Split(requiredDotEnv(t, envFile, tt.typesKey), ","),
						)
					}

					for layer, values := range map[string]map[string]degradeConfigValues{
						"loaded":  loaded,
						"mapped":  mapped,
						"adapted": adapted,
					} {
						got, ok := values[tt.name]
						if !ok {
							t.Fatalf("%s degrade values missing for %s", layer, tt.name)
						}
						if got.enabled != wantEnabled {
							t.Fatalf("%s %s = %t, want %t", layer, tt.enableKey, got.enabled, wantEnabled)
						}
						assertDegradeSpillTypes(t, got.spillTypes, wantTypes)
					}
				})
			}
		})
	}
}

func loadedDegradeValues(cfg AppConfig) map[string]degradeConfigValues {
	values := make(map[string]degradeConfigValues, 12)
	addFailure := func(name string, failureConfig configfailure.FailureHandlerConfig) {
		values[name] = degradeConfigValues{failureConfig.EnableDegrade, failureConfig.SpillType}
	}
	addDelivery := func(name string, failureConfig configfailure.DeliveryFailureHandlerConfig) {
		values[name] = degradeConfigValues{failureConfig.EnableDegrade, failureConfig.SpillType}
	}

	addFailure("opensearch sync", cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig)
	addFailure("opensearch async", cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig)
	addDelivery("opensearch delivery", cfg.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig)
	addFailure("loki sync", cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig)
	addFailure("loki async", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig)
	addDelivery("loki delivery", cfg.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.FailureHandlerConfig)
	addFailure("cloudwatch sync", cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig)
	addFailure("cloudwatch async", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig)
	addDelivery("cloudwatch delivery", cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.FailureHandlerConfig)
	addFailure("clickhouse sync", cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig)
	addFailure("clickhouse async", cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig)
	addDelivery("clickhouse delivery", cfg.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig)
	return values
}

func mappedAndAdaptedDegradeValues(
	t *testing.T,
	cfg AppConfig,
) (map[string]degradeConfigValues, map[string]degradeConfigValues) {
	t.Helper()
	syncConfig := ToLoggerConfig(cfg)
	asyncConfig := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryConfig := ToDeliveryLoggerConfig(cfg)

	if syncConfig.OpenSearch == nil || syncConfig.Loki == nil || syncConfig.CloudWatchLogs == nil || syncConfig.ClickHouse == nil {
		t.Fatal("sync mapper did not build all requested backend configs")
	}
	if asyncConfig.OpenSearch == nil || asyncConfig.Loki == nil || asyncConfig.CloudWatchLogs == nil || asyncConfig.ClickHouse == nil {
		t.Fatal("async mapper did not build all requested backend configs")
	}
	if deliveryConfig.OpenSearch == nil || deliveryConfig.Loki == nil || deliveryConfig.CloudWatchLogs == nil || deliveryConfig.ClickHouse == nil {
		t.Fatal("delivery mapper did not build all requested backend configs")
	}

	mapped := make(map[string]degradeConfigValues, 12)
	adapted := make(map[string]degradeConfigValues, 12)
	addNormal := func(name string, failureConfig pkgdurable.FailureStoreConfig) {
		mapped[name] = degradeConfigValues{failureConfig.EnableDegrade, failureConfig.SpillType}
		adapter := loggerdurable.NewFailureStoreConfigAdapter(failureConfig)
		adapted[name] = degradeConfigValues{adapter.EnableDegrade(), adapter.SpillType()}
	}
	addDelivery := func(name string, failureConfig pkgdurable.DeliveryFailureStoreConfig) {
		mapped[name] = degradeConfigValues{failureConfig.EnableDegrade, failureConfig.SpillType}
		adapter := loggerdurable.NewDeliveryFailureStoreConfigAdapter(failureConfig)
		adapted[name] = degradeConfigValues{adapter.EnableDegrade(), adapter.SpillType()}
	}

	addNormal("opensearch sync", syncConfig.OpenSearch.FailureStoreConfig)
	addNormal("opensearch async", asyncConfig.OpenSearch.FailureStoreConfig)
	addDelivery("opensearch delivery", deliveryConfig.OpenSearch.FailureStoreConfig)
	addNormal("loki sync", syncConfig.Loki.FailureStoreConfig)
	addNormal("loki async", asyncConfig.Loki.FailureStoreConfig)
	addDelivery("loki delivery", deliveryConfig.Loki.FailureStoreConfig)
	addNormal("cloudwatch sync", syncConfig.CloudWatchLogs.FailureStoreConfig)
	addNormal("cloudwatch async", asyncConfig.CloudWatchLogs.FailureStoreConfig)
	addDelivery("cloudwatch delivery", deliveryConfig.CloudWatchLogs.FailureStoreConfig)
	addNormal("clickhouse sync", syncConfig.ClickHouse.FailureStoreConfig)
	addNormal("clickhouse async", asyncConfig.ClickHouse.FailureStoreConfig)
	addDelivery("clickhouse delivery", deliveryConfig.ClickHouse.FailureStoreConfig)
	return mapped, adapted
}

func failureDegradeLoaderCases() []degradeLoaderCase {
	return []degradeLoaderCase{
		{
			name: "opensearch sync", enableKey: "HETEROLOG_ENABLE_OPENSEARCH_DEGRADE", typesKey: "HETEROLOG_OPENSEARCH_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadOpenSearchFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "opensearch async", enableKey: "HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DEGRADE", typesKey: "HETEROLOG_ASYNC_OPENSEARCH_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadAsyncOpenSearchFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "opensearch delivery", enableKey: "HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DEGRADE", typesKey: "HETEROLOG_DELIVERY_OPENSEARCH_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadDeliveryOpenSearchFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "loki sync", enableKey: "HETEROLOG_ENABLE_LOKI_DEGRADE", typesKey: "HETEROLOG_LOKI_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadLokiFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "loki async", enableKey: "HETEROLOG_ENABLE_ASYNC_LOKI_DEGRADE", typesKey: "HETEROLOG_ASYNC_LOKI_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadAsyncLokiFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "loki delivery", enableKey: "HETEROLOG_ENABLE_DELIVERY_LOKI_DEGRADE", typesKey: "HETEROLOG_DELIVERY_LOKI_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadDeliveryLokiFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "cloudwatch sync", enableKey: "HETEROLOG_ENABLE_CLOUDWATCH_DEGRADE", typesKey: "HETEROLOG_CLOUDWATCH_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadCloudWatchLogsFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "cloudwatch async", enableKey: "HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_DEGRADE", typesKey: "HETEROLOG_ASYNC_CLOUDWATCH_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadAsyncCloudWatchLogsFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "cloudwatch delivery", enableKey: "HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_DEGRADE", typesKey: "HETEROLOG_DELIVERY_CLOUDWATCH_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadDeliveryCloudWatchLogsFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "clickhouse sync", enableKey: "HETEROLOG_ENABLE_CLICKHOUSE_DEGRADE", typesKey: "HETEROLOG_CLICKHOUSE_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadClickHouseFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "clickhouse async", enableKey: "HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_DEGRADE", typesKey: "HETEROLOG_ASYNC_CLICKHOUSE_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadAsyncClickHouseFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
		{
			name: "clickhouse delivery", enableKey: "HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_DEGRADE", typesKey: "HETEROLOG_DELIVERY_CLICKHOUSE_DEGRADE_TYPES",
			load: func() (degradeConfigValues, error) {
				cfg, err := loadDeliveryClickHouseFailureHandlerConfig(processEnvironmentForTest())
				return degradeConfigValues{cfg.EnableDegrade, cfg.SpillType}, err
			},
		},
	}
}

func TestFailureDegradeMapperPropagation(t *testing.T) {
	app := &AppConfig{}
	want := []pkgdurable.SpillType{
		configfailure.DegradeDestinationStdout,
		configfailure.DegradeDestinationS3,
	}

	normal := newFailureStoreConfig(
		app,
		checkpointSourceLoggerOpenSearch,
		pkglogger.LoggerContractSync,
		configfailure.FailureHandlerConfig{
			EnableDegrade: true,
			DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			SpillType:     want,
		},
		logconfig.LogBodyConfig{},
	)
	if !normal.EnableDegrade {
		t.Fatal("mapped normal EnableDegrade = false, want true")
	}
	assertDegradeSpillTypes(t, normal.SpillType, want)

	delivery := newDeliveryFailureStoreConfig(
		app,
		checkpointSourceDeliveryLoggerOpenSearch,
		configfailure.DeliveryFailureHandlerConfig{
			EnableDegrade: true,
			DurableType:   []pkgdurable.DurableType{pkgdurable.DurableTypeNone},
			SpillType:     want,
		},
		logconfig.LogBodyConfig{},
	)
	if !delivery.EnableDegrade {
		t.Fatal("mapped delivery EnableDegrade = false, want true")
	}
	assertDegradeSpillTypes(t, delivery.SpillType, want)
}

func assertDegradeSpillTypes(t *testing.T, got, want []pkgdurable.SpillType) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("spill types = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("spill types[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

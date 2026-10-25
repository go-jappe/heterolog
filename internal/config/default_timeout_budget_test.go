package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// minimalNetworkBackendEnvは、network backendごとに出力先へ選ぶ以外で最低限必要な設定です。
// keyにはLoggerContractKindの接頭辞を付けて使います。OpenSearchの既定の認証方式はbasicで
// 認証情報を補完しないため、認証なしを明示します。
var minimalNetworkBackendEnv = map[string]map[string]string{
	"opensearch":     {"HETEROLOG_OPENSEARCH_ENDPOINT": "https://opensearch.example.test", "HETEROLOG_OPENSEARCH_AUTH_TYPE": "none"},
	"loki":           {"HETEROLOG_LOKI_ENDPOINT": "https://loki.example.test"},
	"clickhouse":     {"HETEROLOG_CLICKHOUSE_ENDPOINT": "https://clickhouse.example.test"},
	"cloudwatchlogs": {"HETEROLOG_CLOUDWATCH_LOG_GROUP_NAME": "minimal-group"},
	"slack":          {"HETEROLOG_SLACK_API_KEY": "minimal-token", "HETEROLOG_SLACK_NORMAL_CHANNEL": "#minimal"},
}

// minimalSharedNetworkBackendEnvは、CloudWatch Logsが個別の値の代わりに使うAWSの共通設定です。
var minimalSharedNetworkBackendEnv = map[string]string{
	"AWS_REGION":            "us-east-1",
	"AWS_ACCESS_KEY_ID":     "minimal-access-key",
	"AWS_SECRET_ACCESS_KEY": "minimal-secret-key",
}

// minimalNetworkBackendEnvironmentは、contractsの各LoggerContractKindでbackendsを出力先に選ぶ
// 最小の環境です。選ばないLoggerContractKindは既定のstdoutだけを使います。
func minimalNetworkBackendEnvironment(contracts []string, backends []string) map[string]string {
	env := make(map[string]string)
	for key, value := range minimalSharedNetworkBackendEnv {
		env[key] = value
	}
	for _, prefix := range contracts {
		env["HETEROLOG_"+prefix+"LOG_TYPE"] = strings.Join(backends, ",")
		for _, backend := range backends {
			for key, value := range minimalNetworkBackendEnv[backend] {
				env["HETEROLOG_"+prefix+strings.TrimPrefix(key, "HETEROLOG_")] = value
			}
		}
	}
	return env
}

// loadMinimalEnvConfigは、設定項目を全て外したprocess environmentへenvだけを与えてLoadConfigします。
// 保存先は一時directoryです。
func loadMinimalEnvConfig(t *testing.T, env map[string]string) AppConfig {
	t.Helper()
	root := t.TempDir()
	envPath := filepath.Join(root, "empty.env")
	if err := os.WriteFile(envPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// 一時directoryを作成してから設定項目だけを外します。TMP・TEMPなどの実行環境は残ります。
	testutil.UnsetConfigEnvironment(t)
	t.Setenv("HETEROLOG_ENV_PATH", envPath)
	t.Setenv("HETEROLOG_SYSTEM_LOG_DIR", filepath.Join(root, "log"))
	t.Setenv("HETEROLOG_SYSTEM_LIB_DIR", filepath.Join(root, "lib"))
	t.Setenv("HETEROLOG_APP_STATE_DIR", filepath.Join(root, "state"))
	for key, value := range env {
		t.Setenv(key, value)
	}
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with a minimal environment: %v", err)
	}
	return cfg
}

// TestMinimalNetworkBackendEnvConfigFitsDefaultTimeoutBudgetは、network backendを出力先に選び、
// 接続先など最低限の設定だけを与えた環境が、既定のtimeout budget policy（must_fit）と既定の
// logger・client timeout・retry設定で、設定検証とmapper後の検証を通ることを確かめます。
// 各backendを単独で選ぶ場合と、全backendを同時に選ぶ場合を、sync・async・Deliveryの全てで確認します。
func TestMinimalNetworkBackendEnvConfigFitsDefaultTimeoutBudget(t *testing.T) {
	contracts := map[string]string{"sync": "", "async": "ASYNC_", "delivery": "DELIVERY_"}
	allBackends := []string{"opensearch", "loki", "clickhouse", "cloudwatchlogs", "slack"}
	type testCase struct {
		name      string
		contracts []string
		backends  []string
	}
	var cases []testCase
	for name, prefix := range contracts {
		for _, backend := range allBackends {
			cases = append(cases, testCase{name: name + "/" + backend, contracts: []string{prefix}, backends: []string{backend}})
		}
		cases = append(cases, testCase{name: name + "/all backends", contracts: []string{prefix}, backends: allBackends})
	}
	cases = append(cases, testCase{name: "every contract/all backends", contracts: []string{"", "ASYNC_", "DELIVERY_"}, backends: allBackends})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := loadMinimalEnvConfig(t, minimalNetworkBackendEnvironment(tc.contracts, tc.backends))
			for _, prefix := range tc.contracts {
				destinations, policy := minimalEnvContractTimeoutSettings(cfg, prefix)
				// 既定のpolicyで予算を検証していること、選んだbackendが実際に出力先であることを先に確認します。
				if normalized, err := timeoutbudget.NormalizeTimeoutBudgetPolicy(policy); err != nil ||
					normalized != timeoutbudget.TimeoutBudgetMustFit {
					t.Fatalf("HETEROLOG_%sLOG_TIMEOUT_BUDGET_POLICY default = %q, %v; want must_fit", prefix, policy, err)
				}
				for _, backend := range tc.backends {
					if !destinations.Has(pkglogger.LogOutputDestination(backend)) {
						t.Fatalf("HETEROLOG_%sLOG_TYPE did not select %s: %v", prefix, backend, destinations)
					}
				}
			}
			if err := ValidateApplicationConfig(cfg); err != nil {
				for _, hierarchy := range errortree.All[*TimeoutHierarchyError](err) {
					t.Errorf("default %s %s timeout %s does not exceed the default client retry budget %s",
						hierarchy.Contract.DisplayName(), hierarchy.Backend, hierarchy.ParentTimeout, hierarchy.ClientRetryBudget)
				}
				t.Fatalf("ValidateApplicationConfig() with the minimal environment = %v", err)
			}
			if _, err := MapLoggerConfig(cfg); err != nil {
				t.Fatalf("MapLoggerConfig() with the minimal environment = %v", err)
			}
			if _, err := MapAsyncLoggerConfig(cfg); err != nil {
				t.Fatalf("MapAsyncLoggerConfig() with the minimal environment = %v", err)
			}
			if _, err := MapDeliveryLoggerConfig(cfg); err != nil {
				t.Fatalf("MapDeliveryLoggerConfig() with the minimal environment = %v", err)
			}
		})
	}
}

// minimalEnvContractTimeoutSettingsは、接頭辞prefixのLoggerContractKindの出力先とtimeout budget policyです。
func minimalEnvContractTimeoutSettings(cfg AppConfig, prefix string) (pkglogger.LogOutputDestinations, timeoutbudget.TimeoutBudgetPolicy) {
	switch prefix {
	case "ASYNC_":
		return cfg.AsyncBestEffortMultiLoggerConfig.LogOutputDestinations, cfg.AsyncBestEffortMultiLoggerConfig.TimeoutBudgetPolicy
	case "DELIVERY_":
		return cfg.DeliveryLogger.LogOutputDestinations, cfg.DeliveryLogger.TimeoutBudgetPolicy
	}
	return cfg.Logger.LogOutputDestinations, cfg.Logger.TimeoutBudgetPolicy
}

// TestTimeoutBudgetDefaultsMatchPublicEnvExampleは、timeout予算の検証に使う既定値
// （logger timeout・timeout budget policy・各backend clientのtimeoutとretry設定・DeliveryのSlack
// timeout・OpenSearchのbulk送信方式）が、公開設定例（.env.example）の値と同じことを検証します。
// 公開設定例の値は既定のmust_fitを満たすよう調整済みのため、key を省略した最小の設定も同じ
// 予算で検証されます。OpenSearchのspool cache有効期間も、以前はclient timeoutの既定値を
// 流用していたため、公開設定例の値と同じことを確認します。
func TestTimeoutBudgetDefaultsMatchPublicEnvExample(t *testing.T) {
	example := readPublicEnvExample(t)
	cfg := loadMinimalEnvConfig(t, minimalNetworkBackendEnvironment(
		[]string{"", "ASYNC_", "DELIVERY_"},
		[]string{"opensearch", "loki", "clickhouse", "cloudwatchlogs", "slack"},
	))

	type retryDefaults struct {
		mode       pkglogger.RetryClientMode
		attempts   int
		base, max  time.Duration
		clientWait time.Duration
	}
	type contractDefaults struct {
		prefix  string
		timeout time.Duration
		policy  timeoutbudget.TimeoutBudgetPolicy
		// backendsはenv keyの名前に含まれるbackend名ごとの、client timeoutとretry設定です。
		backends map[string]retryDefaults
	}
	sync, async, delivery := cfg.Logger, cfg.AsyncBestEffortMultiLoggerConfig, cfg.DeliveryLogger
	contracts := []contractDefaults{
		{
			prefix: "", timeout: sync.Timeout, policy: sync.TimeoutBudgetPolicy,
			backends: map[string]retryDefaults{
				"OPENSEARCH": {
					sync.OpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode, sync.OpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxAttempts,
					sync.OpenSearchConfig.OpenSearchRetryAbleClientConfig.BaseDelay, sync.OpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxDelay,
					cfg.OpenSearchClient.Timeout,
				},
				"LOKI": {
					sync.LokiConfig.LokiRetryAbleClientConfig.RetryClientMode, sync.LokiConfig.LokiRetryAbleClientConfig.MaxAttempts,
					sync.LokiConfig.LokiRetryAbleClientConfig.BaseDelay, sync.LokiConfig.LokiRetryAbleClientConfig.MaxDelay,
					cfg.LokiClient.Timeout,
				},
				"CLICKHOUSE": {
					sync.ClickHouseConfig.ClickHouseRetryAbleClientConfig.RetryClientMode, sync.ClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxAttempts,
					sync.ClickHouseConfig.ClickHouseRetryAbleClientConfig.BaseDelay, sync.ClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxDelay,
					cfg.ClickHouseClient.Timeout,
				},
				"CLOUDWATCH": {
					sync.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode, sync.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxAttempts,
					sync.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.BaseDelay, sync.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxDelay,
					cfg.CloudWatchLogsClient.Timeout,
				},
			},
		},
		{
			prefix: "ASYNC_", timeout: async.Timeout, policy: async.TimeoutBudgetPolicy,
			backends: map[string]retryDefaults{
				"OPENSEARCH": {
					async.OpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode, async.OpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxAttempts,
					async.OpenSearchConfig.OpenSearchRetryAbleClientConfig.BaseDelay, async.OpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxDelay,
					cfg.AsyncOpenSearchClient.Timeout,
				},
				"LOKI": {
					async.LokiConfig.LokiRetryAbleClientConfig.RetryClientMode, async.LokiConfig.LokiRetryAbleClientConfig.MaxAttempts,
					async.LokiConfig.LokiRetryAbleClientConfig.BaseDelay, async.LokiConfig.LokiRetryAbleClientConfig.MaxDelay,
					cfg.AsyncLokiClient.Timeout,
				},
				"CLICKHOUSE": {
					async.ClickHouseConfig.ClickHouseRetryAbleClientConfig.RetryClientMode, async.ClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxAttempts,
					async.ClickHouseConfig.ClickHouseRetryAbleClientConfig.BaseDelay, async.ClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxDelay,
					cfg.AsyncClickHouseClient.Timeout,
				},
				"CLOUDWATCH": {
					async.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode, async.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxAttempts,
					async.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.BaseDelay, async.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxDelay,
					cfg.AsyncCloudWatchLogsClient.Timeout,
				},
			},
		},
		{
			prefix: "DELIVERY_", timeout: delivery.Timeout, policy: delivery.TimeoutBudgetPolicy,
			backends: map[string]retryDefaults{
				"OPENSEARCH": {
					delivery.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.RetryClientMode,
					delivery.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxAttempts,
					delivery.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.BaseDelay,
					delivery.DeliveryOpenSearchConfig.OpenSearchRetryAbleClientConfig.MaxDelay,
					cfg.DeliveryOpenSearchClient.Timeout,
				},
				"LOKI": {
					delivery.DeliveryLokiConfig.LokiRetryAbleClientConfig.RetryClientMode, delivery.DeliveryLokiConfig.LokiRetryAbleClientConfig.MaxAttempts,
					delivery.DeliveryLokiConfig.LokiRetryAbleClientConfig.BaseDelay, delivery.DeliveryLokiConfig.LokiRetryAbleClientConfig.MaxDelay,
					cfg.DeliveryLokiClient.Timeout,
				},
				"CLICKHOUSE": {
					delivery.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig.RetryClientMode,
					delivery.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxAttempts,
					delivery.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig.BaseDelay,
					delivery.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig.MaxDelay,
					cfg.DeliveryClickHouseClient.Timeout,
				},
				"CLOUDWATCH": {
					delivery.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.RetryClientMode,
					delivery.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxAttempts,
					delivery.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.BaseDelay,
					delivery.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxDelay,
					cfg.DeliveryCloudWatchLogsClient.Timeout,
				},
			},
		},
	}

	exampleValue := func(t *testing.T, key string) string {
		t.Helper()
		value, ok := example[key]
		if !ok {
			t.Fatalf(".env.example has no %s", key)
		}
		return value
	}
	exampleInt := func(t *testing.T, key string) int {
		t.Helper()
		number, err := strconv.Atoi(exampleValue(t, key))
		if err != nil {
			t.Fatalf(".env.example %s is not an integer: %v", key, err)
		}
		return number
	}
	requireDefault := func(t *testing.T, key string, got, want any) {
		t.Helper()
		if got != want {
			t.Errorf("default %s = %v, want the .env.example value %v", key, got, want)
		}
	}
	for _, contract := range contracts {
		p := contract.prefix
		requireDefault(t, "HETEROLOG_"+p+"LOG_TIMEOUT_SECOND", contract.timeout, time.Duration(exampleInt(t, "HETEROLOG_"+p+"LOG_TIMEOUT_SECOND"))*time.Second)
		requireDefault(t, "HETEROLOG_"+p+"LOG_TIMEOUT_BUDGET_POLICY", contract.policy,
			timeoutbudget.TimeoutBudgetPolicy(exampleValue(t, "HETEROLOG_"+p+"LOG_TIMEOUT_BUDGET_POLICY")))
		for backend, got := range contract.backends {
			requireDefault(t, "HETEROLOG_"+p+backend+"_TIMEOUT_SECOND", got.clientWait,
				time.Duration(exampleInt(t, "HETEROLOG_"+p+backend+"_TIMEOUT_SECOND"))*time.Second)
			requireDefault(t, "HETEROLOG_"+p+backend+"_RETRY_CLIENT_MODE", got.mode,
				pkglogger.RetryClientMode(exampleValue(t, "HETEROLOG_"+p+backend+"_RETRY_CLIENT_MODE")))
			requireDefault(t, "HETEROLOG_MAX_"+p+backend+"_RETRYABLE_CLIENT_ATTEMPTS", got.attempts,
				exampleInt(t, "HETEROLOG_MAX_"+p+backend+"_RETRYABLE_CLIENT_ATTEMPTS"))
			requireDefault(t, "HETEROLOG_"+p+backend+"_RETRYABLE_CLIENT_MILLISECOND", got.base,
				time.Duration(exampleInt(t, "HETEROLOG_"+p+backend+"_RETRYABLE_CLIENT_MILLISECOND"))*time.Millisecond)
			requireDefault(t, "HETEROLOG_MAX_"+p+backend+"_RETRYABLE_CLIENT_DELAY_MILLISECOND", got.max,
				time.Duration(exampleInt(t, "HETEROLOG_MAX_"+p+backend+"_RETRYABLE_CLIENT_DELAY_MILLISECOND"))*time.Millisecond)
		}
	}
	requireDefault(t, "HETEROLOG_DELIVERY_SLACK_TIMEOUT_SECOND", delivery.DeliverySlackConfig.Timeout,
		time.Duration(exampleInt(t, "HETEROLOG_DELIVERY_SLACK_TIMEOUT_SECOND"))*time.Second)
	requireDefault(t, "HETEROLOG_ENABLE_OPENSEARCH_NO_BULK_FLUSH_MODE", strconv.FormatBool(sync.OpenSearchConfig.NoBulkFlushModeEnabled),
		exampleValue(t, "HETEROLOG_ENABLE_OPENSEARCH_NO_BULK_FLUSH_MODE"))
	requireDefault(t, "HETEROLOG_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND",
		sync.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig.Fsstore.SpoolConfig.OverloadPolicyConfig.StatsScanInterval,
		time.Duration(exampleInt(t, "HETEROLOG_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND"))*time.Second)
	requireDefault(t, "HETEROLOG_ASYNC_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND",
		async.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig.Fsstore.SpoolConfig.OverloadPolicyConfig.StatsScanInterval,
		time.Duration(exampleInt(t, "HETEROLOG_ASYNC_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND"))*time.Second)
}

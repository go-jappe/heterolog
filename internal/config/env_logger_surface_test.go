package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	cloudwatchlogscfg "github.com/go-jappe/heterolog/internal/config/cloudwatchlogs"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	lokicfg "github.com/go-jappe/heterolog/internal/config/loki"

	"github.com/joho/godotenv"
)

// TestFixtureEnvLoadsDocumentedLoggerContractValues は、公開例から生成したfixtureの.envに
// 掲載したlogger設定を、LoadConfigの各sync / async best-effort / delivery
// LoggerContractKindまで読み込めることを検証します。fixtureの.env.exampleは同じ公開例から
// 生成した同等のfileなので別途読み込みません。通常の出力先設定に含まれないbackendも
// 検証するため、元ファイルを一度そのまま読み込んだ後、出力先とtimeout policyを上書きします。
// 公開例の値がcode既定値と同じkeyは非既定値でも上書きし、keyを読まないloaderでは失敗する
// ようにします。
func TestFixtureEnvLoadsDocumentedLoggerContractValues(t *testing.T) {
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
	if _, err := LoadConfig(); err != nil {
		t.Fatalf("LoadConfig() with unmodified fixture .env: %v", err)
	}

	setLoggerSurfaceTestDestinations(t)
	overrideFixtureEnvValues(t, envFile, nonDefaultLoggerSurfaceFixtureEnv())
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with all tested destinations from fixture .env: %v", err)
	}

	assertSamplingConfigMatchesEnv(t, "sync Loki", cfg.Logger.LokiConfig.SamplingConfig, envFile, "", "LOKI")
	assertSamplingConfigMatchesEnv(t, "async Loki", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.SamplingConfig, envFile, "ASYNC_", "LOKI")
	assertBoolMatchesEnv(t, "sync Loki push latency", cfg.Logger.LokiConfig.PushLatencyEnabled, envFile, "HETEROLOG_ENABLE_LOKI_PUSH_LATENCY")
	assertBoolMatchesEnv(t, "async Loki push latency", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.PushLatencyEnabled, envFile, "HETEROLOG_ENABLE_ASYNC_LOKI_PUSH_LATENCY")
	assertBoolMatchesEnv(t, "delivery Loki push latency", cfg.DeliveryLogger.DeliveryLokiConfig.PushLatencyEnabled, envFile, "HETEROLOG_ENABLE_DELIVERY_LOKI_PUSH_LATENCY")
	assertLokiClientMatchesEnv(t, "sync Loki", cfg.LokiClient, envFile, "")
	assertLokiClientMatchesEnv(t, "async Loki", cfg.AsyncLokiClient, envFile, "ASYNC_")
	assertLokiClientMatchesEnv(t, "delivery Loki", cfg.DeliveryLokiClient, envFile, "DELIVERY_")

	assertSamplingConfigMatchesEnv(t, "sync CloudWatch Logs", cfg.Logger.CloudWatchLogsConfig.SamplingConfig, envFile, "", "CLOUDWATCH")
	assertSamplingConfigMatchesEnv(t, "async CloudWatch Logs", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.SamplingConfig, envFile, "ASYNC_", "CLOUDWATCH")
	assertBoolMatchesEnv(t, "sync CloudWatch Logs put latency", cfg.Logger.CloudWatchLogsConfig.PutLatencyEnabled, envFile, "HETEROLOG_ENABLE_CLOUDWATCH_PUT_LATENCY")
	assertBoolMatchesEnv(t, "async CloudWatch Logs put latency", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.PutLatencyEnabled, envFile, "HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_PUT_LATENCY")
	assertBoolMatchesEnv(t, "delivery CloudWatch Logs put latency", cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.PutLatencyEnabled, envFile, "HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_PUT_LATENCY")
	assertCloudWatchHeadersMatchEnv(t, "sync CloudWatch Logs", cfg.CloudWatchLogsClient, envFile, "")
	assertCloudWatchHeadersMatchEnv(t, "async CloudWatch Logs", cfg.AsyncCloudWatchLogsClient, envFile, "ASYNC_")
	assertCloudWatchHeadersMatchEnv(t, "delivery CloudWatch Logs", cfg.DeliveryCloudWatchLogsClient, envFile, "DELIVERY_")

	if got, want := cfg.Logger.SlackConfig.SamplingConfig.LoggingByRequestIDRate, requiredDotEnvInt(t, envFile, "HETEROLOG_LOG_SLACK_REQUEST_ID_RATE"); got != want {
		t.Fatalf("sync Slack request ID sampling rate = %d, want %d", got, want)
	}
}

// nonDefaultLoggerSurfaceFixtureEnvは、公開例でcode既定値と同じ値または空値を掲載した
// Loki・CloudWatch Logs・Slackのkeyに、LoggerContractKindごとに異なる非既定値を返します。
func nonDefaultLoggerSurfaceFixtureEnv() map[string]string {
	overrides := map[string]string{"HETEROLOG_LOG_SLACK_REQUEST_ID_RATE": "17"}
	offset := 0
	for _, prefix := range []string{"", "ASYNC_"} {
		for _, backend := range []string{"LOKI", "CLOUDWATCH"} {
			overrides["HETEROLOG_ENABLE_"+prefix+"LOG_"+backend+"_SAMPLING"] = "false"
			overrides["HETEROLOG_ENABLE_"+prefix+"LOGGING_"+backend+"_BY_REQUEST_ID"] = "false"
			overrides["HETEROLOG_"+prefix+"LOG_"+backend+"_REQUEST_ID_RATE"] = strconv.Itoa(21 + offset)
			overrides["HETEROLOG_"+prefix+"LOG_"+backend+"_DEBUG_RATE"] = strconv.Itoa(22 + offset)
			overrides["HETEROLOG_"+prefix+"LOG_"+backend+"_INFO_RATE"] = strconv.Itoa(23 + offset)
			overrides["HETEROLOG_"+prefix+"LOG_"+backend+"_WARN_RATE"] = strconv.Itoa(24 + offset)
			overrides["HETEROLOG_"+prefix+"LOG_"+backend+"_ERROR_RATE"] = strconv.Itoa(25 + offset)
			offset += 10
		}
	}
	for _, contract := range []struct{ name, prefix string }{
		{name: "sync", prefix: ""},
		{name: "async", prefix: "ASYNC_"},
		{name: "delivery", prefix: "DELIVERY_"},
	} {
		overrides["HETEROLOG_ENABLE_"+contract.prefix+"LOKI_PUSH_LATENCY"] = "false"
		overrides["HETEROLOG_ENABLE_"+contract.prefix+"CLOUDWATCH_PUT_LATENCY"] = "false"
		overrides["HETEROLOG_"+contract.prefix+"LOKI_USERNAME"] = contract.name + "-loki-user"
		overrides["HETEROLOG_"+contract.prefix+"LOKI_PASSWORD"] = contract.name + "-loki-password"
		overrides["HETEROLOG_"+contract.prefix+"LOKI_BEARER_TOKEN"] = contract.name + "-loki-token"
	}
	return overrides
}

func TestPublicEnvExampleDocumentsIndependentInternalHandlerRoutingKeys(t *testing.T) {
	prefixes := []string{
		"ASYNC_STDOUT_LOGGER",
		"DELIVERY_FILE_LOGGER",
		"DELIVERY_STDOUT_LOGGER",
		"DELIVERY_SLACK_LOGGER",
		"DELIVERY_OPENSEARCH_LOGGER",
		"DELIVERY_LOKI_LOGGER",
		"DELIVERY_CLOUDWATCH_LOGGER",
		"DELIVERY_CLICKHOUSE_LOGGER",
		"DELIVERY_OPENSEARCH_DURABLE_FILE",
		"DELIVERY_LOKI_DURABLE_FILE",
		"DELIVERY_CLOUDWATCH_DURABLE_FILE",
		"DELIVERY_CLICKHOUSE_DURABLE_FILE",
	}
	levels := []string{"ERROR", "WARN", "INFO", "DEBUG"}

	envFile := readPublicEnvExample(t)
	for _, prefix := range prefixes {
		for _, level := range levels {
			key := "HETEROLOG_" + prefix + "_" + level + "_HANDLER"
			if strings.TrimSpace(envFile[key]) == "" {
				t.Errorf(".env.example does not document a non-empty %s", key)
			}
		}
	}
}

func setLoggerSurfaceTestDestinations(t *testing.T) {
	t.Helper()

	values := map[string]string{
		"HETEROLOG_ENABLE_LOGGING":                     "true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING":   "true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING":            "true",
		"HETEROLOG_LOG_TYPE":                           "loki,cloudwatchlogs,slack",
		"HETEROLOG_ASYNC_LOG_TYPE":                     "loki,cloudwatchlogs,slack",
		"HETEROLOG_DELIVERY_LOG_TYPE":                  "loki,cloudwatchlogs,slack",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY":          "unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY":    "unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY": "unbounded",
	}
	for key, value := range values {
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
}

func assertSamplingConfigMatchesEnv(
	t *testing.T,
	name string,
	got logconfig.SamplingConfig,
	envFile map[string]string,
	scopePrefix string,
	backend string,
) {
	t.Helper()

	assertBoolMatchesEnv(t, name+" sampling enabled", got.IsSamplingEnabled, envFile, "HETEROLOG_ENABLE_"+scopePrefix+"LOG_"+backend+"_SAMPLING")
	assertBoolMatchesEnv(t, name+" request ID sampling enabled", got.EnableLoggingByRequestID, envFile, "HETEROLOG_ENABLE_"+scopePrefix+"LOGGING_"+backend+"_BY_REQUEST_ID")

	keys := []struct {
		name string
		got  int
		key  string
	}{
		{name: "request ID rate", got: got.LoggingByRequestIDRate, key: "HETEROLOG_" + scopePrefix + "LOG_" + backend + "_REQUEST_ID_RATE"},
		{name: "debug rate", got: got.SamplingDebugRate, key: "HETEROLOG_" + scopePrefix + "LOG_" + backend + "_DEBUG_RATE"},
		{name: "info rate", got: got.SamplingInfoRate, key: "HETEROLOG_" + scopePrefix + "LOG_" + backend + "_INFO_RATE"},
		{name: "warn rate", got: got.SamplingWarnRate, key: "HETEROLOG_" + scopePrefix + "LOG_" + backend + "_WARN_RATE"},
		{name: "error rate", got: got.SamplingErrorRate, key: "HETEROLOG_" + scopePrefix + "LOG_" + backend + "_ERROR_RATE"},
	}
	for _, check := range keys {
		if want := requiredDotEnvInt(t, envFile, check.key); check.got != want {
			t.Fatalf("%s %s = %d, want %d", name, check.name, check.got, want)
		}
	}
}

func assertBoolMatchesEnv(
	t *testing.T,
	name string,
	got bool,
	envFile map[string]string,
	key string,
) {
	t.Helper()

	if want := requiredDotEnvBool(t, envFile, key); got != want {
		t.Fatalf("%s = %t, want %t", name, got, want)
	}
}

func assertLokiClientMatchesEnv(
	t *testing.T,
	name string,
	got lokicfg.LokiClientConfig,
	envFile map[string]string,
	scopePrefix string,
) {
	t.Helper()

	username := requiredDotEnv(t, envFile, "HETEROLOG_"+scopePrefix+"LOKI_USERNAME")
	password := requiredDotEnv(t, envFile, "HETEROLOG_"+scopePrefix+"LOKI_PASSWORD")
	if username == "" && password == "" {
		if got.BasicAuth != nil {
			t.Fatalf("%s BasicAuth is non-nil for empty username and password", name)
		}
	} else {
		if got.BasicAuth == nil {
			t.Fatalf("%s BasicAuth is nil for configured credentials", name)
		}
		if got.BasicAuth.Username != username || got.BasicAuth.Password != password {
			t.Fatalf("%s BasicAuth does not match the selected env file", name)
		}
	}

	if got.BearerToken != requiredDotEnv(t, envFile, "HETEROLOG_"+scopePrefix+"LOKI_BEARER_TOKEN") {
		t.Fatalf("%s bearer token does not match the selected env file", name)
	}
	assertHeaderMapMatchesEnv(t, name, got.Headers, envFile, "HETEROLOG_"+scopePrefix+"LOKI_HEADERS")
}

func assertCloudWatchHeadersMatchEnv(
	t *testing.T,
	name string,
	got cloudwatchlogscfg.CloudWatchLogsClientConfig,
	envFile map[string]string,
	scopePrefix string,
) {
	t.Helper()

	assertHeaderMapMatchesEnv(t, name, got.Headers, envFile, "HETEROLOG_"+scopePrefix+"CLOUDWATCH_HEADERS")
}

func assertHeaderMapMatchesEnv(
	t *testing.T,
	name string,
	got map[string]string,
	envFile map[string]string,
	key string,
) {
	t.Helper()

	want := make(map[string]string)
	if err := json.Unmarshal([]byte(requiredDotEnv(t, envFile, key)), &want); err != nil {
		t.Fatalf("parse %s as JSON object: %v", key, err)
	}
	if len(got) != len(want) {
		t.Fatalf("%s headers count = %d, want %d", name, len(got), len(want))
	}
	for header, value := range want {
		if got[header] != value {
			t.Fatalf("%s headers do not match the selected env file", name)
		}
	}
}

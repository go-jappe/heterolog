package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	configclickhouse "github.com/go-jappe/heterolog/internal/config/clickhouse"
	configfailure "github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	loggerdurable "github.com/go-jappe/heterolog/internal/infrastructure/logger/durable"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"

	"github.com/joho/godotenv"
)

// TestLoadConfigReadsFixtureEnvClickHouseContracts reads the fixture .env generated
// from the public example, never the developer's private .env. ClickHouse is not
// listed in the fixture's HETEROLOG_LOG_TYPE values, so this test overrides the three
// destination selectors. The ClickHouse values documented with code-default values
// are also overridden with non-default values, so a loader that ignores a key fails;
// every value is checked before and after mapping.
func TestLoadConfigReadsFixtureEnvClickHouseContracts(t *testing.T) {
	repoRoot := configFixtureRootForTest(t)
	envPath := filepath.Join(repoRoot, ".env")
	envFile, err := godotenv.Read(envPath)
	if err != nil {
		t.Fatalf("read fixture .env: %v", err)
	}

	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)
	for key, value := range map[string]string{
		"HETEROLOG_ENV_PATH":          envPath,
		"HETEROLOG_LOG_TYPE":          "clickhouse",
		"HETEROLOG_ASYNC_LOG_TYPE":    "clickhouse",
		"HETEROLOG_DELIVERY_LOG_TYPE": "clickhouse",
	} {
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	overrideFixtureEnvValues(t, envFile, nonDefaultClickHouseFixtureEnv())

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with fixture .env and ClickHouse selectors: %v", err)
	}
	syncMapped := ToLoggerConfig(cfg)
	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	if syncMapped.ClickHouse == nil || asyncMapped.ClickHouse == nil || deliveryMapped.ClickHouse == nil {
		t.Fatalf("fixture .env did not map every ClickHouse LoggerContractKind: sync=%v async=%v delivery=%v", syncMapped.ClickHouse != nil, asyncMapped.ClickHouse != nil, deliveryMapped.ClickHouse != nil)
	}

	assertFixtureClickHouseClient(t, "sync", envFile, "", cfg.ClickHouseClient, syncMapped.ClickHouseClient)
	assertFixtureClickHouseClient(t, "async", envFile, "ASYNC_", cfg.AsyncClickHouseClient, asyncMapped.ClickHouseClient)
	assertFixtureClickHouseClient(t, "delivery", envFile, "DELIVERY_", cfg.DeliveryClickHouseClient, deliveryMapped.ClickHouseClient)
	assertFixtureClickHouseRetry(t, "sync", envFile, "", cfg.Logger.ClickHouseConfig.ClickHouseRetryAbleClientConfig, syncMapped.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig)
	assertFixtureClickHouseRetry(t, "async", envFile, "ASYNC_", cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseRetryAbleClientConfig, asyncMapped.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig)
	assertFixtureClickHouseRetry(t, "delivery", envFile, "DELIVERY_", cfg.DeliveryLogger.DeliveryClickHouseConfig.ClickHouseRetryAbleClientConfig, deliveryMapped.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig)

	assertFixtureClickHouseFailure(t, "sync", envFile, "", cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig, syncMapped.ClickHouse.FailureStoreConfig)
	assertFixtureClickHouseFailure(t, "async", envFile, "ASYNC_", cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig, asyncMapped.ClickHouse.FailureStoreConfig)
	assertFixtureDeliveryClickHouseFailure(t, envFile, cfg.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig, deliveryMapped.ClickHouse.FailureStoreConfig)

	assertClickHouseValue(t, "fixture sync flush interval", syncMapped.ClickHouse.FlushInterval, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_CLICKHOUSE_FLUSH_INTERVAL_SECOND"))*time.Second)
	assertClickHouseValue(t, "fixture sync max entries", syncMapped.ClickHouse.MaxEntries, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_CLICKHOUSE_INSERT_ENTRIES"))
	assertClickHouseValue(t, "fixture sync buffer flush bytes", syncMapped.ClickHouse.BufferFlushBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_CLICKHOUSE_BUFFER_FLUSH_BYTES"))
	assertClickHouseValue(t, "fixture sync fallback queue max entries", syncMapped.ClickHouse.FallbackQueueMaxEntries, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES"))
	assertClickHouseValue(t, "fixture sync max insert request bytes", syncMapped.ClickHouse.MaxInsertRequestBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_CLICKHOUSE_INSERT_REQUEST_BYTES"))
	assertClickHouseValue(t, "fixture sync latency", syncMapped.ClickHouse.InsertLatencyEnabled, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_CLICKHOUSE_INSERT_LATENCY"))
	assertClickHouseValue(t, "fixture async flush interval", asyncMapped.ClickHouse.FlushInterval, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_ASYNC_CLICKHOUSE_FLUSH_INTERVAL_SECOND"))*time.Second)
	assertClickHouseValue(t, "fixture async max entries", asyncMapped.ClickHouse.MaxEntries, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_ASYNC_CLICKHOUSE_INSERT_ENTRIES"))
	assertClickHouseValue(t, "fixture async buffer flush bytes", asyncMapped.ClickHouse.BufferFlushBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_ASYNC_CLICKHOUSE_BUFFER_FLUSH_BYTES"))
	assertClickHouseValue(t, "fixture async fallback queue max entries", asyncMapped.ClickHouse.FallbackQueueMaxEntries, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_ASYNC_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES"))
	assertClickHouseValue(t, "fixture async max insert request bytes", asyncMapped.ClickHouse.MaxInsertRequestBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_ASYNC_CLICKHOUSE_INSERT_REQUEST_BYTES"))
	assertClickHouseValue(t, "fixture async latency", asyncMapped.ClickHouse.InsertLatencyEnabled, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_INSERT_LATENCY"))
	assertClickHouseValue(t, "fixture delivery flush interval", deliveryMapped.ClickHouse.FlushInterval, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_DELIVERY_CLICKHOUSE_FLUSH_INTERVAL_SECOND"))*time.Second)
	assertClickHouseValue(t, "fixture delivery max entries", deliveryMapped.ClickHouse.MaxEntries, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_DELIVERY_CLICKHOUSE_INSERT_ENTRIES"))
	assertClickHouseValue(t, "fixture delivery buffer flush bytes", deliveryMapped.ClickHouse.BufferFlushBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_DELIVERY_CLICKHOUSE_BUFFER_FLUSH_BYTES"))
	assertClickHouseValue(t, "fixture delivery fallback queue max entries", deliveryMapped.ClickHouse.FallbackQueueMaxEntries, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_DELIVERY_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES"))
	assertClickHouseValue(t, "fixture delivery max insert request bytes", deliveryMapped.ClickHouse.MaxInsertRequestBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_DELIVERY_CLICKHOUSE_INSERT_REQUEST_BYTES"))
	assertClickHouseValue(t, "fixture delivery latency", deliveryMapped.ClickHouse.InsertLatencyEnabled, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_INSERT_LATENCY"))
}

// nonDefaultClickHouseFixtureEnvは、公開例でcode既定値と同じ値を掲載したClickHouseの
// keyに、LoggerContractKindごとに異なる非既定値を返します。
func nonDefaultClickHouseFixtureEnv() map[string]string {
	overrides := make(map[string]string)
	for index, contract := range []struct {
		name, prefix string
		retryMode    pkglogger.RetryClientMode
	}{
		{name: "sync", prefix: "", retryMode: pkglogger.RetryClientModeRetry},
		{name: "async", prefix: "ASYNC_", retryMode: pkglogger.RetryClientModeCircuitBreaker},
		{name: "delivery", prefix: "DELIVERY_", retryMode: pkglogger.RetryClientModeRetryWithBreaker},
	} {
		prefix := contract.prefix
		overrides["HETEROLOG_"+prefix+"CLICKHOUSE_FLUSH_INTERVAL_SECOND"] = strconv.Itoa(7 + index)
		overrides["HETEROLOG_MAX_"+prefix+"CLICKHOUSE_INSERT_ENTRIES"] = strconv.Itoa(1001 + index)
		overrides["HETEROLOG_"+prefix+"CLICKHOUSE_BUFFER_FLUSH_BYTES"] = strconv.Itoa(4194304 + index)
		overrides["HETEROLOG_MAX_"+prefix+"CLICKHOUSE_FALLBACK_QUEUE_ENTRIES"] = strconv.Itoa(2001 + index)
		overrides["HETEROLOG_MAX_"+prefix+"CLICKHOUSE_INSERT_REQUEST_BYTES"] = strconv.Itoa(8388608 + index)
		overrides["HETEROLOG_ENABLE_"+prefix+"CLICKHOUSE_INSERT_LATENCY"] = "false"
		overrides["HETEROLOG_ENABLE_"+prefix+"CLICKHOUSE_COMPRESS_REQUEST"] = "false"
		overrides["HETEROLOG_"+prefix+"CLICKHOUSE_RETRY_CLIENT_MODE"] = string(contract.retryMode)
		overrides["HETEROLOG_MAX_"+prefix+"CLICKHOUSE_RETRYABLE_CLIENT_ATTEMPTS"] = strconv.Itoa(4 + index)
		overrides["HETEROLOG_"+prefix+"CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND"] = strconv.Itoa(211 + index)
		overrides["HETEROLOG_MAX_"+prefix+"CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND"] = strconv.Itoa(1101 + index)
		overrides["HETEROLOG_"+prefix+"CLICKHOUSE_FAILURE_ARCHIVE_DIR"] = contract.name + "-clickhouse-fixture-archive"
		if prefix == "DELIVERY_" {
			// Delivery failure handlerはfileArchive専用で、spoolと並行数のkeyを持ちません。
			continue
		}
		overrides["HETEROLOG_"+prefix+"CLICKHOUSE_FAILURE_SPOOL_DIR"] = contract.name + "-clickhouse-fixture-spool"
		overrides["HETEROLOG_"+prefix+"CLICKHOUSE_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS"] = strconv.Itoa(121 + index)
		overrides["HETEROLOG_USE_MEMORY_PRESSURE_"+prefix+"CLICKHOUSE_SPOOL_OVERLOAD"] = "true"
		overrides["HETEROLOG_MAX_"+prefix+"CLICKHOUSE_FAILURE_HANDLER_CONCURRENT"] = strconv.Itoa(9 + index)
	}
	return overrides
}

func assertFixtureClickHouseClient(
	t *testing.T,
	contractLabel string,
	envFile map[string]string,
	prefix string,
	loaded configclickhouse.ClientConfig,
	mapped *pkgclickhouse.Config,
) {
	t.Helper()
	if mapped == nil {
		t.Fatalf("fixture %s mapped client = nil", contractLabel)
	}
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded endpoint", loaded.Endpoint, requiredDotEnv(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_ENDPOINT"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded database", loaded.Database, requiredDotEnv(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_DATABASE"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded table", loaded.Table, requiredDotEnv(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_TABLE"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded timeout", loaded.Timeout, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_TIMEOUT_SECOND"))*time.Second)
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded compression", loaded.CompressRequest, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_"+prefix+"CLICKHOUSE_COMPRESS_REQUEST"))
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped endpoint", mapped.Endpoint, loaded.Endpoint)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped database", mapped.Database, loaded.Database)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped table", mapped.Table, loaded.Table)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped timeout", mapped.Timeout, loaded.Timeout)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped compression", mapped.CompressRequest, loaded.CompressRequest)
}

func assertFixtureClickHouseRetry(
	t *testing.T,
	contractLabel string,
	envFile map[string]string,
	prefix string,
	loaded logconfig.ClickHouseRetryAbleClientConfig,
	mapped *pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig,
) {
	t.Helper()
	if mapped == nil {
		t.Fatalf("fixture %s mapped retry config = nil", contractLabel)
	}
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded retry mode", string(loaded.RetryClientMode), requiredDotEnv(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_RETRY_CLIENT_MODE"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded retry attempts", loaded.MaxAttempts, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_"+prefix+"CLICKHOUSE_RETRYABLE_CLIENT_ATTEMPTS"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded retry base delay", loaded.BaseDelay, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND"))*time.Millisecond)
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded retry max delay", loaded.MaxDelay, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_"+prefix+"CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND"))*time.Millisecond)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped retry mode", mapped.RetryClientMode, loaded.RetryClientMode)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped retry attempts", mapped.MaxAttempts, loaded.MaxAttempts)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped retry base delay", mapped.BaseDelay, loaded.BaseDelay)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped retry max delay", mapped.MaxDelay, loaded.MaxDelay)
}

func assertFixtureClickHouseFailure(
	t *testing.T,
	contractLabel string,
	envFile map[string]string,
	prefix string,
	loaded configfailure.FailureHandlerConfig,
	mapped pkgdurable.FailureStoreConfig,
) {
	t.Helper()
	archive := loaded.Fsstore.FileArchiveConfig
	spool := loaded.Fsstore.SpoolConfig
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded failure max concurrent", loaded.MaxConcurrent, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_"+prefix+"CLICKHOUSE_FAILURE_HANDLER_CONCURRENT"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded archive dir", archive.Dir, requiredDotEnv(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_FAILURE_ARCHIVE_DIR"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded spool dir", spool.Dir, requiredDotEnv(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_FAILURE_SPOOL_DIR"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded spool backlog warn after days", spool.OverloadPolicyConfig.ActiveBacklogWarnAfterDays, requiredDotEnvInt(t, envFile, "HETEROLOG_"+prefix+"CLICKHOUSE_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS"))
	assertClickHouseValue(t, "fixture "+contractLabel+" loaded memory pressure", spool.OverloadPolicyConfig.UseMemoryPressureForOverload, requiredDotEnvBool(t, envFile, "HETEROLOG_USE_MEMORY_PRESSURE_"+prefix+"CLICKHOUSE_SPOOL_OVERLOAD"))
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped failure max concurrent", mapped.MaxConcurrent, loaded.MaxConcurrent)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped archive dir", mapped.DurableStoreConfig.FileArchiveConfig.Dir, archive.Dir)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped spool dir", mapped.DurableStoreConfig.SpoolConfig.Dir, spool.Dir)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped spool backlog warn after days", mapped.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.ActiveBacklogWarnAfterDays, spool.OverloadPolicyConfig.ActiveBacklogWarnAfterDays)
	assertClickHouseValue(t, "fixture "+contractLabel+" mapped memory pressure", mapped.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload, spool.OverloadPolicyConfig.UseMemoryPressureForOverload)
}

func assertFixtureDeliveryClickHouseFailure(
	t *testing.T,
	envFile map[string]string,
	loaded configfailure.DeliveryFailureHandlerConfig,
	mapped pkgdurable.DeliveryFailureStoreConfig,
) {
	t.Helper()
	archive := loaded.FileArchiveConfig
	assertClickHouseValue(t, "fixture delivery loaded durable types", loaded.DurableType, []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive})
	assertClickHouseValue(t, "fixture delivery loaded archive dir", archive.Dir, requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR"))
	assertClickHouseValue(t, "fixture delivery mapped archive dir", mapped.FileArchiveConfig.Dir, archive.Dir)
}

// TestLoadConfigMapsClickHouseSettingsFromMatchingEnvContract verifies the complete
// .env -> LoadConfig -> pkg/logger DTO -> infrastructure adapter path. Values are
// intentionally different for sync, async best-effort, and delivery so that a cross-contract
// reference cannot pass merely because the repository's normal values coincide.
func TestLoadConfigMapsClickHouseSettingsFromMatchingEnvContract(t *testing.T) {
	root := t.TempDir()
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_APP_NAME=clickhouse-env-contract-test",
		"HETEROLOG_TARGET=clickhouse-env-contract-target",
		"HETEROLOG_APP_STATE_DIR=" + filepath.Join(root, "state"),
		"HETEROLOG_SYSTEM_LIB_DIR=" + filepath.Join(root, "lib"),
		"HETEROLOG_SYSTEM_LOG_DIR=" + filepath.Join(root, "log"),
		// 時刻設定もcode既定値（ローカル時刻無効・UTC）と異なる値で読み取りを確認します。
		"HETEROLOG_ENABLE_LOCAL_TIME_LOG=true",
		"HETEROLOG_TIMEZONE_LOG=" + clickHouseContractTimeZone,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=clickhouse",
		"HETEROLOG_ASYNC_LOG_TYPE=clickhouse",
		"HETEROLOG_DELIVERY_LOG_TYPE=clickhouse",
		"HETEROLOG_LOG_TIMEOUT_SECOND=60",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND=61",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND=62",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",

		"HETEROLOG_CLICKHOUSE_LOG_LEVEL=ERROR",
		"HETEROLOG_CLICKHOUSE_FLUSH_INTERVAL_SECOND=11",
		"HETEROLOG_MAX_CLICKHOUSE_INSERT_ENTRIES=101",
		"HETEROLOG_CLICKHOUSE_BUFFER_FLUSH_BYTES=10201",
		"HETEROLOG_MAX_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES=110",
		"HETEROLOG_MAX_CLICKHOUSE_INSERT_REQUEST_BYTES=10202",
		"HETEROLOG_ENABLE_LOG_CLICKHOUSE_SAMPLING=true",
		"HETEROLOG_ENABLE_LOGGING_CLICKHOUSE_BY_REQUEST_ID=false",
		"HETEROLOG_LOG_CLICKHOUSE_REQUEST_ID_RATE=12",
		"HETEROLOG_LOG_CLICKHOUSE_DEBUG_RATE=13",
		"HETEROLOG_LOG_CLICKHOUSE_INFO_RATE=14",
		"HETEROLOG_LOG_CLICKHOUSE_WARN_RATE=15",
		"HETEROLOG_LOG_CLICKHOUSE_ERROR_RATE=16",
		"HETEROLOG_ENABLE_CLICKHOUSE_INSERT_LATENCY=true",
		"HETEROLOG_CLICKHOUSE_LOGGER_ERROR_HANDLER=sync-error",
		"HETEROLOG_CLICKHOUSE_LOGGER_WARN_HANDLER=sync-warn",
		"HETEROLOG_CLICKHOUSE_LOGGER_INFO_HANDLER=sync-info",
		"HETEROLOG_CLICKHOUSE_LOGGER_DEBUG_HANDLER=sync-debug",
		"HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_MAX_CLICKHOUSE_RETRYABLE_CLIENT_ATTEMPTS=2",
		"HETEROLOG_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND=17",
		"HETEROLOG_MAX_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND=18",
		"HETEROLOG_CLICKHOUSE_ENDPOINT=http://sync-clickhouse:8123",
		"HETEROLOG_CLICKHOUSE_DATABASE=sync_database",
		"HETEROLOG_CLICKHOUSE_TABLE=sync_table",
		"HETEROLOG_CLICKHOUSE_TIMEOUT_SECOND=19",
		"HETEROLOG_CLICKHOUSE_USERNAME=sync_user",
		"HETEROLOG_CLICKHOUSE_PASSWORD=sync_password",
		`HETEROLOG_CLICKHOUSE_HEADERS={"X-Scope":"sync"}`,
		"HETEROLOG_ENABLE_CLICKHOUSE_COMPRESS_REQUEST=true",

		"HETEROLOG_ASYNC_CLICKHOUSE_LOG_LEVEL=WARN",
		"HETEROLOG_ASYNC_CLICKHOUSE_FLUSH_INTERVAL_SECOND=21",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_INSERT_ENTRIES=201",
		"HETEROLOG_ASYNC_CLICKHOUSE_BUFFER_FLUSH_BYTES=20201",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES=220",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_INSERT_REQUEST_BYTES=20202",
		"HETEROLOG_ENABLE_ASYNC_LOG_CLICKHOUSE_SAMPLING=false",
		"HETEROLOG_ENABLE_ASYNC_LOGGING_CLICKHOUSE_BY_REQUEST_ID=true",
		"HETEROLOG_ASYNC_LOG_CLICKHOUSE_REQUEST_ID_RATE=22",
		"HETEROLOG_ASYNC_LOG_CLICKHOUSE_DEBUG_RATE=23",
		"HETEROLOG_ASYNC_LOG_CLICKHOUSE_INFO_RATE=24",
		"HETEROLOG_ASYNC_LOG_CLICKHOUSE_WARN_RATE=25",
		"HETEROLOG_ASYNC_LOG_CLICKHOUSE_ERROR_RATE=26",
		"HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_INSERT_LATENCY=false",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_ERROR_HANDLER=async-error",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_WARN_HANDLER=async-warn",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_INFO_HANDLER=async-info",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_DEBUG_HANDLER=async-debug",
		"HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE=circuit_breaker",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_ATTEMPTS=3",
		"HETEROLOG_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND=27",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND=28",
		"HETEROLOG_ASYNC_CLICKHOUSE_ENDPOINT=http://async-clickhouse:8123",
		"HETEROLOG_ASYNC_CLICKHOUSE_DATABASE=async_database",
		"HETEROLOG_ASYNC_CLICKHOUSE_TABLE=async_table",
		"HETEROLOG_ASYNC_CLICKHOUSE_TIMEOUT_SECOND=29",
		"HETEROLOG_ASYNC_CLICKHOUSE_USERNAME=async_user",
		"HETEROLOG_ASYNC_CLICKHOUSE_PASSWORD=async_password",
		`HETEROLOG_ASYNC_CLICKHOUSE_HEADERS={"X-Scope":"async"}`,
		"HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_COMPRESS_REQUEST=false",

		"HETEROLOG_DELIVERY_CLICKHOUSE_FLUSH_INTERVAL_SECOND=31",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_INSERT_ENTRIES=301",
		"HETEROLOG_DELIVERY_CLICKHOUSE_BUFFER_FLUSH_BYTES=30201",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_FALLBACK_QUEUE_ENTRIES=330",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_INSERT_REQUEST_BYTES=30202",
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_INSERT_LATENCY=true",
		"HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE=retry_policy_with_breaker",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_ATTEMPTS=4",
		"HETEROLOG_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND=37",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND=38",
		"HETEROLOG_DELIVERY_CLICKHOUSE_ENDPOINT=http://delivery-clickhouse:8123",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DATABASE=delivery_database",
		"HETEROLOG_DELIVERY_CLICKHOUSE_TABLE=delivery_table",
		"HETEROLOG_DELIVERY_CLICKHOUSE_TIMEOUT_SECOND=39",
		"HETEROLOG_DELIVERY_CLICKHOUSE_USERNAME=delivery_user",
		"HETEROLOG_DELIVERY_CLICKHOUSE_PASSWORD=delivery_password",
		`HETEROLOG_DELIVERY_CLICKHOUSE_HEADERS={"X-Scope":"delivery"}`,
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_COMPRESS_REQUEST=true",

		"HETEROLOG_MAX_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=41",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR=sync-archive",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=drop",
		"HETEROLOG_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=stdout",
		"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR=sync-spool",
		"HETEROLOG_CLICKHOUSE_SPOOL_OVERLOAD_POLICY=degrade",
		"HETEROLOG_CLICKHOUSE_SPOOL_SECONDARY_DESTINATION=s3",
		"HETEROLOG_CLICKHOUSE_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS=42",
		"HETEROLOG_USE_MEMORY_PRESSURE_CLICKHOUSE_SPOOL_OVERLOAD=true",

		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=51",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES=fileArchive,spool",
		"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_ARCHIVE_DIR=async-archive",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=degrade",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=s3",
		"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_SPOOL_DIR=async-spool",
		"HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_OVERLOAD_POLICY=drop",
		"HETEROLOG_ASYNC_CLICKHOUSE_SPOOL_SECONDARY_DESTINATION=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS=52",
		"HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_CLICKHOUSE_SPOOL_OVERLOAD=false",

		"MAX_DELIVERY_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=61",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES=fileArchive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR=delivery-archive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=drop",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=nfs",
		"DELIVERY_CLICKHOUSE_FAILURE_SPOOL_DIR=delivery-spool",
		"DELIVERY_CLICKHOUSE_SPOOL_OVERLOAD_POLICY=degrade",
		"DELIVERY_CLICKHOUSE_SPOOL_SECONDARY_DESTINATION=nfs",
		"USE_MEMORY_PRESSURE_DELIVERY_CLICKHOUSE_SPOOL_OVERLOAD=true",
	})

	syncMapped := ToLoggerConfig(cfg)
	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	if syncMapped.ClickHouse == nil || asyncMapped.ClickHouse == nil || deliveryMapped.ClickHouse == nil {
		t.Fatalf("mapped ClickHouse configs must all be non-nil: sync=%v async=%v delivery=%v", syncMapped.ClickHouse != nil, asyncMapped.ClickHouse != nil, deliveryMapped.ClickHouse != nil)
	}

	assertClickHouseMappedConfig(t, "sync", syncMapped.ClickHouse, syncMapped.ClickHouseClient, clickHouseMappedExpectation{
		logLevel: pkglogger.LogLevelError, flushInterval: 11 * time.Second, maxEntries: 101, bufferFlushBytes: 10201, fallbackQueueMaxEntries: 110, maxInsertRequestBytes: 10202,
		samplingEnabled: true, requestIDEnabled: false, requestIDRate: 12,
		debugRate: 13, infoRate: 14, warnRate: 15, errorRate: 16,
		latencyEnabled: true, handlers: []string{"sync-error", "sync-warn", "sync-info", "sync-debug"},
		retryMode: pkglogger.RetryClientModeRetry, retryAttempts: 2, baseDelay: 17 * time.Millisecond, maxDelay: 18 * time.Millisecond,
		endpoint: "http://sync-clickhouse:8123", database: "sync_database", table: "sync_table", timeout: 19 * time.Second,
		username: "sync_user", password: "sync_password", headers: map[string]string{"X-Scope": "sync"}, compressRequest: true,
		maxConcurrent: 41,
		archiveDir:    "sync-archive", spoolDir: "sync-spool", spoolActiveBacklogWarnAfterDays: 42, useMemoryPressure: true,
	})
	assertClickHouseMappedConfig(t, "async", asyncMapped.ClickHouse, asyncMapped.ClickHouseClient, clickHouseMappedExpectation{
		logLevel: pkglogger.LogLevelWarn, flushInterval: 21 * time.Second, maxEntries: 201, bufferFlushBytes: 20201, fallbackQueueMaxEntries: 220, maxInsertRequestBytes: 20202,
		samplingEnabled: false, requestIDEnabled: true, requestIDRate: 22,
		debugRate: 23, infoRate: 24, warnRate: 25, errorRate: 26,
		latencyEnabled: false, handlers: []string{"async-error", "async-warn", "async-info", "async-debug"},
		retryMode: pkglogger.RetryClientModeCircuitBreaker, retryAttempts: 3, baseDelay: 27 * time.Millisecond, maxDelay: 28 * time.Millisecond,
		endpoint: "http://async-clickhouse:8123", database: "async_database", table: "async_table", timeout: 29 * time.Second,
		username: "async_user", password: "async_password", headers: map[string]string{"X-Scope": "async"}, compressRequest: false,
		maxConcurrent: 51,
		archiveDir:    "async-archive", spoolDir: "async-spool", spoolActiveBacklogWarnAfterDays: 52, useMemoryPressure: false,
	})
	assertDeliveryClickHouseMappedConfig(t, deliveryMapped.ClickHouse, deliveryMapped.ClickHouseClient, clickHouseMappedExpectation{
		flushInterval: 31 * time.Second, maxEntries: 301, bufferFlushBytes: 30201, fallbackQueueMaxEntries: 330, maxInsertRequestBytes: 30202, latencyEnabled: true,
		retryMode: pkglogger.RetryClientModeRetryPolicyWithBreaker, retryAttempts: 4, baseDelay: 37 * time.Millisecond, maxDelay: 38 * time.Millisecond,
		endpoint: "http://delivery-clickhouse:8123", database: "delivery_database", table: "delivery_table", timeout: 39 * time.Second,
		username: "delivery_user", password: "delivery_password", headers: map[string]string{"X-Scope": "delivery"}, compressRequest: true,
		archiveDir: "delivery-archive",
	})
}

// clickHouseContractTimeZoneは、既定のUTCと異なる表示用timezoneです。
const clickHouseContractTimeZone = "America/New_York"

type clickHouseMappedExpectation struct {
	logLevel                                                         pkglogger.LogLevel
	flushInterval                                                    time.Duration
	maxEntries                                                       int
	bufferFlushBytes, fallbackQueueMaxEntries, maxInsertRequestBytes int
	samplingEnabled, requestIDEnabled                                bool
	requestIDRate, debugRate, infoRate, warnRate, errorRate          int
	latencyEnabled                                                   bool
	handlers                                                         []string
	retryMode                                                        pkglogger.RetryClientMode
	retryAttempts                                                    int
	baseDelay, maxDelay                                              time.Duration
	endpoint, database, table                                        string
	timeout                                                          time.Duration
	username, password                                               string
	headers                                                          map[string]string
	compressRequest                                                  bool
	maxConcurrent                                                    int
	archiveDir, spoolDir                                             string
	spoolActiveBacklogWarnAfterDays                                  int
	useMemoryPressure                                                bool
}

func assertClickHouseMappedConfig(t *testing.T, contractLabel string, got *pkglogger.ClickHouseConfig, client *pkgclickhouse.Config, want clickHouseMappedExpectation) {
	t.Helper()
	assertClickHouseCommonBase(t, contractLabel, got.ClickHouseLoggerBaseConfig, want)
	assertClickHouseFlushAndClient(t, contractLabel, got.ClickHouseFlushManagerConfig, got.ClickHouseRetryableClientCaseTransportErrorConfig, client, want)

	adapter := clickhouselogger.NewClickHouseLoggerConfigAdapter(got.ClickHouseLoggerBaseConfig)
	assertClickHouseValue(t, contractLabel+" adapter Runtime.LogLevel", adapter.Runtime().LogLevel(), want.logLevel)
	assertClickHouseValue(t, contractLabel+" adapter FlushInterval", adapter.FlushInterval(), want.flushInterval)
	assertClickHouseValue(t, contractLabel+" adapter MaxEntries", adapter.MaxEntries(), want.maxEntries)
	assertClickHouseValue(t, contractLabel+" adapter BufferFlushBytes", adapter.BufferFlushBytes(), want.bufferFlushBytes)
	assertClickHouseValue(t, contractLabel+" adapter FallbackQueueMaxEntries", adapter.FallbackQueueMaxEntries(), want.fallbackQueueMaxEntries)
	assertClickHouseValue(t, contractLabel+" adapter MaxInsertRequestBytes", adapter.MaxInsertRequestBytes(), want.maxInsertRequestBytes)
	assertClickHouseValue(t, contractLabel+" adapter Sampling.DebugRate", adapter.Sampling().SamplingDebugRate(), want.debugRate)
	assertClickHouseValue(t, contractLabel+" adapter Handler.Error", adapter.Handler().OnError(), []string{want.handlers[0]})

	flushAdapter := clickhouselogger.NewFlushManagerConfigAdapter(got.ClickHouseFlushManagerConfig)
	assertClickHouseValue(t, contractLabel+" adapter InsertLatencyEnabled", flushAdapter.InsertLatencyEnabled(), want.latencyEnabled)
	assertClickHouseFailureAdapter(t, contractLabel, flushAdapter.FailureStoreConfig(), want)
}

func assertDeliveryClickHouseMappedConfig(t *testing.T, got *pkglogger.DeliveryClickHouseConfig, client *pkgclickhouse.Config, want clickHouseMappedExpectation) {
	t.Helper()
	assertClickHouseValue(t, "delivery Time.Local", got.TimeConfig.IsLocalTimeEnabled, true)
	assertClickHouseValue(t, "delivery Time.Zone", got.TimeConfig.TimeZone, clickHouseContractTimeZone)
	assertClickHouseValue(t, "delivery Runtime.AppName", got.RuntimeConfig.AppName, "clickhouse-env-contract-test")
	assertClickHouseValue(t, "delivery Runtime.Target", got.RuntimeConfig.Target, "clickhouse-env-contract-target")
	assertClickHouseValue(t, "delivery FlushInterval", got.FlushInterval, want.flushInterval)
	assertClickHouseValue(t, "delivery MaxEntries", got.MaxEntries, want.maxEntries)
	assertClickHouseValue(t, "delivery BufferFlushBytes", got.BufferFlushBytes, want.bufferFlushBytes)
	assertClickHouseValue(t, "delivery FallbackQueueMaxEntries", got.FallbackQueueMaxEntries, want.fallbackQueueMaxEntries)
	assertClickHouseValue(t, "delivery MaxInsertRequestBytes", got.MaxInsertRequestBytes, want.maxInsertRequestBytes)
	assertDeliveryClickHouseFlushAndClient(t, got.DeliveryClickHouseFlushManagerConfig, got.ClickHouseRetryableClientCaseTransportErrorConfig, client, want)

	adapter := clickhouselogger.NewDeliveryClickHouseLoggerConfigAdapter(got.DeliveryClickHouseLoggerBaseConfig)
	assertClickHouseValue(t, "delivery adapter FlushInterval", adapter.FlushInterval(), want.flushInterval)
	assertClickHouseValue(t, "delivery adapter MaxEntries", adapter.MaxEntries(), want.maxEntries)
	assertClickHouseValue(t, "delivery adapter BufferFlushBytes", adapter.BufferFlushBytes(), want.bufferFlushBytes)
	assertClickHouseValue(t, "delivery adapter FallbackQueueMaxEntries", adapter.FallbackQueueMaxEntries(), want.fallbackQueueMaxEntries)
	assertClickHouseValue(t, "delivery adapter MaxInsertRequestBytes", adapter.MaxInsertRequestBytes(), want.maxInsertRequestBytes)
	flushAdapter := clickhouselogger.NewDeliveryFlushManagerConfigAdapter(got.DeliveryClickHouseFlushManagerConfig)
	assertClickHouseValue(t, "delivery adapter InsertLatencyEnabled", flushAdapter.InsertLatencyEnabled(), want.latencyEnabled)
	assertDeliveryClickHouseFailureAdapter(t, flushAdapter.DeliveryFailureStoreConfig(), want)
}

func assertClickHouseCommonBase(t *testing.T, contractLabel string, got pkglogger.ClickHouseLoggerBaseConfig, want clickHouseMappedExpectation) {
	t.Helper()
	assertClickHouseValue(t, contractLabel+" Time.Local", got.TimeConfig.IsLocalTimeEnabled, true)
	assertClickHouseValue(t, contractLabel+" Time.Zone", got.TimeConfig.TimeZone, clickHouseContractTimeZone)
	assertClickHouseValue(t, contractLabel+" Runtime.AppName", got.RuntimeConfig.AppName, "clickhouse-env-contract-test")
	assertClickHouseValue(t, contractLabel+" Runtime.Target", got.RuntimeConfig.Target, "clickhouse-env-contract-target")
	assertClickHouseValue(t, contractLabel+" Runtime.LogLevel", got.RuntimeConfig.LogLevel, want.logLevel)
	assertClickHouseValue(t, contractLabel+" FlushInterval", got.FlushInterval, want.flushInterval)
	assertClickHouseValue(t, contractLabel+" MaxEntries", got.MaxEntries, want.maxEntries)
	assertClickHouseValue(t, contractLabel+" BufferFlushBytes", got.BufferFlushBytes, want.bufferFlushBytes)
	assertClickHouseValue(t, contractLabel+" FallbackQueueMaxEntries", got.FallbackQueueMaxEntries, want.fallbackQueueMaxEntries)
	assertClickHouseValue(t, contractLabel+" MaxInsertRequestBytes", got.MaxInsertRequestBytes, want.maxInsertRequestBytes)
	assertClickHouseValue(t, contractLabel+" Sampling.Enabled", got.SamplingConfig.IsSamplingEnabled, want.samplingEnabled)
	assertClickHouseValue(t, contractLabel+" Sampling.RequestIDEnabled", got.SamplingConfig.EnableLoggingByRequestID, want.requestIDEnabled)
	assertClickHouseValue(t, contractLabel+" Sampling.RequestIDRate", got.SamplingConfig.LoggingByRequestIDRate, want.requestIDRate)
	assertClickHouseValue(t, contractLabel+" Sampling.DebugRate", got.SamplingConfig.SamplingDebugRate, want.debugRate)
	assertClickHouseValue(t, contractLabel+" Sampling.InfoRate", got.SamplingConfig.SamplingInfoRate, want.infoRate)
	assertClickHouseValue(t, contractLabel+" Sampling.WarnRate", got.SamplingConfig.SamplingWarnRate, want.warnRate)
	assertClickHouseValue(t, contractLabel+" Sampling.ErrorRate", got.SamplingConfig.SamplingErrorRate, want.errorRate)
	assertClickHouseValue(t, contractLabel+" Handler.Error", got.HandlerConfig.OnErrorConfig, []string{want.handlers[0]})
	assertClickHouseValue(t, contractLabel+" Handler.Warn", got.HandlerConfig.OnWarnConfig, []string{want.handlers[1]})
	assertClickHouseValue(t, contractLabel+" Handler.Info", got.HandlerConfig.OnInfoConfig, []string{want.handlers[2]})
	assertClickHouseValue(t, contractLabel+" Handler.Debug", got.HandlerConfig.OnDebugConfig, []string{want.handlers[3]})
}

type clickHouseFlushConfig interface {
	~struct {
		FailureStoreConfig         pkgdurable.FailureStoreConfig
		Resource                   pkglogger.Resource
		PostWarnThresholdDuration  time.Duration
		PostErrorThresholdDuration time.Duration
		InsertLatencyEnabled       bool
	}
}

func assertClickHouseFlushAndClient[T clickHouseFlushConfig](
	t *testing.T,
	contractLabel string,
	got T,
	retry *pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig,
	client *pkgclickhouse.Config,
	want clickHouseMappedExpectation,
) {
	t.Helper()
	// sync/asyncのClickHouse flush DTOは同じ型を共有します。
	flush := struct {
		FailureStoreConfig         pkgdurable.FailureStoreConfig
		Resource                   pkglogger.Resource
		PostWarnThresholdDuration  time.Duration
		PostErrorThresholdDuration time.Duration
		InsertLatencyEnabled       bool
	}(got)
	assertClickHouseValue(t, contractLabel+" InsertLatencyEnabled", flush.InsertLatencyEnabled, want.latencyEnabled)
	assertClickHouseValue(t, contractLabel+" Resource.ServiceName", flush.Resource.ServiceName, "clickhouse-env-contract-test")
	assertClickHouseFailureDTO(t, contractLabel, flush.FailureStoreConfig, want)

	if retry == nil {
		t.Fatalf("%s retry config = nil", contractLabel)
	}
	assertClickHouseValue(t, contractLabel+" Retry.Mode", retry.RetryClientMode, want.retryMode)
	assertClickHouseValue(t, contractLabel+" Retry.Attempts", retry.MaxAttempts, want.retryAttempts)
	assertClickHouseValue(t, contractLabel+" Retry.BaseDelay", retry.BaseDelay, want.baseDelay)
	assertClickHouseValue(t, contractLabel+" Retry.MaxDelay", retry.MaxDelay, want.maxDelay)

	if client == nil {
		t.Fatalf("%s client config = nil", contractLabel)
	}
	assertClickHouseValue(t, contractLabel+" Client.Endpoint", client.Endpoint, want.endpoint)
	assertClickHouseValue(t, contractLabel+" Client.Database", client.Database, want.database)
	assertClickHouseValue(t, contractLabel+" Client.Table", client.Table, want.table)
	assertClickHouseValue(t, contractLabel+" Client.Timeout", client.Timeout, want.timeout)
	assertClickHouseValue(t, contractLabel+" Client.Username", client.Username, want.username)
	assertClickHouseValue(t, contractLabel+" Client.Password", client.Password, want.password)
	assertClickHouseValue(t, contractLabel+" Client.Headers", client.Headers, want.headers)
	assertClickHouseValue(t, contractLabel+" Client.CompressRequest", client.CompressRequest, want.compressRequest)
}

func assertDeliveryClickHouseFlushAndClient(
	t *testing.T,
	got pkglogger.DeliveryClickHouseFlushManagerConfig,
	retry *pkglogger.ClickHouseRetryableClientCaseTransportErrorConfig,
	client *pkgclickhouse.Config,
	want clickHouseMappedExpectation,
) {
	t.Helper()
	assertClickHouseValue(t, "delivery InsertLatencyEnabled", got.InsertLatencyEnabled, want.latencyEnabled)
	assertClickHouseValue(t, "delivery Resource.ServiceName", got.Resource.ServiceName, "clickhouse-env-contract-test")
	assertDeliveryClickHouseFailureDTO(t, got.FailureStoreConfig, want)

	if retry == nil {
		t.Fatal("delivery retry config = nil")
	}
	assertClickHouseValue(t, "delivery Retry.Mode", retry.RetryClientMode, want.retryMode)
	assertClickHouseValue(t, "delivery Retry.Attempts", retry.MaxAttempts, want.retryAttempts)
	assertClickHouseValue(t, "delivery Retry.BaseDelay", retry.BaseDelay, want.baseDelay)
	assertClickHouseValue(t, "delivery Retry.MaxDelay", retry.MaxDelay, want.maxDelay)

	if client == nil {
		t.Fatal("delivery client config = nil")
	}
	assertClickHouseValue(t, "delivery Client.Endpoint", client.Endpoint, want.endpoint)
	assertClickHouseValue(t, "delivery Client.Database", client.Database, want.database)
	assertClickHouseValue(t, "delivery Client.Table", client.Table, want.table)
	assertClickHouseValue(t, "delivery Client.Timeout", client.Timeout, want.timeout)
	assertClickHouseValue(t, "delivery Client.Username", client.Username, want.username)
	assertClickHouseValue(t, "delivery Client.Password", client.Password, want.password)
	assertClickHouseValue(t, "delivery Client.Headers", client.Headers, want.headers)
	assertClickHouseValue(t, "delivery Client.CompressRequest", client.CompressRequest, want.compressRequest)
}

func assertClickHouseFailureDTO(t *testing.T, contractLabel string, got pkgdurable.FailureStoreConfig, want clickHouseMappedExpectation) {
	t.Helper()
	assertClickHouseValue(t, contractLabel+" Failure.MaxConcurrent", got.MaxConcurrent, want.maxConcurrent)
	assertClickHouseValue(t, contractLabel+" Failure.Archive.Dir", got.DurableStoreConfig.FileArchiveConfig.Dir, want.archiveDir)
	assertClickHouseValue(t, contractLabel+" Failure.Spool.Dir", got.DurableStoreConfig.SpoolConfig.Dir, want.spoolDir)
	assertClickHouseValue(t, contractLabel+" Failure.Spool.ActiveBacklogWarnAfterDays", got.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.ActiveBacklogWarnAfterDays, want.spoolActiveBacklogWarnAfterDays)
	assertClickHouseValue(t, contractLabel+" Failure.Spool.MemoryPressure", got.DurableStoreConfig.SpoolConfig.OverloadPolicyConfig.UseMemoryPressureForOverload, want.useMemoryPressure)
}

func assertDeliveryClickHouseFailureDTO(
	t *testing.T,
	got pkgdurable.DeliveryFailureStoreConfig,
	want clickHouseMappedExpectation,
) {
	t.Helper()
	assertClickHouseValue(t, "delivery Failure.DurableType", got.DurableType, []pkgdurable.DurableType{pkgdurable.DurableTypeFileArchive})
	assertClickHouseValue(t, "delivery Failure.Archive.Dir", got.FileArchiveConfig.Dir, want.archiveDir)
}

func assertClickHouseFailureAdapter(t *testing.T, contractLabel string, got loggerdurable.FailureStoreConfigProvider, want clickHouseMappedExpectation) {
	t.Helper()
	assertClickHouseValue(t, contractLabel+" adapter Failure.MaxConcurrent", got.MaxConcurrent(), want.maxConcurrent)
	assertClickHouseValue(t, contractLabel+" adapter Failure.Archive.Dir", got.DurableStoreConfig().FileArchiveConfig().Dir(), want.archiveDir)
	assertClickHouseValue(t, contractLabel+" adapter Failure.Spool.Dir", got.DurableStoreConfig().SpoolConfig().Dir(), want.spoolDir)
}

func assertDeliveryClickHouseFailureAdapter(
	t *testing.T,
	got loggerdurable.DeliveryFailureStoreConfigProvider,
	want clickHouseMappedExpectation,
) {
	t.Helper()
	assertClickHouseValue(t, "delivery adapter Failure.Archive.Dir", got.DeliveryDurableStoreConfig().FileArchiveConfig().Dir(), want.archiveDir)
}

func assertClickHouseValue(t *testing.T, name string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %#v, want %#v", name, got, want)
	}
}

package config

import (
	"errors"
	configfailure "github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/config/timeoutbudget"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	loggerdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgs3uploader "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
	pkgspoolrunner "github.com/go-jappe/heterolog/pkg/spool/spoolrunner"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

// TestLoadConfigReadsFixtureEnvOpenSearchLoggerConfigsは、公開例から生成したfixtureの
// OpenSearch関連設定が、LoadConfigとmapperを通ってsync・async・deliveryの各DTOへ届くことを
// 検証します。公開例の値がcode既定値と同じkeyは非既定値で上書きし、keyを読まない
// loaderでは失敗するようにします。
func TestLoadConfigReadsFixtureEnvOpenSearchLoggerConfigs(t *testing.T) {
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
	if err := os.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES", "spool,fileArchive"); err != nil {
		t.Fatalf("enable async OpenSearch file archive for this loader test: %v", err)
	}
	if err := os.Setenv("HETEROLOG_OPENSEARCH_DURABLE_TYPES", "spool,fileArchive"); err != nil {
		t.Fatalf("enable sync OpenSearch file archive for this loader test: %v", err)
	}
	// *_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATIONは、degradeでは実装済みのstdoutだけを
	// 受理するため公開例の値のままにします。HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPESも、Deliveryの
	// 唯一の保存先であるfileArchiveのままにします。
	overrideFixtureEnvValues(t, envFile, map[string]string{
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY":          string(timeoutbudget.TimeoutBudgetMayCutoff),
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY":    string(timeoutbudget.TimeoutBudgetUnbounded),
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY": string(timeoutbudget.TimeoutBudgetMayCutoff),
		"HETEROLOG_MAX_LOG_ENTRY_BYTES":                "2097152",
		"HETEROLOG_SLACK_TIMEOUT_SECOND":               "3",
		"HETEROLOG_ASYNC_SLACK_TIMEOUT_SECOND":         "4",
		"HETEROLOG_DELIVERY_SLACK_TIMEOUT_SECOND":      "5",

		"HETEROLOG_ENABLE_OPENSEARCH_OTEL_FORMAT_LOG":          "false",
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH_OTEL_FORMAT_LOG":    "false",
		"HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_OTEL_FORMAT_LOG": "false",
		"HETEROLOG_OPENSEARCH_BUFFER_FLUSH_BYTES":              "4194304",
		"HETEROLOG_MAX_OPENSEARCH_BULK_REQUEST_BYTES":          "16777216",
		"HETEROLOG_ASYNC_OPENSEARCH_BUFFER_FLUSH_BYTES":        "3145728",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_BULK_REQUEST_BYTES":    "14680064",

		"HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR":                                  "sync-opensearch-fixture-archive",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY":                 string(loggerdurable.OverloadPolicyDegrade),
		"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR":                            "async-opensearch-fixture-archive",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_RETRYABLE_DIR":                          "async-retryable-fixture",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_PERMANENT_DIR":                          "async-permanent-fixture",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY":           string(loggerdurable.OverloadPolicyDegrade),
		"HETEROLOG_DELIVERY_OPENSEARCH_FAILURE_ARCHIVE_DIR":                         "delivery-opensearch-fixture-archive",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY":        string(loggerdurable.OverloadPolicyDegrade),
		"HETEROLOG_MAX_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB":            "51",
		"HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS":     "61",
		"HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE":         "62",
		"HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB": "2049",
		"HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS":        "false",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT":        string(pkgfsstore.CompressGzip),
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_ROTATION":               string(pkgfsstore.FileRotationWeekly),

		"HETEROLOG_MAX_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT":        "100001",
		"HETEROLOG_OPENSEARCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS":       "121",
		"HETEROLOG_USE_MEMORY_PRESSURE_OPENSEARCH_SPOOL_OVERLOAD":         "true",
		"HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT":  "100002",
		"HETEROLOG_ASYNC_OPENSEARCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS": "122",
		"HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_OPENSEARCH_SPOOL_OVERLOAD":   "true",
	})

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with fixture .env: %v", err)
	}
	if got, want := cfg.OpenSearchClient.Endpoint, requiredDotEnv(t, envFile, "HETEROLOG_OPENSEARCH_ENDPOINT"); got != want {
		t.Fatalf("OpenSearchClient.Endpoint = %q, want %q", got, want)
	}
	if got, want := cfg.AsyncOpenSearchClient.Endpoint, requiredDotEnv(t, envFile, "HETEROLOG_ASYNC_OPENSEARCH_ENDPOINT"); got != want {
		t.Fatalf("AsyncOpenSearchClient.Endpoint = %q, want %q", got, want)
	}
	if got, want := cfg.DeliveryOpenSearchClient.Endpoint, requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_ENDPOINT"); got != want {
		t.Fatalf("DeliveryOpenSearchClient.Endpoint = %q, want %q", got, want)
	}
	if got, want := cfg.Logger.Timeout, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_LOG_TIMEOUT_SECOND"))*time.Second; got != want {
		t.Fatalf("Logger.Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.Logger.TimeoutBudgetPolicy, timeoutbudget.TimeoutBudgetPolicy(requiredDotEnv(t, envFile, "HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY")); got != want {
		t.Fatalf("Logger.TimeoutBudgetPolicy = %q, want %q", got, want)
	}
	if got, want := cfg.AsyncBestEffortMultiLoggerConfig.Timeout, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND"))*time.Second; got != want {
		t.Fatalf("AsyncBestEffortMultiLoggerConfig.Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.AsyncBestEffortMultiLoggerConfig.TimeoutBudgetPolicy, timeoutbudget.TimeoutBudgetPolicy(requiredDotEnv(t, envFile, "HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY")); got != want {
		t.Fatalf("AsyncBestEffortMultiLoggerConfig.TimeoutBudgetPolicy = %q, want %q", got, want)
	}
	if got, want := cfg.DeliveryLogger.Timeout, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND"))*time.Second; got != want {
		t.Fatalf("DeliveryLogger.Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.DeliveryLogger.TimeoutBudgetPolicy, timeoutbudget.TimeoutBudgetPolicy(requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY")); got != want {
		t.Fatalf("DeliveryLogger.TimeoutBudgetPolicy = %q, want %q", got, want)
	}
	if got, want := cfg.MaxLogEntryBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_LOG_ENTRY_BYTES"); got != want {
		t.Fatalf("MaxLogEntryBytes = %d, want %d", got, want)
	}
	if got, want := cfg.DeliveryOpenSearchClient.Timeout, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_TIMEOUT_SECOND"))*time.Second; got != want {
		t.Fatalf("DeliveryOpenSearchClient.Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.Logger.SlackConfig.Timeout, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_SLACK_TIMEOUT_SECOND"))*time.Second; got != want {
		t.Fatalf("Logger.SlackConfig.Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.AsyncBestEffortMultiLoggerConfig.SlackConfig.Timeout, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_ASYNC_SLACK_TIMEOUT_SECOND"))*time.Second; got != want {
		t.Fatalf("AsyncBestEffortMultiLoggerConfig.SlackConfig.Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.DeliveryLogger.DeliverySlackConfig.Timeout, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_DELIVERY_SLACK_TIMEOUT_SECOND"))*time.Second; got != want {
		t.Fatalf("DeliveryLogger.DeliverySlackConfig.Timeout = %s, want %s", got, want)
	}

	syncFlush := cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig
	if got, want := syncFlush.Index, requiredDotEnv(t, envFile, "HETEROLOG_OPENSEARCH_INDEX"); got != want {
		t.Fatalf("sync OpenSearch index = %q, want %q", got, want)
	}
	if got, want := syncFlush.OpenTelemetryFormatEnabled, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_OPENSEARCH_OTEL_FORMAT_LOG"); got != want {
		t.Fatalf("sync OpenSearch OTel enabled = %t, want %t", got, want)
	}
	if got, want := cfg.Logger.OpenSearchConfig.BufferFlushBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_OPENSEARCH_BUFFER_FLUSH_BYTES"); got != want {
		t.Fatalf("sync OpenSearch buffer flush bytes = %d, want %d", got, want)
	}
	if got, want := cfg.Logger.OpenSearchConfig.MaxBulkRequestBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_OPENSEARCH_BULK_REQUEST_BYTES"); got != want {
		t.Fatalf("sync OpenSearch max bulk request bytes = %d, want %d", got, want)
	}

	asyncFlush := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig
	if got, want := asyncFlush.Index, requiredDotEnv(t, envFile, "HETEROLOG_ASYNC_OPENSEARCH_INDEX"); got != want {
		t.Fatalf("async OpenSearch index = %q, want %q", got, want)
	}
	if got, want := asyncFlush.OpenTelemetryFormatEnabled, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_ASYNC_OPENSEARCH_OTEL_FORMAT_LOG"); got != want {
		t.Fatalf("async OpenSearch OTel enabled = %t, want %t", got, want)
	}
	if got, want := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.BufferFlushBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_ASYNC_OPENSEARCH_BUFFER_FLUSH_BYTES"); got != want {
		t.Fatalf("async OpenSearch buffer flush bytes = %d, want %d", got, want)
	}
	if got, want := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.MaxBulkRequestBytes, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_ASYNC_OPENSEARCH_BULK_REQUEST_BYTES"); got != want {
		t.Fatalf("async OpenSearch max bulk request bytes = %d, want %d", got, want)
	}
	asyncArchive := asyncFlush.FailureHandlerConfig.Fsstore.FileArchiveConfig
	if got, want := asyncArchive.DirCaseRetryable, requiredDotEnv(t, envFile, "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_RETRYABLE_DIR"); got != want {
		t.Fatalf("async OpenSearch retryable archive directory = %q, want %q", got, want)
	}
	if got, want := asyncArchive.DirCasePermanent, requiredDotEnv(t, envFile, "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_PERMANENT_DIR"); got != want {
		t.Fatalf("async OpenSearch permanent archive directory = %q, want %q", got, want)
	}

	deliveryFlush := cfg.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig
	if got, want := deliveryFlush.Index, requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_INDEX"); got != want {
		t.Fatalf("delivery OpenSearch index = %q, want %q", got, want)
	}
	if got, want := deliveryFlush.OpenTelemetryFormatEnabled, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_OTEL_FORMAT_LOG"); got != want {
		t.Fatalf("delivery OpenSearch OTel enabled = %t, want %t", got, want)
	}
	deliveryFailure := deliveryFlush.FailureHandlerConfig
	if len(deliveryFailure.DurableType) != 1 || string(deliveryFailure.DurableType[0]) != requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES") {
		t.Fatalf("delivery durable types = %v, want [%s]", deliveryFailure.DurableType, requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES"))
	}

	fileArchive := deliveryFailure.FileArchiveConfig
	if got, want := fileArchive.MaxFileSize, requiredDotEnvUint64(t, envFile, "HETEROLOG_MAX_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB")*BytesPerMegabyte; got != want {
		t.Fatalf("delivery archive MaxFileSize = %d, want %d", got, want)
	}
	if got, want := fileArchive.Dir, requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_FAILURE_ARCHIVE_DIR"); got != want {
		t.Fatalf("delivery archive Dir = %q, want %q", got, want)
	}
	if got, want := fileArchive.MaxFileBackups, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS"); got != want {
		t.Fatalf("delivery archive MaxFileBackups = %d, want %d", got, want)
	}
	if got, want := fileArchive.MaxFileAge, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE"); got != want {
		t.Fatalf("delivery archive MaxFileAge = %d, want %d", got, want)
	}
	if got, want := fileArchive.MaxDirSize, requiredDotEnvUint64(t, envFile, "HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB")*BytesPerMegabyte; got != want {
		t.Fatalf("delivery archive MaxDirSize = %d, want %d", got, want)
	}
	if got, want := fileArchive.EnableFileCompress, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS"); got != want {
		t.Fatalf("delivery archive compression enabled = %t, want %t", got, want)
	}
	if got, want := string(fileArchive.CompressFormat), requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT"); got != want {
		t.Fatalf("delivery archive CompressFormat = %q, want %q", got, want)
	}
	if got, want := string(fileArchive.FileRotation), requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_ROTATION"); got != want {
		t.Fatalf("delivery archive FileRotation = %q, want %q", got, want)
	}
	if got, want := string(fileArchive.OverloadPolicyConfig.OverloadPolicy), requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY"); got != want {
		t.Fatalf("delivery archive OverloadPolicy = %q, want %q", got, want)
	}
	if got, want := string(fileArchive.OverloadPolicyConfig.SpillDestination), requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION"); got != want {
		t.Fatalf("delivery archive SpillDestination = %q, want %q", got, want)
	}

	syncLogger, err := MapLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapLoggerConfig() with fixture .env: %v", err)
	}
	if syncLogger.OpenSearch == nil || syncLogger.OpenSearchClient == nil {
		t.Fatalf("sync mapper did not build OpenSearch logger/client from fixture .env")
	}
	if got, want := syncLogger.Timeout, cfg.Logger.Timeout; got != want {
		t.Fatalf("sync mapped Timeout = %s, want %s", got, want)
	}
	if got, want := syncLogger.OpenSearch.Index, requiredDotEnv(t, envFile, "HETEROLOG_OPENSEARCH_INDEX"); got != want {
		t.Fatalf("sync mapped OpenSearch index = %q, want %q", got, want)
	}
	if got, want := syncLogger.OpenSearchClient.Endpoint, requiredDotEnv(t, envFile, "HETEROLOG_OPENSEARCH_ENDPOINT"); got != want {
		t.Fatalf("sync mapped OpenSearch endpoint = %q, want %q", got, want)
	}

	asyncLogger, err := MapAsyncLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapAsyncLoggerConfig() with fixture .env: %v", err)
	}
	if asyncLogger.OpenSearch == nil || asyncLogger.OpenSearchClient == nil {
		t.Fatalf("async mapper did not build OpenSearch logger/client from fixture .env")
	}
	if got, want := asyncLogger.Timeout, cfg.AsyncBestEffortMultiLoggerConfig.Timeout; got != want {
		t.Fatalf("async mapped Timeout = %s, want %s", got, want)
	}
	if got, want := asyncLogger.MaxParallel, cfg.AsyncBestEffortMultiLoggerConfig.MaxParallel; got != want {
		t.Fatalf("async mapped MaxParallel = %d, want %d", got, want)
	}
	if got, want := asyncLogger.OpenSearch.Index, requiredDotEnv(t, envFile, "HETEROLOG_ASYNC_OPENSEARCH_INDEX"); got != want {
		t.Fatalf("async mapped OpenSearch index = %q, want %q", got, want)
	}
	if got, want := asyncLogger.OpenSearchClient.Endpoint, requiredDotEnv(t, envFile, "HETEROLOG_ASYNC_OPENSEARCH_ENDPOINT"); got != want {
		t.Fatalf("async mapped OpenSearch endpoint = %q, want %q", got, want)
	}

	assertFixtureEnvOpenSearchFailureStoreContract(
		t,
		"sync",
		envFile,
		syncFlush.FailureHandlerConfig,
		syncLogger.OpenSearch.FailureStoreConfig,
		openSearchFailureStoreEnvKeys{
			archiveDir:                      "HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR",
			overloadPolicy:                  "HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY",
			spillDestination:                "HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION",
			handlerError:                    "HETEROLOG_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER",
			handlerWarn:                     "HETEROLOG_OPENSEARCH_DURABLE_FILE_WARN_HANDLER",
			handlerInfo:                     "HETEROLOG_OPENSEARCH_DURABLE_FILE_INFO_HANDLER",
			handlerDebug:                    "HETEROLOG_OPENSEARCH_DURABLE_FILE_DEBUG_HANDLER",
			spoolMaxCalls:                   "HETEROLOG_MAX_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT",
			spoolScanInterval:               "HETEROLOG_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND",
			spoolActiveBacklogWarnAfterDays: "HETEROLOG_OPENSEARCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS",
			useMemoryPressure:               "HETEROLOG_USE_MEMORY_PRESSURE_OPENSEARCH_SPOOL_OVERLOAD",
		},
	)
	assertFixtureEnvOpenSearchFailureStoreContract(
		t,
		"async",
		envFile,
		asyncFlush.FailureHandlerConfig,
		asyncLogger.OpenSearch.FailureStoreConfig,
		openSearchFailureStoreEnvKeys{
			archiveDir:                      "HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR",
			overloadPolicy:                  "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY",
			spillDestination:                "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION",
			handlerError:                    "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ERROR_HANDLER",
			handlerWarn:                     "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_WARN_HANDLER",
			handlerInfo:                     "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_INFO_HANDLER",
			handlerDebug:                    "HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_DEBUG_HANDLER",
			spoolMaxCalls:                   "HETEROLOG_MAX_ASYNC_OPENSEARCH_SPOOL_SCANNED_CACHE_USAGE_COUNT",
			spoolScanInterval:               "HETEROLOG_ASYNC_OPENSEARCH_SPOOL_CACHE_LIFETIME_SECOND",
			spoolActiveBacklogWarnAfterDays: "HETEROLOG_ASYNC_OPENSEARCH_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS",
			useMemoryPressure:               "HETEROLOG_USE_MEMORY_PRESSURE_ASYNC_OPENSEARCH_SPOOL_OVERLOAD",
		},
	)
	assertFixtureEnvAsyncOpenSearchMapperFields(t, cfg, *asyncLogger.OpenSearch)
	syncHandlerKeys := openSearchHandlerRouteEnvKeys{
		error: "HETEROLOG_OPENSEARCH_LOGGER_ERROR_HANDLER",
		warn:  "HETEROLOG_OPENSEARCH_LOGGER_WARN_HANDLER",
		info:  "HETEROLOG_OPENSEARCH_LOGGER_INFO_HANDLER",
		debug: "HETEROLOG_OPENSEARCH_LOGGER_DEBUG_HANDLER",
	}
	asyncHandlerKeys := openSearchHandlerRouteEnvKeys{
		error: "HETEROLOG_ASYNC_OPENSEARCH_LOGGER_ERROR_HANDLER",
		warn:  "HETEROLOG_ASYNC_OPENSEARCH_LOGGER_WARN_HANDLER",
		info:  "HETEROLOG_ASYNC_OPENSEARCH_LOGGER_INFO_HANDLER",
		debug: "HETEROLOG_ASYNC_OPENSEARCH_LOGGER_DEBUG_HANDLER",
	}
	assertFixtureEnvOpenSearchHandlerRoutes(t, "sync loaded", envFile, syncFlushHandlerRoutes(cfg), syncHandlerKeys)
	assertFixtureEnvOpenSearchHandlerRoutes(t, "sync mapped", envFile, mappedOpenSearchHandlerRoutes(*syncLogger.OpenSearch), syncHandlerKeys)
	assertFixtureEnvOpenSearchHandlerRoutes(t, "async loaded", envFile, asyncFlushHandlerRoutes(cfg), asyncHandlerKeys)
	assertFixtureEnvOpenSearchHandlerRoutes(t, "async mapped", envFile, mappedOpenSearchHandlerRoutes(*asyncLogger.OpenSearch), asyncHandlerKeys)

	deliveryLogger, err := MapDeliveryLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapDeliveryLoggerConfig() with fixture .env: %v", err)
	}
	if deliveryLogger.OpenSearch == nil || deliveryLogger.OpenSearchClient == nil {
		t.Fatalf("delivery mapper did not build OpenSearch logger/client from fixture .env")
	}
	if got, want := deliveryLogger.Timeout, cfg.DeliveryLogger.Timeout; got != want {
		t.Fatalf("delivery mapped Timeout = %s, want %s", got, want)
	}
	if got, want := deliveryLogger.OpenSearch.Index, requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_INDEX"); got != want {
		t.Fatalf("delivery mapped OpenSearch index = %q, want %q", got, want)
	}
	if got, want := deliveryLogger.OpenSearchClient.Endpoint, requiredDotEnv(t, envFile, "HETEROLOG_DELIVERY_OPENSEARCH_ENDPOINT"); got != want {
		t.Fatalf("delivery mapped OpenSearch endpoint = %q, want %q", got, want)
	}
	if got, want := deliveryLogger.OpenSearchClient.CompressRequest, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_COMPRESS_REQUEST"); got != want {
		t.Fatalf("delivery mapped OpenSearch compress request = %t, want %t", got, want)
	}
}

type openSearchFailureStoreEnvKeys struct {
	archiveDir                      string
	overloadPolicy                  string
	spillDestination                string
	handlerError                    string
	handlerWarn                     string
	handlerInfo                     string
	handlerDebug                    string
	spoolMaxCalls                   string
	spoolScanInterval               string
	spoolActiveBacklogWarnAfterDays string
	useMemoryPressure               string
}

// assertFixtureEnvOpenSearchFailureStoreContract は、公開例から生成したfixtureの.envに
// 書かれた値（testで上書きした値を含む）がLoadConfigとmapperの両方を通過して、同じ
// LoggerContractKindへ届いていることを確認します。
func assertFixtureEnvOpenSearchFailureStoreContract(
	t *testing.T,
	scope string,
	envFile map[string]string,
	loaded configfailure.FailureHandlerConfig,
	mapped loggerdurable.FailureStoreConfig,
	keys openSearchFailureStoreEnvKeys,
) {
	t.Helper()

	wantArchiveDir := requiredDotEnv(t, envFile, keys.archiveDir)
	wantOverloadPolicy := loggerdurable.OverloadPolicy(requiredDotEnv(t, envFile, keys.overloadPolicy))
	wantSpillDestination := loggerdurable.SpillDestination(requiredDotEnv(t, envFile, keys.spillDestination))
	wantSpoolMaxCalls := requiredDotEnvUint64(t, envFile, keys.spoolMaxCalls)
	wantSpoolScanInterval := time.Duration(requiredDotEnvInt(t, envFile, keys.spoolScanInterval)) * time.Second
	wantSpoolActiveBacklogWarnAfterDays := requiredDotEnvInt(t, envFile, keys.spoolActiveBacklogWarnAfterDays)
	wantUseMemoryPressure := requiredDotEnvBool(t, envFile, keys.useMemoryPressure)

	loadedArchive := loaded.Fsstore.FileArchiveConfig
	loadedSpool := loaded.Fsstore.SpoolConfig
	if got := loadedArchive.Dir; got != wantArchiveDir {
		t.Fatalf("%s loaded archive Dir = %q, want %q from %s", scope, got, wantArchiveDir, keys.archiveDir)
	}
	if got := loadedArchive.OverloadPolicyConfig.OverloadPolicy; got != wantOverloadPolicy {
		t.Fatalf("%s loaded archive OverloadPolicy = %q, want %q from %s", scope, got, wantOverloadPolicy, keys.overloadPolicy)
	}
	if got := loadedArchive.OverloadPolicyConfig.SpillDestination; got != wantSpillDestination {
		t.Fatalf("%s loaded archive SpillDestination = %q, want %q from %s", scope, got, wantSpillDestination, keys.spillDestination)
	}
	assertFixtureEnvFailureArchiveHandlerRoutes(
		t,
		scope+" loaded archive",
		envFile,
		loadedArchive.HandlerConfig.OnErrorConfig,
		loadedArchive.HandlerConfig.OnWarnConfig,
		loadedArchive.HandlerConfig.OnInfoConfig,
		loadedArchive.HandlerConfig.OnDebugConfig,
		keys,
	)
	if got := loadedSpool.OverloadPolicyConfig.MaxCallsSinceScan; got != wantSpoolMaxCalls {
		t.Fatalf("%s loaded spool MaxCallsSinceScan = %d, want %d from %s", scope, got, wantSpoolMaxCalls, keys.spoolMaxCalls)
	}
	if got := loadedSpool.OverloadPolicyConfig.StatsScanInterval; got != wantSpoolScanInterval {
		t.Fatalf("%s loaded spool StatsScanInterval = %s, want %s from %s", scope, got, wantSpoolScanInterval, keys.spoolScanInterval)
	}
	if got := loadedSpool.OverloadPolicyConfig.ActiveBacklogWarnAfterDays; got != wantSpoolActiveBacklogWarnAfterDays {
		t.Fatalf("%s loaded spool ActiveBacklogWarnAfterDays = %d, want %d from %s", scope, got, wantSpoolActiveBacklogWarnAfterDays, keys.spoolActiveBacklogWarnAfterDays)
	}
	if got := loadedSpool.OverloadPolicyConfig.UseMemoryPressureForOverload; got != wantUseMemoryPressure {
		t.Fatalf("%s loaded spool UseMemoryPressureForOverload = %t, want %t from %s", scope, got, wantUseMemoryPressure, keys.useMemoryPressure)
	}

	mappedArchive := mapped.DurableStoreConfig.FileArchiveConfig
	mappedSpool := mapped.DurableStoreConfig.SpoolConfig
	if got := mappedArchive.Dir; got != wantArchiveDir {
		t.Fatalf("%s mapped archive Dir = %q, want %q from %s", scope, got, wantArchiveDir, keys.archiveDir)
	}
	if got := mappedArchive.OverloadPolicyConfig.OverloadPolicy; got != wantOverloadPolicy {
		t.Fatalf("%s mapped archive OverloadPolicy = %q, want %q from %s", scope, got, wantOverloadPolicy, keys.overloadPolicy)
	}
	if got := mappedArchive.OverloadPolicyConfig.SpillDestination; got != wantSpillDestination {
		t.Fatalf("%s mapped archive SpillDestination = %q, want %q from %s", scope, got, wantSpillDestination, keys.spillDestination)
	}
	assertFixtureEnvFailureArchiveHandlerRoutes(
		t,
		scope+" mapped archive",
		envFile,
		mappedArchive.HandlerConfig.OnErrorConfig,
		mappedArchive.HandlerConfig.OnWarnConfig,
		mappedArchive.HandlerConfig.OnInfoConfig,
		mappedArchive.HandlerConfig.OnDebugConfig,
		keys,
	)
	if got := mappedSpool.OverloadPolicyConfig.MaxCallsSinceScan; got != wantSpoolMaxCalls {
		t.Fatalf("%s mapped spool MaxCallsSinceScan = %d, want %d from %s", scope, got, wantSpoolMaxCalls, keys.spoolMaxCalls)
	}
	if got := mappedSpool.OverloadPolicyConfig.StatsScanInterval; got != wantSpoolScanInterval {
		t.Fatalf("%s mapped spool StatsScanInterval = %s, want %s from %s", scope, got, wantSpoolScanInterval, keys.spoolScanInterval)
	}
	if got := mappedSpool.OverloadPolicyConfig.ActiveBacklogWarnAfterDays; got != wantSpoolActiveBacklogWarnAfterDays {
		t.Fatalf("%s mapped spool ActiveBacklogWarnAfterDays = %d, want %d from %s", scope, got, wantSpoolActiveBacklogWarnAfterDays, keys.spoolActiveBacklogWarnAfterDays)
	}
	if got := mappedSpool.OverloadPolicyConfig.UseMemoryPressureForOverload; got != wantUseMemoryPressure {
		t.Fatalf("%s mapped spool UseMemoryPressureForOverload = %t, want %t from %s", scope, got, wantUseMemoryPressure, keys.useMemoryPressure)
	}
}

func assertFixtureEnvFailureArchiveHandlerRoutes(
	t *testing.T,
	scope string,
	envFile map[string]string,
	onError, onWarn, onInfo, onDebug []string,
	keys openSearchFailureStoreEnvKeys,
) {
	t.Helper()

	checks := []struct {
		name string
		got  []string
		key  string
	}{
		{name: "error", got: onError, key: keys.handlerError},
		{name: "warn", got: onWarn, key: keys.handlerWarn},
		{name: "info", got: onInfo, key: keys.handlerInfo},
		{name: "debug", got: onDebug, key: keys.handlerDebug},
	}
	for _, check := range checks {
		want := splitDotEnvCSV(requiredDotEnv(t, envFile, check.key))
		if !sameStringSlice(check.got, want) {
			t.Fatalf("%s %s handlers = %v, want %v from %s", scope, check.name, check.got, want, check.key)
		}
	}
}

func assertFixtureEnvAsyncOpenSearchMapperFields(t *testing.T, cfg AppConfig, mapped pkglogger.OpenSearchConfig) {
	t.Helper()
	loaded := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig

	if got, want := mapped.PostWarnThresholdDuration, loaded.PostWarnThresholdDuration; got != want {
		t.Fatalf("async mapped PostWarnThresholdDuration = %s, want %s", got, want)
	}
	if got, want := mapped.PostErrorThresholdDuration, loaded.PostErrorThresholdDuration; got != want {
		t.Fatalf("async mapped PostErrorThresholdDuration = %s, want %s", got, want)
	}
	if got, want := mapped.IndexLatencyEnabled, loaded.IndexLatencyEnabled; got != want {
		t.Fatalf("async mapped IndexLatencyEnabled = %t, want %t", got, want)
	}
	if got, want := mapped.MaxItemRetries, loaded.MaxItemRetries; got != want {
		t.Fatalf("async mapped MaxItemRetries = %d, want %d", got, want)
	}
	if got, want := mapped.RetryWaitTime, loaded.RetryWaitTime; got != want {
		t.Fatalf("async mapped RetryWaitTime = %s, want %s", got, want)
	}
	if got, want := mapped.TimeConfig.IsLocalTimeEnabled, loaded.ClockConfig.IsLocalTimeEnabled; got != want {
		t.Fatalf("async mapped IsLocalTimeEnabled = %t, want %t", got, want)
	}
	if got, want := mapped.TimeConfig.TimeZone, loaded.ClockConfig.TimeZone; got != want {
		t.Fatalf("async mapped TimeZone = %q, want %q", got, want)
	}
	if got, want := mapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ErrorTraceDepth, loaded.LogBodyConfig.ErrorTraceDepth; got != want {
		t.Fatalf("async mapped ErrorTraceDepth = %d, want %d", got, want)
	}
	if got, want := mapped.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ErrorTraceDepthDebugMode, loaded.LogBodyConfig.ErrorTraceDepthDebugMode; got != want {
		t.Fatalf("async mapped ErrorTraceDepthDebugMode = %d, want %d", got, want)
	}
	if got, want := mapped.Resource.ServiceName, cfg.AppConfig.AppName; got != want {
		t.Fatalf("async mapped Resource.ServiceName = %q, want %q", got, want)
	}
}

type openSearchHandlerRoutes struct {
	error []string
	warn  []string
	info  []string
	debug []string
}

type openSearchHandlerRouteEnvKeys struct {
	error string
	warn  string
	info  string
	debug string
}

func syncFlushHandlerRoutes(cfg AppConfig) openSearchHandlerRoutes {
	routes := cfg.Logger.OpenSearchConfig.HandlerConfig
	return openSearchHandlerRoutes{
		error: routes.OnErrorConfig,
		warn:  routes.OnWarnConfig,
		info:  routes.OnInfoConfig,
		debug: routes.OnDebugConfig,
	}
}

func asyncFlushHandlerRoutes(cfg AppConfig) openSearchHandlerRoutes {
	routes := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.HandlerConfig
	return openSearchHandlerRoutes{
		error: routes.OnErrorConfig,
		warn:  routes.OnWarnConfig,
		info:  routes.OnInfoConfig,
		debug: routes.OnDebugConfig,
	}
}

func mappedOpenSearchHandlerRoutes(cfg pkglogger.OpenSearchConfig) openSearchHandlerRoutes {
	routes := cfg.HandlerConfig
	return openSearchHandlerRoutes{
		error: routes.OnErrorConfig,
		warn:  routes.OnWarnConfig,
		info:  routes.OnInfoConfig,
		debug: routes.OnDebugConfig,
	}
}

func assertFixtureEnvOpenSearchHandlerRoutes(
	t *testing.T,
	scope string,
	envFile map[string]string,
	got openSearchHandlerRoutes,
	keys openSearchHandlerRouteEnvKeys,
) {
	t.Helper()

	checks := []struct {
		name string
		got  []string
		key  string
	}{
		{name: "error", got: got.error, key: keys.error},
		{name: "warn", got: got.warn, key: keys.warn},
		{name: "info", got: got.info, key: keys.info},
		{name: "debug", got: got.debug, key: keys.debug},
	}
	for _, check := range checks {
		want := splitDotEnvCSV(requiredDotEnv(t, envFile, check.key))
		if !sameStringSlice(check.got, want) {
			t.Fatalf("%s %s handlers = %v, want %v from %s", scope, check.name, check.got, want, check.key)
		}
	}
}

func TestPublicEnvExampleRetryClientModesUseRetryPolicy(t *testing.T) {
	retryModeKeys := []string{
		"HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE",
		"HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE",
		"HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE",
		"HETEROLOG_LOKI_RETRY_CLIENT_MODE",
		"HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE",
		"HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE",
		"HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE",
		"HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE",
		"HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE",
		"HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE",
		"HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE",
		"HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE",
	}

	envFile := readPublicEnvExample(t)
	for _, key := range retryModeKeys {
		if got, want := requiredDotEnv(t, envFile, key), string(pkglogger.RetryClientModeRetryPolicy); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestLoadClickHouseConfigDefaultsFlushBatch(t *testing.T) {
	t.Setenv("HETEROLOG_CLICKHOUSE_FLUSH_INTERVAL_SECOND", "")
	t.Setenv("HETEROLOG_MAX_CLICKHOUSE_INSERT_ENTRIES", "")
	t.Setenv("HETEROLOG_CLICKHOUSE_LOGGER_ERROR_HANDLER", "stdout")
	t.Setenv("HETEROLOG_CLICKHOUSE_LOGGER_WARN_HANDLER", "stdout")
	t.Setenv("HETEROLOG_CLICKHOUSE_LOGGER_INFO_HANDLER", "stdout")
	t.Setenv("HETEROLOG_CLICKHOUSE_LOGGER_DEBUG_HANDLER", "stdout")

	cfg := mustLoadConfig(t, fromProcessEnvironment(loadClickHouseConfig))
	if cfg.FlushInterval != 5*time.Second {
		t.Fatalf("FlushInterval = %s, want 5s", cfg.FlushInterval)
	}
	if cfg.MaxEntries != 10000 {
		t.Fatalf("MaxEntries = %d, want 10000", cfg.MaxEntries)
	}
}

func TestLoadAsyncBestEffortMultiLoggerConfigReadsTimeoutInSeconds(t *testing.T) {
	t.Setenv("HETEROLOG_ASYNC_LOG_TYPE", "stdout")
	t.Setenv("HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND", "12")
	t.Setenv("HETEROLOG_MAX_ASYNC_LOG_PARALLEL", "8")

	cfg, err := loadAsyncBestEffortMultiLoggerConfig(processEnvironmentForTest())
	if err != nil {
		t.Fatalf("loadAsyncBestEffortMultiLoggerConfig() error = %v", err)
	}

	if got, want := cfg.Timeout, 12*time.Second; got != want {
		t.Fatalf("Timeout = %s, want %s", got, want)
	}
}

func TestLoadLoggerTimeoutBudgetPolicyDefaultsToMustFit(t *testing.T) {
	t.Setenv("HETEROLOG_LOG_TYPE", "stdout")
	// 開発者のshellに残る値ではなく、未設定時の既定値を検証します。
	t.Setenv("HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY", "")

	cfg, err := loadLoggerConfig(processEnvironmentForTest())
	if err != nil {
		t.Fatalf("loadLoggerConfig() error = %v", err)
	}
	if got, want := cfg.TimeoutBudgetPolicy, timeoutbudget.TimeoutBudgetMustFit; got != want {
		t.Fatalf("TimeoutBudgetPolicy = %q, want %q", got, want)
	}
}

// TestLoadLoggerTimeoutBudgetPoliciesFromEnvは、3つのpolicyを各LoggerContractKindへ
// 巡回して割り当てます。各keyに既定値must_fit以外の値を与える場合を含め、keyの無視や
// LoggerContractKind間の取り違えを検出します。
func TestLoadLoggerTimeoutBudgetPoliciesFromEnv(t *testing.T) {
	policies := []timeoutbudget.TimeoutBudgetPolicy{
		timeoutbudget.TimeoutBudgetMayCutoff,
		timeoutbudget.TimeoutBudgetUnbounded,
		timeoutbudget.TimeoutBudgetMustFit,
	}
	for rotation := range policies {
		loggerPolicy := policies[rotation%len(policies)]
		asyncPolicy := policies[(rotation+1)%len(policies)]
		deliveryPolicy := policies[(rotation+2)%len(policies)]
		t.Run(string(loggerPolicy)+"/"+string(asyncPolicy)+"/"+string(deliveryPolicy), func(t *testing.T) {
			t.Setenv("HETEROLOG_LOG_TYPE", "stdout")
			t.Setenv("HETEROLOG_ASYNC_LOG_TYPE", "stdout")
			t.Setenv("HETEROLOG_DELIVERY_LOG_TYPE", "stdout")
			t.Setenv("HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY", string(loggerPolicy))
			t.Setenv("HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY", string(asyncPolicy))
			t.Setenv("HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY", string(deliveryPolicy))

			loggerCfg, err := loadLoggerConfig(processEnvironmentForTest())
			if err != nil {
				t.Fatalf("loadLoggerConfig() error = %v", err)
			}
			asyncCfg, err := loadAsyncBestEffortMultiLoggerConfig(processEnvironmentForTest())
			if err != nil {
				t.Fatalf("loadAsyncBestEffortMultiLoggerConfig() error = %v", err)
			}
			deliveryCfg, err := loadDeliveryLoggerConfig(processEnvironmentForTest())
			if err != nil {
				t.Fatalf("loadDeliveryLoggerConfig() error = %v", err)
			}

			if got := loggerCfg.TimeoutBudgetPolicy; got != loggerPolicy {
				t.Fatalf("Logger TimeoutBudgetPolicy = %q, want %q", got, loggerPolicy)
			}
			if got := asyncCfg.TimeoutBudgetPolicy; got != asyncPolicy {
				t.Fatalf("Async TimeoutBudgetPolicy = %q, want %q", got, asyncPolicy)
			}
			if got := deliveryCfg.TimeoutBudgetPolicy; got != deliveryPolicy {
				t.Fatalf("Delivery TimeoutBudgetPolicy = %q, want %q", got, deliveryPolicy)
			}
		})
	}
}

func TestLoadConfigReadsFileObjectStorageUploadConfig(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION=us-east-1",
		"HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT=http://localhost:9000",
		"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=test-access-key",
		"HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=test-secret-key",
		"HETEROLOG_OBJECT_STORAGE_S3_TIMEOUT_SECOND=2",
		"HETEROLOG_ENABLE_OBJECT_STORAGE_S3_FORCE_PATH_STYLE=true",
		"HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_BUCKET=default-bucket",
		"HETEROLOG_LOG_FILE_OBJECT_STORAGE_S3_PREFIX=default-prefix",
		"HETEROLOG_LOG_FILE_ERROR_OBJECT_STORAGE_S3_BUCKET=error-bucket",
		"HETEROLOG_LOG_FILE_ERROR_OBJECT_STORAGE_S3_PREFIX=error-prefix",
		"HETEROLOG_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_BUCKET=debug-bucket",
		"HETEROLOG_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_PREFIX=debug-prefix",
	})

	uploadCfg := cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig
	if !uploadCfg.Enabled {
		t.Fatalf("object storage upload enabled = false, want true")
	}
	if !uploadCfg.Default.Enabled || !uploadCfg.Error.Enabled || !uploadCfg.Debug.Enabled {
		t.Fatalf("classification upload enabled = default:%t error:%t debug:%t, want all true", uploadCfg.Default.Enabled, uploadCfg.Error.Enabled, uploadCfg.Debug.Enabled)
	}
	if len(uploadCfg.Default.Targets) != 1 || string(uploadCfg.Default.Targets[0]) != "s3" {
		t.Fatalf("default upload targets = %v, want [s3]", uploadCfg.Default.Targets)
	}
	if got, want := uploadCfg.Default.S3.Bucket, "default-bucket"; got != want {
		t.Fatalf("default bucket = %q, want %q", got, want)
	}
	if got, want := uploadCfg.Error.S3.Prefix, "error-prefix"; got != want {
		t.Fatalf("error prefix = %q, want %q", got, want)
	}
	if got, want := uploadCfg.Debug.S3.Bucket, "debug-bucket"; got != want {
		t.Fatalf("debug bucket = %q, want %q", got, want)
	}
	mapped := ToLoggerConfig(cfg)
	if mapped.File == nil {
		t.Fatalf("mapped file logger config is nil")
	}
	if got, want := mapped.File.ObjectStorageUploadConfig.Default.S3.Endpoint, "http://localhost:9000"; got != want {
		t.Fatalf("mapped default endpoint = %q, want %q", got, want)
	}
	if !mapped.File.ObjectStorageUploadConfig.Default.S3.ForcePathStyle {
		t.Fatalf("mapped ForcePathStyle = false, want true")
	}
}

func TestLoadConfigReadsOpenSearchFailureArchiveObjectStorageUploadConfig(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_TARGET=archive-target",
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=opensearch",
		"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
		"HETEROLOG_DELIVERY_LOG_TYPE=opensearch",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE=S3",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET=sync-archive-bucket",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX=sync-archive-prefix",
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET=async-archive-bucket",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX=async-archive-prefix",
		"HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET=delivery-archive-bucket",
		"HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX=delivery-archive-prefix",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION=us-east-1",
		"HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT=http://localhost:9000",
		"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=test-access-key",
		"HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=test-secret-key",
		"HETEROLOG_OBJECT_STORAGE_S3_TIMEOUT_SECOND=2",
	})

	syncUpload := cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig
	assertArchiveUploadConfig(t, syncUpload, "sync-archive-bucket", "sync-archive-prefix")
	asyncUpload := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig
	assertArchiveUploadConfig(t, asyncUpload, "async-archive-bucket", "async-archive-prefix")
	deliveryUpload := cfg.DeliveryLogger.DeliveryOpenSearchConfig.DeliveryOpenSearchFlushManagerConfig.FailureHandlerConfig.FileArchiveConfig.ObjectStorageUploadConfig
	assertArchiveUploadConfig(t, deliveryUpload, "delivery-archive-bucket", "delivery-archive-prefix")

	mapped := ToLoggerConfig(cfg)
	if mapped.OpenSearch == nil {
		t.Fatal("mapped OpenSearch config is nil")
	}
	assertArchiveUploadConfig(
		t,
		mapped.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		"sync-archive-bucket",
		"sync-archive-prefix",
	)
	if got := mapped.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Target; got != "archive-target" {
		t.Fatalf("mapped OpenSearch archive target = %q, want archive-target", got)
	}

	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	if asyncMapped.OpenSearch == nil {
		t.Fatal("mapped async OpenSearch config is nil")
	}
	assertArchiveUploadConfig(
		t,
		asyncMapped.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		"async-archive-bucket",
		"async-archive-prefix",
	)
	if got := asyncMapped.OpenSearch.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Target; got != "archive-target" {
		t.Fatalf("mapped async OpenSearch archive target = %q, want archive-target", got)
	}

	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	if deliveryMapped.OpenSearch == nil {
		t.Fatal("mapped delivery OpenSearch config is nil")
	}
	assertArchiveUploadConfig(
		t,
		deliveryMapped.OpenSearch.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		"delivery-archive-bucket",
		"delivery-archive-prefix",
	)
	if got := deliveryMapped.OpenSearch.FailureStoreConfig.FileArchiveConfig.Target; got != "archive-target" {
		t.Fatalf("mapped delivery OpenSearch archive target = %q, want archive-target", got)
	}
}

func TestLoadConfigDefersOpenSearchFailureArchiveS3ValidationToMapper(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=opensearch",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_OPENSEARCH_USERNAME=test-user",
		"HETEROLOG_OPENSEARCH_PASSWORD=test-password",
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ENABLE_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_OPENSEARCH_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
	})
	upload := cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
		FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig
	if !upload.Enabled || !upload.Has(pkgobjectstorage.UploadTargetS3) {
		t.Fatalf("OpenSearch archive upload config = %+v, want enabled s3 config", upload)
	}
	assertMapperRejectsMissingFailureArchiveS3Bucket(t, cfg)
}

func TestLoadConfigReadsBackendFailureArchiveObjectStorageUploadConfig(t *testing.T) {
	type archiveScope struct {
		scope  string
		bucket string
		prefix string
	}

	scopes := []archiveScope{
		{scope: "LOKI_DURABLE_FILE_ARCHIVE", bucket: "loki-sync-bucket", prefix: "loki/sync"},
		{scope: "ASYNC_LOKI_DURABLE_FILE_ARCHIVE", bucket: "loki-async-bucket", prefix: "loki/async"},
		{scope: "DELIVERY_LOKI_DURABLE_FILE_ARCHIVE", bucket: "loki-delivery-bucket", prefix: "loki/delivery"},
		{scope: "CLOUDWATCH_DURABLE_FILE_ARCHIVE", bucket: "cloudwatch-sync-bucket", prefix: "cloudwatch/sync"},
		{scope: "ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE", bucket: "cloudwatch-async-bucket", prefix: "cloudwatch/async"},
		{scope: "DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE", bucket: "cloudwatch-delivery-bucket", prefix: "cloudwatch/delivery"},
		{scope: "CLICKHOUSE_DURABLE_FILE_ARCHIVE", bucket: "clickhouse-sync-bucket", prefix: "clickhouse/sync"},
		{scope: "ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE", bucket: "clickhouse-async-bucket", prefix: "clickhouse/async"},
		{scope: "DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE", bucket: "clickhouse-delivery-bucket", prefix: "clickhouse/delivery"},
	}

	envLines := []string{
		"HETEROLOG_TARGET=failure-target",
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_ASYNC_LOG_TYPE=loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_DELIVERY_LOG_TYPE=loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_LOKI_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES=fileArchive",
		"HETEROLOG_CLOUDWATCH_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES=fileArchive",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION=us-east-1",
		"HETEROLOG_OBJECT_STORAGE_S3_ENDPOINT=http://localhost:9000",
		"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=test-access-key",
		"HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=test-secret-key",
		"HETEROLOG_OBJECT_STORAGE_S3_TIMEOUT_SECOND=2",
	}
	for _, scope := range scopes {
		envLines = append(
			envLines,
			"HETEROLOG_ENABLE_"+scope.scope+"_OBJECT_STORAGE_UPLOAD=true",
			"HETEROLOG_"+scope.scope+"_OBJECT_STORAGE_UPLOAD_TYPE=S3",
			"HETEROLOG_"+scope.scope+"_OBJECT_STORAGE_S3_BUCKET="+scope.bucket,
			"HETEROLOG_"+scope.scope+"_OBJECT_STORAGE_S3_PREFIX="+scope.prefix,
		)
	}

	cfg := loadConfigFromTempEnv(t, envLines)
	syncMapped := ToLoggerConfig(cfg)
	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	if syncMapped.Loki == nil || syncMapped.CloudWatchLogs == nil || syncMapped.ClickHouse == nil {
		t.Fatal("sync backend failure configs are not mapped")
	}
	if asyncMapped.Loki == nil || asyncMapped.CloudWatchLogs == nil || asyncMapped.ClickHouse == nil {
		t.Fatal("async backend failure configs are not mapped")
	}
	if deliveryMapped.Loki == nil || deliveryMapped.CloudWatchLogs == nil || deliveryMapped.ClickHouse == nil {
		t.Fatal("delivery backend failure configs are not mapped")
	}

	configs := []pkgobjectstorage.UploadTargetConfig{
		cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig,
		cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig,
		cfg.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.FailureHandlerConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig,
		cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig,
		cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.FailureHandlerConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig,
		cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig,
		cfg.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		syncMapped.Loki.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		asyncMapped.Loki.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		deliveryMapped.Loki.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		syncMapped.CloudWatchLogs.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		asyncMapped.CloudWatchLogs.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		deliveryMapped.CloudWatchLogs.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		syncMapped.ClickHouse.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		asyncMapped.ClickHouse.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
		deliveryMapped.ClickHouse.FailureStoreConfig.FileArchiveConfig.ObjectStorageUploadConfig,
	}

	for i, scope := range append(scopes, scopes...) {
		assertArchiveUploadConfig(t, configs[i], scope.bucket, scope.prefix)
	}

	mappedTargets := []string{
		syncMapped.Loki.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Target,
		asyncMapped.Loki.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Target,
		deliveryMapped.Loki.FailureStoreConfig.FileArchiveConfig.Target,
		syncMapped.CloudWatchLogs.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Target,
		asyncMapped.CloudWatchLogs.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Target,
		deliveryMapped.CloudWatchLogs.FailureStoreConfig.FileArchiveConfig.Target,
		syncMapped.ClickHouse.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Target,
		asyncMapped.ClickHouse.FailureStoreConfig.DurableStoreConfig.FileArchiveConfig.Target,
		deliveryMapped.ClickHouse.FailureStoreConfig.FileArchiveConfig.Target,
	}
	for i, target := range mappedTargets {
		if target != "failure-target" {
			t.Fatalf("mapped file archive target[%d] = %q, want failure-target", i, target)
		}
	}

	assertDurableTypes(t, cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, cfg.AsyncBestEffortMultiLoggerConfig.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig.DurableType, "spool", "fileArchive")
	assertDurableTypes(t, cfg.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.FailureHandlerConfig.DurableType, "fileArchive")
	assertDurableTypes(t, cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.FailureHandlerConfig.DurableType, "fileArchive")
	assertDurableTypes(t, cfg.DeliveryLogger.DeliveryClickHouseConfig.DeliveryClickHouseFlushManagerConfig.FailureHandlerConfig.DurableType, "fileArchive")

}

// TestLoadConfigDefersBackendFailureArchiveS3ValidationToMapperは、fileArchiveを選択した
// backendのS3 upload設定をLoadConfigが読み込み、S3の必須値の検証をmapperへ委ねることを
// 確認します。DURABLE_TYPESの既定値はspoolなので、fileArchiveを明示しないとarchive設定は
// 読み込まれません。
func TestLoadConfigDefersBackendFailureArchiveS3ValidationToMapper(t *testing.T) {
	tests := []struct {
		name        string
		destination string
		durableKey  string
		scope       string
		extra       []string
		upload      func(AppConfig) pkgobjectstorage.UploadTargetConfig
	}{
		{
			name: "loki", destination: "loki", durableKey: "HETEROLOG_LOKI_DURABLE_TYPES", scope: "LOKI_DURABLE_FILE_ARCHIVE",
			upload: func(cfg AppConfig) pkgobjectstorage.UploadTargetConfig {
				return cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig
			},
		},
		{
			name: "cloudwatchlogs", destination: "cloudwatchlogs", durableKey: "HETEROLOG_CLOUDWATCH_DURABLE_TYPES", scope: "CLOUDWATCH_DURABLE_FILE_ARCHIVE",
			// S3以外の理由でmapperが拒否しないよう、CloudWatch Logsの認証情報は与えます。
			extra: []string{"HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID=test-access", "HETEROLOG_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=test-secret"},
			upload: func(cfg AppConfig) pkgobjectstorage.UploadTargetConfig {
				return cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig
			},
		},
		{
			name: "clickhouse", destination: "clickhouse", durableKey: "HETEROLOG_CLICKHOUSE_DURABLE_TYPES", scope: "CLICKHOUSE_DURABLE_FILE_ARCHIVE",
			upload: func(cfg AppConfig) pkgobjectstorage.UploadTargetConfig {
				return cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig.Fsstore.FileArchiveConfig.ObjectStorageUploadConfig
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, append([]string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=" + tt.destination,
				"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
				tt.durableKey + "=fileArchive",
				"HETEROLOG_ENABLE_" + tt.scope + "_OBJECT_STORAGE_UPLOAD=true",
				"HETEROLOG_" + tt.scope + "_OBJECT_STORAGE_UPLOAD_TYPE=s3",
			}, tt.extra...))
			upload := tt.upload(cfg)
			if !upload.Enabled || !upload.Has(pkgobjectstorage.UploadTargetS3) {
				t.Fatalf("%s archive upload config = %+v, want enabled s3 config", tt.name, upload)
			}
			assertMapperRejectsMissingFailureArchiveS3Bucket(t, cfg)
		})
	}
}

// assertMapperRejectsMissingFailureArchiveS3Bucketは、LoadConfigが受理したS3 upload設定を
// mapper後のvalidationが必須bucketの不足として拒否することを確認します。
func assertMapperRejectsMissingFailureArchiveS3Bucket(t *testing.T, cfg AppConfig) {
	t.Helper()

	if _, err := MapLoggerConfig(cfg); !errors.Is(err, pkgs3uploader.ErrBucketRequired) {
		t.Fatalf("MapLoggerConfig() error = %v, want %v", err, pkgs3uploader.ErrBucketRequired)
	}
}

// TestPublicEnvExampleExposesBackendFailureArchiveDurableAndObjectStorageConfigは、
// 配布用.env.exampleを直接読み込みます。fixtureの.envと.env.exampleは同じ公開例から
// 生成されるため、両者の一致を比べても公開設定の検証にはなりません。
func TestPublicEnvExampleExposesBackendFailureArchiveDurableAndObjectStorageConfig(t *testing.T) {
	example := readPublicEnvExample(t)

	// local archive directoryはmodeとbackendを省略せず、全LoggerContractKindで同じ語順にします。
	// 公開例をdefault定数と同じ値へ固定し、旧directoryへ戻るのを防ぎます。
	archiveDirectories := map[string]string{
		"HETEROLOG_OPENSEARCH_FAILURE_ARCHIVE_DIR":          "opensearch_failure_archives",
		"HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR":    "async_opensearch_failure_archives",
		"HETEROLOG_DELIVERY_OPENSEARCH_FAILURE_ARCHIVE_DIR": "delivery_opensearch_failure_archives",
		"HETEROLOG_LOKI_FAILURE_ARCHIVE_DIR":                "loki_failure_archives",
		"HETEROLOG_ASYNC_LOKI_FAILURE_ARCHIVE_DIR":          "async_loki_failure_archives",
		"HETEROLOG_DELIVERY_LOKI_FAILURE_ARCHIVE_DIR":       "delivery_loki_failure_archives",
		"HETEROLOG_CLOUDWATCH_FAILURE_ARCHIVE_DIR":          "cloudwatchlogs_failure_archives",
		"HETEROLOG_ASYNC_CLOUDWATCH_FAILURE_ARCHIVE_DIR":    "async_cloudwatchlogs_failure_archives",
		"HETEROLOG_DELIVERY_CLOUDWATCH_FAILURE_ARCHIVE_DIR": "delivery_cloudwatchlogs_failure_archives",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR":          "clickhouse_failure_archives",
		"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_ARCHIVE_DIR":    "async_clickhouse_failure_archives",
		"HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR": "delivery_clickhouse_failure_archives",
	}
	for key, want := range archiveDirectories {
		if got := example[key]; got != want {
			t.Fatalf(".env.example %s = %q, want %q", key, got, want)
		}
	}

	scopes := []string{
		"LOKI_DURABLE_FILE_ARCHIVE",
		"ASYNC_LOKI_DURABLE_FILE_ARCHIVE",
		"DELIVERY_LOKI_DURABLE_FILE_ARCHIVE",
		"CLOUDWATCH_DURABLE_FILE_ARCHIVE",
		"ASYNC_CLOUDWATCH_DURABLE_FILE_ARCHIVE",
		"DELIVERY_CLOUDWATCH_DURABLE_FILE_ARCHIVE",
		"CLICKHOUSE_DURABLE_FILE_ARCHIVE",
		"ASYNC_CLICKHOUSE_DURABLE_FILE_ARCHIVE",
		"DELIVERY_CLICKHOUSE_DURABLE_FILE_ARCHIVE",
	}
	for _, scope := range scopes {
		keys := []string{
			"HETEROLOG_ENABLE_" + scope + "_OBJECT_STORAGE_UPLOAD",
			"HETEROLOG_" + scope + "_OBJECT_STORAGE_UPLOAD_TYPE",
			"HETEROLOG_" + scope + "_OBJECT_STORAGE_S3_BUCKET",
			"HETEROLOG_" + scope + "_OBJECT_STORAGE_S3_PREFIX",
		}
		for _, key := range keys {
			if _, ok := example[key]; !ok {
				t.Fatalf(".env.example is missing failure archive object storage key %s", key)
			}
		}
	}

	durableValues := map[string]string{
		"HETEROLOG_LOKI_DURABLE_TYPES":                "spool,fileArchive",
		"HETEROLOG_ASYNC_LOKI_DURABLE_TYPES":          "spool,fileArchive",
		"HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES":       "fileArchive",
		"HETEROLOG_CLOUDWATCH_DURABLE_TYPES":          "spool,fileArchive",
		"HETEROLOG_ASYNC_CLOUDWATCH_DURABLE_TYPES":    "spool,fileArchive",
		"HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES": "fileArchive",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES":          "spool,fileArchive",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES":    "spool,fileArchive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES": "fileArchive",
	}
	for key, want := range durableValues {
		if got := example[key]; got != want {
			t.Fatalf(".env.example %s = %q, want %q", key, got, want)
		}
	}

	// Delivery用failure handlerはfileArchiveだけを扱い、Delivery専用factoryが
	// 同時実行数を固定するため、同期・非同期どちらの呼び出し経路でも並行数のenv keyは公開しません。
	obsoleteDeliveryKeys := []string{
		"HETEROLOG_MAX_DELIVERY_LOKI_FAILURE_HANDLER_CONCURRENT",
		"HETEROLOG_MAX_DELIVERY_CLOUDWATCH_FAILURE_HANDLER_CONCURRENT",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT",
	}
	for _, key := range obsoleteDeliveryKeys {
		if _, ok := example[key]; ok {
			t.Fatalf("obsolete delivery key %s remains in .env.example", key)
		}
	}

	// sync/async/delivery failure archiveの内部エラーは設定したloggerへ通知するため、
	// 4 backend × 3 LoggerContractKindのrouting keyを公開例に掲載します。
	// 配布例は認証情報なしで利用できるようstdoutを既定の通知先にします。
	failureArchiveRoutingValues := map[string]string{
		"OPENSEARCH":          "stdout",
		"ASYNC_OPENSEARCH":    "stdout",
		"DELIVERY_OPENSEARCH": "stdout",
		"LOKI":                "stdout",
		"ASYNC_LOKI":          "stdout",
		"DELIVERY_LOKI":       "stdout",
		"CLOUDWATCH":          "stdout",
		"ASYNC_CLOUDWATCH":    "stdout",
		"DELIVERY_CLOUDWATCH": "stdout",
		"CLICKHOUSE":          "stdout",
		"ASYNC_CLICKHOUSE":    "stdout",
		"DELIVERY_CLICKHOUSE": "stdout",
	}
	failureArchiveHandlerSuffixes := []string{
		"_DURABLE_FILE_ERROR_HANDLER",
		"_DURABLE_FILE_WARN_HANDLER",
		"_DURABLE_FILE_INFO_HANDLER",
		"_DURABLE_FILE_DEBUG_HANDLER",
	}
	for scope, want := range failureArchiveRoutingValues {
		for _, suffix := range failureArchiveHandlerSuffixes {
			key := "HETEROLOG_" + scope + suffix
			if got := example[key]; got != want {
				t.Fatalf(".env.example %s = %q, want %q", key, got, want)
			}
		}
	}

	// sync/asyncの4 backendは、spool保存処理自身の内部ログを設定したloggerへ通知します。
	spoolHandlerValues := map[string]string{
		"OPENSEARCH":       "stdout",
		"ASYNC_OPENSEARCH": "stdout",
		"LOKI":             "stdout",
		"ASYNC_LOKI":       "stdout",
		"CLOUDWATCH":       "stdout",
		"ASYNC_CLOUDWATCH": "stdout",
		"CLICKHOUSE":       "stdout",
		"ASYNC_CLICKHOUSE": "stdout",
	}
	unsupportedSpoolHandlerScopes := []string{"DELIVERY_OPENSEARCH", "DELIVERY_LOKI", "DELIVERY_CLOUDWATCH", "DELIVERY_CLICKHOUSE"}
	spoolHandlerSuffixes := []string{
		"_SPOOL_FILE_ERROR_HANDLER",
		"_SPOOL_FILE_WARN_HANDLER",
		"_SPOOL_FILE_INFO_HANDLER",
		"_SPOOL_FILE_DEBUG_HANDLER",
	}
	for scope, want := range spoolHandlerValues {
		for _, suffix := range spoolHandlerSuffixes {
			key := "HETEROLOG_" + scope + suffix
			if got := example[key]; got != want {
				t.Fatalf(".env.example %s = %q, want %q", key, got, want)
			}
		}
	}
	for _, scope := range unsupportedSpoolHandlerScopes {
		for _, suffix := range spoolHandlerSuffixes {
			key := "HETEROLOG_" + scope + suffix
			if _, ok := example[key]; ok {
				t.Fatalf("unsupported spool handler key %s remains in .env.example", key)
			}
		}
	}
}

func assertDurableTypes(t *testing.T, got []loggerdurable.DurableType, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("durable types = %v, want %v", got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("durable types = %v, want %v", got, want)
		}
	}
}

func assertArchiveUploadConfig(
	t *testing.T,
	cfg pkgobjectstorage.UploadTargetConfig,
	wantBucket string,
	wantPrefix string,
) {
	t.Helper()
	if !cfg.Enabled {
		t.Fatal("failure archive object storage upload enabled = false, want true")
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0] != pkgobjectstorage.UploadTargetS3 {
		t.Fatalf("failure archive upload targets = %v, want [s3]", cfg.Targets)
	}
	if cfg.S3.Bucket != wantBucket {
		t.Fatalf("failure archive s3 bucket = %q, want %q", cfg.S3.Bucket, wantBucket)
	}
	if cfg.S3.Prefix != wantPrefix {
		t.Fatalf("failure archive s3 prefix = %q, want %q", cfg.S3.Prefix, wantPrefix)
	}
}

func TestLoadConfigReadsFileLifecycleMode(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_ASYNC_LOG_TYPE=file",
		"HETEROLOG_DELIVERY_LOG_TYPE=file",
		"HETEROLOG_LOG_FILE_LIFECYCLE_MODE=runner",
		"HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_MODE=disable",
		"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_MODE=runner",
	})

	mapped := ToLoggerConfig(cfg)
	if got, want := mapped.File.LifecycleMode, pkgfsstore.FileLifecycleModeRunner; got != want {
		t.Fatalf("mapped file lifecycle mode = %q, want %q", got, want)
	}
	if mapped.File.LifecycleRunnerConfig() == nil {
		t.Fatalf("file lifecycle runner config is nil, want config")
	}

	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	if got, want := asyncMapped.File.LifecycleMode, pkgfsstore.FileLifecycleModeDisable; got != want {
		t.Fatalf("mapped async file lifecycle mode = %q, want %q", got, want)
	}
	if asyncMapped.File.LifecycleRunnerConfig() != nil {
		t.Fatalf("async lifecycle runner config is not nil, want nil")
	}

	if ToDeliveryLoggerConfig(cfg).File.LifecycleRunnerConfig() == nil {
		t.Fatalf("delivery lifecycle runner config is nil, want config")
	}
}

func TestPublicEnvExampleExposesInternalLogDestinationsWithoutLifecycleProfiles(t *testing.T) {
	prefixes := []string{
		"HETEROLOG_LOG_INTERNAL_LOG_",
		"HETEROLOG_ASYNC_LOG_INTERNAL_LOG_",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_",
	}
	suffixes := []string{
		"FILE_LOG_PATH",
		"FILE_TARGET",
		"FILE_LOG_DIR",
		"FILE_ERROR_LOG_DIR",
		"FILE_DEBUG_LOG_DIR",
		"SLACK_CHANNEL",
		"SLACK_ERROR_CHANNEL",
		"SLACK_WARN_CHANNEL",
		"SLACK_INFO_CHANNEL",
		"SLACK_DEBUG_CHANNEL",
		"OPENSEARCH_INDEX",
		"LOKI_LABELS",
		"CLOUDWATCH_LOG_GROUP_NAME",
		"CLOUDWATCH_LOG_STREAM_NAME",
		"CLICKHOUSE_DATABASE",
		"CLICKHOUSE_TABLE",
	}

	keys := readUniqueEnvKeys(t, publicEnvExamplePath(t))
	for key := range keys {
		if strings.Contains(key, "_FILE_LIFECYCLE_PROFILE_") {
			t.Errorf(".env.example exposes removed lifecycle profile key %s", key)
		}
	}
	for _, prefix := range prefixes {
		for _, suffix := range suffixes {
			key := prefix + suffix
			if _, ok := keys[key]; !ok {
				t.Errorf(".env.example is missing internal-log destination key %s", key)
			}
		}
	}
}

func TestLoadConfigMapsDebugLogDirectoriesForEveryLoggerMode(t *testing.T) {
	prefixedDir := func(prefix, dir string) string {
		if dir == "" {
			return ""
		}
		return prefix + dir
	}
	for _, tt := range []struct {
		name             string
		setDebugDirs     bool
		debugDir         string
		internalDebugDir string
		internalLogDir   string
	}{
		{name: "omitted debug directory"},
		{name: "empty debug directory", setDebugDirs: true},
		{name: "explicit debug directories", setDebugDirs: true, debugDir: "debug", internalDebugDir: "internal-debug"},
		{name: "debug-only internal override", setDebugDirs: true, internalDebugDir: "internal-debug"},
		{name: "shared debug follows internal log directory", internalLogDir: "internal"},
		{name: "internal log directory inherits explicit debug directory", setDebugDirs: true, debugDir: "debug", internalLogDir: "internal"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			envLines := []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
			}
			for _, prefix := range []string{"", "ASYNC_", "DELIVERY_"} {
				envLines = append(envLines,
					"HETEROLOG_"+prefix+"LOG_TYPE=stdout,file",
					"HETEROLOG_"+prefix+"STDOUT_LOGGER_ERROR_HANDLER=file",
					"HETEROLOG_"+prefix+"LOG_DIR="+prefix+"logs",
				)
				if tt.setDebugDirs {
					envLines = append(envLines,
						"HETEROLOG_"+prefix+"DEBUG_LOG_DIR="+prefixedDir(prefix, tt.debugDir),
						"HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_FILE_DEBUG_LOG_DIR="+prefixedDir(prefix, tt.internalDebugDir),
					)
				}
				if tt.debugDir != "" {
					envLines = append(envLines, "HETEROLOG_ENABLE_"+prefix+"LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true")
				}
				if tt.internalDebugDir != "" {
					envLines = append(envLines, "HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_FILE_SEPARATE_DEBUG_LOG_DIR=true")
				}
				if tt.internalLogDir != "" {
					envLines = append(envLines, "HETEROLOG_"+prefix+"LOG_INTERNAL_LOG_FILE_LOG_DIR="+tt.internalLogDir)
				}
			}
			cfg := loadConfigFromTempEnv(t, envLines)
			syncMapped, err := MapLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapLoggerConfig(): %v", err)
			}
			asyncMapped, err := MapAsyncLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapAsyncLoggerConfig(): %v", err)
			}
			deliveryMapped, err := MapDeliveryLoggerConfig(cfg)
			if err != nil {
				t.Fatalf("MapDeliveryLoggerConfig(): %v", err)
			}
			var syncInternal, asyncInternal, deliveryInternal *pkgfsstore.FsstoreConfig
			if syncMapped.InternalLogDestinations != nil && syncMapped.InternalLogDestinations.File != nil {
				syncInternal = &syncMapped.InternalLogDestinations.File.FsstoreConfig.Layout
			}
			if asyncMapped.InternalLogDestinations != nil && asyncMapped.InternalLogDestinations.File != nil {
				asyncInternal = &asyncMapped.InternalLogDestinations.File.FsstoreConfig.Layout
			}
			if deliveryMapped.InternalLogDestinations != nil && deliveryMapped.InternalLogDestinations.File != nil {
				deliveryInternal = &deliveryMapped.InternalLogDestinations.File.FsstoreConfig.Layout
			}
			for _, contract := range []struct {
				name           string
				prefix         string
				loadedDebug    string
				loadedOverride string
				base           *pkgfsstore.FsstoreConfig
				internal       *pkgfsstore.FsstoreConfig
			}{
				{"sync", "", cfg.Logger.FileSystemStoreConfig.DebugLogDir, cfg.Logger.InternalLogDestinationConfig.FileDebugLogDir, &syncMapped.File.FsstoreConfig.Layout, syncInternal},
				{"async", "ASYNC_", cfg.AsyncBestEffortMultiLoggerConfig.FileSystemStoreConfig.DebugLogDir, cfg.AsyncBestEffortMultiLoggerConfig.InternalLogDestinationConfig.FileDebugLogDir, &asyncMapped.File.FsstoreConfig.Layout, asyncInternal},
				{"delivery", "DELIVERY_", cfg.DeliveryLogger.FileSystemStoreConfig.DebugLogDir, cfg.DeliveryLogger.InternalLogDestinationConfig.FileDebugLogDir, &deliveryMapped.File.FsstoreConfig.Layout, deliveryInternal},
			} {
				wantBaseDebugDir := prefixedDir(contract.prefix, tt.debugDir)
				wantOverride := prefixedDir(contract.prefix, tt.internalDebugDir)
				if contract.loadedDebug != wantBaseDebugDir || contract.base.DebugLogDir != wantBaseDebugDir {
					t.Errorf("%s debug directory: loaded = %q, mapped = %q, want %q", contract.name, contract.loadedDebug, contract.base.DebugLogDir, wantBaseDebugDir)
				}
				if contract.loadedOverride != wantOverride {
					t.Errorf("%s loaded internal debug directory = %q, want %q", contract.name, contract.loadedOverride, wantOverride)
				}
				if tt.internalDebugDir == "" && tt.internalLogDir == "" {
					if contract.internal != nil {
						t.Errorf("%s created an internal file destination without an override", contract.name)
					}
					continue
				}
				if contract.internal == nil {
					t.Fatalf("%s internal file destination is nil", contract.name)
				}
				wantDebugDir := wantOverride
				if wantDebugDir == "" {
					wantDebugDir = wantBaseDebugDir
				}
				if contract.internal.DebugLogDir != wantDebugDir {
					t.Errorf("%s internal debug directory = %q, want %q", contract.name, contract.internal.DebugLogDir, wantDebugDir)
				}
				wantLogDir := contract.base.LogDir
				if tt.internalLogDir != "" {
					wantLogDir = tt.internalLogDir
				}
				if contract.internal.LogDir != wantLogDir {
					t.Errorf("%s internal log directory = %q, want %q", contract.name, contract.internal.LogDir, wantLogDir)
				}
			}
		})
	}
}

func TestLoadConfigMapsLifecycleHandlerRoutingForEveryLoggerMode(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_ASYNC_LOG_TYPE=file",
		"HETEROLOG_DELIVERY_LOG_TYPE=file",
		"HETEROLOG_LOG_FILE_LIFECYCLE_MODE=runner",
		"HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_MODE=runner",
		"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_MODE=runner",
		"HETEROLOG_LOG_FILE_LIFECYCLE_ERROR_HANDLER=file",
		"HETEROLOG_LOG_FILE_LIFECYCLE_DEBUG_HANDLER=stdout",
		"HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_WARN_HANDLER=stdout",
		"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_INFO_HANDLER=stdout",
	})

	syncMapped := ToLoggerConfig(cfg)
	syncLifecycle := syncMapped.File.LifecycleRunnerConfig()
	if syncLifecycle == nil {
		t.Fatal("sync lifecycle runner config is nil")
	}
	if got := syncLifecycle.HandlerConfig.OnErrorConfig; len(got) != 1 || got[0] != "file" {
		t.Fatalf("sync lifecycle error handlers = %v", got)
	}
	if syncMapped.Stdout == nil {
		t.Fatal("sync lifecycle stdout handler target config is nil")
	}

	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	asyncLifecycle := asyncMapped.File.LifecycleRunnerConfig()
	if asyncLifecycle == nil {
		t.Fatal("async lifecycle runner config is nil")
	}
	if got := asyncLifecycle.HandlerConfig.OnWarnConfig; len(got) != 1 || got[0] != "stdout" {
		t.Fatalf("async lifecycle warn handlers = %v", got)
	}
	if asyncMapped.Stdout == nil {
		t.Fatal("async lifecycle stdout handler target config is nil")
	}

	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	deliveryLifecycle := deliveryMapped.File.LifecycleRunnerConfig()
	if deliveryLifecycle == nil {
		t.Fatal("delivery lifecycle runner config is nil")
	}
	if got := deliveryLifecycle.HandlerConfig.OnInfoConfig; len(got) != 1 || got[0] != "stdout" {
		t.Fatalf("delivery lifecycle info handlers = %v", got)
	}
	if deliveryMapped.Stdout == nil {
		t.Fatal("delivery lifecycle stdout handler target config is nil")
	}
}

func TestLoadConfigMapsInternalLogDestinationsFromEnvFile(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=stdout",
		"HETEROLOG_STDOUT_LOGGER_ERROR_HANDLER=file,opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_STDOUT_LOGGER_WARN_HANDLER=slack",
		"HETEROLOG_LOG_INTERNAL_LOG_FILE_LOG_PATH=/tmp/internal-log",
		"HETEROLOG_LOG_INTERNAL_LOG_FILE_TARGET=system-internal",
		"HETEROLOG_LOG_INTERNAL_LOG_FILE_LOG_DIR=internal",
		"HETEROLOG_LOG_INTERNAL_LOG_FILE_ERROR_LOG_DIR=internal-errors",
		"HETEROLOG_SLACK_API_KEY=test-token",
		"HETEROLOG_SLACK_NORMAL_CHANNEL=base-channel",
		"HETEROLOG_LOG_INTERNAL_LOG_SLACK_CHANNEL=internal-channel",
		"HETEROLOG_LOG_INTERNAL_LOG_OPENSEARCH_INDEX=logger-internal",
		"HETEROLOG_OPENSEARCH_USERNAME=test-user",
		"HETEROLOG_OPENSEARCH_PASSWORD=test-password",
		`HETEROLOG_LOG_INTERNAL_LOG_LOKI_LABELS={"event_scope":"logger_internal"}`,
		"HETEROLOG_LOG_INTERNAL_LOG_CLOUDWATCH_LOG_GROUP_NAME=internal-group",
		"HETEROLOG_LOG_INTERNAL_LOG_CLOUDWATCH_LOG_STREAM_NAME=internal-stream",
		"HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID=test-access",
		"HETEROLOG_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=test-secret",
		"HETEROLOG_LOG_INTERNAL_LOG_CLICKHOUSE_DATABASE=observability",
		"HETEROLOG_LOG_INTERNAL_LOG_CLICKHOUSE_TABLE=logger_internal",
	})

	mapped, err := MapLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapLoggerConfig() with internal-log destinations: %v", err)
	}
	destinations := mapped.InternalLogDestinations
	if destinations == nil {
		t.Fatal("mapped internal-log destinations are nil")
	}
	if got := destinations.File.FsstoreConfig.Layout.LogPath; got != "/tmp/internal-log" {
		t.Fatalf("internal-log file log path = %q", got)
	}
	if got := destinations.File.FsstoreConfig.Layout.Target; got != "system-internal" {
		t.Fatalf("internal-log file target = %q", got)
	}
	if got := destinations.OpenSearch.Index; got != "logger-internal" {
		t.Fatalf("internal-log opensearch index = %q", got)
	}
	if got := destinations.Loki.StaticLabels["event_scope"]; got != "logger_internal" {
		t.Fatalf("internal-log loki event_scope label = %q", got)
	}
	if got := destinations.CloudWatchLogs.LogStreamName; got != "internal-stream" {
		t.Fatalf("internal-log cloudwatch stream = %q", got)
	}
	if got := destinations.ClickHouseClient.Table; got != "logger_internal" {
		t.Fatalf("internal-log clickhouse table = %q", got)
	}
	if got := destinations.Slack.Channel; got != "internal-channel" {
		t.Fatalf("internal-log slack channel = %q", got)
	}
}

func TestLoadConfigMapsInternalLogDestinationsForAsyncAndDelivery(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_ASYNC_LOG_TYPE=stdout",
		"HETEROLOG_DELIVERY_LOG_TYPE=stdout",
		"HETEROLOG_ASYNC_STDOUT_LOGGER_ERROR_HANDLER=loki",
		"HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER=loki",
		`HETEROLOG_ASYNC_LOG_INTERNAL_LOG_LOKI_LABELS={"event_scope":"async_internal"}`,
		`HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_LABELS={"event_scope":"delivery_internal"}`,
	})

	asyncMapped, err := MapAsyncLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapAsyncLoggerConfig() with internal-log destination: %v", err)
	}
	if asyncMapped.InternalLogDestinations == nil ||
		asyncMapped.InternalLogDestinations.Loki == nil {
		t.Fatal("async internal-log Loki destination is nil")
	}
	if got := asyncMapped.InternalLogDestinations.Loki.StaticLabels["event_scope"]; got != "async_internal" {
		t.Fatalf("async internal-log label = %q", got)
	}
	deliveryMapped, err := MapDeliveryLoggerConfig(cfg)
	if err != nil {
		t.Fatalf("MapDeliveryLoggerConfig() with internal-log destination: %v", err)
	}
	if deliveryMapped.InternalLogDestinations == nil ||
		deliveryMapped.InternalLogDestinations.Loki == nil {
		t.Fatal("delivery internal-log Loki destination is nil")
	}
	if got := deliveryMapped.InternalLogDestinations.Loki.StaticLabels["event_scope"]; got != "delivery_internal" {
		t.Fatalf("delivery internal-log label = %q", got)
	}
}

func TestMapLoggerConfigsRejectProfileQualifiedLifecycleSelectorFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		env       []string
		mapConfig func(AppConfig) error
	}{
		{
			name: "sync",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=true",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_LOG_TYPE=file",
				"HETEROLOG_LOG_FILE_LIFECYCLE_MODE=runner",
				"HETEROLOG_LOG_FILE_LIFECYCLE_ERROR_HANDLER=file:maintenance",
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "async",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
				"HETEROLOG_ASYNC_LOG_TYPE=file",
				"HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_MODE=runner",
				"HETEROLOG_ASYNC_LOG_FILE_LIFECYCLE_WARN_HANDLER=file:maintenance",
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
		},
		{
			name: "delivery",
			env: []string{
				"HETEROLOG_ENABLE_LOGGING=false",
				"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
				"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
				"HETEROLOG_DELIVERY_LOG_TYPE=file",
				"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_MODE=runner",
				"HETEROLOG_DELIVERY_LOG_FILE_LIFECYCLE_INFO_HANDLER=file:maintenance",
			},
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, tt.env)
			err := tt.mapConfig(cfg)
			var selectorErr *pkglogger.HandlerSelectorError
			if !errors.As(err, &selectorErr) || selectorErr.Selector != "file:maintenance" ||
				!errors.Is(err, pkglogger.ErrHandlerSelectorNotConfigured) {
				t.Fatalf("mapping error = %v, want unconfigured handler selector %q", err, "file:maintenance")
			}
		})
	}
}

func TestMapLoggerConfigIgnoresInactiveLifecycleRouting(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_LOG_FILE_LIFECYCLE_MODE=inline",
		"HETEROLOG_LOG_FILE_LIFECYCLE_ERROR_HANDLER=unknown",
	})

	if _, err := MapLoggerConfig(cfg); err != nil {
		t.Fatalf("MapLoggerConfig() with inactive lifecycle routing: %v", err)
	}
}

func TestLoadConfigReadsFileObjectStorageUploadConfigPerClassificationEnabled(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_ERROR_LOGS=true",
		"HETEROLOG_ENABLE_LOG_FILE_SEPARATE_DEBUG_LOGS=true",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_ENABLE_LOG_FILE_DEFAULT_OBJECT_STORAGE_UPLOAD=false",
		"HETEROLOG_ENABLE_LOG_FILE_ERROR_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_ENABLE_LOG_FILE_DEBUG_OBJECT_STORAGE_UPLOAD=false",
		"HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION=us-east-1",
		"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=test-access-key",
		"HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=test-secret-key",
		"HETEROLOG_LOG_FILE_ERROR_OBJECT_STORAGE_S3_BUCKET=error-bucket",
		"HETEROLOG_LOG_FILE_ERROR_OBJECT_STORAGE_S3_PREFIX=error-prefix",
	})

	uploadCfg := cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig
	if !uploadCfg.Enabled {
		t.Fatalf("object storage upload enabled = false, want true")
	}
	if uploadCfg.Default.Enabled {
		t.Fatalf("default upload enabled = true, want false")
	}
	if !uploadCfg.Error.Enabled {
		t.Fatalf("error upload enabled = false, want true")
	}
	if uploadCfg.Debug.Enabled {
		t.Fatalf("debug upload enabled = true, want false")
	}
	if got, want := uploadCfg.Default.S3.Bucket, ""; got != want {
		t.Fatalf("default bucket = %q, want %q", got, want)
	}
	if got, want := uploadCfg.Error.S3.Bucket, "error-bucket"; got != want {
		t.Fatalf("error bucket = %q, want %q", got, want)
	}

	mapped := ToLoggerConfig(cfg)
	if mapped.File == nil {
		t.Fatalf("mapped file logger config is nil")
	}
	if mapped.File.ObjectStorageUploadConfig.Default.Enabled {
		t.Fatalf("mapped default upload enabled = true, want false")
	}
	if !mapped.File.ObjectStorageUploadConfig.Error.Enabled {
		t.Fatalf("mapped error upload enabled = false, want true")
	}
}

func TestLoadConfigReadsAsyncFileObjectStorageUploadConfig(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_ASYNC_LOG_TYPE=file",
		"HETEROLOG_DELIVERY_LOG_TYPE=file",
		"HETEROLOG_ENABLE_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_ASYNC_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION=us-east-1",
		"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=test-access-key",
		"HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=test-secret-key",
		"HETEROLOG_ASYNC_LOG_FILE_OBJECT_STORAGE_S3_BUCKET=async-default-bucket",
		"HETEROLOG_ASYNC_LOG_FILE_OBJECT_STORAGE_S3_PREFIX=async-default-prefix",
		"HETEROLOG_ASYNC_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_BUCKET=async-debug-bucket",
		"HETEROLOG_ASYNC_LOG_FILE_DEBUG_OBJECT_STORAGE_S3_PREFIX=async-debug-prefix",
	})

	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	if asyncMapped.File == nil {
		t.Fatalf("mapped async file logger config is nil")
	}
	if got, want := asyncMapped.File.ObjectStorageUploadConfig.Debug.S3.Bucket, "async-debug-bucket"; got != want {
		t.Fatalf("mapped async debug bucket = %q, want %q", got, want)
	}
	if got, want := asyncMapped.File.ObjectStorageUploadConfig.Error.S3.Bucket, "async-default-bucket"; got != want {
		t.Fatalf("mapped async inherited error bucket = %q, want %q", got, want)
	}
}

func TestLoadConfigReadsDeliveryFileLifecycleAndObjectStorageUploadConfig(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_DELIVERY_LOG_TYPE=file",
		"HETEROLOG_MAX_DELIVERY_LOG_FILE_BACKUPS=7",
		"HETEROLOG_MAX_DELIVERY_LOG_FILE_AGE=8",
		"HETEROLOG_MAX_DELIVERY_LOG_FILE_DIR_SIZE_MB=9",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_COMPRESS=true",
		"HETEROLOG_DELIVERY_LOG_COMPRESS_FORMAT=xz",
		"HETEROLOG_ENABLE_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_DELIVERY_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
		"HETEROLOG_OBJECT_STORAGE_S3_REGION=us-east-1",
		"HETEROLOG_OBJECT_STORAGE_S3_ACCESS_KEY_ID=test-access-key",
		"HETEROLOG_OBJECT_STORAGE_S3_SECRET_ACCESS_KEY=test-secret-key",
		"HETEROLOG_DELIVERY_LOG_FILE_OBJECT_STORAGE_S3_BUCKET=delivery-default-bucket",
		"HETEROLOG_DELIVERY_LOG_FILE_OBJECT_STORAGE_S3_PREFIX=delivery-default-prefix",
	})

	loggerMapped := ToDeliveryLoggerConfig(cfg)
	if loggerMapped.File == nil {
		t.Fatalf("mapped delivery file logger config is nil")
	}
	mapped := loggerMapped.File.LifecycleRunnerConfig()
	if mapped == nil {
		t.Fatalf("mapped delivery file lifecycle config is nil")
	}
	if !mapped.FsstoreConfig.Lifecycle.EnableFileCompress {
		t.Fatalf("mapped delivery file compress enabled = false, want true")
	}
	if got, want := mapped.FsstoreConfig.Lifecycle.CompressFormat, pkgfsstore.CompressXz; got != want {
		t.Fatalf("mapped delivery file compress format = %q, want %q", got, want)
	}
	if got, want := mapped.FsstoreConfig.Lifecycle.MaxFileBackups, 7; got != want {
		t.Fatalf("mapped delivery file backups = %d, want %d", got, want)
	}
	if got, want := mapped.FsstoreConfig.Lifecycle.MaxFileAge, 8; got != want {
		t.Fatalf("mapped delivery file age = %d, want %d", got, want)
	}
	if got, want := mapped.FsstoreConfig.Lifecycle.MaxDirSize, uint64(9)*BytesPerMegabyte; got != want {
		t.Fatalf("mapped delivery file dir size = %d, want %d", got, want)
	}
	if got, want := mapped.ObjectStorageUploadConfig.Default.S3.Bucket, "delivery-default-bucket"; got != want {
		t.Fatalf("mapped delivery default bucket = %q, want %q", got, want)
	}
}

func TestLoadConfigDefersFileObjectStorageS3ValidationToMapper(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=file",
		"HETEROLOG_ENABLE_LOG_FILE_OBJECT_STORAGE_UPLOAD=true",
		"HETEROLOG_LOG_FILE_OBJECT_STORAGE_UPLOAD_TYPE=s3",
	})
	upload := cfg.Logger.FileConfig.FileSystemStoreConfig.ObjectStorageUploadConfig
	if !upload.Enabled || !upload.Default.Has(pkgobjectstorage.UploadTargetS3) {
		t.Fatalf("file upload config = %+v, want enabled default s3 config", upload)
	}
}

func TestLoadLoggerTimeoutBudgetPolicyRejectsInvalidEnvValue(t *testing.T) {
	t.Setenv("HETEROLOG_LOG_TYPE", "stdout")
	t.Setenv("HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY", "forever")

	if _, err := loadLoggerConfig(processEnvironmentForTest()); err == nil {
		t.Fatalf("loadLoggerConfig() error = nil, want invalid policy error")
	}
}

func TestValidateLoggerTimeoutHierarchyRejectsRetryBudgetOverParentTimeout(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
	cfg.Logger.Timeout = 3 * time.Second
	cfg.Logger.LokiConfig.LokiRetryAbleClientConfig = logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetry,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	cfg.LokiClient.Timeout = time.Second

	if err := validateLoggerTimeoutHierarchy(cfg); err == nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() error = nil, want retry budget error")
	}
}

func TestValidateLoggerTimeoutHierarchyUsesRetryPolicyWorstCaseAttempts(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
	// 旧計算の初回 + 1 retryなら2.2秒で収まりますが、transient/throttledの
	// 最悪ケースは3 attempts + backoffで3.6秒になるため、親3秒では不足します。
	cfg.Logger.Timeout = 3 * time.Second
	cfg.Logger.LokiConfig.LokiRetryAbleClientConfig = logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicy,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	cfg.LokiClient.Timeout = time.Second

	if err := validateLoggerTimeoutHierarchy(cfg); err == nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() error = nil, want retry_policy worst-case budget error")
	}
}

func TestValidateLoggerTimeoutHierarchyAllowsRetryBudgetOverParentTimeoutWithMayCutoff(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
	cfg.Logger.Timeout = 3 * time.Second
	cfg.Logger.TimeoutBudgetPolicy = timeoutbudget.TimeoutBudgetMayCutoff
	cfg.Logger.LokiConfig.LokiRetryAbleClientConfig = logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetry,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	cfg.LokiClient.Timeout = time.Second

	if err := validateLoggerTimeoutHierarchy(cfg); err != nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() error = %v, want nil", err)
	}
}

func TestValidateLoggerTimeoutHierarchyRejectsNonPositiveParentTimeout(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
	cfg.Logger.Timeout = 0
	cfg.LokiClient.Timeout = time.Second

	err := validateLoggerTimeoutHierarchy(cfg)
	requireTimeoutBudgetConfigError(
		t,
		err,
		pkglogger.LoggerContractSync,
		logconfig.LogOutputLoki,
		ErrNonPositiveParentTimeout,
	)
}

func TestValidateLoggerTimeoutHierarchyAllowsNonPositiveParentTimeoutWithUnbounded(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
	cfg.Logger.Timeout = 0
	cfg.Logger.TimeoutBudgetPolicy = timeoutbudget.TimeoutBudgetUnbounded
	cfg.LokiClient.Timeout = time.Second

	if err := validateLoggerTimeoutHierarchy(cfg); err != nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() error = %v, want nil", err)
	}
}

func TestValidateLoggerTimeoutHierarchyRejectsNonPositiveClientTimeoutWithUnbounded(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputLoki}
	cfg.Logger.Timeout = 0
	cfg.Logger.TimeoutBudgetPolicy = timeoutbudget.TimeoutBudgetUnbounded
	cfg.LokiClient.Timeout = 0

	err := validateLoggerTimeoutHierarchy(cfg)
	requireTimeoutBudgetConfigError(
		t,
		err,
		pkglogger.LoggerContractSync,
		logconfig.LogOutputLoki,
		ErrNonPositiveClientTimeout,
	)
}

func TestValidateLoggerTimeoutHierarchyIncludesOpenSearchFlushAttempts(t *testing.T) {
	var cfg AppConfig
	cfg.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{logconfig.LogOutputOpenSearch}
	cfg.Logger.Timeout = 10 * time.Second
	cfg.Logger.OpenSearchConfig.MaxItemRetries = 2
	cfg.Logger.OpenSearchConfig.OpenSearchRetryAbleClientConfig = logconfig.OpenSearchRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetry,
		MaxRetries:      2,
		BaseDelay:       200 * time.Millisecond,
		MaxDelay:        time.Second,
	}
	cfg.OpenSearchClient.Timeout = time.Second

	if err := validateLoggerTimeoutHierarchy(cfg); err == nil {
		t.Fatalf("validateLoggerTimeoutHierarchy() error = nil, want OpenSearch flush retry budget error")
	}
}

func TestLoadConfigLoadsOpenSearchClientForLoggerDestination(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_APP_NAME=opensearch-destination-test",
		"HETEROLOG_TARGET=opensearch-destination-target",
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_ENABLE_OPENSEARCH=false",
		"HETEROLOG_LOG_TYPE=opensearch",
		"HETEROLOG_LOG_TIMEOUT_SECOND=5",
		"HETEROLOG_OPENSEARCH_ENDPOINT=http://sync-opensearch:9200",
		"HETEROLOG_OPENSEARCH_TIMEOUT_SECOND=1",
		"HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE=none",
		"HETEROLOG_OPENSEARCH_INDEX=sync-index",
	})

	if got, want := cfg.OpenSearchClient.Endpoint, "http://sync-opensearch:9200"; got != want {
		t.Fatalf("OpenSearchClient.Endpoint = %q, want %q", got, want)
	}
	if got, want := cfg.OpenSearchClient.Timeout, time.Second; got != want {
		t.Fatalf("OpenSearchClient.Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.Index, "sync-index"; got != want {
		t.Fatalf("OpenSearch index = %q, want %q", got, want)
	}
}

func TestLoadConfigLoadsAsyncOpenSearchClientForAsyncLoggerDestination(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_APP_NAME=async-opensearch-destination-test",
		"HETEROLOG_TARGET=async-opensearch-destination-target",
		"HETEROLOG_ENABLE_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH=false",
		"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND=5",
		"HETEROLOG_MAX_ASYNC_LOG_PARALLEL=2",
		"HETEROLOG_ASYNC_OPENSEARCH_ENDPOINT=http://async-opensearch:9200",
		"HETEROLOG_ASYNC_OPENSEARCH_TIMEOUT_SECOND=1",
		"HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE=none",
		"HETEROLOG_ASYNC_OPENSEARCH_INDEX=async-index",
	})

	if got, want := cfg.AsyncOpenSearchClient.Endpoint, "http://async-opensearch:9200"; got != want {
		t.Fatalf("AsyncOpenSearchClient.Endpoint = %q, want %q", got, want)
	}
	if got, want := cfg.AsyncOpenSearchClient.Timeout, time.Second; got != want {
		t.Fatalf("AsyncOpenSearchClient.Timeout = %s, want %s", got, want)
	}
	if got, want := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.Index, "async-index"; got != want {
		t.Fatalf("async OpenSearch index = %q, want %q", got, want)
	}
}

func TestLoadConfigKeepsExplicitOpenSearchClientEnableWithoutLoggerDestination(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_APP_NAME=opensearch-client-enable-test",
		"HETEROLOG_TARGET=opensearch-client-enable-target",
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_ENABLE_OPENSEARCH=true",
		"HETEROLOG_ENABLE_ASYNC_OPENSEARCH=true",
		"HETEROLOG_LOG_TYPE=stdout",
		"HETEROLOG_ASYNC_LOG_TYPE=stdout",
		"HETEROLOG_LOG_TIMEOUT_SECOND=2",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND=2",
		"HETEROLOG_MAX_ASYNC_LOG_PARALLEL=2",
		"HETEROLOG_OPENSEARCH_ENDPOINT=http://sync-enabled-opensearch:9200",
		"HETEROLOG_OPENSEARCH_TIMEOUT_SECOND=1",
		"HETEROLOG_ASYNC_OPENSEARCH_ENDPOINT=http://async-enabled-opensearch:9200",
		"HETEROLOG_ASYNC_OPENSEARCH_TIMEOUT_SECOND=1",
	})

	if got, want := cfg.OpenSearchClient.Endpoint, "http://sync-enabled-opensearch:9200"; got != want {
		t.Fatalf("OpenSearchClient.Endpoint = %q, want %q", got, want)
	}
	if got, want := cfg.AsyncOpenSearchClient.Endpoint, "http://async-enabled-opensearch:9200"; got != want {
		t.Fatalf("AsyncOpenSearchClient.Endpoint = %q, want %q", got, want)
	}
	if loggerCfg := ToLoggerConfig(cfg); loggerCfg.OpenSearch != nil || loggerCfg.OpenSearchClient != nil {
		t.Fatalf("ToLoggerConfig() built OpenSearch logger/client without opensearch destination")
	}
	if loggerCfg := ToAsyncBestEffortMultiLoggerConfig(cfg); loggerCfg.OpenSearch != nil || loggerCfg.OpenSearchClient != nil {
		t.Fatalf("ToAsyncBestEffortMultiLoggerConfig() built OpenSearch logger/client without opensearch destination")
	}
}

func TestLoadConfigMovesClickHouseConfigAcrossLayers(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	envPath := filepath.Join(t.TempDir(), ".env")
	// spool rootは実体pathへ解決されるため、期待値も解決済みの一時directoryから作ります。
	systemLibDir := filepath.Join(resolvedTempDirForTest(t), "var-lib")
	clearProcessEnvForTest(t)

	envBody := strings.Join([]string{
		"HETEROLOG_APP_NAME=clickhouse-test",
		"HETEROLOG_TARGET=clickhouse-target",
		"HETEROLOG_SYSTEM_LIB_DIR=" + systemLibDir,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=clickhouse",
		"HETEROLOG_ASYNC_LOG_TYPE=clickhouse",
		"HETEROLOG_DELIVERY_LOG_TYPE=clickhouse",
		"HETEROLOG_LOG_TIMEOUT_SECOND=2",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND=30",
		"HETEROLOG_MAX_ASYNC_LOG_PARALLEL=9",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND=12",
		"HETEROLOG_CLICKHOUSE_ENDPOINT=http://sync-clickhouse:8123",
		"HETEROLOG_CLICKHOUSE_DATABASE=sync_db",
		"HETEROLOG_CLICKHOUSE_TABLE=sync_logs",
		"HETEROLOG_CLICKHOUSE_TIMEOUT_SECOND=1",
		"HETEROLOG_CLICKHOUSE_USERNAME=sync_user",
		"HETEROLOG_CLICKHOUSE_PASSWORD=sync_pass",
		"HETEROLOG_CLICKHOUSE_HEADERS={\"X-Logger\":\"sync\"}",
		"HETEROLOG_ENABLE_CLICKHOUSE_COMPRESS_REQUEST=false",
		"HETEROLOG_CLICKHOUSE_LOG_LEVEL=INFO",
		"HETEROLOG_CLICKHOUSE_FLUSH_INTERVAL_SECOND=7",
		"HETEROLOG_MAX_CLICKHOUSE_INSERT_ENTRIES=123",
		"HETEROLOG_ENABLE_CLICKHOUSE_INSERT_LATENCY=true",
		"HETEROLOG_CLICKHOUSE_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_LOGGER_DEBUG_HANDLER=stdout",
		"HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE=none",
		"HETEROLOG_MAX_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES=5",
		"HETEROLOG_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND=111",
		"HETEROLOG_MAX_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND=222",
		"HETEROLOG_MAX_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=11",
		"HETEROLOG_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_CLICKHOUSE_FAILURE_ARCHIVE_DIR=sync-archive",
		"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR=sync-spool",
		"HETEROLOG_ASYNC_CLICKHOUSE_ENDPOINT=http://async-clickhouse:8123",
		"HETEROLOG_ASYNC_CLICKHOUSE_DATABASE=async_db",
		"HETEROLOG_ASYNC_CLICKHOUSE_TABLE=async_logs",
		"HETEROLOG_ASYNC_CLICKHOUSE_TIMEOUT_SECOND=3",
		"HETEROLOG_ASYNC_CLICKHOUSE_USERNAME=async_user",
		"HETEROLOG_ASYNC_CLICKHOUSE_PASSWORD=async_pass",
		"HETEROLOG_ASYNC_CLICKHOUSE_HEADERS={\"X-Logger\":\"async\"}",
		"HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_COMPRESS_REQUEST=true",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOG_LEVEL=WARN",
		"HETEROLOG_ASYNC_CLICKHOUSE_FLUSH_INTERVAL_SECOND=8",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_INSERT_ENTRIES=234",
		"HETEROLOG_ENABLE_ASYNC_CLICKHOUSE_INSERT_LATENCY=false",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_ERROR_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_LOGGER_DEBUG_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES=6",
		"HETEROLOG_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND=333",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND=444",
		"HETEROLOG_MAX_ASYNC_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=12",
		"HETEROLOG_ASYNC_CLICKHOUSE_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_ARCHIVE_DIR=async-archive",
		"HETEROLOG_ASYNC_CLICKHOUSE_FAILURE_SPOOL_DIR=async-spool",
		"HETEROLOG_DELIVERY_CLICKHOUSE_ENDPOINT=http://delivery-clickhouse:8123",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DATABASE=delivery_db",
		"HETEROLOG_DELIVERY_CLICKHOUSE_TABLE=delivery_logs",
		"HETEROLOG_DELIVERY_CLICKHOUSE_TIMEOUT_SECOND=1",
		"HETEROLOG_DELIVERY_CLICKHOUSE_USERNAME=delivery_user",
		"HETEROLOG_DELIVERY_CLICKHOUSE_PASSWORD=delivery_pass",
		"HETEROLOG_DELIVERY_CLICKHOUSE_HEADERS={\"X-Logger\":\"delivery\"}",
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_COMPRESS_REQUEST=false",
		"HETEROLOG_DELIVERY_CLICKHOUSE_FLUSH_INTERVAL_SECOND=9",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_INSERT_ENTRIES=345",
		"HETEROLOG_ENABLE_DELIVERY_CLICKHOUSE_INSERT_LATENCY=true",
		"HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_RETRIES=7",
		"HETEROLOG_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_MILLISECOND=555",
		"HETEROLOG_MAX_DELIVERY_CLICKHOUSE_RETRYABLE_CLIENT_DELAY_MILLISECOND=666",
		"MAX_DELIVERY_CLICKHOUSE_FAILURE_HANDLER_CONCURRENT=13",
		"HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES=fileArchive",
		"HETEROLOG_DELIVERY_CLICKHOUSE_FAILURE_ARCHIVE_DIR=delivery-archive",
		"DELIVERY_CLICKHOUSE_FAILURE_SPOOL_DIR=delivery-spool",
	}, "\n")
	if err := os.WriteFile(envPath, []byte(envBody), 0o600); err != nil {
		t.Fatalf("write temp env: %v", err)
	}
	if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
		t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
	}

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with clickhouse env: %v", err)
	}

	if got, want := cfg.ClickHouseClient.Endpoint, "http://sync-clickhouse:8123"; got != want {
		t.Fatalf("ClickHouseClient.Endpoint = %q, want %q", got, want)
	}
	if got, want := cfg.ClickHouseClient.Database, "sync_db"; got != want {
		t.Fatalf("ClickHouseClient.Database = %q, want %q", got, want)
	}
	if got, want := cfg.ClickHouseClient.Headers["X-Logger"], "sync"; got != want {
		t.Fatalf("ClickHouseClient header = %q, want %q", got, want)
	}
	if got, want := cfg.Logger.ClickHouseConfig.FlushInterval, 7*time.Second; got != want {
		t.Fatalf("ClickHouse FlushInterval = %s, want %s", got, want)
	}
	if got, want := cfg.Logger.ClickHouseConfig.MaxEntries, 123; got != want {
		t.Fatalf("ClickHouse MaxEntries = %d, want %d", got, want)
	}
	if got, want := cfg.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig.Fsstore.SpoolConfig.Dir, "sync-spool"; got != want {
		t.Fatalf("ClickHouse failure spool dir = %q, want %q", got, want)
	}
	if got, want := cfg.FsstoreConfig.SpoolPath, systemLibDir; got != want {
		t.Fatalf("FsstoreConfig.SpoolPath = %q, want %q", got, want)
	}

	cfg.RuntimeConfig.Target = "runtime-clickhouse-target"

	syncLogger := ToLoggerConfig(cfg)
	if syncLogger.ClickHouse == nil || syncLogger.ClickHouseClient == nil {
		t.Fatalf("sync mapper did not build ClickHouse logger/client")
	}
	if got, want := syncLogger.ClickHouse.MaxEntries, 123; got != want {
		t.Fatalf("mapped sync MaxEntries = %d, want %d", got, want)
	}
	if got, want := syncLogger.ClickHouse.FlushInterval, 7*time.Second; got != want {
		t.Fatalf("mapped sync FlushInterval = %s, want %s", got, want)
	}
	if got, want := syncLogger.ClickHouseClient.Table, "sync_logs"; got != want {
		t.Fatalf("mapped sync table = %q, want %q", got, want)
	}
	if got, want := syncLogger.ClickHouse.FailureStoreConfig.DurableStoreConfig.SpoolConfig.Dir, "sync-spool"; got != want {
		t.Fatalf("mapped sync spool dir = %q, want %q", got, want)
	}
	assertSpoolRoot(t, "mapped sync clickhouse", syncLogger.ClickHouse.FailureStoreConfig.DurableStoreConfig.SpoolConfig, filepath.Join(systemLibDir, "runtime-clickhouse-target", "spool", "sync-spool"))
	if got, want := syncLogger.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig.RetryClientMode, pkglogger.RetryClientModeNone; got != want {
		t.Fatalf("mapped sync retry client mode = %q, want %q", got, want)
	}

	asyncLogger := ToAsyncBestEffortMultiLoggerConfig(cfg)
	if asyncLogger.ClickHouse == nil || asyncLogger.ClickHouseClient == nil {
		t.Fatalf("async mapper did not build ClickHouse logger/client")
	}
	if got, want := asyncLogger.ClickHouse.MaxEntries, 234; got != want {
		t.Fatalf("mapped async MaxEntries = %d, want %d", got, want)
	}
	if got, want := asyncLogger.ClickHouseClient.Endpoint, "http://async-clickhouse:8123"; got != want {
		t.Fatalf("mapped async endpoint = %q, want %q", got, want)
	}
	if got, want := asyncLogger.ClickHouse.FailureStoreConfig.DurableStoreConfig.SpoolConfig.Dir, "async-spool"; got != want {
		t.Fatalf("mapped async spool dir = %q, want %q", got, want)
	}
	assertSpoolRoot(t, "mapped async clickhouse", asyncLogger.ClickHouse.FailureStoreConfig.DurableStoreConfig.SpoolConfig, filepath.Join(systemLibDir, "runtime-clickhouse-target", "spool", "async-spool"))
	asyncRetrySelection := asyncLogger.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig.RetryClientSelection()
	if got, want := asyncRetrySelection.Mode, pkglogger.RetryClientModeRetry; got != want {
		t.Fatalf("mapped async retry client mode = %q, want %q", got, want)
	}
	if !asyncRetrySelection.UseRetryingClient {
		t.Fatalf("mapped async retrying client selected = false, want true")
	}

	deliveryLogger := ToDeliveryLoggerConfig(cfg)
	if deliveryLogger.ClickHouse == nil || deliveryLogger.ClickHouseClient == nil {
		t.Fatalf("delivery mapper did not build ClickHouse logger/client")
	}
	if got, want := deliveryLogger.ClickHouseClient.Endpoint, "http://delivery-clickhouse:8123"; got != want {
		t.Fatalf("mapped delivery endpoint = %q, want %q", got, want)
	}
	if got, want := deliveryLogger.ClickHouse.FlushInterval, 9*time.Second; got != want {
		t.Fatalf("mapped delivery FlushInterval = %s, want %s", got, want)
	}
	if got, want := deliveryLogger.ClickHouse.MaxEntries, 345; got != want {
		t.Fatalf("mapped delivery MaxEntries = %d, want %d", got, want)
	}
	if got, want := deliveryLogger.ClickHouse.FailureStoreConfig.FileArchiveConfig.Dir, "delivery-archive"; got != want {
		t.Fatalf("mapped delivery archive dir = %q, want %q", got, want)
	}
	deliveryRetrySelection := deliveryLogger.ClickHouse.ClickHouseRetryableClientCaseTransportErrorConfig.RetryClientSelection()
	if got, want := deliveryRetrySelection.Mode, pkglogger.RetryClientModeRetry; got != want {
		t.Fatalf("mapped delivery retry client mode = %q, want %q", got, want)
	}
	if !deliveryRetrySelection.UseRetryingClient {
		t.Fatalf("mapped delivery retrying client selected = false, want true")
	}
}

func TestLoggerFailureSpoolPathsUseSystemLibDirAsBaseOnce(t *testing.T) {
	// spool rootは実体pathへ解決されるため、期待値も解決済みの一時directoryから作ります。
	systemLibDir := filepath.Join(resolvedTempDirForTest(t), "var-lib")
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_APP_NAME=spool-path-test",
		"HETEROLOG_TARGET=env-target",
		"HETEROLOG_SYSTEM_LIB_DIR=" + systemLibDir,
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=false",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=false",
		"HETEROLOG_LOG_TYPE=loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_LOG_TIMEOUT_SECOND=60",
		"HETEROLOG_LOKI_FAILURE_SPOOL_DIR=loki",
		"HETEROLOG_CLOUDWATCH_FAILURE_SPOOL_DIR=cloudwatchlogs",
		"HETEROLOG_CLICKHOUSE_FAILURE_SPOOL_DIR=clickhouse",
	})
	cfg.RuntimeConfig.Target = "runtime-target"

	loggerCfg := ToLoggerConfig(cfg)
	if loggerCfg.Loki == nil {
		t.Fatal("ToLoggerConfig() did not build Loki config")
	}
	if loggerCfg.CloudWatchLogs == nil {
		t.Fatal("ToLoggerConfig() did not build CloudWatch Logs config")
	}
	if loggerCfg.ClickHouse == nil {
		t.Fatal("ToLoggerConfig() did not build ClickHouse config")
	}

	assertSpoolRoot(t, "loki", loggerCfg.Loki.FailureStoreConfig.DurableStoreConfig.SpoolConfig, filepath.Join(systemLibDir, "runtime-target", "spool", "loki"))
	assertSpoolRoot(t, "cloudwatchlogs", loggerCfg.CloudWatchLogs.FailureStoreConfig.DurableStoreConfig.SpoolConfig, filepath.Join(systemLibDir, "runtime-target", "spool", "cloudwatchlogs"))
	assertSpoolRoot(t, "clickhouse", loggerCfg.ClickHouse.FailureStoreConfig.DurableStoreConfig.SpoolConfig, filepath.Join(systemLibDir, "runtime-target", "spool", "clickhouse"))
}

func TestLoadOpenSearchRetryableClientConfigsUseDedicatedEnvKeys(t *testing.T) {
	tests := []struct {
		name       string
		setEnv     func(*testing.T)
		loadConfig func() (mode pkglogger.RetryClientMode, maxRetries int, baseDelay time.Duration, maxDelay time.Duration, err error)
	}{
		{
			name: "sync",
			setEnv: func(t *testing.T) {
				t.Setenv("HETEROLOG_ENABLE_OPENSEARCH_COMPRESS_REQUEST", "true")
				t.Setenv("HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE", "none")
				t.Setenv("HETEROLOG_MAX_OPENSEARCH_RETRYABLE_CLIENT_RETRIES", "5")
				t.Setenv("HETEROLOG_OPENSEARCH_RETRYABLE_CLIENT_MILLISECOND", "123")
				t.Setenv("HETEROLOG_MAX_OPENSEARCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", "456")
			},
			loadConfig: func() (pkglogger.RetryClientMode, int, time.Duration, time.Duration, error) {
				cfg, err := loadOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, cfg.MaxRetries, cfg.BaseDelay, cfg.MaxDelay, err
			},
		},
		{
			name: "async",
			setEnv: func(t *testing.T) {
				t.Setenv("HETEROLOG_ENABLE_ASYNC_OPENSEARCH_COMPRESS_REQUEST", "true")
				t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE", "none")
				t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_RETRYABLE_CLIENT_RETRIES", "6")
				t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_RETRYABLE_CLIENT_MILLISECOND", "234")
				t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", "567")
			},
			loadConfig: func() (pkglogger.RetryClientMode, int, time.Duration, time.Duration, error) {
				cfg, err := loadAsyncOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, cfg.MaxRetries, cfg.BaseDelay, cfg.MaxDelay, err
			},
		},
		{
			name: "delivery",
			setEnv: func(t *testing.T) {
				t.Setenv("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_COMPRESS_REQUEST", "true")
				t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE", "none")
				t.Setenv("HETEROLOG_MAX_DELIVERY_OPENSEARCH_RETRYABLE_CLIENT_RETRIES", "7")
				t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_RETRYABLE_CLIENT_MILLISECOND", "345")
				t.Setenv("HETEROLOG_MAX_DELIVERY_OPENSEARCH_RETRYABLE_CLIENT_DELAY_MILLISECOND", "678")
			},
			loadConfig: func() (pkglogger.RetryClientMode, int, time.Duration, time.Duration, error) {
				cfg, err := loadDeliveryOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, cfg.MaxRetries, cfg.BaseDelay, cfg.MaxDelay, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setEnv(t)

			mode, maxRetries, baseDelay, maxDelay, err := tt.loadConfig()
			if err != nil {
				t.Fatalf("load retry client config: %v", err)
			}
			if mode != pkglogger.RetryClientModeNone {
				t.Fatalf("RetryClientMode = %q, want %q from retry mode env key", mode, pkglogger.RetryClientModeNone)
			}

			expectedRetries := map[string]int{
				"sync":     5,
				"async":    6,
				"delivery": 7,
			}[tt.name]
			if maxRetries != expectedRetries {
				t.Fatalf("MaxRetries = %d, want %d", maxRetries, expectedRetries)
			}

			expectedBaseDelay := map[string]time.Duration{
				"sync":     123 * time.Millisecond,
				"async":    234 * time.Millisecond,
				"delivery": 345 * time.Millisecond,
			}[tt.name]
			if baseDelay != expectedBaseDelay {
				t.Fatalf("BaseDelay = %s, want %s", baseDelay, expectedBaseDelay)
			}

			expectedMaxDelay := map[string]time.Duration{
				"sync":     456 * time.Millisecond,
				"async":    567 * time.Millisecond,
				"delivery": 678 * time.Millisecond,
			}[tt.name]
			if maxDelay != expectedMaxDelay {
				t.Fatalf("MaxDelay = %s, want %s", maxDelay, expectedMaxDelay)
			}
		})
	}
}

func TestLoadRetryClientModeConfigsUseDedicatedEnvKeys(t *testing.T) {
	tests := []struct {
		name    string
		modeKey string
		load    func() (pkglogger.RetryClientMode, error)
	}{
		{
			name:    "opensearch sync",
			modeKey: "HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "opensearch async",
			modeKey: "HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "opensearch delivery",
			modeKey: "HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "loki sync",
			modeKey: "HETEROLOG_LOKI_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "loki async",
			modeKey: "HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "loki delivery",
			modeKey: "HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "cloudwatchlogs sync",
			modeKey: "HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "cloudwatchlogs async",
			modeKey: "HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "cloudwatchlogs delivery",
			modeKey: "HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "clickhouse sync",
			modeKey: "HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "clickhouse async",
			modeKey: "HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "clickhouse delivery",
			modeKey: "HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.modeKey, "retry")

			mode, err := tt.load()
			if err != nil {
				t.Fatalf("load retry client mode: %v", err)
			}
			if mode != pkglogger.RetryClientModeRetry {
				t.Fatalf("RetryClientMode = %q, want %q", mode, pkglogger.RetryClientModeRetry)
			}
		})
	}
}

func TestRetryClientModesSurviveLoggerConfigMapping(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TIMEOUT_SECOND=300",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_SECOND=300",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_SECOND=300",
		"HETEROLOG_LOG_TYPE=opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_ASYNC_LOG_TYPE=opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_DELIVERY_LOG_TYPE=opensearch,loki,cloudwatchlogs,clickhouse",
		"HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_LOKI_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE=retry",
	})

	syncCfg := ToLoggerConfig(cfg)
	asyncCfg := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryCfg := ToDeliveryLoggerConfig(cfg)

	selections := []struct {
		name      string
		selection pkglogger.RetryClientSelection
	}{
		{name: "opensearch sync", selection: syncCfg.OpenSearch.RetryClientSelection()},
		{name: "opensearch async", selection: asyncCfg.OpenSearch.RetryClientSelection()},
		{name: "opensearch delivery", selection: deliveryCfg.OpenSearch.RetryClientSelection()},
		{name: "loki sync", selection: syncCfg.Loki.RetryClientSelection()},
		{name: "loki async", selection: asyncCfg.Loki.RetryClientSelection()},
		{name: "loki delivery", selection: deliveryCfg.Loki.RetryClientSelection()},
		{name: "cloudwatchlogs sync", selection: syncCfg.CloudWatchLogs.RetryClientSelection()},
		{name: "cloudwatchlogs async", selection: asyncCfg.CloudWatchLogs.RetryClientSelection()},
		{name: "cloudwatchlogs delivery", selection: deliveryCfg.CloudWatchLogs.RetryClientSelection()},
		{name: "clickhouse sync", selection: syncCfg.ClickHouse.RetryClientSelection()},
		{name: "clickhouse async", selection: asyncCfg.ClickHouse.RetryClientSelection()},
		{name: "clickhouse delivery", selection: deliveryCfg.ClickHouse.RetryClientSelection()},
	}

	for _, tt := range selections {
		t.Run(tt.name, func(t *testing.T) {
			if tt.selection.Mode != pkglogger.RetryClientModeRetry {
				t.Fatalf("RetryClientMode = %q, want %q", tt.selection.Mode, pkglogger.RetryClientModeRetry)
			}
			if !tt.selection.UseRetryingClient || tt.selection.UseRetryPolicyClient || tt.selection.UseCircuitBreakerClient {
				t.Fatalf("RetryClientSelection = %+v, want retry-only chain", tt.selection)
			}
		})
	}
}

func TestLoadRetryClientModeConfigsDefaultRetryPolicy(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)

	tests := []struct {
		name string
		load func() (pkglogger.RetryClientMode, error)
	}{
		{
			name: "opensearch sync",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "opensearch async",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "opensearch delivery",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "loki sync",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "loki async",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "loki delivery",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "cloudwatchlogs sync",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "cloudwatchlogs async",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "cloudwatchlogs delivery",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "clickhouse sync",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "clickhouse async",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name: "clickhouse delivery",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.load()
			if err != nil {
				t.Fatalf("load retry client mode: %v", err)
			}
			if want := pkglogger.RetryClientModeRetryPolicy; got != want {
				t.Fatalf("RetryClientMode = %q, want default %q", got, want)
			}
		})
	}
}

func TestLoadRetryClientModeConfigsCanSelectCircuitBreakerMode(t *testing.T) {
	restoreProcessEnvAfterTest(t)
	clearProcessEnvForTest(t)

	tests := []struct {
		name    string
		modeKey string
		load    func() (pkglogger.RetryClientMode, error)
	}{
		{
			name:    "opensearch sync",
			modeKey: "HETEROLOG_OPENSEARCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "opensearch async",
			modeKey: "HETEROLOG_ASYNC_OPENSEARCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "opensearch delivery",
			modeKey: "HETEROLOG_DELIVERY_OPENSEARCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryOpenSearchRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "loki sync",
			modeKey: "HETEROLOG_LOKI_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "loki async",
			modeKey: "HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "loki delivery",
			modeKey: "HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryLokiRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "cloudwatchlogs sync",
			modeKey: "HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "cloudwatchlogs async",
			modeKey: "HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "cloudwatchlogs delivery",
			modeKey: "HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryCloudWatchLogsRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "clickhouse sync",
			modeKey: "HETEROLOG_CLICKHOUSE_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "clickhouse async",
			modeKey: "HETEROLOG_ASYNC_CLICKHOUSE_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadAsyncClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
		{
			name:    "clickhouse delivery",
			modeKey: "HETEROLOG_DELIVERY_CLICKHOUSE_RETRY_CLIENT_MODE",
			load: func() (pkglogger.RetryClientMode, error) {
				cfg, err := loadDeliveryClickHouseRetryableClientConfig(processEnvironmentForTest())
				return cfg.RetryClientMode, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.load()
			if err != nil {
				t.Fatalf("load default retry client mode: %v", err)
			}
			if want := pkglogger.RetryClientModeRetryPolicy; got != want {
				t.Fatalf("RetryClientMode default = %q, want %q", got, want)
			}

			t.Setenv(tt.modeKey, "circuit_breaker")
			got, err = tt.load()
			if err != nil {
				t.Fatalf("load circuit breaker retry client mode: %v", err)
			}
			if want := pkglogger.RetryClientModeCircuitBreaker; got != want {
				t.Fatalf("RetryClientMode = %q, want %q from %s", got, want, tt.modeKey)
			}
		})
	}
}

func TestLoadDeliveryOpenSearchFlushManagerConfigUsesDeliveryEnvKeys(t *testing.T) {
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_INDEX", "async-index")
	t.Setenv("HETEROLOG_ENABLE_ASYNC_OPENSEARCH_OTEL_FORMAT_LOG", "false")
	t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB", "11")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR", "async-archive")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", "false")
	t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", "12")
	t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", "13")
	t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", "14")
	t.Setenv("HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS", "true")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", "gzip")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_ROTATION", "daily")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", "drop")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", "stdout")

	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_INDEX", "delivery-index")
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_OTEL_FORMAT_LOG", "true")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES", "fileArchive")
	t.Setenv("HETEROLOG_MAX_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB", "21")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_FAILURE_ARCHIVE_DIR", "delivery-archive")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_RETRYABLE_DIR", "delivery-retryable")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_PERMANENT_DIR", "delivery-permanent")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", "true")
	t.Setenv("HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", "22")
	t.Setenv("HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", "23")
	t.Setenv("HETEROLOG_MAX_DELIVERY_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", "24")
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS", "false")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", "xz")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_ROTATION", "weekly")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY", "degrade")
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION", "nfs")

	cfg := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryOpenSearchFlushManagerConfig))
	if cfg.Index != "delivery-index" {
		t.Fatalf("Index = %q, want delivery-index", cfg.Index)
	}
	if !cfg.OpenTelemetryFormatEnabled {
		t.Fatalf("OpenTelemetryFormatEnabled = false, want true")
	}

	failureCfg := cfg.FailureHandlerConfig
	if len(failureCfg.DurableType) != 1 || string(failureCfg.DurableType[0]) != "fileArchive" {
		t.Fatalf("DurableType = %v, want [fileArchive]", failureCfg.DurableType)
	}

	fileArchive := failureCfg.FileArchiveConfig
	if fileArchive.MaxFileSize != 21*BytesPerMegabyte {
		t.Fatalf("MaxFileSize = %d, want %d", fileArchive.MaxFileSize, 21*BytesPerMegabyte)
	}
	if fileArchive.Dir != "delivery-archive" {
		t.Fatalf("Dir = %q, want delivery-archive", fileArchive.Dir)
	}
	if fileArchive.DirCaseRetryable != "delivery-retryable" {
		t.Fatalf("DirCaseRetryable = %q, want delivery-retryable", fileArchive.DirCaseRetryable)
	}
	if fileArchive.DirCasePermanent != "delivery-permanent" {
		t.Fatalf("DirCasePermanent = %q, want delivery-permanent", fileArchive.DirCasePermanent)
	}
	if !fileArchive.EnableRotate {
		t.Fatalf("EnableRotate = false, want true")
	}
	if fileArchive.MaxFileBackups != 22 {
		t.Fatalf("MaxFileBackups = %d, want 22", fileArchive.MaxFileBackups)
	}
	if fileArchive.MaxFileAge != 23 {
		t.Fatalf("MaxFileAge = %d, want 23", fileArchive.MaxFileAge)
	}
	if fileArchive.MaxDirSize != 24*BytesPerMegabyte {
		t.Fatalf("MaxDirSize = %d, want %d", fileArchive.MaxDirSize, 24*BytesPerMegabyte)
	}
	if fileArchive.EnableFileCompress {
		t.Fatalf("EnableFileCompress = true, want false")
	}
	if string(fileArchive.CompressFormat) != "xz" {
		t.Fatalf("CompressFormat = %q, want xz", fileArchive.CompressFormat)
	}
	if string(fileArchive.FileRotation) != "weekly" {
		t.Fatalf("FileRotation = %q, want weekly", fileArchive.FileRotation)
	}
	if string(fileArchive.OverloadPolicyConfig.OverloadPolicy) != "degrade" {
		t.Fatalf("OverloadPolicy = %q, want degrade", fileArchive.OverloadPolicyConfig.OverloadPolicy)
	}
	if string(fileArchive.OverloadPolicyConfig.SpillDestination) != "nfs" {
		t.Fatalf("SpillDestination = %q, want nfs", fileArchive.OverloadPolicyConfig.SpillDestination)
	}
}

func TestLoadAsyncOpenSearchFlushManagerConfigUsesFileArchiveEnvKeys(t *testing.T) {
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_TYPES", "fileArchive")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES", "fileArchive")
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_RETRYABLE_DIR", "sync-retryable")
	t.Setenv("HETEROLOG_OPENSEARCH_DURABLE_PERMANENT_DIR", "sync-permanent")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_RETRYABLE_DIR", "async-retryable")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_PERMANENT_DIR", "async-permanent")
	t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_SIZE_MB", "11")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_FAILURE_ARCHIVE_DIR", "async-archive")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE", "false")
	t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS", "12")
	t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_AGE", "13")
	t.Setenv("HETEROLOG_MAX_ASYNC_OPENSEARCH_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB", "14")
	t.Setenv("HETEROLOG_ENABLE_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS", "true")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT", "xz")
	t.Setenv("HETEROLOG_ASYNC_OPENSEARCH_DURABLE_FILE_ARCHIVE_ROTATION", "weekly")

	cfg := mustLoadConfig(t, fromProcessEnvironment(loadAsyncOpenSearchFlushManagerConfig))
	fileArchive := cfg.FailureHandlerConfig.Fsstore.FileArchiveConfig
	if fileArchive.MaxFileSize != 11*BytesPerMegabyte {
		t.Fatalf("MaxFileSize = %d, want %d", fileArchive.MaxFileSize, 11*BytesPerMegabyte)
	}
	if fileArchive.Dir != "async-archive" {
		t.Fatalf("Dir = %q, want async-archive", fileArchive.Dir)
	}
	if fileArchive.DirCaseRetryable != "async-retryable" {
		t.Fatalf("DirCaseRetryable = %q, want async-retryable", fileArchive.DirCaseRetryable)
	}
	if fileArchive.DirCasePermanent != "async-permanent" {
		t.Fatalf("DirCasePermanent = %q, want async-permanent", fileArchive.DirCasePermanent)
	}
	if fileArchive.EnableRotate {
		t.Fatalf("EnableRotate = true, want false")
	}
	if fileArchive.MaxFileBackups != 12 {
		t.Fatalf("MaxFileBackups = %d, want 12", fileArchive.MaxFileBackups)
	}
	if fileArchive.MaxFileAge != 13 {
		t.Fatalf("MaxFileAge = %d, want 13", fileArchive.MaxFileAge)
	}
	if fileArchive.MaxDirSize != 14*BytesPerMegabyte {
		t.Fatalf("MaxDirSize = %d, want %d", fileArchive.MaxDirSize, 14*BytesPerMegabyte)
	}
	if !fileArchive.EnableFileCompress {
		t.Fatalf("EnableFileCompress = false, want true")
	}
	if string(fileArchive.CompressFormat) != "xz" {
		t.Fatalf("CompressFormat = %q, want xz", fileArchive.CompressFormat)
	}
	if string(fileArchive.FileRotation) != "weekly" {
		t.Fatalf("FileRotation = %q, want weekly", fileArchive.FileRotation)
	}

	syncArchive := mustLoadConfig(t, fromProcessEnvironment(loadOpenSearchFailureHandlerConfig)).Fsstore.FileArchiveConfig
	if syncArchive.DirCaseRetryable != "sync-retryable" {
		t.Fatalf("sync DirCaseRetryable = %q, want sync-retryable", syncArchive.DirCaseRetryable)
	}
	if syncArchive.DirCasePermanent != "sync-permanent" {
		t.Fatalf("sync DirCasePermanent = %q, want sync-permanent", syncArchive.DirCasePermanent)
	}
}

func TestLoadConfigReadsOpenSearchFailureArchiveDirectoriesIndependently(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, []string{
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_LOG_TYPE=opensearch",
		"HETEROLOG_ASYNC_LOG_TYPE=opensearch",
		"HETEROLOG_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_TYPES=fileArchive",
		"HETEROLOG_OPENSEARCH_DURABLE_RETRYABLE_DIR=sync-retryable",
		"HETEROLOG_OPENSEARCH_DURABLE_PERMANENT_DIR=sync-permanent",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_RETRYABLE_DIR=async-retryable",
		"HETEROLOG_ASYNC_OPENSEARCH_DURABLE_PERMANENT_DIR=async-permanent",
	})

	syncArchive := cfg.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.
		FailureHandlerConfig.Fsstore.FileArchiveConfig
	asyncArchive := cfg.AsyncBestEffortMultiLoggerConfig.OpenSearchConfig.OpenSearchFlushManagerConfig.
		FailureHandlerConfig.Fsstore.FileArchiveConfig

	if got := syncArchive.DirCaseRetryable; got != "sync-retryable" {
		t.Fatalf("sync DirCaseRetryable = %q, want sync-retryable", got)
	}
	if got := syncArchive.DirCasePermanent; got != "sync-permanent" {
		t.Fatalf("sync DirCasePermanent = %q, want sync-permanent", got)
	}
	if got := asyncArchive.DirCaseRetryable; got != "async-retryable" {
		t.Fatalf("async DirCaseRetryable = %q, want async-retryable", got)
	}
	if got := asyncArchive.DirCasePermanent; got != "async-permanent" {
		t.Fatalf("async DirCasePermanent = %q, want async-permanent", got)
	}
}

func TestLoadDeliveryOpenSearchFlushManagerConfigPreservesSpoolForValidation(t *testing.T) {
	t.Setenv("HETEROLOG_DELIVERY_OPENSEARCH_DURABLE_TYPES", "spool")

	cfg := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryOpenSearchFlushManagerConfig))
	durableTypes := cfg.FailureHandlerConfig.DurableType
	if len(durableTypes) != 1 || string(durableTypes[0]) != "spool" {
		t.Fatalf("DurableType = %v, want [spool]", durableTypes)
	}
}

func TestLoadDeliveryLokiFlushManagerConfigPreservesSpoolForValidation(t *testing.T) {
	t.Setenv("HETEROLOG_DELIVERY_LOKI_DURABLE_TYPES", "spool")

	cfg := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryLokiFlushManagerConfig))
	durableTypes := cfg.FailureHandlerConfig.DurableType
	if len(durableTypes) != 1 || string(durableTypes[0]) != "spool" {
		t.Fatalf("DurableType = %v, want [spool]", durableTypes)
	}
}

func TestLoadDeliveryCloudWatchLogsFlushManagerConfigPreservesSpoolForValidation(t *testing.T) {
	t.Setenv("HETEROLOG_DELIVERY_CLOUDWATCH_DURABLE_TYPES", "spool")

	cfg := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryCloudWatchLogsFlushManagerConfig))
	durableTypes := cfg.FailureHandlerConfig.DurableType
	if len(durableTypes) != 1 || string(durableTypes[0]) != "spool" {
		t.Fatalf("DurableType = %v, want [spool]", durableTypes)
	}
}

func TestLoadDeliveryClickHouseFlushManagerConfigPreservesSpoolForValidation(t *testing.T) {
	t.Setenv("HETEROLOG_DELIVERY_CLICKHOUSE_DURABLE_TYPES", "spool")
	t.Setenv("DELIVERY_CLICKHOUSE_FAILURE_SPOOL_DIR", "must-not-be-loaded")

	cfg := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryClickHouseFlushManagerConfig))
	durableTypes := cfg.FailureHandlerConfig.DurableType
	if len(durableTypes) != 1 || string(durableTypes[0]) != "spool" {
		t.Fatalf("DurableType = %v, want [spool]", durableTypes)
	}
}

func TestLoadCloudWatchLogsClientConfigCompressRequest(t *testing.T) {
	t.Setenv("HETEROLOG_ENABLE_CLOUDWATCH_COMPRESS_REQUEST", "false")
	t.Setenv("HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_COMPRESS_REQUEST", "true")
	t.Setenv("HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_COMPRESS_REQUEST", "false")

	if got := mustLoadConfig(t, fromProcessEnvironment(loadCloudWatchLogsClientConfig)).CompressRequest; got {
		t.Fatalf("CloudWatchLogsClient.CompressRequest = %t, want false", got)
	}
	if got := mustLoadConfig(t, fromProcessEnvironment(loadAsyncCloudWatchLogsClientConfig)).CompressRequest; !got {
		t.Fatalf("AsyncCloudWatchLogsClient.CompressRequest = %t, want true", got)
	}
	if got := mustLoadConfig(t, fromProcessEnvironment(loadDeliveryCloudWatchLogsClientConfig)).CompressRequest; got {
		t.Fatalf("DeliveryCloudWatchLogsClient.CompressRequest = %t, want false", got)
	}
}

// TestLoadConfigReadsFixtureEnvSpoolSupervisorAndLifecycleConfigsは、公開例に掲載した
// spool supervisor・runnerのkeyがLoadConfigとmapperへ届くことを検証します。公開例の
// 値はcode既定値と同じなので、全keyを非既定値で上書きして読み取りを確認します。
func TestLoadConfigReadsFixtureEnvSpoolSupervisorAndLifecycleConfigs(t *testing.T) {
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
	overrideFixtureEnvValues(t, envFile, map[string]string{"HETEROLOG_MAX_RESTART_SPOOL_TIMES": "7"})
	overrideFixtureEnvValues(t, envFile, nonDefaultBackendSpoolRunnerEnv("OPENSEARCH", 11))
	overrideFixtureEnvValues(t, envFile, nonDefaultBackendSpoolRunnerEnv("LOKI", 22))

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with fixture .env: %v", err)
	}

	if got, want := cfg.SpoolConfig.SupervisorConfig.MaxRestarts, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_RESTART_SPOOL_TIMES"); got != want {
		t.Fatalf("fixture env MaxRestarts = %d, want %d", got, want)
	}
	assertBackendSpoolRunnerConfigMatchesEnv(t, "fixture opensearch", cfg.SpoolConfig.RunnerConfig.OpensearchConfig, envFile, "OPENSEARCH")
	assertBackendSpoolRunnerConfigMatchesEnv(t, "fixture loki", cfg.SpoolConfig.RunnerConfig.LokiConfig, envFile, "LOKI")

	mapped := NewSpoolConfig(&cfg)
	if got, want := mapped.SupervisorConfig.MaxRestarts, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_RESTART_SPOOL_TIMES"); got != want {
		t.Fatalf("mapped fixture env MaxRestarts = %d, want %d", got, want)
	}
	assertBackendSpoolRunnerConfigMatchesEnv(t, "mapped fixture opensearch", mapped.RunnerConfig.OpensearchConfig, envFile, "OPENSEARCH")
	assertBackendSpoolRunnerConfigMatchesEnv(t, "mapped fixture loki", mapped.RunnerConfig.LokiConfig, envFile, "LOKI")
}

func TestLoadSpoolConfigReadsSupervisorAndBackendLifecycleConfigs(t *testing.T) {
	t.Setenv("HETEROLOG_MAX_RESTART_SPOOL_TIMES", "9")
	setBackendSpoolRunnerEnv(t, "OPENSEARCH", 101, true, 11)
	setBackendSpoolRunnerEnv(t, "LOKI", 202, false, 22)
	setBackendSpoolRunnerEnv(t, "CLOUDWATCH", 303, true, 33)
	setBackendSpoolRunnerEnv(t, "CLICKHOUSE", 404, false, 44)

	appCfgForLoad := AppConfig{}
	appCfgForLoad.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		logconfig.LogOutputOpenSearch,
		logconfig.LogOutputLoki,
		logconfig.LogOutputCloudWatchLogs,
		logconfig.LogOutputClickHouse,
	}
	spoolFailureConfig := configfailure.FailureHandlerConfig{
		DurableType: []loggerdurable.DurableType{loggerdurable.DurableTypeSpool},
	}
	appCfgForLoad.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig

	cfg, err := loadSpoolConfig(processEnvironmentForTest(), appCfgForLoad)
	if err != nil {
		t.Fatalf("loadSpoolConfig() error = %v", err)
	}
	if cfg.SupervisorConfig.MaxRestarts != 9 {
		t.Fatalf("MaxRestarts = %d, want 9", cfg.SupervisorConfig.MaxRestarts)
	}
	assertBackendSpoolRunnerConfig(t, "opensearch", cfg.RunnerConfig.OpensearchConfig, 101, true, 11)
	assertBackendSpoolRunnerConfig(t, "loki", cfg.RunnerConfig.LokiConfig, 202, false, 22)
	assertBackendSpoolRunnerConfig(t, "cloudwatchlogs", cfg.RunnerConfig.CloudWatchLogsConfig, 303, true, 33)
	assertBackendSpoolRunnerConfig(t, "clickhouse", cfg.RunnerConfig.ClickHouseConfig, 404, false, 44)

	appCfg := AppConfig{SpoolConfig: cfg}
	mapped := NewSpoolConfig(&appCfg)
	if mapped.SupervisorConfig.MaxRestarts != 9 {
		t.Fatalf("mapped MaxRestarts = %d, want 9", mapped.SupervisorConfig.MaxRestarts)
	}
	assertBackendSpoolRunnerConfig(t, "mapped opensearch", mapped.RunnerConfig.OpensearchConfig, 101, true, 11)
	assertBackendSpoolRunnerConfig(t, "mapped loki", mapped.RunnerConfig.LokiConfig, 202, false, 22)
	assertBackendSpoolRunnerConfig(t, "mapped cloudwatchlogs", mapped.RunnerConfig.CloudWatchLogsConfig, 303, true, 33)
	assertBackendSpoolRunnerConfig(t, "mapped clickhouse", mapped.RunnerConfig.ClickHouseConfig, 404, false, 44)
}

// TestLoadSpoolConfigKeepsRetainedFailureRecordsByDefaultは、spoolの保持期間・容量による
// 削除の既定値を検証します。再送済みのdoneは削除しますが、配送不能のdeadと成否不明の
// quarantineは、公開APIのDefaultSpoolConfigと同じく明示しない限り削除しません。
func TestLoadSpoolConfigKeepsRetainedFailureRecordsByDefault(t *testing.T) {
	for _, backend := range []string{"OPENSEARCH", "LOKI", "CLOUDWATCH", "CLICKHOUSE"} {
		for _, kind := range []string{"DONE", "DEAD", "QUARANTINE"} {
			t.Setenv("HETEROLOG_ENABLE_PURGE_"+backend+"_SPOOL_"+kind, "")
		}
	}
	appCfgForLoad := AppConfig{}
	appCfgForLoad.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		logconfig.LogOutputOpenSearch,
		logconfig.LogOutputLoki,
		logconfig.LogOutputCloudWatchLogs,
		logconfig.LogOutputClickHouse,
	}
	spoolFailureConfig := configfailure.FailureHandlerConfig{
		DurableType: []loggerdurable.DurableType{loggerdurable.DurableTypeSpool},
	}
	appCfgForLoad.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig

	cfg, err := loadSpoolConfig(processEnvironmentForTest(), appCfgForLoad)
	if err != nil {
		t.Fatalf("loadSpoolConfig() error = %v", err)
	}
	mapped := NewSpoolConfig(&AppConfig{SpoolConfig: cfg})
	for name, lifecycle := range map[string]pkgspoolrunner.BackendSpoolRunnerConfig{
		"opensearch":            cfg.RunnerConfig.OpensearchConfig,
		"loki":                  cfg.RunnerConfig.LokiConfig,
		"cloudwatchlogs":        cfg.RunnerConfig.CloudWatchLogsConfig,
		"clickhouse":            cfg.RunnerConfig.ClickHouseConfig,
		"mapped opensearch":     mapped.RunnerConfig.OpensearchConfig,
		"mapped loki":           mapped.RunnerConfig.LokiConfig,
		"mapped cloudwatchlogs": mapped.RunnerConfig.CloudWatchLogsConfig,
		"mapped clickhouse":     mapped.RunnerConfig.ClickHouseConfig,
	} {
		if !lifecycle.LifeCycle.EnablePurgeDone {
			t.Errorf("%s EnablePurgeDone = false, want true", name)
		}
		if lifecycle.LifeCycle.EnablePurgeDead || lifecycle.LifeCycle.EnablePurgeQuarantine {
			t.Errorf("%s dead/quarantine purge = %t/%t, want disabled by default",
				name, lifecycle.LifeCycle.EnablePurgeDead, lifecycle.LifeCycle.EnablePurgeQuarantine)
		}
	}
}

// TestLoadSpoolConfigReadsNonDefaultBackendRunnerConfigsは、4 backendのspool runnerの
// 全keyへcode既定値と重ならない値を与え、keyの無視を検出します。boolean keyの取り違えは
// TestLoadSpoolConfigReadsSupervisorAndBackendLifecycleConfigsが検出します。
func TestLoadSpoolConfigReadsNonDefaultBackendRunnerConfigs(t *testing.T) {
	values := map[string]map[string]string{
		"OPENSEARCH": nonDefaultBackendSpoolRunnerEnv("OPENSEARCH", 11),
		"LOKI":       nonDefaultBackendSpoolRunnerEnv("LOKI", 22),
		"CLOUDWATCH": nonDefaultBackendSpoolRunnerEnv("CLOUDWATCH", 33),
		"CLICKHOUSE": nonDefaultBackendSpoolRunnerEnv("CLICKHOUSE", 44),
	}
	for _, backendValues := range values {
		for key, value := range backendValues {
			t.Setenv(key, value)
		}
	}

	appCfgForLoad := AppConfig{}
	appCfgForLoad.Logger.LogOutputDestinations = pkglogger.LogOutputDestinations{
		logconfig.LogOutputOpenSearch,
		logconfig.LogOutputLoki,
		logconfig.LogOutputCloudWatchLogs,
		logconfig.LogOutputClickHouse,
	}
	spoolFailureConfig := configfailure.FailureHandlerConfig{
		DurableType: []loggerdurable.DurableType{loggerdurable.DurableTypeSpool},
	}
	appCfgForLoad.Logger.OpenSearchConfig.OpenSearchFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig
	appCfgForLoad.Logger.ClickHouseConfig.ClickHouseFlushManagerConfig.FailureHandlerConfig = spoolFailureConfig

	cfg, err := loadSpoolConfig(processEnvironmentForTest(), appCfgForLoad)
	if err != nil {
		t.Fatalf("loadSpoolConfig() error = %v", err)
	}
	assertBackendSpoolRunnerConfigMatchesEnv(t, "opensearch", cfg.RunnerConfig.OpensearchConfig, values["OPENSEARCH"], "OPENSEARCH")
	assertBackendSpoolRunnerConfigMatchesEnv(t, "loki", cfg.RunnerConfig.LokiConfig, values["LOKI"], "LOKI")
	assertBackendSpoolRunnerConfigMatchesEnv(t, "cloudwatchlogs", cfg.RunnerConfig.CloudWatchLogsConfig, values["CLOUDWATCH"], "CLOUDWATCH")
	assertBackendSpoolRunnerConfigMatchesEnv(t, "clickhouse", cfg.RunnerConfig.ClickHouseConfig, values["CLICKHOUSE"], "CLICKHOUSE")
}

func assertSpoolRoot(t *testing.T, name string, cfg loggerdurable.SpoolConfig, want string) {
	t.Helper()

	root, err := spooldir.NewRootDir(cfg.SpoolPath, cfg.Target, cfg.Dir)
	if err != nil {
		t.Fatalf("%s NewRootDir returned error: %v", name, err)
	}
	if got := root.String(); got != filepath.Clean(want) {
		t.Fatalf("%s spool root = %q, want %q", name, got, filepath.Clean(want))
	}
}

func TestFileLoggerMapperUsesRuntimeTargetForLayoutTarget(t *testing.T) {
	cfg := AppConfig{}
	cfg.RuntimeConfig.Target = "runtime-target"

	fileCfg := NewFileLoggerConfig(&cfg)
	if fileCfg.FsstoreConfig.Layout.Target != "runtime-target" {
		t.Fatalf("file layout target = %q, want %q", fileCfg.FsstoreConfig.Layout.Target, "runtime-target")
	}
	if fileCfg.RuntimeConfig.Target != "runtime-target" {
		t.Fatalf("file runtime target = %q, want %q", fileCfg.RuntimeConfig.Target, "runtime-target")
	}

	asyncFileCfg := NewAsyncFileLoggerConfig(&cfg)
	if asyncFileCfg.FsstoreConfig.Layout.Target != "runtime-target" {
		t.Fatalf("async file layout target = %q, want %q", asyncFileCfg.FsstoreConfig.Layout.Target, "runtime-target")
	}
	if asyncFileCfg.RuntimeConfig.Target != "runtime-target" {
		t.Fatalf("async file runtime target = %q, want %q", asyncFileCfg.RuntimeConfig.Target, "runtime-target")
	}

	deliveryFileCfg := NewDeliveryFileLoggerConfig(&cfg)
	if deliveryFileCfg.FsstoreConfig.Layout.Target != "runtime-target" {
		t.Fatalf("delivery file layout target = %q, want %q", deliveryFileCfg.FsstoreConfig.Layout.Target, "runtime-target")
	}
	if deliveryFileCfg.RuntimeConfig.Target != "runtime-target" {
		t.Fatalf("delivery runtime target = %q, want %q", deliveryFileCfg.RuntimeConfig.Target, "runtime-target")
	}
}

// nonDefaultBackendSpoolRunnerEnvは、backend spool runnerの全keyについてcode既定値と
// 重ならない値を返します。fixtureの値を上書きし、keyを読まないloaderを検出するために使います。
func nonDefaultBackendSpoolRunnerEnv(prefix string, base int) map[string]string {
	return map[string]string{
		"HETEROLOG_" + prefix + "_SPOOL_RUNNER_INTERVAL_MILLISECOND":             strconv.Itoa(base + 100),
		"HETEROLOG_ENABLE_" + prefix + "_SPOOL_RUNNER_STOP_ON_ERROR":             "true",
		"HETEROLOG_ENABLE_PURGE_" + prefix + "_SPOOL_DONE":                       "false",
		"HETEROLOG_MAX_" + prefix + "_SPOOL_DONE_AGE":                            strconv.Itoa(base + 1),
		"HETEROLOG_MAX_" + prefix + "_SPOOL_DONE_DIR_SIZE_MB":                    strconv.Itoa(base + 2),
		"HETEROLOG_ENABLE_PURGE_" + prefix + "_SPOOL_DEAD":                       "true",
		"HETEROLOG_MAX_" + prefix + "_SPOOL_DEAD_AGE":                            strconv.Itoa(base + 3),
		"HETEROLOG_MAX_" + prefix + "_SPOOL_DEAD_DIR_SIZE_MB":                    strconv.Itoa(base + 4),
		"HETEROLOG_ENABLE_PURGE_" + prefix + "_SPOOL_QUARANTINE":                 "true",
		"HETEROLOG_MAX_" + prefix + "_SPOOL_QUARANTINE_AGE":                      strconv.Itoa(base + 5),
		"HETEROLOG_MAX_" + prefix + "_SPOOL_QUARANTINE_DIR_SIZE_MB":              strconv.Itoa(base + 6),
		"HETEROLOG_" + prefix + "_SPOOL_RUNNER_PURGE_INTERVAL_SECOND":            strconv.Itoa(base + 7),
		"HETEROLOG_ENABLE_" + prefix + "_SPOOL_SIZE_ESTIMATE_CACHE":              "false",
		"HETEROLOG_" + prefix + "_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS":            strconv.Itoa(base + 8),
		"HETEROLOG_MAX_" + prefix + "_SPOOL_FULL_SCAN_INTERVAL_SECOND":           strconv.Itoa(base + 9),
		"HETEROLOG_" + prefix + "_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND": strconv.Itoa(base + 10),
		"HETEROLOG_MAX_" + prefix + "_SPOOL_FINALIZE_FAILURE_ENTRIES":            strconv.Itoa(base + 11),
	}
}

func setBackendSpoolRunnerEnv(t *testing.T, prefix string, intervalMs int, stopOnError bool, base int) {
	t.Helper()

	t.Setenv("HETEROLOG_"+prefix+"_SPOOL_RUNNER_INTERVAL_MILLISECOND", strconv.Itoa(intervalMs))
	t.Setenv("HETEROLOG_ENABLE_"+prefix+"_SPOOL_RUNNER_STOP_ON_ERROR", strconv.FormatBool(stopOnError))
	t.Setenv("HETEROLOG_ENABLE_PURGE_"+prefix+"_SPOOL_DONE", "false")
	t.Setenv("HETEROLOG_MAX_"+prefix+"_SPOOL_DONE_AGE", strconv.Itoa(base+1))
	t.Setenv("HETEROLOG_MAX_"+prefix+"_SPOOL_DONE_DIR_SIZE_MB", strconv.Itoa(base+2))
	t.Setenv("HETEROLOG_ENABLE_PURGE_"+prefix+"_SPOOL_DEAD", "true")
	t.Setenv("HETEROLOG_MAX_"+prefix+"_SPOOL_DEAD_AGE", strconv.Itoa(base+3))
	t.Setenv("HETEROLOG_MAX_"+prefix+"_SPOOL_DEAD_DIR_SIZE_MB", strconv.Itoa(base+4))
	t.Setenv("HETEROLOG_ENABLE_PURGE_"+prefix+"_SPOOL_QUARANTINE", "false")
	t.Setenv("HETEROLOG_MAX_"+prefix+"_SPOOL_QUARANTINE_AGE", strconv.Itoa(base+5))
	t.Setenv("HETEROLOG_MAX_"+prefix+"_SPOOL_QUARANTINE_DIR_SIZE_MB", strconv.Itoa(base+6))
	t.Setenv("HETEROLOG_"+prefix+"_SPOOL_RUNNER_PURGE_INTERVAL_SECOND", strconv.Itoa(base+7))
	t.Setenv("HETEROLOG_ENABLE_"+prefix+"_SPOOL_SIZE_ESTIMATE_CACHE", "false")
	t.Setenv("HETEROLOG_"+prefix+"_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS", strconv.Itoa(base+8))
	t.Setenv("HETEROLOG_MAX_"+prefix+"_SPOOL_FULL_SCAN_INTERVAL_SECOND", strconv.Itoa(base+9))
	t.Setenv("HETEROLOG_"+prefix+"_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND", strconv.Itoa(base+10))
	t.Setenv("HETEROLOG_MAX_"+prefix+"_SPOOL_FINALIZE_FAILURE_ENTRIES", strconv.Itoa(base+11))
}

func assertBackendSpoolRunnerConfig(
	t *testing.T,
	name string,
	cfg pkgspoolrunner.BackendSpoolRunnerConfig,
	intervalMs int,
	stopOnError bool,
	base int,
) {
	t.Helper()

	if cfg.Runner.Interval != time.Duration(intervalMs)*time.Millisecond {
		t.Fatalf("%s Runner.Interval = %s, want %dms", name, cfg.Runner.Interval, intervalMs)
	}
	if cfg.Runner.StopOnError != stopOnError {
		t.Fatalf("%s Runner.StopOnError = %t, want %t", name, cfg.Runner.StopOnError, stopOnError)
	}
	if cfg.LifeCycle.EnablePurgeDone {
		t.Fatalf("%s EnablePurgeDone = true, want false", name)
	}
	if cfg.LifeCycle.DoneMaxFileAge != base+1 {
		t.Fatalf("%s DoneMaxFileAge = %d, want %d", name, cfg.LifeCycle.DoneMaxFileAge, base+1)
	}
	if cfg.LifeCycle.DoneMaxDirSize != uint64(base+2)*BytesPerMegabyte {
		t.Fatalf("%s DoneMaxDirSize = %d, want %d", name, cfg.LifeCycle.DoneMaxDirSize, uint64(base+2)*BytesPerMegabyte)
	}
	if !cfg.LifeCycle.EnablePurgeDead {
		t.Fatalf("%s EnablePurgeDead = false, want true", name)
	}
	if cfg.LifeCycle.DeadMaxFileAge != base+3 {
		t.Fatalf("%s DeadMaxFileAge = %d, want %d", name, cfg.LifeCycle.DeadMaxFileAge, base+3)
	}
	if cfg.LifeCycle.DeadMaxDirSize != uint64(base+4)*BytesPerMegabyte {
		t.Fatalf("%s DeadMaxDirSize = %d, want %d", name, cfg.LifeCycle.DeadMaxDirSize, uint64(base+4)*BytesPerMegabyte)
	}
	if cfg.LifeCycle.EnablePurgeQuarantine {
		t.Fatalf("%s EnablePurgeQuarantine = true, want false", name)
	}
	if cfg.LifeCycle.QuarantineMaxFileAge != base+5 {
		t.Fatalf("%s QuarantineMaxFileAge = %d, want %d", name, cfg.LifeCycle.QuarantineMaxFileAge, base+5)
	}
	if cfg.LifeCycle.QuarantineMaxDirSize != uint64(base+6)*BytesPerMegabyte {
		t.Fatalf("%s QuarantineMaxDirSize = %d, want %d", name, cfg.LifeCycle.QuarantineMaxDirSize, uint64(base+6)*BytesPerMegabyte)
	}
	if cfg.LifeCycle.SizePurgeInterval != time.Duration(base+7)*time.Second {
		t.Fatalf("%s SizePurgeInterval = %s, want %ds", name, cfg.LifeCycle.SizePurgeInterval, base+7)
	}
	if cfg.LifeCycle.EnableSizeEstimateCache {
		t.Fatalf("%s EnableSizeEstimateCache = true, want false", name)
	}
	if cfg.LifeCycle.SizeFullScanEveryNChecks != base+8 {
		t.Fatalf("%s SizeFullScanEveryNChecks = %d, want %d", name, cfg.LifeCycle.SizeFullScanEveryNChecks, base+8)
	}
	if cfg.LifeCycle.SizeFullScanMaxInterval != time.Duration(base+9)*time.Second {
		t.Fatalf("%s SizeFullScanMaxInterval = %s, want %ds", name, cfg.LifeCycle.SizeFullScanMaxInterval, base+9)
	}
	if cfg.Worker.Finalize.RetryDelay != time.Duration(base+10)*time.Millisecond {
		t.Fatalf("%s Finalize RetryDelay = %s, want %dms", name, cfg.Worker.Finalize.RetryDelay, base+10)
	}
	if cfg.Worker.Finalize.MaxEntries != base+11 {
		t.Fatalf("%s Finalize MaxEntries = %d, want %d", name, cfg.Worker.Finalize.MaxEntries, base+11)
	}
}

func assertBackendSpoolRunnerConfigMatchesEnv(
	t *testing.T,
	name string,
	cfg pkgspoolrunner.BackendSpoolRunnerConfig,
	envFile map[string]string,
	prefix string,
) {
	t.Helper()

	wantInterval := time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_"+prefix+"_SPOOL_RUNNER_INTERVAL_MILLISECOND")) * time.Millisecond
	if cfg.Runner.Interval != wantInterval {
		t.Fatalf("%s Runner.Interval = %s, want %s", name, cfg.Runner.Interval, wantInterval)
	}
	if got, want := cfg.Runner.StopOnError, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_"+prefix+"_SPOOL_RUNNER_STOP_ON_ERROR"); got != want {
		t.Fatalf("%s Runner.StopOnError = %t, want %t", name, got, want)
	}
	if got, want := cfg.LifeCycle.EnablePurgeDone, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_PURGE_"+prefix+"_SPOOL_DONE"); got != want {
		t.Fatalf("%s EnablePurgeDone = %t, want %t", name, got, want)
	}
	if got, want := cfg.LifeCycle.DoneMaxFileAge, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_"+prefix+"_SPOOL_DONE_AGE"); got != want {
		t.Fatalf("%s DoneMaxFileAge = %d, want %d", name, got, want)
	}
	if got, want := cfg.LifeCycle.DoneMaxDirSize, requiredDotEnvUint64(t, envFile, "HETEROLOG_MAX_"+prefix+"_SPOOL_DONE_DIR_SIZE_MB")*BytesPerMegabyte; got != want {
		t.Fatalf("%s DoneMaxDirSize = %d, want %d", name, got, want)
	}
	if got, want := cfg.LifeCycle.EnablePurgeDead, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_PURGE_"+prefix+"_SPOOL_DEAD"); got != want {
		t.Fatalf("%s EnablePurgeDead = %t, want %t", name, got, want)
	}
	if got, want := cfg.LifeCycle.DeadMaxFileAge, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_"+prefix+"_SPOOL_DEAD_AGE"); got != want {
		t.Fatalf("%s DeadMaxFileAge = %d, want %d", name, got, want)
	}
	if got, want := cfg.LifeCycle.DeadMaxDirSize, requiredDotEnvUint64(t, envFile, "HETEROLOG_MAX_"+prefix+"_SPOOL_DEAD_DIR_SIZE_MB")*BytesPerMegabyte; got != want {
		t.Fatalf("%s DeadMaxDirSize = %d, want %d", name, got, want)
	}
	if got, want := cfg.LifeCycle.EnablePurgeQuarantine, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_PURGE_"+prefix+"_SPOOL_QUARANTINE"); got != want {
		t.Fatalf("%s EnablePurgeQuarantine = %t, want %t", name, got, want)
	}
	if got, want := cfg.LifeCycle.QuarantineMaxFileAge, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_"+prefix+"_SPOOL_QUARANTINE_AGE"); got != want {
		t.Fatalf("%s QuarantineMaxFileAge = %d, want %d", name, got, want)
	}
	if got, want := cfg.LifeCycle.QuarantineMaxDirSize, requiredDotEnvUint64(t, envFile, "HETEROLOG_MAX_"+prefix+"_SPOOL_QUARANTINE_DIR_SIZE_MB")*BytesPerMegabyte; got != want {
		t.Fatalf("%s QuarantineMaxDirSize = %d, want %d", name, got, want)
	}
	if got, want := cfg.LifeCycle.SizePurgeInterval, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_"+prefix+"_SPOOL_RUNNER_PURGE_INTERVAL_SECOND"))*time.Second; got != want {
		t.Fatalf("%s SizePurgeInterval = %s, want %s", name, got, want)
	}
	if got, want := cfg.LifeCycle.EnableSizeEstimateCache, requiredDotEnvBool(t, envFile, "HETEROLOG_ENABLE_"+prefix+"_SPOOL_SIZE_ESTIMATE_CACHE"); got != want {
		t.Fatalf("%s EnableSizeEstimateCache = %t, want %t", name, got, want)
	}
	if got, want := cfg.LifeCycle.SizeFullScanEveryNChecks, requiredDotEnvInt(t, envFile, "HETEROLOG_"+prefix+"_SPOOL_DIR_FULL_SCAN_EVERY_N_CHECKS"); got != want {
		t.Fatalf("%s SizeFullScanEveryNChecks = %d, want %d", name, got, want)
	}
	if got, want := cfg.LifeCycle.SizeFullScanMaxInterval, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_"+prefix+"_SPOOL_FULL_SCAN_INTERVAL_SECOND"))*time.Second; got != want {
		t.Fatalf("%s SizeFullScanMaxInterval = %s, want %s", name, got, want)
	}
	if got, want := cfg.Worker.Finalize.RetryDelay, time.Duration(requiredDotEnvInt(t, envFile, "HETEROLOG_"+prefix+"_SPOOL_RUNNER_RETRY_FINALIZE_DELAY_MILLISECOND"))*time.Millisecond; got != want {
		t.Fatalf("%s Finalize RetryDelay = %s, want %s", name, got, want)
	}
	if got, want := cfg.Worker.Finalize.MaxEntries, requiredDotEnvInt(t, envFile, "HETEROLOG_MAX_"+prefix+"_SPOOL_FINALIZE_FAILURE_ENTRIES"); got != want {
		t.Fatalf("%s Finalize MaxEntries = %d, want %d", name, got, want)
	}
}

func configFixtureRootForTest(t *testing.T) string {
	t.Helper()
	return testutil.ConfigFixtureRoot(t)
}

// publicEnvExamplePathは配布用.env.exampleのpathを返します。公開設定の掲載内容は
// このfileを直接検証し、fixtureの.envと.env.exampleを相互比較しません。両者は同じ
// 公開例から生成されるため、比較してもfixture生成処理を再検証するだけだからです。
func publicEnvExamplePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(testutil.RepositoryRoot(t), ".env.example")
}

// readPublicEnvExampleは配布用.env.exampleを、重複keyを拒否したうえで読み込みます。
func readPublicEnvExample(t *testing.T) map[string]string {
	t.Helper()

	path := publicEnvExamplePath(t)
	readUniqueEnvKeys(t, path)
	values, err := godotenv.Read(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return values
}

// restoreProcessEnvAfterTestは、os.Clearenvを使うtestの終了時にprocess environmentを
// 元へ戻します。Windowsの「=C:」のような名前も復元できるtestutilの実装を共有します。
func restoreProcessEnvAfterTest(t *testing.T) {
	t.Helper()
	testutil.PreserveEnvironment(t)
}

// clearProcessEnvForTestは、設定値を隔離するためprocess environmentを消去し、test終了時に
// 元へ戻します。Windowsのsocket作成や既定の保存先の解決に使うSystemRoot・LOCALAPPDATA・
// TEMPなどOSの実行環境の変数だけは残します。
func clearProcessEnvForTest(t *testing.T) {
	t.Helper()
	testutil.ClearEnvironment(t)
}

func loadConfigFromTempEnv(t *testing.T, lines []string) AppConfig {
	t.Helper()

	restoreProcessEnvAfterTest(t)
	envPath := filepath.Join(t.TempDir(), ".env")
	clearProcessEnvForTest(t)

	if err := os.WriteFile(envPath, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatalf("write temp env: %v", err)
	}
	if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
		t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
	}

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() with temp env: %v", err)
	}
	return cfg
}

func loadConfigErrorFromTempEnv(t *testing.T, lines []string) (AppConfig, error) {
	t.Helper()

	restoreProcessEnvAfterTest(t)
	envPath := filepath.Join(t.TempDir(), ".env")
	clearProcessEnvForTest(t)

	if err := os.WriteFile(envPath, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatalf("write temp env: %v", err)
	}
	if err := os.Setenv("HETEROLOG_ENV_PATH", envPath); err != nil {
		t.Fatalf("set HETEROLOG_ENV_PATH: %v", err)
	}

	cfg, err := LoadConfig()
	if err != nil {
		return cfg, err
	}
	return cfg, ValidateApplicationConfig(cfg)
}

func splitDotEnvCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// overrideFixtureEnvValuesは、fixtureに掲載されたkeyをprocess environmentで上書きし、
// 期待値の算出に使うenvFileにも同じ値を反映します。公開例の値がcode既定値と同じkeyへ
// 非既定値を与え、keyを読まないloaderではtestが失敗するようにします。process
// environmentは.envより優先されるため、公開例の値自体は変更しません。
// os.Clearenvの後、LoadConfigより前に呼び出します。
func overrideFixtureEnvValues(t *testing.T, envFile map[string]string, overrides map[string]string) {
	t.Helper()

	for key, value := range overrides {
		if _, ok := envFile[key]; !ok {
			t.Fatalf("fixture .env does not document %s", key)
		}
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
		envFile[key] = value
	}
}

func requiredDotEnv(t *testing.T, envFile map[string]string, key string) string {
	t.Helper()

	value, ok := envFile[key]
	if !ok {
		t.Fatalf("fixture .env is missing %s", key)
	}
	return value
}

func requiredDotEnvBool(t *testing.T, envFile map[string]string, key string) bool {
	t.Helper()

	value, err := strconv.ParseBool(requiredDotEnv(t, envFile, key))
	if err != nil {
		t.Fatalf("parse %s as bool: %v", key, err)
	}
	return value
}

func requiredDotEnvInt(t *testing.T, envFile map[string]string, key string) int {
	t.Helper()

	value, err := strconv.Atoi(requiredDotEnv(t, envFile, key))
	if err != nil {
		t.Fatalf("parse %s as int: %v", key, err)
	}
	return value
}

func requiredDotEnvUint64(t *testing.T, envFile map[string]string, key string) uint64 {
	t.Helper()

	value, err := strconv.ParseUint(requiredDotEnv(t, envFile, key), 10, 64)
	if err != nil {
		t.Fatalf("parse %s as uint64: %v", key, err)
	}
	return value
}

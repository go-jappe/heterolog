package config

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	configcloudwatchlogs "github.com/go-jappe/heterolog/internal/config/cloudwatchlogs"
	configfailure "github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/internalerror"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	lokicfg "github.com/go-jappe/heterolog/internal/config/loki"
	mappedvalidation "github.com/go-jappe/heterolog/internal/config/validation/mapped"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	cloudwatchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

func TestLoadConfigAppliesApplicationCloudWatchRegionDefault(t *testing.T) {
	cfg := loadConfigFromTempEnv(t, loggerContractTestBaseEnv("cloudwatchlogs"))

	contracts := []struct {
		name   string
		region string
	}{
		{name: "sync", region: cfg.CloudWatchLogsClient.Region},
		{name: "async", region: cfg.AsyncCloudWatchLogsClient.Region},
		{name: "delivery", region: cfg.DeliveryCloudWatchLogsClient.Region},
	}
	for _, contract := range contracts {
		if contract.region != configcloudwatchlogs.DefaultRegion {
			t.Fatalf(
				"%s CloudWatch Logs region = %q, want application default %q",
				contract.name,
				contract.region,
				configcloudwatchlogs.DefaultRegion,
			)
		}
	}
}

// TestLoadConfigMapsLokiSettingsFromMatchingEnvContract verifies the complete configuration route
// from a real HETEROLOG_ENV_PATH file through LoadConfig, the public logger DTO, and the infrastructure
// adapters. Every LoggerContractKind intentionally uses different values so a sync/async/delivery mix-up
// cannot pass because the repository's normal .env happens to contain equal values.
func TestLoadConfigMapsLokiSettingsFromMatchingEnvContract(t *testing.T) {
	base := loggerContractTestBaseEnv("loki")
	// 各行は既定値と異なる値にし、全てのkeyが読込結果に現れるようにします。
	contractLines := []string{
		"HETEROLOG_LOKI_LOG_LEVEL=INFO",
		"HETEROLOG_LOKI_FLUSH_INTERVAL_SECOND=31",
		"HETEROLOG_MAX_LOKI_PUSH_ENTRIES=32",
		"HETEROLOG_LOKI_BUFFER_FLUSH_BYTES=3201",
		"HETEROLOG_MAX_LOKI_FALLBACK_QUEUE_ENTRIES=320",
		"HETEROLOG_MAX_LOKI_LINE_BYTES=3301",
		"HETEROLOG_MAX_LOKI_PUSH_REQUEST_BYTES=3302",
		"HETEROLOG_ENABLE_LOKI_NO_BULK_FLUSH_MODE=true",
		"HETEROLOG_ENABLE_LOG_LOKI_SAMPLING=false",
		"HETEROLOG_ENABLE_LOGGING_LOKI_BY_REQUEST_ID=false",
		"HETEROLOG_LOG_LOKI_REQUEST_ID_RATE=33",
		"HETEROLOG_LOG_LOKI_DEBUG_RATE=34",
		"HETEROLOG_LOG_LOKI_INFO_RATE=35",
		"HETEROLOG_LOG_LOKI_WARN_RATE=36",
		"HETEROLOG_LOG_LOKI_ERROR_RATE=37",
		"HETEROLOG_ENABLE_LOKI_PUSH_LATENCY=false",
		"HETEROLOG_LOKI_LOGGER_ERROR_HANDLER=stdout,slack",
		"HETEROLOG_LOKI_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_LOKI_LOGGER_INFO_HANDLER=slack",
		"HETEROLOG_LOKI_LOGGER_DEBUG_HANDLER=file",
		"HETEROLOG_LOKI_ENDPOINT=http://loki-sync.example",
		"HETEROLOG_LOKI_TIMEOUT_SECOND=38",
		"HETEROLOG_LOKI_TENANT_ID=loki-sync-tenant",
		"HETEROLOG_LOKI_USERNAME=loki-sync-user",
		"HETEROLOG_LOKI_PASSWORD=loki-sync-password",
		"HETEROLOG_LOKI_BEARER_TOKEN=loki-sync-token",
		`HETEROLOG_LOKI_HEADERS={"X-Scope":"loki-sync"}`,
		"HETEROLOG_ENABLE_LOKI_COMPRESS_REQUEST=false",
		"HETEROLOG_LOKI_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_ATTEMPTS=39",
		"HETEROLOG_LOKI_RETRYABLE_CLIENT_MILLISECOND=40",
		"HETEROLOG_MAX_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND=41",
		"HETEROLOG_ASYNC_LOKI_LOG_LEVEL=ERROR",
		"HETEROLOG_ASYNC_LOKI_FLUSH_INTERVAL_SECOND=51",
		"HETEROLOG_MAX_ASYNC_LOKI_PUSH_ENTRIES=52",
		"HETEROLOG_ASYNC_LOKI_BUFFER_FLUSH_BYTES=5201",
		"HETEROLOG_MAX_ASYNC_LOKI_FALLBACK_QUEUE_ENTRIES=520",
		"HETEROLOG_MAX_ASYNC_LOKI_LINE_BYTES=5301",
		"HETEROLOG_MAX_ASYNC_LOKI_PUSH_REQUEST_BYTES=5302",
		"HETEROLOG_ENABLE_ASYNC_LOG_LOKI_SAMPLING=false",
		"HETEROLOG_ENABLE_ASYNC_LOGGING_LOKI_BY_REQUEST_ID=false",
		"HETEROLOG_ASYNC_LOG_LOKI_REQUEST_ID_RATE=53",
		"HETEROLOG_ASYNC_LOG_LOKI_DEBUG_RATE=54",
		"HETEROLOG_ASYNC_LOG_LOKI_INFO_RATE=55",
		"HETEROLOG_ASYNC_LOG_LOKI_WARN_RATE=56",
		"HETEROLOG_ASYNC_LOG_LOKI_ERROR_RATE=57",
		"HETEROLOG_ENABLE_ASYNC_LOKI_PUSH_LATENCY=false",
		"HETEROLOG_ASYNC_LOKI_LOGGER_ERROR_HANDLER=file,stdout",
		"HETEROLOG_ASYNC_LOKI_LOGGER_WARN_HANDLER=file",
		"HETEROLOG_ASYNC_LOKI_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_ASYNC_LOKI_LOGGER_DEBUG_HANDLER=slack",
		"HETEROLOG_ASYNC_LOKI_ENDPOINT=http://loki-async.example",
		"HETEROLOG_ASYNC_LOKI_TIMEOUT_SECOND=58",
		"HETEROLOG_ASYNC_LOKI_TENANT_ID=loki-async-tenant",
		"HETEROLOG_ASYNC_LOKI_USERNAME=loki-async-user",
		"HETEROLOG_ASYNC_LOKI_PASSWORD=loki-async-password",
		"HETEROLOG_ASYNC_LOKI_BEARER_TOKEN=loki-async-token",
		`HETEROLOG_ASYNC_LOKI_HEADERS={"X-Scope":"loki-async"}`,
		"HETEROLOG_ENABLE_ASYNC_LOKI_COMPRESS_REQUEST=false",
		"HETEROLOG_ASYNC_LOKI_RETRY_CLIENT_MODE=retry_policy_with_breaker",
		"HETEROLOG_MAX_ASYNC_LOKI_RETRYABLE_CLIENT_ATTEMPTS=59",
		"HETEROLOG_ASYNC_LOKI_RETRYABLE_CLIENT_MILLISECOND=60",
		"HETEROLOG_MAX_ASYNC_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND=61",
		"HETEROLOG_ENABLE_DELIVERY_LOKI_PUSH_LATENCY=false",
		"HETEROLOG_MAX_DELIVERY_LOKI_LINE_BYTES=7301",
		"HETEROLOG_MAX_DELIVERY_LOKI_PUSH_REQUEST_BYTES=7302",
		"HETEROLOG_DELIVERY_LOKI_ENDPOINT=http://loki-delivery.example",
		"HETEROLOG_DELIVERY_LOKI_TIMEOUT_SECOND=68",
		"HETEROLOG_DELIVERY_LOKI_TENANT_ID=loki-delivery-tenant",
		"HETEROLOG_DELIVERY_LOKI_USERNAME=loki-delivery-user",
		"HETEROLOG_DELIVERY_LOKI_PASSWORD=loki-delivery-password",
		"HETEROLOG_DELIVERY_LOKI_BEARER_TOKEN=loki-delivery-token",
		`HETEROLOG_DELIVERY_LOKI_HEADERS={"X-Scope":"loki-delivery"}`,
		"HETEROLOG_ENABLE_DELIVERY_LOKI_COMPRESS_REQUEST=false",
		"HETEROLOG_DELIVERY_LOKI_RETRY_CLIENT_MODE=circuit_breaker",
		"HETEROLOG_MAX_DELIVERY_LOKI_RETRYABLE_CLIENT_ATTEMPTS=69",
		"HETEROLOG_DELIVERY_LOKI_RETRYABLE_CLIENT_MILLISECOND=70",
		"HETEROLOG_MAX_DELIVERY_LOKI_RETRYABLE_CLIENT_DELAY_MILLISECOND=71",
	}
	lines := append(append([]string(nil), base...), contractLines...)
	cfg := loadConfigFromTempEnv(t, lines)
	// 全ての行が読み込まれ、その行のLoggerContractKindの設定だけに効くことを確認します。
	assertEveryContractEnvLineChangesItsContract(t, lines, contractLines, nil)
	assertLokiLoadedEnvFields(t, cfg)

	syncFailureWant := fullFailureContractWant("loki-sync", 101, false)
	asyncFailureWant := fullFailureContractWant("loki-async", 201, true)
	deliveryFailureWant := deliveryFailureContractWant("loki-delivery", 301)

	assertLokiLoadedContracts(t, cfg, syncFailureWant, asyncFailureWant, deliveryFailureWant)

	syncMapped := ToLoggerConfig(cfg)
	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	if syncMapped.Loki == nil || syncMapped.LokiClient == nil || asyncMapped.Loki == nil || asyncMapped.LokiClient == nil || deliveryMapped.Loki == nil || deliveryMapped.LokiClient == nil {
		t.Fatal("Loki mapper returned a nil logger or client for an enabled LoggerContractKind")
	}

	assertLokiMappedContract(t, "sync", syncMapped.Loki.LokiLoggerBaseConfig, syncMapped.Loki.LokiFlushManagerConfig, syncMapped.LokiClient.Endpoint, syncMapped.LokiClient.TenantID, syncMapped.LokiClient.Headers, syncMapped.LokiClient.CompressRequest, syncMapped.Loki.LokiRetryableClientCaseTransportErrorConfig, syncFailureWant, pkglogger.LogLevelInfo, 31*time.Second, 32, 3201, 320, 3301, 3302, false)
	assertLokiMappedContract(t, "async", asyncMapped.Loki.LokiLoggerBaseConfig, asyncMapped.Loki.LokiFlushManagerConfig, asyncMapped.LokiClient.Endpoint, asyncMapped.LokiClient.TenantID, asyncMapped.LokiClient.Headers, asyncMapped.LokiClient.CompressRequest, asyncMapped.Loki.LokiRetryableClientCaseTransportErrorConfig, asyncFailureWant, pkglogger.LogLevelError, 51*time.Second, 52, 5201, 520, 5301, 5302, false)
	assertLokiDeliveryMappedContract(t, deliveryMapped, deliveryFailureWant)

	// The final adapter boundary is checked separately from DTO assertions. This catches an
	// adapter that silently drops a field even when LoadConfig and mapper are both correct.
	syncLoggerAdapter := lokilogger.NewLokiLoggerConfigAdapter(syncMapped.Loki.LokiLoggerBaseConfig)
	assertContractValue(t, "sync Loki adapter log level", syncLoggerAdapter.Runtime().LogLevel(), pkglogger.LogLevelInfo)
	assertContractValue(t, "sync Loki adapter max entries", syncLoggerAdapter.MaxEntries(), 32)
	assertContractValue(t, "sync Loki adapter max line bytes", syncLoggerAdapter.MaxLineBytes(), 3301)
	assertContractValue(t, "sync Loki adapter max push request bytes", syncLoggerAdapter.MaxPushRequestBytes(), 3302)
	assertContractValue(t, "sync Loki adapter handler", syncLoggerAdapter.Handler().OnError(), []string{"stdout", "slack"})
	syncFlushAdapter := lokilogger.NewFlushManagerConfigAdapter(syncMapped.Loki.LokiFlushManagerConfig)
	assertContractValue(t, "sync Loki adapter archive dir", syncFlushAdapter.FailureStoreConfig().DurableStoreConfig().FileArchiveConfig().Dir(), syncFailureWant.archiveDir)
	assertContractValue(t, "sync Loki adapter spool dir", syncFlushAdapter.FailureStoreConfig().DurableStoreConfig().SpoolConfig().Dir(), syncFailureWant.spoolDir)
	assertContractValue(t, "sync Loki adapter latency", syncFlushAdapter.PushLatencyEnabled(), false)

	asyncLoggerAdapter := lokilogger.NewLokiLoggerConfigAdapter(asyncMapped.Loki.LokiLoggerBaseConfig)
	assertContractValue(t, "async Loki adapter log level", asyncLoggerAdapter.Runtime().LogLevel(), pkglogger.LogLevelError)
	assertContractValue(t, "async Loki adapter max entries", asyncLoggerAdapter.MaxEntries(), 52)
	assertContractValue(t, "async Loki adapter max line bytes", asyncLoggerAdapter.MaxLineBytes(), 5301)
	assertContractValue(t, "async Loki adapter max push request bytes", asyncLoggerAdapter.MaxPushRequestBytes(), 5302)
	assertContractValue(t, "async Loki adapter handler", asyncLoggerAdapter.Handler().OnError(), []string{"file", "stdout"})
	asyncFlushAdapter := lokilogger.NewFlushManagerConfigAdapter(asyncMapped.Loki.LokiFlushManagerConfig)
	assertContractValue(t, "async Loki adapter archive dir", asyncFlushAdapter.FailureStoreConfig().DurableStoreConfig().FileArchiveConfig().Dir(), asyncFailureWant.archiveDir)
	assertContractValue(t, "async Loki adapter spool memory pressure", asyncFlushAdapter.FailureStoreConfig().DurableStoreConfig().SpoolConfig().OverloadPolicyConfig().UseMemoryPressureForOverload(), true)
	assertContractValue(t, "async Loki adapter latency", asyncFlushAdapter.PushLatencyEnabled(), false)

	deliveryLoggerAdapter := lokilogger.NewDeliveryLokiLoggerConfigAdapter(deliveryMapped.Loki.DeliveryLokiLoggerBaseConfig)
	assertContractValue(t, "delivery Loki adapter max line bytes", deliveryLoggerAdapter.MaxLineBytes(), 7301)
	assertContractValue(t, "delivery Loki adapter max push request bytes", deliveryLoggerAdapter.MaxPushRequestBytes(), 7302)
	deliveryFlushAdapter := lokilogger.NewDeliveryFlushManagerConfigAdapter(deliveryMapped.Loki.DeliveryLokiFlushManagerConfig)
	assertContractValue(t, "delivery Loki adapter archive dir", deliveryFlushAdapter.DeliveryFailureStoreConfig().DeliveryDurableStoreConfig().FileArchiveConfig().Dir(), deliveryFailureWant.archiveDir)
	assertContractValue(t, "delivery Loki adapter latency", deliveryFlushAdapter.PushLatencyEnabled(), false)
}

// TestLoadConfigMapsCloudWatchLogsSettingsFromMatchingEnvContract applies the same contract-separation
// proof to CloudWatch Logs, including its contract-specific group/stream and AWS client settings.
func TestLoadConfigMapsCloudWatchLogsSettingsFromMatchingEnvContract(t *testing.T) {
	base := loggerContractTestBaseEnv("cloudwatchlogs")
	// 各行は既定値と異なる値にし、全てのkeyが読込結果に現れるようにします。
	contractLines := []string{
		"HETEROLOG_CLOUDWATCH_LOG_LEVEL=WARN",
		"HETEROLOG_CLOUDWATCH_FLUSH_INTERVAL_SECOND=31",
		"HETEROLOG_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS=32",
		"HETEROLOG_CLOUDWATCH_BUFFER_FLUSH_BYTES=3201",
		"HETEROLOG_MAX_CLOUDWATCH_FALLBACK_QUEUE_EVENTS=320",
		"HETEROLOG_ENABLE_CLOUDWATCH_NO_BULK_FLUSH_MODE=true",
		"HETEROLOG_ENABLE_LOG_CLOUDWATCH_SAMPLING=false",
		"HETEROLOG_ENABLE_LOGGING_CLOUDWATCH_BY_REQUEST_ID=false",
		"HETEROLOG_LOG_CLOUDWATCH_REQUEST_ID_RATE=33",
		"HETEROLOG_LOG_CLOUDWATCH_DEBUG_RATE=34",
		"HETEROLOG_LOG_CLOUDWATCH_INFO_RATE=35",
		"HETEROLOG_LOG_CLOUDWATCH_WARN_RATE=36",
		"HETEROLOG_LOG_CLOUDWATCH_ERROR_RATE=37",
		"HETEROLOG_ENABLE_CLOUDWATCH_PUT_LATENCY=false",
		"HETEROLOG_CLOUDWATCH_LOGGER_ERROR_HANDLER=stdout,slack",
		"HETEROLOG_CLOUDWATCH_LOGGER_WARN_HANDLER=stdout",
		"HETEROLOG_CLOUDWATCH_LOGGER_INFO_HANDLER=slack",
		"HETEROLOG_CLOUDWATCH_LOGGER_DEBUG_HANDLER=file",
		"HETEROLOG_CLOUDWATCH_REGION=sync-region",
		"HETEROLOG_CLOUDWATCH_ENDPOINT=http://cloudwatch-sync.example",
		"HETEROLOG_CLOUDWATCH_LOG_GROUP_NAME=sync-group",
		"HETEROLOG_CLOUDWATCH_LOG_STREAM_NAME=sync-stream",
		"HETEROLOG_CLOUDWATCH_LOG_STREAM_ROTATION=monthly",
		"HETEROLOG_CLOUDWATCH_TIMEOUT_SECOND=38",
		"HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID=sync-access",
		"HETEROLOG_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=sync-secret",
		"HETEROLOG_CLOUDWATCH_AWS_SESSION_TOKEN=sync-session",
		`HETEROLOG_CLOUDWATCH_HEADERS={"X-Scope":"cloudwatch-sync"}`,
		"HETEROLOG_ENABLE_CLOUDWATCH_AUTO_CREATE_LOG_STREAM=true",
		"HETEROLOG_ENABLE_CLOUDWATCH_COMPRESS_REQUEST=false",
		"HETEROLOG_CLOUDWATCH_RETRY_CLIENT_MODE=retry",
		"HETEROLOG_MAX_CLOUDWATCH_RETRYABLE_CLIENT_ATTEMPTS=39",
		"HETEROLOG_CLOUDWATCH_RETRYABLE_CLIENT_MILLISECOND=40",
		"HETEROLOG_MAX_CLOUDWATCH_RETRYABLE_CLIENT_DELAY_MILLISECOND=41",
		"HETEROLOG_ASYNC_CLOUDWATCH_LOG_LEVEL=ERROR",
		"HETEROLOG_ASYNC_CLOUDWATCH_FLUSH_INTERVAL_SECOND=51",
		"HETEROLOG_ASYNC_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS=52",
		"HETEROLOG_ASYNC_CLOUDWATCH_BUFFER_FLUSH_BYTES=5201",
		"HETEROLOG_MAX_ASYNC_CLOUDWATCH_FALLBACK_QUEUE_EVENTS=520",
		"HETEROLOG_ENABLE_ASYNC_LOG_CLOUDWATCH_SAMPLING=false",
		"HETEROLOG_ENABLE_ASYNC_LOGGING_CLOUDWATCH_BY_REQUEST_ID=false",
		"HETEROLOG_ASYNC_LOG_CLOUDWATCH_REQUEST_ID_RATE=53",
		"HETEROLOG_ASYNC_LOG_CLOUDWATCH_DEBUG_RATE=54",
		"HETEROLOG_ASYNC_LOG_CLOUDWATCH_INFO_RATE=55",
		"HETEROLOG_ASYNC_LOG_CLOUDWATCH_WARN_RATE=56",
		"HETEROLOG_ASYNC_LOG_CLOUDWATCH_ERROR_RATE=57",
		"HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_PUT_LATENCY=false",
		"HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_ERROR_HANDLER=file,stdout",
		"HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_WARN_HANDLER=file",
		"HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_INFO_HANDLER=stdout",
		"HETEROLOG_ASYNC_CLOUDWATCH_LOGGER_DEBUG_HANDLER=slack",
		"HETEROLOG_ASYNC_CLOUDWATCH_REGION=async-region",
		"HETEROLOG_ASYNC_CLOUDWATCH_ENDPOINT=http://cloudwatch-async.example",
		"HETEROLOG_ASYNC_CLOUDWATCH_LOG_GROUP_NAME=async-group",
		"HETEROLOG_ASYNC_CLOUDWATCH_LOG_STREAM_NAME=async-stream",
		"HETEROLOG_ASYNC_CLOUDWATCH_LOG_STREAM_ROTATION=DAILY",
		"HETEROLOG_ASYNC_CLOUDWATCH_TIMEOUT_SECOND=58",
		"HETEROLOG_ASYNC_CLOUDWATCH_AWS_ACCESS_KEY_ID=async-access",
		"HETEROLOG_ASYNC_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=async-secret",
		"HETEROLOG_ASYNC_CLOUDWATCH_AWS_SESSION_TOKEN=async-session",
		`HETEROLOG_ASYNC_CLOUDWATCH_HEADERS={"X-Scope":"cloudwatch-async"}`,
		"HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_AUTO_CREATE_LOG_STREAM=true",
		"HETEROLOG_ENABLE_ASYNC_CLOUDWATCH_COMPRESS_REQUEST=false",
		"HETEROLOG_ASYNC_CLOUDWATCH_RETRY_CLIENT_MODE=retry_policy_with_breaker",
		"HETEROLOG_MAX_ASYNC_CLOUDWATCH_RETRYABLE_CLIENT_ATTEMPTS=59",
		"HETEROLOG_ASYNC_CLOUDWATCH_RETRYABLE_CLIENT_MILLISECOND=60",
		"HETEROLOG_MAX_ASYNC_CLOUDWATCH_RETRYABLE_CLIENT_DELAY_MILLISECOND=61",
		"HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_PUT_LATENCY=false",
		"HETEROLOG_DELIVERY_CLOUDWATCH_REGION=delivery-region",
		"HETEROLOG_DELIVERY_CLOUDWATCH_ENDPOINT=http://cloudwatch-delivery.example",
		"HETEROLOG_DELIVERY_CLOUDWATCH_LOG_GROUP_NAME=delivery-group",
		"HETEROLOG_DELIVERY_CLOUDWATCH_LOG_STREAM_NAME=delivery-stream",
		"HETEROLOG_DELIVERY_CLOUDWATCH_LOG_STREAM_ROTATION=hourly",
		"HETEROLOG_DELIVERY_CLOUDWATCH_TIMEOUT_SECOND=68",
		"HETEROLOG_DELIVERY_CLOUDWATCH_AWS_ACCESS_KEY_ID=delivery-access",
		"HETEROLOG_DELIVERY_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=delivery-secret",
		"HETEROLOG_DELIVERY_CLOUDWATCH_AWS_SESSION_TOKEN=delivery-session",
		`HETEROLOG_DELIVERY_CLOUDWATCH_HEADERS={"X-Scope":"cloudwatch-delivery"}`,
		"HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_AUTO_CREATE_LOG_STREAM=true",
		"HETEROLOG_ENABLE_DELIVERY_CLOUDWATCH_COMPRESS_REQUEST=false",
		"HETEROLOG_DELIVERY_CLOUDWATCH_RETRY_CLIENT_MODE=circuit_breaker",
		"HETEROLOG_MAX_DELIVERY_CLOUDWATCH_RETRYABLE_CLIENT_ATTEMPTS=69",
		"HETEROLOG_DELIVERY_CLOUDWATCH_RETRYABLE_CLIENT_MILLISECOND=70",
		"HETEROLOG_MAX_DELIVERY_CLOUDWATCH_RETRYABLE_CLIENT_DELAY_MILLISECOND=71",
	}
	lines := append(append([]string(nil), base...), contractLines...)
	cfg := loadConfigFromTempEnv(t, lines)
	// 全ての行が読み込まれ、その行のLoggerContractKindの設定だけに効くことを確認します。
	assertEveryContractEnvLineChangesItsContract(t, lines, contractLines, nil)
	assertCloudWatchLoadedEnvFields(t, cfg)

	syncFailureWant := fullFailureContractWant("cloudwatch-sync", 401, false)
	asyncFailureWant := fullFailureContractWant("cloudwatch-async", 501, true)
	deliveryFailureWant := deliveryFailureContractWant("cloudwatch-delivery", 601)

	assertCloudWatchLoadedContracts(t, cfg, syncFailureWant, asyncFailureWant, deliveryFailureWant)
	assertContractValue(t, "loaded sync CloudWatch stream routing granularity", cfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity, streamrouting.Monthly)
	assertContractValue(t, "loaded async CloudWatch stream routing granularity", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamRoutingGranularity, streamrouting.Daily)
	assertContractValue(t, "loaded delivery CloudWatch stream routing granularity", cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamRoutingGranularity, streamrouting.Hourly)

	syncMapped := ToLoggerConfig(cfg)
	asyncMapped := ToAsyncBestEffortMultiLoggerConfig(cfg)
	deliveryMapped := ToDeliveryLoggerConfig(cfg)
	if syncMapped.CloudWatchLogs == nil || syncMapped.CloudWatchLogsClient == nil || asyncMapped.CloudWatchLogs == nil || asyncMapped.CloudWatchLogsClient == nil || deliveryMapped.CloudWatchLogs == nil || deliveryMapped.CloudWatchLogsClient == nil {
		t.Fatal("CloudWatch Logs mapper returned a nil logger or client for an enabled LoggerContractKind")
	}
	assertContractValue(t, "mapped sync CloudWatch log date format", syncMapped.CloudWatchLogs.LogBodyConfig.LogLineDateFormat, logconfig.LogLineDateFormat)
	assertContractValue(t, "mapped async CloudWatch log date format", asyncMapped.CloudWatchLogs.LogBodyConfig.LogLineDateFormat, logconfig.LogLineDateFormat)
	assertContractValue(t, "mapped delivery CloudWatch log date format", deliveryMapped.CloudWatchLogs.LogBodyConfig.LogLineDateFormat, logconfig.LogLineDateFormat)

	assertCloudWatchMappedContract(t, "sync", syncMapped.CloudWatchLogs.CloudWatchLogsLoggerBaseConfig, syncMapped.CloudWatchLogs.CloudWatchLogsFlushManagerConfig, syncMapped.CloudWatchLogsClient.Region, syncMapped.CloudWatchLogsClient.Endpoint, syncMapped.CloudWatchLogsClient.Headers, syncMapped.CloudWatchLogsClient.AutoCreateLogStream, syncMapped.CloudWatchLogsClient.CompressRequest, syncMapped.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig, syncFailureWant, pkglogger.LogLevelWarn, 31*time.Second, 32, 3201, 320, false, "sync-group", "sync-stream")
	assertCloudWatchMappedContract(t, "async", asyncMapped.CloudWatchLogs.CloudWatchLogsLoggerBaseConfig, asyncMapped.CloudWatchLogs.CloudWatchLogsFlushManagerConfig, asyncMapped.CloudWatchLogsClient.Region, asyncMapped.CloudWatchLogsClient.Endpoint, asyncMapped.CloudWatchLogsClient.Headers, asyncMapped.CloudWatchLogsClient.AutoCreateLogStream, asyncMapped.CloudWatchLogsClient.CompressRequest, asyncMapped.CloudWatchLogs.CloudWatchLogsRetryableClientCaseTransportErrorConfig, asyncFailureWant, pkglogger.LogLevelError, 51*time.Second, 52, 5201, 520, false, "async-group", "async-stream")
	assertCloudWatchDeliveryMappedContract(t, deliveryMapped, deliveryFailureWant)

	syncLoggerAdapter := cloudwatchlogger.NewCloudWatchLogsLoggerConfigAdapter(syncMapped.CloudWatchLogs.CloudWatchLogsLoggerBaseConfig)
	// 時刻設定を指定しない場合は、README・DefaultConfigと同じくUTCで記録します。
	assertContractValue(t, "sync CloudWatch adapter local time", syncLoggerAdapter.Time().EnableLocalTime(), false)
	assertContractValue(t, "sync CloudWatch adapter timezone", syncLoggerAdapter.Time().TimeZone(), "UTC")
	assertContractValue(t, "sync CloudWatch adapter log level", syncLoggerAdapter.Runtime().LogLevel(), pkglogger.LogLevelWarn)
	assertContractValue(t, "sync CloudWatch adapter buffer flush max events", syncLoggerAdapter.BufferFlushMaxEvents(), 32)
	assertContractValue(t, "sync CloudWatch adapter handler", syncLoggerAdapter.Handler().OnError(), []string{"stdout", "slack"})
	syncFlushAdapter := cloudwatchlogger.NewFlushManagerConfigAdapter(
		syncMapped.CloudWatchLogs.CloudWatchLogsFlushManagerConfig,
		cfg.Logger.CloudWatchLogsConfig.LogStreamRoutingGranularity,
	)
	assertContractValue(t, "sync CloudWatch adapter routing granularity", syncFlushAdapter.LogStreamRoutingGranularity(), streamrouting.Monthly)
	assertContractValue(t, "sync CloudWatch adapter group", syncFlushAdapter.LogGroupName(), "sync-group")
	assertContractValue(t, "sync CloudWatch adapter archive dir", syncFlushAdapter.FailureStoreConfig().DurableStoreConfig().FileArchiveConfig().Dir(), syncFailureWant.archiveDir)
	assertContractValue(t, "sync CloudWatch adapter spool dir", syncFlushAdapter.FailureStoreConfig().DurableStoreConfig().SpoolConfig().Dir(), syncFailureWant.spoolDir)

	asyncLoggerAdapter := cloudwatchlogger.NewCloudWatchLogsLoggerConfigAdapter(asyncMapped.CloudWatchLogs.CloudWatchLogsLoggerBaseConfig)
	assertContractValue(t, "async CloudWatch adapter log level", asyncLoggerAdapter.Runtime().LogLevel(), pkglogger.LogLevelError)
	assertContractValue(t, "async CloudWatch adapter buffer flush max events", asyncLoggerAdapter.BufferFlushMaxEvents(), 52)
	assertContractValue(t, "async CloudWatch adapter handler", asyncLoggerAdapter.Handler().OnError(), []string{"file", "stdout"})
	asyncFlushAdapter := cloudwatchlogger.NewFlushManagerConfigAdapter(
		asyncMapped.CloudWatchLogs.CloudWatchLogsFlushManagerConfig,
		cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogStreamRoutingGranularity,
	)
	assertContractValue(t, "async CloudWatch adapter routing granularity", asyncFlushAdapter.LogStreamRoutingGranularity(), streamrouting.Daily)
	assertContractValue(t, "async CloudWatch adapter group", asyncFlushAdapter.LogGroupName(), "async-group")
	assertContractValue(t, "async CloudWatch adapter archive dir", asyncFlushAdapter.FailureStoreConfig().DurableStoreConfig().FileArchiveConfig().Dir(), asyncFailureWant.archiveDir)
	assertContractValue(t, "async CloudWatch adapter spool memory pressure", asyncFlushAdapter.FailureStoreConfig().DurableStoreConfig().SpoolConfig().OverloadPolicyConfig().UseMemoryPressureForOverload(), true)

	deliveryFlushAdapter := cloudwatchlogger.NewDeliveryFlushManagerConfigAdapter(
		deliveryMapped.CloudWatchLogs.DeliveryCloudWatchLogsFlushManagerConfig,
		cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.LogStreamRoutingGranularity,
	)
	assertContractValue(t, "delivery CloudWatch adapter routing granularity", deliveryFlushAdapter.LogStreamRoutingGranularity(), streamrouting.Hourly)
	assertContractValue(t, "delivery CloudWatch adapter group", deliveryFlushAdapter.LogGroupName(), "delivery-group")
	assertContractValue(t, "delivery CloudWatch adapter archive dir", deliveryFlushAdapter.DeliveryFailureStoreConfig().DeliveryDurableStoreConfig().FileArchiveConfig().Dir(), deliveryFailureWant.archiveDir)
}

func TestMapLoggerConfigsRejectUnsupportedCloudWatchStreamRoutingGranularityFromEnvironment(t *testing.T) {
	tests := []struct {
		name       string
		setting    string
		wantSource string
		mapConfig  func(AppConfig) error
	}{
		{
			name:       "sync",
			setting:    "HETEROLOG_CLOUDWATCH_LOG_STREAM_ROTATION=weekly",
			wantSource: "logger",
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
		},
		{
			name:       "async",
			setting:    "HETEROLOG_ASYNC_CLOUDWATCH_LOG_STREAM_ROTATION=weekly",
			wantSource: "async logger",
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
		},
		{
			name:       "delivery",
			setting:    "HETEROLOG_DELIVERY_CLOUDWATCH_LOG_STREAM_ROTATION=weekly",
			wantSource: "delivery logger",
			mapConfig: func(cfg AppConfig) error {
				_, err := MapDeliveryLoggerConfig(cfg)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, append(
				loggerContractTestBaseEnv("cloudwatchlogs"),
				tt.setting,
				"HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID=sync-access",
				"HETEROLOG_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=sync-secret",
				"HETEROLOG_ASYNC_CLOUDWATCH_AWS_ACCESS_KEY_ID=async-access",
				"HETEROLOG_ASYNC_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=async-secret",
				"HETEROLOG_DELIVERY_CLOUDWATCH_AWS_ACCESS_KEY_ID=delivery-access",
				"HETEROLOG_DELIVERY_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=delivery-secret",
			))

			err := tt.mapConfig(cfg)
			configErr := requireMappedConfigError(
				t,
				err,
				tt.wantSource,
				mappedvalidation.ConfigSectionCloudWatchLogsStreamRouting,
				streamrouting.ErrInvalidGranularity,
			)
			var granularityErr *streamrouting.InvalidGranularityError
			if !errors.As(configErr, &granularityErr) || granularityErr.Granularity != "weekly" {
				t.Fatalf("stream routing error = %v, want rejected granularity %q", configErr, "weekly")
			}
		})
	}
}

func TestMapLoggerConfigsAllowCloudWatchBufferFlushCountAbovePhysicalRequestLimit(t *testing.T) {
	tests := []struct {
		name          string
		maxEvents     string
		fallbackQueue string
		mapConfig     func(AppConfig) error
	}{
		{
			name:          "sync",
			maxEvents:     "HETEROLOG_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS=10001",
			fallbackQueue: "HETEROLOG_MAX_CLOUDWATCH_FALLBACK_QUEUE_EVENTS=10001",
			mapConfig: func(cfg AppConfig) error {
				_, err := MapLoggerConfig(cfg)
				return err
			},
		},
		{
			name:          "async",
			maxEvents:     "HETEROLOG_ASYNC_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS=10001",
			fallbackQueue: "HETEROLOG_MAX_ASYNC_CLOUDWATCH_FALLBACK_QUEUE_EVENTS=10001",
			mapConfig: func(cfg AppConfig) error {
				_, err := MapAsyncLoggerConfig(cfg)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := loadConfigFromTempEnv(t, append(
				loggerContractTestBaseEnv("cloudwatchlogs"),
				tt.maxEvents,
				tt.fallbackQueue,
				"HETEROLOG_CLOUDWATCH_LOG_GROUP_NAME=sync-group",
				"HETEROLOG_CLOUDWATCH_LOG_STREAM_NAME=sync-stream",
				"HETEROLOG_CLOUDWATCH_AWS_ACCESS_KEY_ID=sync-access",
				"HETEROLOG_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=sync-secret",
				"HETEROLOG_ASYNC_CLOUDWATCH_LOG_GROUP_NAME=async-group",
				"HETEROLOG_ASYNC_CLOUDWATCH_LOG_STREAM_NAME=async-stream",
				"HETEROLOG_ASYNC_CLOUDWATCH_AWS_ACCESS_KEY_ID=async-access",
				"HETEROLOG_ASYNC_CLOUDWATCH_AWS_SECRET_ACCESS_KEY=async-secret",
			))

			if err := tt.mapConfig(cfg); err != nil {
				t.Fatalf("mapped .env config error = %v, want nil", err)
			}
		})
	}
}

func loggerContractTestBaseEnv(backend string) []string {
	lines := []string{
		"HETEROLOG_APP_NAME=logger-env-contract-test",
		"HETEROLOG_TARGET=logger-env-contract-target",
		"HETEROLOG_SYSTEM_LOG_DIR=/tmp/logger-env-contract-log",
		"HETEROLOG_SYSTEM_LIB_DIR=/tmp/logger-env-contract-lib",
		"HETEROLOG_APP_STATE_DIR=/tmp/logger-env-contract-state",
		"HETEROLOG_ENABLE_LOGGING=true",
		"HETEROLOG_ENABLE_ASYNC_BEST_EFFORT_LOGGING=true",
		"HETEROLOG_ENABLE_DELIVERY_LOGGING=true",
		"HETEROLOG_LOG_TYPE=" + backend,
		"HETEROLOG_ASYNC_LOG_TYPE=" + backend,
		"HETEROLOG_DELIVERY_LOG_TYPE=" + backend,
		"HETEROLOG_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_ASYNC_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
		"HETEROLOG_DELIVERY_LOG_TIMEOUT_BUDGET_POLICY=unbounded",
	}
	if backend == "loki" {
		return append(lines, lokiFailureContractEnv()...)
	}
	return append(lines, cloudWatchFailureContractEnv()...)
}

func lokiFailureContractEnv() []string {
	return append(append(
		fullFailureContractEnv("LOKI", "loki-sync", 101, false),
		fullFailureContractEnv("ASYNC_LOKI", "loki-async", 201, true)...,
	), deliveryFailureContractEnv("DELIVERY_LOKI", "loki-delivery", 301)...)
}

func cloudWatchFailureContractEnv() []string {
	return append(append(
		fullFailureContractEnv("CLOUDWATCH", "cloudwatch-sync", 401, false),
		fullFailureContractEnv("ASYNC_CLOUDWATCH", "cloudwatch-async", 501, true)...,
	), deliveryFailureContractEnv("DELIVERY_CLOUDWATCH", "cloudwatch-delivery", 601)...)
}

func fullFailureContractEnv(prefix, valuePrefix string, base int, useMemoryPressure bool) []string {
	return []string{
		"HETEROLOG_MAX_" + prefix + "_FAILURE_HANDLER_CONCURRENT=" + strconv.Itoa(base+1),
		"HETEROLOG_" + prefix + "_DURABLE_TYPES=spool,fileArchive",
		"HETEROLOG_MAX_" + prefix + "_DURABLE_FILE_ARCHIVE_SIZE_MB=" + strconv.Itoa(base+2),
		"HETEROLOG_" + prefix + "_FAILURE_ARCHIVE_DIR=" + valuePrefix + "-archive",
		"HETEROLOG_" + prefix + "_DURABLE_RETRYABLE_DIR=" + valuePrefix + "-retryable",
		"HETEROLOG_" + prefix + "_DURABLE_PERMANENT_DIR=" + valuePrefix + "-permanent",
		"HETEROLOG_" + prefix + "_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE=true",
		"HETEROLOG_MAX_" + prefix + "_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS=" + strconv.Itoa(base+3),
		"HETEROLOG_MAX_" + prefix + "_ENABLE_DURABLE_FILE_ARCHIVE_AGE=" + strconv.Itoa(base+4),
		"HETEROLOG_MAX_" + prefix + "_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB=" + strconv.Itoa(base+5),
		"HETEROLOG_ENABLE_" + prefix + "_DURABLE_FILE_ARCHIVE_COMPRESS=true",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT=zstd",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_ROTATION=weekly",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET=" + valuePrefix + "-bucket",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX=" + valuePrefix + "/prefix",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=degrade",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=s3",
		"HETEROLOG_" + prefix + "_FAILURE_SPOOL_DIR=" + valuePrefix + "-spool",
		"HETEROLOG_" + prefix + "_SPOOL_OVERLOAD_POLICY=drop",
		"HETEROLOG_" + prefix + "_SPOOL_SECONDARY_DESTINATION=stdout",
		"HETEROLOG_MAX_" + prefix + "_SPOOL_SCANNED_CACHE_USAGE_COUNT=" + strconv.Itoa(base+6),
		"HETEROLOG_" + prefix + "_SPOOL_CACHE_LIFETIME_SECOND=" + strconv.Itoa(base+7),
		"HETEROLOG_MAX_" + prefix + "_SPOOL_FILE_ACTIVE_DIR_SIZE_MB=" + strconv.Itoa(base+8),
		"HETEROLOG_" + prefix + "_ACTIVE_SPOOL_BACKLOG_WARN_AFTER_DAYS=" + strconv.Itoa(base+9),
		"HETEROLOG_USE_MEMORY_PRESSURE_" + prefix + "_SPOOL_OVERLOAD=" + strconv.FormatBool(useMemoryPressure),
	}
}

func deliveryFailureContractEnv(prefix, valuePrefix string, base int) []string {
	return []string{
		"HETEROLOG_" + prefix + "_DURABLE_TYPES=fileArchive",
		"HETEROLOG_MAX_" + prefix + "_DURABLE_FILE_ARCHIVE_SIZE_MB=" + strconv.Itoa(base+2),
		"HETEROLOG_" + prefix + "_FAILURE_ARCHIVE_DIR=" + valuePrefix + "-archive",
		"HETEROLOG_" + prefix + "_DURABLE_RETRYABLE_DIR=" + valuePrefix + "-retryable",
		"HETEROLOG_" + prefix + "_DURABLE_PERMANENT_DIR=" + valuePrefix + "-permanent",
		"HETEROLOG_" + prefix + "_ENABLE_DURABLE_FILE_ARCHIVE_ROTATE=true",
		"HETEROLOG_MAX_" + prefix + "_ENABLE_DURABLE_FILE_ARCHIVE_BACKUPS=" + strconv.Itoa(base+3),
		"HETEROLOG_MAX_" + prefix + "_ENABLE_DURABLE_FILE_ARCHIVE_AGE=" + strconv.Itoa(base+4),
		"HETEROLOG_MAX_" + prefix + "_ENABLE_DURABLE_FILE_ARCHIVE_DIR_SIZE_MB=" + strconv.Itoa(base+5),
		"HETEROLOG_ENABLE_" + prefix + "_DURABLE_FILE_ARCHIVE_COMPRESS=true",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_COMPRESS_FORMAT=lz4",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_ROTATION=monthly",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_BUCKET=" + valuePrefix + "-bucket",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_OBJECT_STORAGE_S3_PREFIX=" + valuePrefix + "/prefix",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_OVERLOAD_POLICY=drop",
		"HETEROLOG_" + prefix + "_DURABLE_FILE_ARCHIVE_SECONDARY_DESTINATION=stdout",
	}
}

type fullFailureSnapshot struct {
	maxConcurrent                   int
	archiveMaxSize                  uint64
	archiveDir                      string
	retryableDir                    string
	permanentDir                    string
	archiveBackups                  int
	archiveAge                      int
	archiveMaxDirSize               uint64
	archiveBucket                   string
	archivePrefix                   string
	archiveOverload                 pkgdurable.OverloadPolicy
	archiveSpill                    pkgdurable.SpillDestination
	spoolDir                        string
	spoolMaxCalls                   uint64
	spoolScanInterval               time.Duration
	spoolMaxDirSize                 uint64
	spoolActiveBacklogWarnAfterDays int
	spoolMemoryPressure             bool
}

type deliveryFailureSnapshot struct {
	archiveMaxSize    uint64
	archiveDir        string
	retryableDir      string
	permanentDir      string
	archiveBackups    int
	archiveAge        int
	archiveMaxDirSize uint64
	archiveBucket     string
	archivePrefix     string
	archiveOverload   pkgdurable.OverloadPolicy
	archiveSpill      pkgdurable.SpillDestination
}

func fullFailureContractWant(prefix string, base int, useMemoryPressure bool) fullFailureSnapshot {
	return fullFailureSnapshot{
		maxConcurrent:                   base + 1,
		archiveMaxSize:                  uint64(base+2) * BytesPerMegabyte,
		archiveDir:                      prefix + "-archive",
		retryableDir:                    prefix + "-retryable",
		permanentDir:                    prefix + "-permanent",
		archiveBackups:                  base + 3,
		archiveAge:                      base + 4,
		archiveMaxDirSize:               uint64(base+5) * BytesPerMegabyte,
		archiveBucket:                   prefix + "-bucket",
		archivePrefix:                   prefix + "/prefix",
		archiveOverload:                 pkgdurable.OverloadPolicyDegrade,
		archiveSpill:                    pkgdurable.SpillDestinationS3,
		spoolDir:                        prefix + "-spool",
		spoolMaxCalls:                   uint64(base + 6),
		spoolScanInterval:               time.Duration(base+7) * time.Second,
		spoolMaxDirSize:                 uint64(base+8) * BytesPerMegabyte,
		spoolActiveBacklogWarnAfterDays: base + 9,
		spoolMemoryPressure:             useMemoryPressure,
	}
}

func deliveryFailureContractWant(prefix string, base int) deliveryFailureSnapshot {
	return deliveryFailureSnapshot{
		archiveMaxSize:    uint64(base+2) * BytesPerMegabyte,
		archiveDir:        prefix + "-archive",
		retryableDir:      prefix + "-retryable",
		permanentDir:      prefix + "-permanent",
		archiveBackups:    base + 3,
		archiveAge:        base + 4,
		archiveMaxDirSize: uint64(base+5) * BytesPerMegabyte,
		archiveBucket:     prefix + "-bucket",
		archivePrefix:     prefix + "/prefix",
		archiveOverload:   pkgdurable.OverloadPolicyDrop,
		archiveSpill:      pkgdurable.SpillDestinationStdout,
	}
}

func fullFailureSnapshotFromLoaded(cfg configfailure.FailureHandlerConfig) fullFailureSnapshot {
	archive := cfg.Fsstore.FileArchiveConfig
	spool := cfg.Fsstore.SpoolConfig
	return fullFailureSnapshot{
		maxConcurrent:                   cfg.MaxConcurrent,
		archiveMaxSize:                  archive.MaxFileSize,
		archiveDir:                      archive.Dir,
		retryableDir:                    archive.DirCaseRetryable,
		permanentDir:                    archive.DirCasePermanent,
		archiveBackups:                  archive.MaxFileBackups,
		archiveAge:                      archive.MaxFileAge,
		archiveMaxDirSize:               archive.MaxDirSize,
		archiveBucket:                   archive.ObjectStorageUploadConfig.S3.Bucket,
		archivePrefix:                   archive.ObjectStorageUploadConfig.S3.Prefix,
		archiveOverload:                 archive.OverloadPolicyConfig.OverloadPolicy,
		archiveSpill:                    archive.OverloadPolicyConfig.SpillDestination,
		spoolDir:                        spool.Dir,
		spoolMaxCalls:                   spool.OverloadPolicyConfig.MaxCallsSinceScan,
		spoolScanInterval:               spool.OverloadPolicyConfig.StatsScanInterval,
		spoolMaxDirSize:                 spool.OverloadPolicyConfig.MaxDirSize,
		spoolActiveBacklogWarnAfterDays: spool.OverloadPolicyConfig.ActiveBacklogWarnAfterDays,
		spoolMemoryPressure:             spool.OverloadPolicyConfig.UseMemoryPressureForOverload,
	}
}

func deliveryFailureSnapshotFromLoaded(cfg configfailure.DeliveryFailureHandlerConfig) deliveryFailureSnapshot {
	archive := cfg.FileArchiveConfig
	return deliveryFailureSnapshot{
		archiveMaxSize:    archive.MaxFileSize,
		archiveDir:        archive.Dir,
		retryableDir:      archive.DirCaseRetryable,
		permanentDir:      archive.DirCasePermanent,
		archiveBackups:    archive.MaxFileBackups,
		archiveAge:        archive.MaxFileAge,
		archiveMaxDirSize: archive.MaxDirSize,
		archiveBucket:     archive.ObjectStorageUploadConfig.S3.Bucket,
		archivePrefix:     archive.ObjectStorageUploadConfig.S3.Prefix,
		archiveOverload:   archive.OverloadPolicyConfig.OverloadPolicy,
		archiveSpill:      archive.OverloadPolicyConfig.SpillDestination,
	}
}

func fullFailureSnapshotFromMapped(cfg pkgdurable.FailureStoreConfig) fullFailureSnapshot {
	archive := cfg.DurableStoreConfig.FileArchiveConfig
	spool := cfg.DurableStoreConfig.SpoolConfig
	return fullFailureSnapshot{
		maxConcurrent:                   cfg.MaxConcurrent,
		archiveMaxSize:                  archive.MaxFileSize,
		archiveDir:                      archive.Dir,
		retryableDir:                    archive.DirCaseRetryable,
		permanentDir:                    archive.DirCasePermanent,
		archiveBackups:                  archive.MaxFileBackups,
		archiveAge:                      archive.MaxFileAge,
		archiveMaxDirSize:               archive.MaxDirSize,
		archiveBucket:                   archive.ObjectStorageUploadConfig.S3.Bucket,
		archivePrefix:                   archive.ObjectStorageUploadConfig.S3.Prefix,
		archiveOverload:                 archive.OverloadPolicyConfig.OverloadPolicy,
		archiveSpill:                    archive.OverloadPolicyConfig.SpillDestination,
		spoolDir:                        spool.Dir,
		spoolMaxCalls:                   spool.OverloadPolicyConfig.MaxCallsSinceScan,
		spoolScanInterval:               spool.OverloadPolicyConfig.StatsScanInterval,
		spoolMaxDirSize:                 spool.OverloadPolicyConfig.MaxDirSize,
		spoolActiveBacklogWarnAfterDays: spool.OverloadPolicyConfig.ActiveBacklogWarnAfterDays,
		spoolMemoryPressure:             spool.OverloadPolicyConfig.UseMemoryPressureForOverload,
	}
}

func deliveryFailureSnapshotFromMapped(cfg pkgdurable.DeliveryFailureStoreConfig) deliveryFailureSnapshot {
	archive := cfg.FileArchiveConfig
	return deliveryFailureSnapshot{
		archiveMaxSize:    archive.MaxFileSize,
		archiveDir:        archive.Dir,
		retryableDir:      archive.DirCaseRetryable,
		permanentDir:      archive.DirCasePermanent,
		archiveBackups:    archive.MaxFileBackups,
		archiveAge:        archive.MaxFileAge,
		archiveMaxDirSize: archive.MaxDirSize,
		archiveBucket:     archive.ObjectStorageUploadConfig.S3.Bucket,
		archivePrefix:     archive.ObjectStorageUploadConfig.S3.Prefix,
		archiveOverload:   archive.OverloadPolicyConfig.OverloadPolicy,
		archiveSpill:      archive.OverloadPolicyConfig.SpillDestination,
	}
}

func assertLokiLoadedContracts(t *testing.T, cfg AppConfig, syncWant, asyncWant fullFailureSnapshot, deliveryWant deliveryFailureSnapshot) {
	t.Helper()
	assertContractValue(t, "loaded sync Loki log level", cfg.Logger.LokiConfig.LogLevel, pkglogger.LogLevelInfo)
	assertContractValue(t, "loaded sync Loki flush interval", cfg.Logger.LokiConfig.FlushInterval, 31*time.Second)
	assertContractValue(t, "loaded sync Loki max entries", cfg.Logger.LokiConfig.MaxEntries, 32)
	assertContractValue(t, "loaded sync Loki buffer flush bytes", cfg.Logger.LokiConfig.BufferFlushBytes, 3201)
	assertContractValue(t, "loaded sync Loki fallback queue max entries", cfg.Logger.LokiConfig.FallbackQueueMaxEntries, 320)
	assertContractValue(t, "loaded sync Loki max line bytes", cfg.Logger.LokiConfig.MaxLineBytes, 3301)
	assertContractValue(t, "loaded sync Loki max push request bytes", cfg.Logger.LokiConfig.MaxPushRequestBytes, 3302)
	assertContractValue(t, "loaded sync Loki handler", cfg.Logger.LokiConfig.HandlerConfig.OnErrorConfig, []string{"stdout", "slack"})
	assertContractValue(t, "loaded sync Loki endpoint", cfg.LokiClient.Endpoint, "http://loki-sync.example")
	assertContractValue(t, "loaded sync Loki tenant", cfg.LokiClient.TenantID, "loki-sync-tenant")
	assertContractValue(t, "loaded sync Loki headers", cfg.LokiClient.Headers, map[string]string{"X-Scope": "loki-sync"})
	assertContractValue(t, "loaded sync Loki retry attempts", cfg.Logger.LokiConfig.LokiRetryAbleClientConfig.MaxAttempts, 39)
	assertContractValue(t, "loaded sync Loki failure", fullFailureSnapshotFromLoaded(cfg.Logger.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig), syncWant)

	assertContractValue(t, "loaded async Loki log level", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LogLevel, pkglogger.LogLevelError)
	assertContractValue(t, "loaded async Loki flush interval", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.FlushInterval, 51*time.Second)
	assertContractValue(t, "loaded async Loki max entries", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxEntries, 52)
	assertContractValue(t, "loaded async Loki buffer flush bytes", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.BufferFlushBytes, 5201)
	assertContractValue(t, "loaded async Loki fallback queue max entries", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.FallbackQueueMaxEntries, 520)
	assertContractValue(t, "loaded async Loki max line bytes", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxLineBytes, 5301)
	assertContractValue(t, "loaded async Loki max push request bytes", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.MaxPushRequestBytes, 5302)
	assertContractValue(t, "loaded async Loki handler", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.HandlerConfig.OnErrorConfig, []string{"file", "stdout"})
	assertContractValue(t, "loaded async Loki endpoint", cfg.AsyncLokiClient.Endpoint, "http://loki-async.example")
	assertContractValue(t, "loaded async Loki tenant", cfg.AsyncLokiClient.TenantID, "loki-async-tenant")
	assertContractValue(t, "loaded async Loki headers", cfg.AsyncLokiClient.Headers, map[string]string{"X-Scope": "loki-async"})
	assertContractValue(t, "loaded async Loki retry attempts", cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiRetryAbleClientConfig.MaxAttempts, 59)
	assertContractValue(t, "loaded async Loki failure", fullFailureSnapshotFromLoaded(cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig.LokiFlushManagerConfig.FailureHandlerConfig), asyncWant)

	assertContractValue(t, "loaded delivery Loki latency", cfg.DeliveryLogger.DeliveryLokiConfig.PushLatencyEnabled, false)
	assertContractValue(t, "loaded delivery Loki max line bytes", cfg.DeliveryLogger.DeliveryLokiConfig.MaxLineBytes, 7301)
	assertContractValue(t, "loaded delivery Loki max push request bytes", cfg.DeliveryLogger.DeliveryLokiConfig.MaxPushRequestBytes, 7302)
	assertContractValue(t, "loaded delivery Loki endpoint", cfg.DeliveryLokiClient.Endpoint, "http://loki-delivery.example")
	assertContractValue(t, "loaded delivery Loki tenant", cfg.DeliveryLokiClient.TenantID, "loki-delivery-tenant")
	assertContractValue(t, "loaded delivery Loki headers", cfg.DeliveryLokiClient.Headers, map[string]string{"X-Scope": "loki-delivery"})
	assertContractValue(t, "loaded delivery Loki retry attempts", cfg.DeliveryLogger.DeliveryLokiConfig.LokiRetryAbleClientConfig.MaxAttempts, 69)
	assertContractValue(t, "loaded delivery Loki failure", deliveryFailureSnapshotFromLoaded(cfg.DeliveryLogger.DeliveryLokiConfig.DeliveryLokiFlushManagerConfig.FailureHandlerConfig), deliveryWant)
}

func assertCloudWatchLoadedContracts(t *testing.T, cfg AppConfig, syncWant, asyncWant fullFailureSnapshot, deliveryWant deliveryFailureSnapshot) {
	t.Helper()
	assertContractValue(t, "loaded sync CloudWatch log level", cfg.Logger.CloudWatchLogsConfig.LogLevel, pkglogger.LogLevelWarn)
	assertContractValue(t, "loaded sync CloudWatch flush interval", cfg.Logger.CloudWatchLogsConfig.FlushInterval, 31*time.Second)
	assertContractValue(t, "loaded sync CloudWatch buffer flush max events", cfg.Logger.CloudWatchLogsConfig.BufferFlushMaxEvents, 32)
	assertContractValue(t, "loaded sync CloudWatch buffer flush bytes", cfg.Logger.CloudWatchLogsConfig.BufferFlushBytes, 3201)
	assertContractValue(t, "loaded sync CloudWatch fallback queue max events", cfg.Logger.CloudWatchLogsConfig.FallbackQueueMaxEvents, 320)
	assertContractValue(t, "loaded sync CloudWatch handler", cfg.Logger.CloudWatchLogsConfig.HandlerConfig.OnErrorConfig, []string{"stdout", "slack"})
	assertContractValue(t, "loaded sync CloudWatch region", cfg.CloudWatchLogsClient.Region, "sync-region")
	assertContractValue(t, "loaded sync CloudWatch group", cfg.CloudWatchLogsClient.LogGroupName, "sync-group")
	assertContractValue(t, "loaded sync CloudWatch headers", cfg.CloudWatchLogsClient.Headers, map[string]string{"X-Scope": "cloudwatch-sync"})
	assertContractValue(t, "loaded sync CloudWatch retry attempts", cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxAttempts, 39)
	assertContractValue(t, "loaded sync CloudWatch failure", fullFailureSnapshotFromLoaded(cfg.Logger.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig), syncWant)

	assertContractValue(t, "loaded async CloudWatch log level", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.LogLevel, pkglogger.LogLevelError)
	assertContractValue(t, "loaded async CloudWatch flush interval", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.FlushInterval, 51*time.Second)
	assertContractValue(t, "loaded async CloudWatch buffer flush max events", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.BufferFlushMaxEvents, 52)
	assertContractValue(t, "loaded async CloudWatch buffer flush bytes", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.BufferFlushBytes, 5201)
	assertContractValue(t, "loaded async CloudWatch fallback queue max events", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.FallbackQueueMaxEvents, 520)
	assertContractValue(t, "loaded async CloudWatch handler", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.HandlerConfig.OnErrorConfig, []string{"file", "stdout"})
	assertContractValue(t, "loaded async CloudWatch region", cfg.AsyncCloudWatchLogsClient.Region, "async-region")
	assertContractValue(t, "loaded async CloudWatch group", cfg.AsyncCloudWatchLogsClient.LogGroupName, "async-group")
	assertContractValue(t, "loaded async CloudWatch headers", cfg.AsyncCloudWatchLogsClient.Headers, map[string]string{"X-Scope": "cloudwatch-async"})
	assertContractValue(t, "loaded async CloudWatch retry attempts", cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxAttempts, 59)
	assertContractValue(t, "loaded async CloudWatch failure", fullFailureSnapshotFromLoaded(cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig.CloudWatchLogsFlushManagerConfig.FailureHandlerConfig), asyncWant)

	assertContractValue(t, "loaded delivery CloudWatch latency", cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.PutLatencyEnabled, false)
	assertContractValue(t, "loaded delivery CloudWatch region", cfg.DeliveryCloudWatchLogsClient.Region, "delivery-region")
	assertContractValue(t, "loaded delivery CloudWatch group", cfg.DeliveryCloudWatchLogsClient.LogGroupName, "delivery-group")
	assertContractValue(t, "loaded delivery CloudWatch headers", cfg.DeliveryCloudWatchLogsClient.Headers, map[string]string{"X-Scope": "cloudwatch-delivery"})
	assertContractValue(t, "loaded delivery CloudWatch retry attempts", cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.CloudWatchLogsRetryAbleClientConfig.MaxAttempts, 69)
	assertContractValue(t, "loaded delivery CloudWatch failure", deliveryFailureSnapshotFromLoaded(cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig.DeliveryCloudWatchLogsFlushManagerConfig.FailureHandlerConfig), deliveryWant)
}

func assertLokiMappedContract(t *testing.T, contractLabel string, loggerCfg pkglogger.LokiLoggerBaseConfig, flushCfg pkglogger.LokiFlushManagerConfig, endpoint, tenant string, headers map[string]string, compress bool, retryCfg *pkglogger.LokiRetryableClientCaseTransportErrorConfig, failureWant fullFailureSnapshot, level pkglogger.LogLevel, flushInterval time.Duration, maxEntries, bufferFlushBytes, fallbackQueueMaxEntries, maxLineBytes, maxPushRequestBytes int, latency bool) {
	t.Helper()
	assertContractValue(t, contractLabel+" mapped Loki log level", loggerCfg.RuntimeConfig.LogLevel, level)
	assertContractValue(t, contractLabel+" mapped Loki flush interval", loggerCfg.FlushInterval, flushInterval)
	assertContractValue(t, contractLabel+" mapped Loki max entries", loggerCfg.MaxEntries, maxEntries)
	assertContractValue(t, contractLabel+" mapped Loki buffer flush bytes", loggerCfg.BufferFlushBytes, bufferFlushBytes)
	assertContractValue(t, contractLabel+" mapped Loki fallback queue max entries", loggerCfg.FallbackQueueMaxEntries, fallbackQueueMaxEntries)
	assertContractValue(t, contractLabel+" mapped Loki max line bytes", loggerCfg.MaxLineBytes, maxLineBytes)
	assertContractValue(t, contractLabel+" mapped Loki max push request bytes", loggerCfg.MaxPushRequestBytes, maxPushRequestBytes)
	assertContractValue(t, contractLabel+" mapped Loki latency", flushCfg.PushLatencyEnabled, latency)
	assertContractValue(t, contractLabel+" mapped Loki endpoint", endpoint, "http://loki-"+contractLabel+".example")
	assertContractValue(t, contractLabel+" mapped Loki tenant", tenant, "loki-"+contractLabel+"-tenant")
	assertContractValue(t, contractLabel+" mapped Loki headers", headers, map[string]string{"X-Scope": "loki-" + contractLabel})
	// 全LoggerContractKindで既定の圧縮を無効にし、各keyの分離は行ごとの読込比較で確認します。
	assertContractValue(t, contractLabel+" mapped Loki compression", compress, false)
	if retryCfg == nil {
		t.Fatalf("%s mapped Loki retry config = nil", contractLabel)
	}
	assertContractValue(t, contractLabel+" mapped Loki failure", fullFailureSnapshotFromMapped(flushCfg.FailureStoreConfig), failureWant)
}

func assertLokiDeliveryMappedContract(t *testing.T, cfg pkglogger.DeliveryLoggerConfig, failureWant deliveryFailureSnapshot) {
	t.Helper()
	assertContractValue(t, "delivery mapped Loki endpoint", cfg.LokiClient.Endpoint, "http://loki-delivery.example")
	assertContractValue(t, "delivery mapped Loki tenant", cfg.LokiClient.TenantID, "loki-delivery-tenant")
	assertContractValue(t, "delivery mapped Loki headers", cfg.LokiClient.Headers, map[string]string{"X-Scope": "loki-delivery"})
	assertContractValue(t, "delivery mapped Loki latency", cfg.Loki.PushLatencyEnabled, false)
	assertContractValue(t, "delivery mapped Loki max line bytes", cfg.Loki.MaxLineBytes, 7301)
	assertContractValue(t, "delivery mapped Loki max push request bytes", cfg.Loki.MaxPushRequestBytes, 7302)
	assertContractValue(t, "delivery mapped Loki failure", deliveryFailureSnapshotFromMapped(cfg.Loki.FailureStoreConfig), failureWant)
}

func assertCloudWatchMappedContract(t *testing.T, contractLabel string, loggerCfg pkglogger.CloudWatchLogsLoggerBaseConfig, flushCfg pkglogger.CloudWatchLogsFlushManagerConfig, region, endpoint string, headers map[string]string, autoCreate, compress bool, retryCfg *pkglogger.CloudWatchLogsRetryableClientCaseTransportErrorConfig, failureWant fullFailureSnapshot, level pkglogger.LogLevel, flushInterval time.Duration, maxEvents, bufferFlushBytes, fallbackQueueMaxEvents int, latency bool, group, stream string) {
	t.Helper()
	assertContractValue(t, contractLabel+" mapped CloudWatch log level", loggerCfg.RuntimeConfig.LogLevel, level)
	assertContractValue(t, contractLabel+" mapped CloudWatch flush interval", loggerCfg.FlushInterval, flushInterval)
	assertContractValue(t, contractLabel+" mapped CloudWatch buffer flush max events", loggerCfg.BufferFlushMaxEvents, maxEvents)
	assertContractValue(t, contractLabel+" mapped CloudWatch buffer flush bytes", loggerCfg.BufferFlushBytes, bufferFlushBytes)
	assertContractValue(t, contractLabel+" mapped CloudWatch fallback queue max events", loggerCfg.FallbackQueueMaxEvents, fallbackQueueMaxEvents)
	assertContractValue(t, contractLabel+" mapped CloudWatch latency", flushCfg.PutLatencyEnabled, latency)
	assertContractValue(t, contractLabel+" mapped CloudWatch group", flushCfg.LogGroupName, group)
	assertContractValue(t, contractLabel+" mapped CloudWatch stream", flushCfg.LogStreamName, stream)
	assertContractValue(t, contractLabel+" mapped CloudWatch region", region, contractLabel+"-region")
	assertContractValue(t, contractLabel+" mapped CloudWatch endpoint", endpoint, "http://cloudwatch-"+contractLabel+".example")
	assertContractValue(t, contractLabel+" mapped CloudWatch headers", headers, map[string]string{"X-Scope": "cloudwatch-" + contractLabel})
	// 全LoggerContractKindで既定と逆の値にし、各keyの分離は行ごとの読込比較で確認します。
	assertContractValue(t, contractLabel+" mapped CloudWatch auto create", autoCreate, true)
	assertContractValue(t, contractLabel+" mapped CloudWatch compression", compress, false)
	if retryCfg == nil {
		t.Fatalf("%s mapped CloudWatch retry config = nil", contractLabel)
	}
	assertContractValue(t, contractLabel+" mapped CloudWatch failure", fullFailureSnapshotFromMapped(flushCfg.FailureStoreConfig), failureWant)
}

func assertCloudWatchDeliveryMappedContract(t *testing.T, cfg pkglogger.DeliveryLoggerConfig, failureWant deliveryFailureSnapshot) {
	t.Helper()
	assertContractValue(t, "delivery mapped CloudWatch region", cfg.CloudWatchLogsClient.Region, "delivery-region")
	assertContractValue(t, "delivery mapped CloudWatch endpoint", cfg.CloudWatchLogsClient.Endpoint, "http://cloudwatch-delivery.example")
	assertContractValue(t, "delivery mapped CloudWatch group", cfg.CloudWatchLogs.LogGroupName, "delivery-group")
	assertContractValue(t, "delivery mapped CloudWatch stream", cfg.CloudWatchLogs.LogStreamName, "delivery-stream")
	assertContractValue(t, "delivery mapped CloudWatch latency", cfg.CloudWatchLogs.PutLatencyEnabled, false)
	assertContractValue(t, "delivery mapped CloudWatch failure", deliveryFailureSnapshotFromMapped(cfg.CloudWatchLogs.FailureStoreConfig), failureWant)
}

func assertContractValue(t *testing.T, name string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %#v, want %#v", name, got, want)
	}
}

// assertLokiLoadedEnvFieldsは、Lokiの契約試験が与えたkeyのうち、他の確認で値を比べていない
// internal logの通知先・再試行・timeout・送信方式の値が、対応するLoggerContractKindの項目に
// 入っていることを確認します。
func assertLokiLoadedEnvFields(t *testing.T, cfg AppConfig) {
	t.Helper()
	sync, async, delivery := cfg.Logger.LokiConfig, cfg.AsyncBestEffortMultiLoggerConfig.LokiConfig, cfg.DeliveryLogger.DeliveryLokiConfig
	assertContractValue(t, "loaded sync Loki handlers", sync.HandlerConfig, internalerror.RoutingConfig{
		OnErrorConfig: []string{"stdout", "slack"}, OnWarnConfig: []string{"stdout"},
		OnInfoConfig: []string{"slack"}, OnDebugConfig: []string{"file"},
	})
	assertContractValue(t, "loaded async Loki handlers", async.HandlerConfig, internalerror.RoutingConfig{
		OnErrorConfig: []string{"file", "stdout"}, OnWarnConfig: []string{"file"},
		OnInfoConfig: []string{"stdout"}, OnDebugConfig: []string{"slack"},
	})
	assertContractValue(t, "loaded sync Loki no bulk flush mode", sync.NoBulkFlushModeEnabled, true)
	assertContractValue(t, "loaded sync Loki retry", sync.LokiRetryAbleClientConfig, logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetry, MaxAttempts: 39,
		BaseDelay: 40 * time.Millisecond, MaxDelay: 41 * time.Millisecond,
	})
	assertContractValue(t, "loaded async Loki retry", async.LokiRetryAbleClientConfig, logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicyWithBreaker, MaxAttempts: 59,
		BaseDelay: 60 * time.Millisecond, MaxDelay: 61 * time.Millisecond,
	})
	assertContractValue(t, "loaded delivery Loki retry", delivery.LokiRetryAbleClientConfig, logconfig.LokiRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeCircuitBreaker, MaxAttempts: 69,
		BaseDelay: 70 * time.Millisecond, MaxDelay: 71 * time.Millisecond,
	})
	for _, client := range []struct {
		name     string
		got      lokicfg.LokiClientConfig
		timeout  time.Duration
		contract string
	}{
		{name: "sync", got: cfg.LokiClient, timeout: 38 * time.Second, contract: "sync"},
		{name: "async", got: cfg.AsyncLokiClient, timeout: 58 * time.Second, contract: "async"},
		{name: "delivery", got: cfg.DeliveryLokiClient, timeout: 68 * time.Second, contract: "delivery"},
	} {
		assertContractValue(t, "loaded "+client.name+" Loki timeout", client.got.Timeout, client.timeout)
		assertContractValue(t, "loaded "+client.name+" Loki compression", client.got.CompressRequest, false)
		assertContractValue(t, "loaded "+client.name+" Loki bearer token", client.got.BearerToken, "loki-"+client.contract+"-token")
		if client.got.BasicAuth == nil {
			t.Fatalf("loaded %s Loki basic auth = nil", client.name)
		}
		assertContractValue(t, "loaded "+client.name+" Loki basic auth", *client.got.BasicAuth, lokicfg.BasicAuthConfig{
			Username: "loki-" + client.contract + "-user", Password: "loki-" + client.contract + "-password",
		})
	}
}

// assertCloudWatchLoadedEnvFieldsは、CloudWatch Logsの契約試験が与えたkeyのうち、他の確認で
// 値を比べていないinternal logの通知先・再試行・timeout・認証情報・送信方式の値が、対応する
// LoggerContractKindの項目に入っていることを確認します。
func assertCloudWatchLoadedEnvFields(t *testing.T, cfg AppConfig) {
	t.Helper()
	sync := cfg.Logger.CloudWatchLogsConfig
	async := cfg.AsyncBestEffortMultiLoggerConfig.CloudWatchLogsConfig
	delivery := cfg.DeliveryLogger.DeliveryCloudWatchLogsConfig
	assertContractValue(t, "loaded sync CloudWatch handlers", sync.HandlerConfig, internalerror.RoutingConfig{
		OnErrorConfig: []string{"stdout", "slack"}, OnWarnConfig: []string{"stdout"},
		OnInfoConfig: []string{"slack"}, OnDebugConfig: []string{"file"},
	})
	assertContractValue(t, "loaded async CloudWatch handlers", async.HandlerConfig, internalerror.RoutingConfig{
		OnErrorConfig: []string{"file", "stdout"}, OnWarnConfig: []string{"file"},
		OnInfoConfig: []string{"stdout"}, OnDebugConfig: []string{"slack"},
	})
	assertContractValue(t, "loaded sync CloudWatch no bulk flush mode", sync.NoBulkFlushModeEnabled, true)
	assertContractValue(t, "loaded sync CloudWatch retry", sync.CloudWatchLogsRetryAbleClientConfig, logconfig.CloudWatchLogsRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetry, MaxAttempts: 39,
		BaseDelay: 40 * time.Millisecond, MaxDelay: 41 * time.Millisecond,
	})
	assertContractValue(t, "loaded async CloudWatch retry", async.CloudWatchLogsRetryAbleClientConfig, logconfig.CloudWatchLogsRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeRetryPolicyWithBreaker, MaxAttempts: 59,
		BaseDelay: 60 * time.Millisecond, MaxDelay: 61 * time.Millisecond,
	})
	assertContractValue(t, "loaded delivery CloudWatch retry", delivery.CloudWatchLogsRetryAbleClientConfig, logconfig.CloudWatchLogsRetryAbleClientConfig{
		RetryClientMode: pkglogger.RetryClientModeCircuitBreaker, MaxAttempts: 69,
		BaseDelay: 70 * time.Millisecond, MaxDelay: 71 * time.Millisecond,
	})
	for _, client := range []struct {
		name    string
		got     configcloudwatchlogs.CloudWatchLogsClientConfig
		timeout time.Duration
	}{
		{name: "sync", got: cfg.CloudWatchLogsClient, timeout: 38 * time.Second},
		{name: "async", got: cfg.AsyncCloudWatchLogsClient, timeout: 58 * time.Second},
		{name: "delivery", got: cfg.DeliveryCloudWatchLogsClient, timeout: 68 * time.Second},
	} {
		assertContractValue(t, "loaded "+client.name+" CloudWatch timeout", client.got.Timeout, client.timeout)
		assertContractValue(t, "loaded "+client.name+" CloudWatch access key", client.got.AccessKeyID, client.name+"-access")
		assertContractValue(t, "loaded "+client.name+" CloudWatch secret", client.got.SecretAccessKey, client.name+"-secret")
		assertContractValue(t, "loaded "+client.name+" CloudWatch session token", client.got.SessionToken, client.name+"-session")
		assertContractValue(t, "loaded "+client.name+" CloudWatch auto create", client.got.AutoCreateLogStream, true)
		assertContractValue(t, "loaded "+client.name+" CloudWatch compression", client.got.CompressRequest, false)
	}
}

package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestLoadDeliveryInternalLogBatchConfigMapsUsedTargetEnv(t *testing.T) {
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_FLUSH_INTERVAL_SECOND", "11")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BULK_DOCUMENTS", "12")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BUFFER_FLUSH_BYTES", "1200")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BULK_REQUEST_BYTES", "2400")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_FLUSH_INTERVAL_SECOND", "13")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_LOKI_PUSH_ENTRIES", "14")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_BUFFER_FLUSH_BYTES", "1400")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_LOKI_FALLBACK_QUEUE_ENTRIES", "140")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_FLUSH_INTERVAL_SECOND", "15")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS", "16")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_BUFFER_FLUSH_BYTES", "1600")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_FALLBACK_QUEUE_EVENTS", "160")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_SLACK_INTERVALS", "2,3")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_SLACK_LOGS_PER_NOTIFICATION", "17")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_SLACK_REST_THRESHOLD_MINUTE", "18")

	loaded, err := loadDeliveryInternalLogBatchConfig(
		processEnvironmentForTest(),
		func(pkglogger.LogOutputDestination) bool { return true },
	)
	if err != nil {
		t.Fatalf("loadDeliveryInternalLogBatchConfig() error = %v", err)
	}

	want := logconfig.DeliveryInternalLogBatchConfig{
		OpenSearch: logconfig.DeliveryInternalLogOpenSearchBatchConfig{
			FlushInterval:       11 * time.Second,
			MaxBulkDocument:     12,
			BufferFlushBytes:    1200,
			MaxBulkRequestBytes: 2400,
		},
		Loki: logconfig.DeliveryInternalLogLokiBatchConfig{
			FlushInterval:           13 * time.Second,
			MaxEntries:              14,
			BufferFlushBytes:        1400,
			FallbackQueueMaxEntries: 140,
		},
		CloudWatchLogs: logconfig.DeliveryInternalLogCloudWatchLogsBatchConfig{
			FlushInterval:          15 * time.Second,
			BufferFlushMaxEvents:   16,
			BufferFlushBytes:       1600,
			FallbackQueueMaxEvents: 160,
		},
		Slack: logconfig.DeliveryInternalLogSlackBatchConfig{
			Intervals:              []time.Duration{2 * time.Minute, 3 * time.Minute},
			MaxLogsPerNotification: 17,
			MaxRestThreshold:       18 * time.Minute,
		},
	}
	if !reflect.DeepEqual(loaded, want) {
		t.Fatalf("loaded batch config = %#v, want %#v", loaded, want)
	}

	mapped := newDeliveryInternalLogBatchConfig(loaded)
	if got := mapped.OpenSearch.MaxBulkDocument; got != 12 {
		t.Fatalf("mapped OpenSearch MaxBulkDocument = %d, want 12", got)
	}
	if got := mapped.OpenSearch.BufferFlushBytes; got != 1200 {
		t.Fatalf("mapped OpenSearch BufferFlushBytes = %d, want 1200", got)
	}
	if got := mapped.OpenSearch.MaxBulkRequestBytes; got != 2400 {
		t.Fatalf("mapped OpenSearch MaxBulkRequestBytes = %d, want 2400", got)
	}
	if got := mapped.Loki.MaxEntries; got != 14 {
		t.Fatalf("mapped Loki MaxEntries = %d, want 14", got)
	}
	if got := mapped.Loki.BufferFlushBytes; got != 1400 {
		t.Fatalf("mapped Loki BufferFlushBytes = %d, want 1400", got)
	}
	if got := mapped.Loki.FallbackQueueMaxEntries; got != 140 {
		t.Fatalf("mapped Loki FallbackQueueMaxEntries = %d, want 140", got)
	}
	if got := mapped.CloudWatchLogs.BufferFlushMaxEvents; got != 16 {
		t.Fatalf("mapped CloudWatch Logs BufferFlushMaxEvents = %d, want 16", got)
	}
	if got := mapped.CloudWatchLogs.BufferFlushBytes; got != 1600 {
		t.Fatalf("mapped CloudWatch Logs BufferFlushBytes = %d, want 1600", got)
	}
	if got := mapped.CloudWatchLogs.FallbackQueueMaxEvents; got != 160 {
		t.Fatalf("mapped CloudWatch Logs FallbackQueueMaxEvents = %d, want 160", got)
	}
	if got := mapped.Slack.Intervals; !reflect.DeepEqual(
		got,
		[]time.Duration{2 * time.Minute, 3 * time.Minute},
	) {
		t.Fatalf("mapped Slack Intervals = %v, want [2m 3m]", got)
	}

	loaded.Slack.Intervals[0] = time.Hour
	if got := mapped.Slack.Intervals[0]; got != 2*time.Minute {
		t.Fatalf("mapped Slack Intervals aliases internal config: first = %s", got)
	}
}

func TestLoadDeliveryInternalLogBatchConfigIgnoresMalformedUnusedTargets(t *testing.T) {
	for _, key := range []string{
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_FLUSH_INTERVAL_SECOND",
		"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BULK_DOCUMENTS",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BUFFER_FLUSH_BYTES",
		"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BULK_REQUEST_BYTES",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_FLUSH_INTERVAL_SECOND",
		"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_LOKI_PUSH_ENTRIES",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_LOKI_BUFFER_FLUSH_BYTES",
		"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_LOKI_FALLBACK_QUEUE_ENTRIES",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_FLUSH_INTERVAL_SECOND",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_BUFFER_FLUSH_MAX_EVENTS",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_BUFFER_FLUSH_BYTES",
		"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_CLOUDWATCH_FALLBACK_QUEUE_EVENTS",
		"HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_SLACK_INTERVALS",
		"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_SLACK_LOGS_PER_NOTIFICATION",
		"HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_SLACK_REST_THRESHOLD_MINUTE",
	} {
		t.Setenv(key, "not-valid")
	}

	loaded, err := loadDeliveryInternalLogBatchConfig(
		processEnvironmentForTest(),
		func(pkglogger.LogOutputDestination) bool { return false },
	)
	if err != nil {
		t.Fatalf("unused malformed batch env produced error: %v", err)
	}
	if got := loaded.OpenSearch.FlushInterval; got != logconfig.DefaultDeliveryInternalLogOpenSearchFlushInterval {
		t.Fatalf("unused OpenSearch FlushInterval = %s, want default", got)
	}
	if got := loaded.Loki.MaxEntries; got != logconfig.DefaultDeliveryInternalLogLokiMaxEntries {
		t.Fatalf("unused Loki MaxEntries = %d, want default", got)
	}
	if got := loaded.Loki.BufferFlushBytes; got != logconfig.DefaultDeliveryInternalLogLokiBufferFlushBytes {
		t.Fatalf("unused Loki BufferFlushBytes = %d, want default", got)
	}
	if got := loaded.Loki.FallbackQueueMaxEntries; got != logconfig.DefaultDeliveryInternalLogLokiFallbackQueueMaxEntries {
		t.Fatalf("unused Loki FallbackQueueMaxEntries = %d, want default", got)
	}
	if got := loaded.CloudWatchLogs.BufferFlushMaxEvents; got != logconfig.DefaultDeliveryInternalLogCloudWatchLogsBufferFlushMaxEvents {
		t.Fatalf("unused CloudWatch Logs BufferFlushMaxEvents = %d, want default", got)
	}
	if got := loaded.CloudWatchLogs.BufferFlushBytes; got != logconfig.DefaultDeliveryInternalLogCloudWatchLogsBufferFlushBytes {
		t.Fatalf("unused CloudWatch Logs BufferFlushBytes = %d, want default", got)
	}
	if got := loaded.CloudWatchLogs.FallbackQueueMaxEvents; got != logconfig.DefaultDeliveryInternalLogCloudWatchLogsFallbackQueueMaxEvents {
		t.Fatalf("unused CloudWatch Logs FallbackQueueMaxEvents = %d, want default", got)
	}
	if got := loaded.Slack.MaxLogsPerNotification; got != logconfig.DefaultDeliveryInternalLogSlackMaxLogsPerNotification {
		t.Fatalf("unused Slack MaxLogsPerNotification = %d, want default", got)
	}

	t.Setenv("HETEROLOG_DELIVERY_LOG_TYPE", "stdout")
	t.Setenv("HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER", "")
	t.Setenv("HETEROLOG_DELIVERY_STDOUT_LOGGER_WARN_HANDLER", "")
	t.Setenv("HETEROLOG_DELIVERY_STDOUT_LOGGER_INFO_HANDLER", "")
	t.Setenv("HETEROLOG_DELIVERY_STDOUT_LOGGER_DEBUG_HANDLER", "")
	loggerCfg, err := loadDeliveryLoggerConfig(processEnvironmentForTest())
	if err != nil {
		t.Fatalf("loadDeliveryLoggerConfig() parsed unused malformed batch env: %v", err)
	}
	if got := loggerCfg.InternalLogBatchConfig.OpenSearch.MaxBulkDocument; got != logconfig.DefaultDeliveryInternalLogOpenSearchMaxBulkDocument {
		t.Fatalf("logger unused OpenSearch MaxBulkDocument = %d, want default", got)
	}
}

func TestLoadDeliveryLoggerConfigLoadsBatchForPrimaryAlsoUsedAsFallback(t *testing.T) {
	t.Setenv("HETEROLOG_DELIVERY_LOG_TYPE", "stdout,opensearch")
	t.Setenv("HETEROLOG_DELIVERY_STDOUT_LOGGER_ERROR_HANDLER", "opensearch")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_FLUSH_INTERVAL_SECOND", "19")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BULK_DOCUMENTS", "20")
	t.Setenv("HETEROLOG_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BUFFER_FLUSH_BYTES", "2000")
	t.Setenv("HETEROLOG_MAX_DELIVERY_LOG_INTERNAL_LOG_OPENSEARCH_BULK_REQUEST_BYTES", "4000")

	cfg, err := loadDeliveryLoggerConfig(processEnvironmentForTest())
	if err != nil {
		t.Fatalf("loadDeliveryLoggerConfig() error = %v", err)
	}
	if cfg.HandlerTargetDestinations.Has(logconfig.LogOutputOpenSearch) {
		t.Fatal("primary OpenSearch must not be duplicated in HandlerTargetDestinations")
	}
	if got := cfg.InternalLogBatchConfig.OpenSearch.FlushInterval; got != 19*time.Second {
		t.Fatalf("primary/fallback OpenSearch FlushInterval = %s, want 19s", got)
	}
	if got := cfg.InternalLogBatchConfig.OpenSearch.MaxBulkDocument; got != 20 {
		t.Fatalf("primary/fallback OpenSearch MaxBulkDocument = %d, want 20", got)
	}
	if got := cfg.InternalLogBatchConfig.OpenSearch.BufferFlushBytes; got != 2000 {
		t.Fatalf("primary/fallback OpenSearch BufferFlushBytes = %d, want 2000", got)
	}
	if got := cfg.InternalLogBatchConfig.OpenSearch.MaxBulkRequestBytes; got != 4000 {
		t.Fatalf("primary/fallback OpenSearch MaxBulkRequestBytes = %d, want 4000", got)
	}
}

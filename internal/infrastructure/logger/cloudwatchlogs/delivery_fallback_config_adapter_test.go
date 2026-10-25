package cloudwatchlogs

import (
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestDeliveryCloudWatchLogsPrimaryAndFallbackAdaptersKeepSeparateBatchContracts(t *testing.T) {
	primary := NewDeliveryCloudWatchLogsLoggerConfigAdapter(
		pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig{},
	)
	if got := primary.BufferFlushMaxEvents(); got != 1 {
		t.Fatalf("delivery primary BufferFlushMaxEvents() = %d, want 1", got)
	}
	if got := primary.BufferFlushBytes(); got != 1 {
		t.Fatalf("delivery primary BufferFlushBytes() = %d, want 1", got)
	}
	if got := primary.FallbackQueueMaxEvents(); got != 1 {
		t.Fatalf("delivery primary FallbackQueueMaxEvents() = %d, want 1", got)
	}
	if !primary.NoBulkFlushModeEnabled() {
		t.Fatal("delivery primary must use no-bulk mode")
	}

	fallback := NewDeliveryCloudWatchLogsFallbackLoggerConfigAdapter(
		pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig{},
		pkglogger.DeliveryInternalLogCloudWatchLogsBatchConfig{
			FlushInterval:          9 * time.Second,
			BufferFlushMaxEvents:   11,
			BufferFlushBytes:       4096,
			FallbackQueueMaxEvents: 21,
		},
	)
	if got := fallback.FlushInterval(); got != 9*time.Second {
		t.Fatalf("delivery fallback FlushInterval() = %s, want 9s", got)
	}
	if got := fallback.BufferFlushMaxEvents(); got != 11 {
		t.Fatalf("delivery fallback BufferFlushMaxEvents() = %d, want 11", got)
	}
	if got := fallback.BufferFlushBytes(); got != 4096 {
		t.Fatalf("delivery fallback BufferFlushBytes() = %d, want 4096", got)
	}
	if got := fallback.FallbackQueueMaxEvents(); got != 21 {
		t.Fatalf("delivery fallback FallbackQueueMaxEvents() = %d, want 21", got)
	}
	if fallback.NoBulkFlushModeEnabled() {
		t.Fatal("delivery fallback must use batch mode")
	}
}

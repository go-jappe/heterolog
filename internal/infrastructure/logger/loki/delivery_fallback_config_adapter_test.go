package loki

import (
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestDeliveryLokiPrimaryAndFallbackAdaptersKeepSeparateBatchContracts(t *testing.T) {
	primary := NewDeliveryLokiLoggerConfigAdapter(pkglogger.DeliveryLokiLoggerBaseConfig{})
	if got := primary.MaxEntries(); got != 1 {
		t.Fatalf("delivery primary MaxEntries() = %d, want 1", got)
	}
	if got := primary.BufferFlushBytes(); got != 1 {
		t.Fatalf("delivery primary BufferFlushBytes() = %d, want internal sentinel 1", got)
	}
	if got := primary.FallbackQueueMaxEntries(); got != 1 {
		t.Fatalf("delivery primary FallbackQueueMaxEntries() = %d, want internal sentinel 1", got)
	}
	if !primary.NoBulkFlushModeEnabled() {
		t.Fatal("delivery primary must use no-bulk mode")
	}

	fallback := NewDeliveryLokiFallbackLoggerConfigAdapter(
		pkglogger.DeliveryLokiLoggerBaseConfig{
			MaxLineBytes:        123,
			MaxPushRequestBytes: 456,
		},
		pkglogger.DeliveryInternalLogLokiBatchConfig{
			FlushInterval:           8 * time.Second,
			MaxEntries:              10,
			BufferFlushBytes:        789,
			FallbackQueueMaxEntries: 42,
		},
	)
	if got := fallback.FlushInterval(); got != 8*time.Second {
		t.Fatalf("delivery fallback FlushInterval() = %s, want 8s", got)
	}
	if got := fallback.MaxEntries(); got != 10 {
		t.Fatalf("delivery fallback MaxEntries() = %d, want 10", got)
	}
	if got := fallback.BufferFlushBytes(); got != 789 {
		t.Fatalf("delivery fallback BufferFlushBytes() = %d, want 789", got)
	}
	if got := fallback.FallbackQueueMaxEntries(); got != 42 {
		t.Fatalf("delivery fallback FallbackQueueMaxEntries() = %d, want 42", got)
	}
	if got := fallback.MaxLineBytes(); got != 123 {
		t.Fatalf("delivery fallback MaxLineBytes() = %d, want 123", got)
	}
	if got := fallback.MaxPushRequestBytes(); got != 456 {
		t.Fatalf("delivery fallback MaxPushRequestBytes() = %d, want 456", got)
	}
	if fallback.NoBulkFlushModeEnabled() {
		t.Fatal("delivery fallback must use batch mode")
	}
}

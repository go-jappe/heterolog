package file

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

const testRecordsMetric = "heterolog_backend_records_total"

// TestFileLoggerCountsWritesWithoutPerEntrySpansは、通常の書き込みを結果ごとに1件ずつ数え、
// overloadの方針dropで捨てたentryと内部ログの通知の書き込みはrecordの数に含めず、書き込みの
// spanを作らないことを確認します。
func TestFileLoggerCountsWritesWithoutPerEntrySpans(t *testing.T) {
	ctx := context.Background()
	tracer := &observertest.Tracer{}
	meter := observertest.NewMeter()
	cfg := fileOverloadTestConfig(t, pkgfsstore.FileLifecycleModeDisable, pkgfsstore.OverloadPolicyDrop)
	logger, err := newStandaloneFileLogger(ctx, NewConfigAdapter(cfg), tracer, meter)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	spansAfterCreate := len(tracer.Names())

	free := uint64(100)
	var probeErr error
	logger.overload.freeAndTotal = func(string) (uint64, uint64, error) { return free, 1000, probeErr }
	logger.OnError = func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {}
	entry := func(message string) *logging.LogEntry {
		return &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: message}
	}

	require.NoError(t, logger.HandleLogEntry(ctx, entry("written")))
	probeErr = errors.New("probe failed")
	require.ErrorIs(t, logger.HandleLogEntry(ctx, entry("failed")), probeErr)
	probeErr = nil
	free = 99
	require.ErrorIs(t, logger.HandleLogEntry(ctx, entry("dropped")), pkglogger.ErrLogDropped)
	free = 100
	require.NoError(t, logger.writeFallbackEntry(ctx, entry("internal log")))

	require.Equal(t, map[string]int64{
		observertest.Key(testRecordsMetric, "backend", "file", "outcome", "success"): 1,
		observertest.Key(testRecordsMetric, "backend", "file", "outcome", "failure"): 1,
		observertest.Key("heterolog_file_dropped_entries_total"):                     1,
	}, meter.Counters())
	require.Equal(t, spansAfterCreate, len(tracer.Names()), "writes started spans: %v", tracer.Names())
}

// TestDeliveryFileLoggerCountsWritesWithoutPerEntrySpansは、Delivery APIのfile loggerも
// 通常の書き込みを結果ごとに数え、Close後の拒否は数えず、書き込みのspanを作らないことを
// 確認します。
func TestDeliveryFileLoggerCountsWritesWithoutPerEntrySpans(t *testing.T) {
	ctx := context.Background()
	tracer := &observertest.Tracer{}
	meter := observertest.NewMeter()
	cfg := newTestDeliveryFileLoggerConfig(t.TempDir(), "delivery_telemetry_logs", "delivery-telemetry")
	logger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), tracer, meter)
	require.NoError(t, err)
	logger.OnError = func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {}
	spansAfterCreate := len(tracer.Names())

	_, err = logger.testInfo(ctx, "written")
	require.NoError(t, err)
	// 開いているfileを閉じ、次の書き込みを失敗させます。
	require.NoError(t, logger.Fsstore.File.Close())
	_, err = logger.testInfo(ctx, "failed")
	require.Error(t, err)
	_ = closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger)
	_, err = logger.testInfo(ctx, "rejected")
	require.ErrorIs(t, err, pkglogger.ErrLoggerClosed)

	require.Equal(t, map[string]int64{
		observertest.Key(testRecordsMetric, "backend", "file", "outcome", "success"): 1,
		observertest.Key(testRecordsMetric, "backend", "file", "outcome", "failure"): 1,
	}, meter.Counters())
	require.Equal(t, spansAfterCreate, len(tracer.Names()), "writes started spans: %v", tracer.Names())
}

package file

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestFileLatencyNotificationsUseOperationThresholds(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, test := range []struct {
			name       string
			compressed bool
			elapsed    time.Duration
			level      pkglogger.LogLevel
			threshold  time.Duration
			message    string
		}{
			{name: "write debug", elapsed: 5 * time.Millisecond, level: pkglogger.LogLevelDebug, message: MsgWriteLogNoDelayed},
			{name: "write warn", elapsed: 11 * time.Millisecond, level: pkglogger.LogLevelWarn, threshold: 10 * time.Millisecond, message: MsgWriteLogDelayed},
			{name: "write error", elapsed: 21 * time.Millisecond, level: pkglogger.LogLevelError, threshold: 20 * time.Millisecond, message: ErrorMsgWriteLogDelayed},
			{name: "compress debug", compressed: true, elapsed: 50 * time.Millisecond, level: pkglogger.LogLevelDebug, message: MsgFileCompressNoDelayed},
			{name: "compress warn", compressed: true, elapsed: 101 * time.Millisecond, level: pkglogger.LogLevelWarn, threshold: 100 * time.Millisecond, message: MsgFileCompressDelayed},
			{name: "compress error", compressed: true, elapsed: 201 * time.Millisecond, level: pkglogger.LogLevelError, threshold: 200 * time.Millisecond, message: ErrorMsgFileCompressDelayed},
		} {
			t.Run(fmt.Sprintf("%s/latency_enabled=%t", test.name, enabled), func(t *testing.T) {
				cfg := newTestFileLoggerConfig(t.TempDir(), "logs", "latency")
				cfg.FsstoreConfig.Duration = pkgfsstore.DurationConfig{
					WriteFileWarnThresholdDuration:  10 * time.Millisecond,
					WriteFileErrorThresholdDuration: 20 * time.Millisecond,
					CompressWarnThresholdDuration:   100 * time.Millisecond,
					CompressErrorThresholdDuration:  200 * time.Millisecond,
					WriteFileLatencyEnabled:         enabled,
				}
				at := time.Date(2026, time.October, 2, 0, 0, 0, 0, time.UTC)
				logger := &FileLogger{config: NewConfigAdapter(cfg), Clock: clock.NewFixedClock(at)}
				var entries []*logging.LogEntry
				capture := func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					entries = append(entries, entry)
				}
				logger.OnDebug, logger.OnWarn = capture, capture
				logger.OnError = func(ctx context.Context, _ error, entry *logging.LogEntry, tracer pkgobserver.Tracer, meter pkgobserver.Meter) {
					capture(ctx, entry, tracer, meter)
				}
				const meta = "request-or-file"
				if test.compressed {
					notify := logger.compressDelayNotification(context.Background(), test.elapsed, meta)
					if len(entries) != 0 {
						t.Fatal("compression notification dispatched before callback")
					}
					logger.Clock = clock.NewFixedClock(at.Add(time.Hour))
					notify()
				} else {
					logger.handleWriteLogDelay(context.Background(), test.elapsed, meta)
				}
				if !enabled && test.level == pkglogger.LogLevelDebug {
					if len(entries) != 0 {
						t.Fatal("disabled normal latency notification was emitted")
					}
					return
				}
				if len(entries) != 1 {
					t.Fatalf("notification count = %d, want 1", len(entries))
				}
				var wantMessage string
				if test.level == pkglogger.LogLevelDebug {
					wantMessage = fmt.Sprintf(test.message, test.elapsed, meta)
				} else {
					wantMessage = fmt.Sprintf(test.message, test.elapsed, test.threshold, meta)
				}
				if entries[0].Level != test.level || entries[0].Message != wantMessage || !entries[0].OccurredAt.Equal(at) {
					t.Fatalf("notification = %+v, want level=%v message=%q time=%v", entries[0], test.level, wantMessage, at)
				}
			})
		}
	}
}

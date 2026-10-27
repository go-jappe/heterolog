package loki

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// slowLokiClientは、Pushをdelayだけ遅らせてから送信したlineを記録します。synctestの
// bubble内で使い、遅延は仮想時間で進みます。
type slowLokiClient struct {
	delay time.Duration
	mu    sync.Mutex
	lines []string
}

func (c *slowLokiClient) Push(_ context.Context, streams []pkgloki.Stream) error {
	time.Sleep(c.delay)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stream := range streams {
		for _, entry := range stream.Entries {
			c.lines = append(c.lines, entry.Line)
		}
	}
	return nil
}

// internalLines は、PlainTextFormatterが内部ログの先頭に付ける[INTERNAL]のlineを返します。
// handlerは送信しないため、送られた内部ログはすべて直接書き込みです。
func (c *slowLokiClient) internalLines() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var internal []string
	for _, line := range c.lines {
		if strings.HasPrefix(line, "[INTERNAL]") {
			internal = append(internal, line)
		}
	}
	return internal
}

func failOnUnexpectedLokiWarn(t *testing.T, base *LoggerBase) {
	t.Helper()
	base.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		t.Errorf("unexpected warn internal log notification: %s", entry.Message)
	}
}

// requireOneDirectInternalLineは、再入した最も深い書き込みのPush遅延だけが、通知されずに
// 同じ送信先へ直接書き込まれたことを確認します。
func requireOneDirectInternalLine(t *testing.T, client *slowLokiClient) {
	t.Helper()
	internal := client.internalLines()
	if len(internal) != 1 || !strings.HasPrefix(internal[0], "[INTERNAL][ERROR]") {
		t.Fatalf("directly written internal logs = %q, want one internal ERROR line", internal)
	}
}

// lokiReentryLatencyConfigは、error閾値（200ms）を超えるPushで毎回Push遅延のerrorを通知させます。
func lokiReentryLatencyConfig() pkglogger.LokiFlushManagerConfig {
	return pkglogger.LokiFlushManagerConfig{
		Resource:                   pkglogger.Resource{Cloud: &pkglogger.CloudResourceInfo{}},
		PostWarnThresholdDuration:  100 * time.Millisecond,
		PostErrorThresholdDuration: 200 * time.Millisecond,
		PushLatencyEnabled:         true,
	}
}

func TestLokiLoggerNoBulkHandlerWriteDoesNotRedispatchPushDelay(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &slowLokiClient{delay: 300 * time.Millisecond}
				logger, err := NewLokiLogger(
					context.Background(),
					newTestLokiLoggerConfig(10, time.Hour, true),
					client,
					NewFlushManagerConfigAdapter(lokiReentryLatencyConfig()),
					newTestLokiResourceProvider(),
					nil,
					observer.NopTracer{},
					observer.NopMeter{},
					&LoggerBase{},
				)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := logger.Close(context.Background()); err != nil {
						t.Errorf("Close() error = %v", err)
					}
				})
				failOnUnexpectedLokiWarn(t, logger.LoggerBase)
				probe := reentrytest.NewProbe(t)
				logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					depth, ok := probe.Enter()
					defer probe.Exit()
					if !ok {
						return
					}
					mode.Write(t, ctx, func(ctx context.Context) {
						if err := logger.HandleLogEntry(ctx, &logging.LogEntry{
							Level: pkglogger.LogLevelInfo, Message: fmt.Sprintf("write from OnError %d", depth),
						}); err != nil {
							t.Errorf("write from OnError error = %v", err)
						}
					})
				}

				if err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write",
				}); err != nil {
					t.Fatalf("original write error = %v", err)
				}

				probe.RequireCounts(t, mode, 1)
				requireOneDirectInternalLine(t, client)
			})
		})
	}
}

func TestDeliveryLokiLoggerHandlerWriteDoesNotRedispatchPushDelay(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &slowLokiClient{delay: 300 * time.Millisecond}
				latency := lokiReentryLatencyConfig()
				logger, err := NewDeliveryLokiLogger(
					context.Background(),
					NewDeliveryLokiLoggerConfigAdapter(pkglogger.DeliveryLokiLoggerBaseConfig{
						RuntimeConfig:       pkglogger.DeliveryRuntimeConfig{AppName: "delivery-reentry-test", Target: "loki"},
						LogBodyConfig:       pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
						MaxLineBytes:        256 * 1024,
						MaxPushRequestBytes: 4 * 1024 * 1024,
					}),
					client,
					NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryLokiFlushManagerConfig{
						Resource:                   latency.Resource,
						PostWarnThresholdDuration:  latency.PostWarnThresholdDuration,
						PostErrorThresholdDuration: latency.PostErrorThresholdDuration,
						PushLatencyEnabled:         latency.PushLatencyEnabled,
					}),
					newTestLokiResourceProvider(),
					nil,
					observer.NopTracer{},
					observer.NopMeter{},
					&LoggerBase{},
				)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := logger.Close(); err != nil {
						t.Errorf("Close() error = %v", err)
					}
				})
				failOnUnexpectedLokiWarn(t, logger.LoggerBase)
				probe := reentrytest.NewProbe(t)
				logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					depth, ok := probe.Enter()
					defer probe.Exit()
					if !ok {
						return
					}
					mode.Write(t, ctx, func(ctx context.Context) {
						report, err := logger.HandleLogEntry(ctx, &logging.LogEntry{
							Level: pkglogger.LogLevelInfo, Message: fmt.Sprintf("write from OnError %d", depth),
						})
						if err != nil || report.Targets[0].Status != pkglogger.DeliverySucceeded {
							t.Errorf("write from OnError report = %+v, error = %v", report, err)
						}
					})
				}

				report, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write",
				})
				if err != nil || report.Targets[0].Status != pkglogger.DeliverySucceeded {
					t.Fatalf("original write report = %+v, error = %v", report, err)
				}

				probe.RequireCounts(t, mode, 1)
				requireOneDirectInternalLine(t, client)
			})
		})
	}
}

// lineCountは、送信したlineの数を返します。
func (c *slowLokiClient) lineCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.lines)
}

// newLokiReentryLevelLoggerConfigは、log levelをlevelにした即時Push（NoBulk）の設定です。
func newLokiReentryLevelLoggerConfig(level pkglogger.LogLevel) LokiConfigProvider {
	return NewLokiLoggerConfigAdapter(pkglogger.LokiLoggerBaseConfig{
		RuntimeConfig: pkglogger.RuntimeConfig{AppName: "runtime-app", Target: "stg", LogLevel: level},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          3,
			ErrorTraceDepthDebugMode: 10,
		},
		FlushInterval:           time.Hour,
		MaxEntries:              10,
		BufferFlushBytes:        1024 * 1024,
		FallbackQueueMaxEntries: 10,
		MaxLineBytes:            256 * 1024,
		MaxPushRequestBytes:     4 * 1024 * 1024,
		NoBulkFlushModeEnabled:  true,
	})
}

// TestLokiLoggerReentrantDirectWriteAppliesLogLevelは、再入した書き込みの内部ログを直接
// 書き込む場合もloggerのlog levelを適用することを確認します。log levelがERRORのloggerで、
// Push遅延のwarningを通知するOnWarnからERRORを書き込むと、その書き込みの遅延のwarningは
// 通知されず、log level未満のため直接も送信されません。
func TestLokiLoggerReentrantDirectWriteAppliesLogLevel(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// warn閾値（100ms）だけを超えるPushで、毎回Push遅延のwarningを通知させます。
				client := &slowLokiClient{delay: 300 * time.Millisecond}
				logger, err := NewLokiLogger(
					context.Background(),
					newLokiReentryLevelLoggerConfig(pkglogger.LogLevelError),
					client,
					NewFlushManagerConfigAdapter(pkglogger.LokiFlushManagerConfig{
						Resource:                   pkglogger.Resource{Cloud: &pkglogger.CloudResourceInfo{}},
						PostWarnThresholdDuration:  100 * time.Millisecond,
						PostErrorThresholdDuration: time.Hour,
						PushLatencyEnabled:         true,
					}),
					newTestLokiResourceProvider(),
					nil,
					observer.NopTracer{},
					observer.NopMeter{},
					&LoggerBase{},
				)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := logger.Close(context.Background()); err != nil {
						t.Errorf("Close() error = %v", err)
					}
				})
				logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					t.Errorf("unexpected error internal log notification: %v", err)
				}
				probe := reentrytest.NewProbe(t)
				logger.OnWarn = func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					depth, ok := probe.Enter()
					defer probe.Exit()
					if !ok {
						return
					}
					mode.Write(t, ctx, func(ctx context.Context) {
						if err := logger.HandleLogEntry(ctx, &logging.LogEntry{
							Level: pkglogger.LogLevelError, Message: fmt.Sprintf("write from OnWarn %d", depth),
						}); err != nil {
							t.Errorf("write from OnWarn error = %v", err)
						}
					})
				}

				if err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelError, Message: "original write",
				}); err != nil {
					t.Fatalf("original write error = %v", err)
				}

				probe.RequireCounts(t, mode, 1)
				if internal := client.internalLines(); len(internal) != 0 {
					t.Fatalf("directly written internal logs = %q, want none below the log level", internal)
				}
				if got := client.lineCount(); got != 2 {
					t.Fatalf("pushed lines = %d, want the original and the nested write", got)
				}
			})
		})
	}
}

// TestLokiLoggerBulkFlushNotifiesAfterReleasingFlushLockは、件数の閾値でflushした書き込みの
// 内部ログ（clientが未初期化）をflushMuの解放後に通知し、通知の中から同じloggerへの書き込みが
// 閾値でflushしても停止しないことを確認します。flushMuを保持したまま通知すると、通知の中の
// 書き込みがflushMuを待ち続けるため、完了をhang検出の上限付きで待ちます。
func TestLokiLoggerBulkFlushNotifiesAfterReleasingFlushLock(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			reentrytest.DiscardStandardLog(t)
			logger, err := NewLokiLogger(
				context.Background(),
				newTestLokiLoggerConfig(1, time.Hour, false),
				nil,
				newTestLokiFlushManagerConfig(),
				newTestLokiResourceProvider(),
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				&LoggerBase{},
			)
			if err != nil {
				t.Fatal(err)
			}
			failOnUnexpectedLokiWarn(t, logger.LoggerBase)
			probe := reentrytest.NewProbe(t)
			var heldDuringNotification atomic.Bool
			logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				if logger.flushMu.TryLock() {
					logger.flushMu.Unlock()
				} else {
					heldDuringNotification.Store(true)
				}
				_, ok := probe.Enter()
				defer probe.Exit()
				if !ok {
					return
				}
				mode.Write(t, ctx, func(ctx context.Context) {
					// 件数の閾値に達するため、この書き込みもその場でflushします。
					_ = logger.HandleLogEntry(ctx, &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "write from OnError"})
				})
			}

			err, finished := reentrytest.CompleteWithin(t, "a write whose flush notifies OnError", func() error {
				return logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write",
				})
			})
			if !finished {
				t.Fatalf("the flush deadlocked (OnError observed flushMu held: %t)", heldDuringNotification.Load())
			}
			t.Cleanup(func() {
				if err := logger.Close(context.Background()); err != nil {
					t.Errorf("Close() error = %v", err)
				}
			})
			if !errors.Is(err, ErrClientNotInitialized) {
				t.Fatalf("flush error = %v, want %v", err, ErrClientNotInitialized)
			}
			if heldDuringNotification.Load() {
				t.Fatal("OnError must run after the flush released flushMu")
			}
			probe.RequireCounts(t, mode, 1)
		})
	}
}

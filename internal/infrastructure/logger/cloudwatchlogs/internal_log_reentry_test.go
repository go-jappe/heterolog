package cloudwatchlogs

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
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// slowCloudWatchLogsClientは、PutLogEventsをdelayだけ遅らせてから送信したeventを記録します。
// synctestのbubble内で使い、遅延は仮想時間で進みます。
type slowCloudWatchLogsClient struct {
	delay    time.Duration
	mu       sync.Mutex
	messages []string
}

func (c *slowCloudWatchLogsClient) PutLogEvents(
	_ context.Context,
	request pkgcloudwatchlogs.PutLogEventsRequest,
) (*pkgcloudwatchlogs.PutLogEventsOutput, error) {
	time.Sleep(c.delay)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, event := range request.Events {
		c.messages = append(c.messages, event.Message)
	}
	return &pkgcloudwatchlogs.PutLogEventsOutput{}, nil
}

// internalMessages は、PlainTextFormatterが内部ログの先頭に付ける[INTERNAL]のeventを返します。
// handlerは送信しないため、送られた内部ログはすべて直接書き込みです。
func (c *slowCloudWatchLogsClient) internalMessages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var internal []string
	for _, message := range c.messages {
		if strings.HasPrefix(message, "[INTERNAL]") {
			internal = append(internal, message)
		}
	}
	return internal
}

func failOnUnexpectedCloudWatchLogsWarn(t *testing.T, base *LoggerBase) {
	t.Helper()
	base.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		t.Errorf("unexpected warn internal log notification: %s", entry.Message)
	}
}

// requireOneDirectInternalEventは、再入した最も深い書き込みのPutLogEvents遅延だけが、通知されずに
// 同じlog streamへ直接書き込まれたことを確認します。
func requireOneDirectInternalEvent(t *testing.T, client *slowCloudWatchLogsClient) {
	t.Helper()
	internal := client.internalMessages()
	if len(internal) != 1 || !strings.HasPrefix(internal[0], "[INTERNAL][ERROR]") {
		t.Fatalf("directly written internal logs = %q, want one internal ERROR event", internal)
	}
}

func TestCloudWatchLogsLoggerNoBulkHandlerWriteDoesNotRedispatchPutDelay(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// error閾値（200ms）を超えるPutLogEventsで、毎回送信遅延のerrorを通知させます。
				client := &slowCloudWatchLogsClient{delay: 300 * time.Millisecond}
				logger, err := NewCloudWatchLogsLogger(
					context.Background(),
					newTestCloudWatchLogsLoggerConfig(10, time.Hour, true),
					client,
					NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
						LogGroupName:               "/app/test",
						LogStreamName:              "stream-a",
						Resource:                   pkglogger.Resource{Cloud: &pkglogger.CloudResourceInfo{}},
						PostWarnThresholdDuration:  100 * time.Millisecond,
						PostErrorThresholdDuration: 200 * time.Millisecond,
						PutLatencyEnabled:          true,
					}, streamrouting.None),
					newTestCloudWatchLogsResourceProvider(),
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
				failOnUnexpectedCloudWatchLogsWarn(t, logger.LoggerBase)
				probe := reentrytest.NewProbe(t)
				logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
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
				requireOneDirectInternalEvent(t, client)
			})
		})
	}
}

func TestDeliveryCloudWatchLogsLoggerHandlerWriteDoesNotRedispatchPutDelay(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &slowCloudWatchLogsClient{delay: 300 * time.Millisecond}
				logger, err := NewDeliveryCloudWatchLogsLogger(
					context.Background(),
					NewDeliveryCloudWatchLogsLoggerConfigAdapter(pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig{
						TimeConfig:    pkglogger.TimeConfig{TimeZone: "UTC"},
						RuntimeConfig: pkglogger.DeliveryRuntimeConfig{AppName: "delivery-reentry-test", Target: "cloudwatchlogs"},
						LogBodyConfig: pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339Nano},
					}),
					client,
					NewDeliveryFlushManagerConfigAdapter(pkglogger.DeliveryCloudWatchLogsFlushManagerConfig{
						LogGroupName:               "/app/test",
						LogStreamName:              "stream-a",
						Resource:                   pkglogger.Resource{Cloud: &pkglogger.CloudResourceInfo{}},
						PostWarnThresholdDuration:  100 * time.Millisecond,
						PostErrorThresholdDuration: 200 * time.Millisecond,
						PutLatencyEnabled:          true,
					}, streamrouting.None),
					newTestCloudWatchLogsResourceProvider(),
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
				failOnUnexpectedCloudWatchLogsWarn(t, logger.LoggerBase)
				probe := reentrytest.NewProbe(t)
				logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
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
				requireOneDirectInternalEvent(t, client)
			})
		})
	}
}

// messageCountは、送信したeventの数を返します。
func (c *slowCloudWatchLogsClient) messageCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages)
}

// newCloudWatchLogsReentryLevelLoggerConfigは、log levelをlevelにした即時送信（NoBulk）の設定です。
func newCloudWatchLogsReentryLevelLoggerConfig(level pkglogger.LogLevel) CloudWatchLogsConfigProvider {
	return NewCloudWatchLogsLoggerConfigAdapter(pkglogger.CloudWatchLogsLoggerBaseConfig{
		RuntimeConfig: pkglogger.RuntimeConfig{AppName: "runtime-app", Target: "stg", LogLevel: level},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat:        time.RFC3339Nano,
			ErrorTraceDepth:          3,
			ErrorTraceDepthDebugMode: 10,
		},
		FlushInterval:          time.Hour,
		BufferFlushMaxEvents:   10,
		BufferFlushBytes:       64 * 1024 * 1024,
		FallbackQueueMaxEvents: 10,
		NoBulkFlushModeEnabled: true,
	})
}

// TestCloudWatchLogsLoggerReentrantDirectWriteAppliesLogLevelは、再入した書き込みの内部ログを
// 直接書き込む場合もloggerのlog levelを適用することを確認します。log levelがERRORのloggerで、
// PutLogEvents遅延のwarningを通知するOnWarnからERRORを書き込むと、その書き込みの遅延の
// warningは通知されず、log level未満のため直接も送信されません。
func TestCloudWatchLogsLoggerReentrantDirectWriteAppliesLogLevel(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// warn閾値（100ms）だけを超えるPutLogEventsで、毎回送信遅延のwarningを通知させます。
				client := &slowCloudWatchLogsClient{delay: 300 * time.Millisecond}
				logger, err := NewCloudWatchLogsLogger(
					context.Background(),
					newCloudWatchLogsReentryLevelLoggerConfig(pkglogger.LogLevelError),
					client,
					NewFlushManagerConfigAdapter(pkglogger.CloudWatchLogsFlushManagerConfig{
						LogGroupName:               "/app/test",
						LogStreamName:              "stream-a",
						Resource:                   pkglogger.Resource{Cloud: &pkglogger.CloudResourceInfo{}},
						PostWarnThresholdDuration:  100 * time.Millisecond,
						PostErrorThresholdDuration: time.Hour,
						PutLatencyEnabled:          true,
					}, streamrouting.None),
					newTestCloudWatchLogsResourceProvider(),
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
				logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					t.Errorf("unexpected error internal log notification: %v", err)
				}
				probe := reentrytest.NewProbe(t)
				logger.OnWarn = func(ctx context.Context, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
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
				if internal := client.internalMessages(); len(internal) != 0 {
					t.Fatalf("directly written internal logs = %q, want none below the log level", internal)
				}
				if got := client.messageCount(); got != 2 {
					t.Fatalf("sent events = %d, want the original and the nested write", got)
				}
			})
		})
	}
}

// TestCloudWatchLogsLoggerBulkFlushNotifiesAfterReleasingFlushLockは、件数の閾値でflushした
// 書き込みの内部ログ（clientが未初期化）をflushMuの解放後に通知し、通知の中から同じloggerへの
// 書き込みが閾値でflushしても停止しないことを確認します。flushMuを保持したまま通知すると、
// 通知の中の書き込みがflushMuを待ち続けるため、完了をhang検出の上限付きで待ちます。
func TestCloudWatchLogsLoggerBulkFlushNotifiesAfterReleasingFlushLock(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			reentrytest.DiscardStandardLog(t)
			logger, err := NewCloudWatchLogsLogger(
				context.Background(),
				newTestCloudWatchLogsLoggerConfig(1, time.Hour, false),
				nil,
				newTestCloudWatchLogsFlushManagerConfig(),
				newTestCloudWatchLogsResourceProvider(),
				nil,
				observer.NopTracer{},
				observer.NopMeter{},
				&LoggerBase{},
			)
			if err != nil {
				t.Fatal(err)
			}
			failOnUnexpectedCloudWatchLogsWarn(t, logger.LoggerBase)
			probe := reentrytest.NewProbe(t)
			var heldDuringNotification atomic.Bool
			logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
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

package slack

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// slowSlackAPITransportは、Slack APIへのrequestをdelayだけ遅らせてから記録し、成功応答を
// 返します。synctestのbubble内で使い、遅延は仮想時間で進みます。
type slowSlackAPITransport struct {
	delay time.Duration
	*slackAPITransport
}

func (rt *slowSlackAPITransport) RoundTrip(req *http.Request) (*http.Response, error) {
	time.Sleep(rt.delay)
	return rt.slackAPITransport.RoundTrip(req)
}

// internalTexts は、PlainTextFormatterが内部ログの先頭に付ける[INTERNAL]を含む送信本文を返します。
// handlerは送信しないため、送られた内部ログはすべて直接送信です。
func (rt *slowSlackAPITransport) internalTexts() []string {
	var internal []string
	for _, request := range rt.snapshot() {
		if strings.HasPrefix(request.text, "[INTERNAL]") {
			internal = append(internal, request.text)
		}
	}
	return internal
}

func TestDeliverySlackLoggerHandlerWriteDoesNotRedispatchPostDelay(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// error閾値（200ms）を超えるSlack API呼び出しで、毎回送信遅延のerrorを通知させます。
				transport := &slowSlackAPITransport{delay: 300 * time.Millisecond, slackAPITransport: &slackAPITransport{}}
				cfg := deliverySlackLevelTestConfig(pkglogger.LogLevelDebug)
				cfg.Timeout = time.Minute
				cfg.MaxMessageLength = 10_000
				cfg.SlackPostWarnThresholdDuration = 100 * time.Millisecond
				cfg.SlackPostErrorThresholdDuration = 200 * time.Millisecond
				cfg.HTTPClient = &http.Client{Transport: transport}
				logger, err := NewDeliverySlackLogger(
					context.Background(), NewDeliveryConfigAdapter(cfg), observer.NopTracer{}, observer.NopMeter{},
				)
				if err != nil {
					t.Fatal(err)
				}
				logger.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					t.Errorf("unexpected warn internal log notification: %s", entry.Message)
				}
				probe := reentrytest.NewProbe(t)
				logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					depth, ok := probe.Enter()
					defer probe.Exit()
					if !ok {
						return
					}
					mode.Write(t, ctx, func(ctx context.Context) {
						report, err := logger.HandleLogEntry(ctx, &logging.LogEntry{
							Level: pkglogger.LogLevelInfo, Message: "write from OnError", RequestID: fmt.Sprintf("nested-%d", depth),
						})
						if err != nil || report.Targets[0].Status != pkglogger.DeliverySucceeded {
							t.Errorf("write from OnError report = %+v, error = %v", report, err)
						}
					})
				}

				report, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelInfo, Message: "original write", RequestID: "original",
				})
				if err != nil || report.Targets[0].Status != pkglogger.DeliverySucceeded {
					t.Fatalf("original write report = %+v, error = %v", report, err)
				}

				probe.RequireCounts(t, mode, 1)
				// 再入した最も深い送信の遅延だけを、通知せず同じSlackへ直接送信します。
				internal := transport.internalTexts()
				if len(internal) != 1 || !strings.HasPrefix(internal[0], "[INTERNAL][ERROR]") ||
					!strings.Contains(internal[0], fmt.Sprintf("request_id: nested-%d", reentrytest.HandlerNestingDepth)) {
					t.Fatalf("directly sent internal logs = %q", internal)
				}
			})
		})
	}
}

// TestSlackLoggerOnWarnWriteDoesNotRedispatchLevelWarningは、log levelより低いERRORの
// warningを通知するOnWarnから同じ条件の書き込みを繰り返しても、通知が止まることを確認します。
func TestSlackLoggerOnWarnWriteDoesNotRedispatchLevelWarning(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			logger := newSlackLevelTestLogger(t, pkglogger.LogLevelFatal)
			probe := reentrytest.NewProbe(t)
			logger.OnWarn = func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				_, ok := probe.Enter()
				defer probe.Exit()
				if !ok {
					return
				}
				mode.Write(t, ctx, func(ctx context.Context) {
					if err := logger.HandleLogEntry(ctx, &logging.LogEntry{
						Level: pkglogger.LogLevelError, Message: "error from OnWarn below the log level",
					}); err != nil {
						t.Errorf("write from OnWarn error = %v", err)
					}
				})
			}

			if err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
				Level: pkglogger.LogLevelError, Message: "error below the log level",
			}); err != nil {
				t.Fatalf("original write error = %v", err)
			}

			probe.RequireCounts(t, mode, 1)
		})
	}
}

// TestSlackLoggerOnDebugWriteFromEnqueueDoesNotDeadlockOrRedispatchは、buffer管理のdebug logを
// 通知するOnDebugから同じSlackLoggerへ書き込んでも停止せず（s.muを解放してから通知します）、
// 通知が止まることを確認します。再入した書き込みのbuffer管理のdebug logは通知せずbufferへ
// 追加し（log level DEBUG以上のため）、Closeの最終送信で同じSlackへ送ります。
func TestSlackLoggerOnDebugWriteFromEnqueueDoesNotDeadlockOrRedispatch(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			sent := runSlackOnDebugReentry(t, mode, pkglogger.LogLevelDebug, pkglogger.LogLevelInfo)
			if got := strings.Count(sent, "[INTERNAL][DEBUG]"); got != 1 {
				t.Fatalf("internal debug logs sent by Close = %d, want the nested write's buffer debug log only:\n%s", got, sent)
			}
		})
	}
}

// TestSlackLoggerReentrantBufferDebugLogAppliesLogLevelは、再入した書き込みのbuffer管理の
// debug logを直接bufferへ追加する場合もloggerのlog levelを適用することを確認します。log levelが
// ERRORのloggerでは、その書き込みのdebug logは通知も送信もされません。
func TestSlackLoggerReentrantBufferDebugLogAppliesLogLevel(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			sent := runSlackOnDebugReentry(t, mode, pkglogger.LogLevelError, pkglogger.LogLevelError)
			if got := strings.Count(sent, "[INTERNAL]"); got != 0 {
				t.Fatalf("internal logs sent by Close = %d, want none below the log level:\n%s", got, sent)
			}
		})
	}
}

// runSlackOnDebugReentryは、buffer管理のdebug logを通知するOnDebugから同じSlackLoggerへ
// writeLevelのentryを書き込み、Closeの最終送信までにSlack APIへ送られた本文を返します。
// s.muを保持したまま通知すると、通知の中の書き込みがs.muを待ち続けるため、書き込みとCloseの
// 完了をhang検出の上限付きで待ちます。元の書き込みと通知の中の書き込みは、それぞれ一度だけ
// 送られていることを確認します。
func runSlackOnDebugReentry(t *testing.T, mode reentrytest.Mode, loggerLevel, writeLevel pkglogger.LogLevel) string {
	t.Helper()
	transport := &slackAPITransport{}
	cfg := normalSlackLifecycleConfig([]time.Duration{time.Hour})
	cfg.RuntimeConfig.LogLevel = loggerLevel
	cfg.HTTPClient = &http.Client{Transport: transport}
	logger, err := NewSlackLogger(context.Background(), NewConfigAdapter(cfg), observer.NopTracer{}, observer.NopMeter{})
	if err != nil {
		t.Fatal(err)
	}
	// 最初のbatchも一時間後に予約し、Closeまではtimerの送信と通知を起こしません。
	logger.mu.Lock()
	logger.nextNotificationImmediate = false
	logger.mu.Unlock()
	probe := reentrytest.NewProbe(t)
	var mu sync.Mutex
	var nestedErrs []error
	logger.OnDebug = func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		depth, ok := probe.Enter()
		defer probe.Exit()
		if !ok {
			return
		}
		mode.Write(t, ctx, func(ctx context.Context) {
			err := logger.HandleLogEntry(ctx, &logging.LogEntry{
				Level: writeLevel, Message: fmt.Sprintf("write from OnDebug %d", depth),
			})
			mu.Lock()
			nestedErrs = append(nestedErrs, err)
			mu.Unlock()
		})
	}

	writeErr, finished := reentrytest.CompleteWithin(t, "a write whose buffer debug log notifies OnDebug", func() error {
		return logger.HandleLogEntry(context.Background(), &logging.LogEntry{Level: writeLevel, Message: "original write"})
	})
	if !finished {
		// 停止したwriteがs.muを保持したままのため、Closeは呼びません。
		t.Fatal("OnDebug was called while holding the buffer lock")
	}
	closeStarted := false
	t.Cleanup(func() {
		if !closeStarted {
			_ = logger.Close()
		}
	})
	if writeErr != nil {
		t.Fatalf("original write error = %v", writeErr)
	}
	// 元の書き込みのdebug log（timerの予約）一件ごとの通知回数です。
	probe.RequireCounts(t, mode, 1)
	mu.Lock()
	errs := append([]error(nil), nestedErrs...)
	mu.Unlock()
	for _, nestedErr := range errs {
		if nestedErr != nil {
			t.Fatalf("write from OnDebug error = %v", nestedErr)
		}
	}

	// Closeの送信が通知するdebug logは、この試験の対象ではありません。
	logger.OnDebug = nil
	closeStarted = true
	closeErr, finished := reentrytest.CompleteWithin(t, "Close after the write", logger.Close)
	if !finished {
		t.FailNow()
	}
	if closeErr != nil {
		t.Fatalf("Close() error = %v", closeErr)
	}
	var texts []string
	for _, request := range transport.snapshot() {
		texts = append(texts, request.text)
	}
	sent := strings.Join(texts, "\n")
	for _, message := range []string{"original write", fmt.Sprintf("write from OnDebug %d", reentrytest.HandlerNestingDepth)} {
		if got := strings.Count(sent, message); got != 1 {
			t.Fatalf("%q sent %d times by Close, want once:\n%s", message, got, sent)
		}
	}
	return sent
}

// TestDeliverySlackLoggerReentrantDirectSendAppliesLogLevelは、再入した送信の内部ログを直接
// 送信する場合もloggerのlog levelを適用することを確認します。log levelがERRORのloggerで、送信
// 遅延のwarningを通知するOnWarnからERRORを送信すると、その送信の遅延のwarningは通知されず、
// log level未満のため直接も送信されません。
func TestDeliverySlackLoggerReentrantDirectSendAppliesLogLevel(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// warn閾値（100ms）だけを超えるSlack API呼び出しで、毎回送信遅延のwarningを通知させます。
				transport := &slowSlackAPITransport{delay: 300 * time.Millisecond, slackAPITransport: &slackAPITransport{}}
				cfg := deliverySlackLevelTestConfig(pkglogger.LogLevelError)
				cfg.Timeout = time.Minute
				cfg.MaxMessageLength = 10_000
				cfg.SlackPostWarnThresholdDuration = 100 * time.Millisecond
				cfg.SlackPostErrorThresholdDuration = time.Hour
				cfg.HTTPClient = &http.Client{Transport: transport}
				logger, err := NewDeliverySlackLogger(
					context.Background(), NewDeliveryConfigAdapter(cfg), observer.NopTracer{}, observer.NopMeter{},
				)
				if err != nil {
					t.Fatal(err)
				}
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
						report, err := logger.HandleLogEntry(ctx, &logging.LogEntry{
							Level: pkglogger.LogLevelError, Message: "write from OnWarn", RequestID: fmt.Sprintf("nested-%d", depth),
						})
						if err != nil || report.Targets[0].Status != pkglogger.DeliverySucceeded {
							t.Errorf("write from OnWarn report = %+v, error = %v", report, err)
						}
					})
				}

				report, err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					Level: pkglogger.LogLevelError, Message: "original write", RequestID: "original",
				})
				if err != nil || report.Targets[0].Status != pkglogger.DeliverySucceeded {
					t.Fatalf("original write report = %+v, error = %v", report, err)
				}

				probe.RequireCounts(t, mode, 1)
				if internal := transport.internalTexts(); len(internal) != 0 {
					t.Fatalf("directly sent internal logs = %q, want none below the log level", internal)
				}
				if got := len(transport.snapshot()); got != 2 {
					t.Fatalf("Slack API requests = %d, want the original and the nested write", got)
				}
			})
		})
	}
}

// newSlackLevelTestLoggerは、levelより低いentryをbufferへ追加しないSlackLoggerを作ります。
// level未満のentryは送信しないため、Slack APIへは接続しません。
func newSlackLevelTestLogger(t *testing.T, level pkglogger.LogLevel) *SlackLogger {
	t.Helper()
	cfg := normalSlackLifecycleConfig([]time.Duration{time.Hour})
	cfg.RuntimeConfig.LogLevel = level
	logger, err := NewSlackLogger(context.Background(), NewConfigAdapter(cfg), observer.NopTracer{}, observer.NopMeter{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := logger.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return logger
}

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler/reentrytest"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// TestClickHouseLoggerOnWarnWriteDoesNotRedispatchLevelWarningは、log levelより低い
// stack trace要求のwarningを通知するOnWarnから、同じ条件の書き込みを繰り返しても、通知が
// 止まることを確認します。ClickHouseの通常の書き込みはbufferへの追加だけで、書き込みの中で
// 同期的に通知する内部ログはこのwarningです（flush中の通知はflush用の試験で確認します）。
func TestClickHouseLoggerOnWarnWriteDoesNotRedispatchLevelWarning(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			baseCfg := newTestClickHouseLoggerBaseConfig()
			baseCfg.RuntimeConfig.LogLevel = pkglogger.LogLevelError
			client := &recordingClickHouseClient{}
			logger := newTestClickHouseLoggerWithConfigAndOptions(t, client, baseCfg)
			probe := reentrytest.NewProbe(t)
			logger.OnWarn = func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				_, ok := probe.Enter()
				defer probe.Exit()
				if !ok {
					return
				}
				mode.Write(t, ctx, func(ctx context.Context) {
					if err := logger.HandleLogEntry(ctx, &logging.LogEntry{
						Level: pkglogger.LogLevelInfo, Message: "write from OnWarn", StackTraceRequested: true,
					}); err != nil {
						t.Errorf("write from OnWarn error = %v", err)
					}
				})
			}

			if err := logger.HandleLogEntry(context.Background(), &logging.LogEntry{
				Level: pkglogger.LogLevelInfo, Message: "below the log level", StackTraceRequested: true,
			}); err != nil {
				t.Fatalf("original write error = %v", err)
			}

			probe.RequireCounts(t, mode, 1)
			// 再入した書き込みのwarningはlog level（ERROR）未満のため、直接の書き込みでも
			// tableへINSERTしません。log level未満の書き込み自体もbufferへ追加しません。
			if got := client.batchCount(); got != 0 {
				t.Fatalf("INSERT calls = %d, want none below the log level", got)
			}
		})
	}
}

// slowClickHouseClientは、一回のINSERTをdelayだけ遅らせてから受理し、受け取ったrowを記録します。
// synctestのbubble内で使い、遅延は仮想時間で進みます。
type slowClickHouseClient struct {
	delay time.Duration
	mu    sync.Mutex
	rows  []pkgclickhouse.LogRecord
}

func (c *slowClickHouseClient) InsertLogsWithOptions(
	_ context.Context,
	records []pkgclickhouse.LogRecord,
	_ pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	time.Sleep(c.delay)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = append(c.rows, records...)
	return &pkgclickhouse.InsertResponse{StatusCode: http.StatusOK}, nil
}

// insertedRowsは、INSERTされたrowを受け取った順に返します。
func (c *slowClickHouseClient) insertedRows() []pkgclickhouse.LogRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]pkgclickhouse.LogRecord(nil), c.rows...)
}

// internalRowsは、PlainTextFormatterがlineの先頭に[INTERNAL]を付けた内部ログのrowを返します。
// handlerは書き込まないため、INSERTされた内部ログはすべて再入した書き込みの直接の書き込みです。
func (c *slowClickHouseClient) internalRows() []pkgclickhouse.LogRecord {
	var internal []pkgclickhouse.LogRecord
	for _, row := range c.insertedRows() {
		if strings.HasPrefix(row.Line, "[INTERNAL]") {
			internal = append(internal, row)
		}
	}
	return internal
}

// newClickHouseReentryLatencyLoggerは、件数の閾値（1件）で書き込みごとにflushし、INSERTの
// 遅延をwarnThreshold・errorThresholdで内部ログとして通知するloggerを作ります。
func newClickHouseReentryLatencyLogger(
	t *testing.T,
	client Client,
	level pkglogger.LogLevel,
	warnThreshold time.Duration,
	errorThreshold time.Duration,
) *ClickHouseLogger {
	t.Helper()
	baseCfg := newTestClickHouseLoggerBaseConfig()
	baseCfg.RuntimeConfig.LogLevel = level
	baseCfg.MaxEntries = 1
	logger, err := NewClickHouseLogger(
		context.Background(),
		NewClickHouseLoggerConfigAdapter(baseCfg),
		client,
		NewFlushManagerConfigAdapter(pkglogger.ClickHouseFlushManagerConfig{
			Resource:                   pkglogger.Resource{ServiceName: "test-app", Cloud: &pkglogger.CloudResourceInfo{}},
			PostWarnThresholdDuration:  warnThreshold,
			PostErrorThresholdDuration: errorThreshold,
			InsertLatencyEnabled:       true,
		}),
		newTestClickHouseResourceProvider(),
		nil,
		nil,
		nil,
		newTestClickHouseLoggerBase(),
	)
	if err != nil {
		t.Fatalf("NewClickHouseLogger() error: %v", err)
	}
	t.Cleanup(func() {
		if err := logger.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return logger
}

// TestClickHouseLoggerHandlerWriteDoesNotRedispatchInsertDelayは、INSERT遅延のerrorを通知する
// OnErrorから同じloggerへ書き込んでも通知が繰り返されず、再入した書き込みのINSERT遅延の
// 内部ログが、通知されずに同じtableへ直接INSERTされることを確認します。
func TestClickHouseLoggerHandlerWriteDoesNotRedispatchInsertDelay(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// error閾値（200ms）を超えるINSERTで、flushのたびにINSERT遅延のerrorを通知させます。
				client := &slowClickHouseClient{delay: 300 * time.Millisecond}
				logger := newClickHouseReentryLatencyLogger(t, client, pkglogger.LogLevelDebug, 100*time.Millisecond, 200*time.Millisecond)
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
				// 再入した最も深い書き込みのINSERT遅延だけを、通知せず同じtableへ直接INSERTします。
				internal := client.internalRows()
				if len(internal) != 1 || internal[0].Level != pkglogger.LogLevelError.String() ||
					!strings.HasPrefix(internal[0].Line, "[INTERNAL][ERROR]") {
					t.Fatalf("directly inserted internal rows = %+v, want one internal ERROR row", internal)
				}
			})
		})
	}
}

// TestClickHouseLoggerReentrantDirectWriteAppliesLogLevelは、再入した書き込みの内部ログを直接
// 書き込む場合もloggerのlog levelを適用することを確認します。log levelがERRORのloggerで、INSERT
// 遅延のwarningを通知するOnWarnからERRORを書き込むと、その書き込みの遅延のwarningは通知されず、
// log level未満のため直接もINSERTされません。
func TestClickHouseLoggerReentrantDirectWriteAppliesLogLevel(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// warn閾値（100ms）だけを超えるINSERTで、flushのたびにINSERT遅延のwarningを通知させます。
				client := &slowClickHouseClient{delay: 300 * time.Millisecond}
				logger := newClickHouseReentryLatencyLogger(t, client, pkglogger.LogLevelError, 100*time.Millisecond, time.Hour)
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
				if internal := client.internalRows(); len(internal) != 0 {
					t.Fatalf("directly inserted internal rows = %+v, want none below the log level", internal)
				}
				if got := len(client.insertedRows()); got != 2 {
					t.Fatalf("inserted rows = %d, want the original and the nested write", got)
				}
			})
		})
	}
}

// TestClickHouseLoggerBulkFlushNotifiesAfterReleasingFlushLockは、件数の閾値でflushした書き込みの
// 内部ログ（clientが未初期化）をflushMuの解放後に通知し、通知の中から同じloggerへの書き込みが
// 閾値でflushしても停止しないことを確認します。flushMuを保持したまま通知すると、通知の中の
// 書き込みがflushMuを待ち続けるため、完了をhang検出の上限付きで待ちます。
func TestClickHouseLoggerBulkFlushNotifiesAfterReleasingFlushLock(t *testing.T) {
	for _, mode := range reentrytest.Modes {
		t.Run(mode.Name, func(t *testing.T) {
			reentrytest.DiscardStandardLog(t)
			baseCfg := newTestClickHouseLoggerBaseConfig()
			baseCfg.MaxEntries = 1
			logger, err := NewClickHouseLogger(
				context.Background(),
				NewClickHouseLoggerConfigAdapter(baseCfg),
				nil,
				newTestClickHouseFlushManagerConfig(),
				newTestClickHouseResourceProvider(),
				nil,
				nil,
				nil,
				newTestClickHouseLoggerBase(),
			)
			if err != nil {
				t.Fatal(err)
			}
			logger.OnWarn = func(_ context.Context, entry *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				t.Errorf("unexpected warn internal log notification: %s", entry.Message)
			}
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
			closeClickHouseTestLoggerOnCleanup(t, func() error { return logger.Close(context.Background()) })
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

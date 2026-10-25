package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 通常終了は送信結果の通知も受理済み処理として待ち、通知先が使用中のmanagerを先に閉じません。
func TestClickHouseLoggerCloseWaitsForTimerErrorNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := newLifecycleClickHouseLogger(t, time.Hour)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseNotification := func() { releaseOnce.Do(func() { close(release) }) }
		// helperが登録したCloseより先に通知を解放するcleanupを登録し、失敗時も
		// bubble内のworkerをdeadlockではなく停止させます。
		t.Cleanup(releaseNotification)
		var notificationCtx context.Context
		logger.mu.Lock()
		// managerの欠落を検出した実際のOnError経路を、timerから実行します。
		logger.flushManager = nil
		logger.OnError = func(ctx context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			if err == nil {
				t.Error("missing flush manager did not produce an error notification")
			}
			notificationCtx = ctx
			close(entered)
			<-release
			if err := ctx.Err(); err != nil {
				t.Errorf("normal Close canceled an accepted notification: %v", err)
			}
		}
		logger.entries = append(logger.entries, &logging.LogEntry{Message: "timer failure"})
		logger.mu.Unlock()

		time.Sleep(time.Hour)
		<-entered
		closed := make(chan error, 1)
		go func() { closed <- logger.Close(context.Background()) }()
		synctest.Wait()
		select {
		case <-logger.stopCh:
		default:
			t.Fatal("Close did not stop future timer ticks")
		}
		select {
		case err := <-closed:
			t.Fatalf("Close returned before OnError completed: %v", err)
		default:
		}
		if err := notificationCtx.Err(); err != nil {
			t.Fatalf("notification context after normal Close = %v", err)
		}
		if logger.ShutdownComplete() {
			t.Fatal("logger declared shutdown complete while OnError was active")
		}
		releaseNotification()
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if !logger.ShutdownComplete() {
			t.Fatal("logger shutdown did not complete after OnError returned")
		}
	})
}

// 呼び出し元のgoroutineで件数の閾値flushを実行した書き込みも受理済み処理です。Closeは、その
// flushで発生した内部ログの通知（flushMuの解放後に行います）が終わるまで戻らず、通知の
// contextも止めません。
func TestClickHouseLoggerCloseWaitsForThresholdFlushErrorNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.MaxEntries = 1
		// clientが未初期化のため、閾値のflushは内部ログのerrorを通知します。
		logger, err := NewClickHouseLogger(
			context.Background(),
			NewClickHouseLoggerConfigAdapter(cfg),
			nil,
			newTestClickHouseFlushManagerConfig(),
			newTestClickHouseResourceProvider(),
			nil,
			nil,
			nil,
			newTestClickHouseLoggerBase(),
		)
		if err != nil {
			t.Fatalf("NewClickHouseLogger() error: %v", err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseNotification := func() { releaseOnce.Do(func() { close(release) }) }
		// cleanupは登録と逆順に実行するため、失敗時も先に通知を解放してからCloseします。
		t.Cleanup(func() { _ = logger.Close(context.Background()) })
		t.Cleanup(releaseNotification)
		var notificationCtx context.Context
		logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			notificationCtx = ctx
			close(entered)
			<-release
		}

		written := make(chan error, 1)
		go func() {
			written <- handleTestInfo(context.Background(), logger, "flushed by the entry threshold")
		}()
		waitClickHouseLifecycleSignal(t, entered, "the threshold flush did not notify OnError")
		closed := make(chan error, 1)
		go func() { closed <- logger.Close(context.Background()) }()
		// Closeは受付済みの書き込みの通知を待ちます。待機はbubble内で停止状態として扱われるため、
		// Closeが進める所まで進んだ後に、戻っていないことを確認できます。
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("Close returned while the threshold flush was still notifying OnError: %v", err)
		default:
		}
		if err := notificationCtx.Err(); err != nil {
			t.Fatalf("notification context while closing = %v, want active", err)
		}
		if logger.ShutdownComplete() {
			t.Fatal("logger declared shutdown complete while OnError was active")
		}

		releaseNotification()
		select {
		case err := <-written:
			if !errors.Is(err, ErrClientNotInitialized) {
				t.Fatalf("write error = %v, want %v", err, ErrClientNotInitialized)
			}
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("the threshold-flushing write did not finish after OnError returned")
		}
		waitClickHouseLifecycleError(t, closed, "Close did not finish after OnError returned")
		if !logger.ShutdownComplete() {
			t.Fatal("logger shutdown did not complete after OnError returned")
		}
	})
}

func TestClickHouseLoggerCloseWaitsForTimerFlushAndSerializesFlushManager(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := newLifecycleClickHouseLogger(t, time.Millisecond)
		manager := newLifecycleClickHouseFlushManager(true)
		// logger構築時に登録したCloseより先に実行し、失敗時もblockしたflushを解放します。
		t.Cleanup(manager.releaseFlush)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.entries = append(logger.entries, &logging.LogEntry{Message: "timer batch"})
		logger.mu.Unlock()

		waitClickHouseLifecycleSignal(t, manager.started, "timer flush did not start")

		logger.mu.Lock()
		logger.entries = append(logger.entries, &logging.LogEntry{Message: "close batch"})
		queuedBeforeClose := len(logger.entries)
		logger.mu.Unlock()

		concurrentFlushDone := make(chan error, 1)
		go func() {
			concurrentFlushDone <- logger.doFlush(context.Background())
		}()
		// Close開始前に、並行するdoFlushが実行中のtimer flushの完了を待つ所まで進めます。
		synctest.Wait()
		closeDone := make(chan error, 1)
		closeCtx, cancelClose := context.WithCancel(context.Background())
		t.Cleanup(cancelClose)
		go func() {
			closeDone <- logger.Close(closeCtx)
		}()
		// Closeは新規受付を止めて実行中のtimer flushを待ちます。待機はbubble内で停止状態として
		// 扱われるため、各goroutineが進める所まで進んだ後に、Closeが戻っていないことを確認できます。
		synctest.Wait()

		if err := handleTestInfo(context.Background(), logger, "after close started"); !errors.Is(err, applogging.ErrLoggerClosed) {
			t.Fatalf("HandleLogEntry() while closing error = %v, want ErrLoggerClosed", err)
		}
		if err := handleTestErrorWithStack(context.Background(), logger, "stack without error after close started", nil); !errors.Is(err, applogging.ErrLoggerClosed) {
			t.Fatalf("HandleLogEntry(stack, nil error) while closing error = %v, want ErrLoggerClosed", err)
		}
		if got := lifecycleClickHouseEntryCount(logger); got != queuedBeforeClose {
			t.Fatalf("entries while closing = %d, want %d", got, queuedBeforeClose)
		}

		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before in-flight timer flush completed: %v", err)
		default:
		}
		if snapshot := manager.snapshot(); snapshot.closeCalls != 0 {
			t.Fatalf("FlushManager.Close calls while Flush is active = %d, want 0", snapshot.closeCalls)
		}

		manager.releaseFlush()
		waitClickHouseLifecycleError(t, concurrentFlushDone, "concurrent doFlush did not finish")
		waitClickHouseLifecycleError(t, closeDone, "Close did not finish")

		snapshot := manager.snapshot()
		if snapshot.flushCalls != 2 {
			t.Fatalf("Flush calls = %d, want 2", snapshot.flushCalls)
		}
		if snapshot.maxActiveFlushes != 1 {
			t.Fatalf("max concurrent Flush calls = %d, want 1", snapshot.maxActiveFlushes)
		}
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileFlushActive {
			t.Fatal("FlushManager.Close overlapped an active Flush")
		}
		// FlushManager.CloseはLogger.Closeのctxの取り消しを受け取ります（同じctxである必要はありません）。
		cancelClose()
		if err := snapshot.closeContext.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("FlushManager.Close context error after canceling the Close context = %v, want %v", err, context.Canceled)
		}

		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("second Close() returned error: %v", err)
		}
		if got := manager.snapshot().closeCalls; got != 1 {
			t.Fatalf("FlushManager.Close calls after second Close = %d, want 1", got)
		}
	})
}

func TestClickHouseLoggerCloseJoinsFlushAndManagerErrorsWithoutResendingSnapshot(t *testing.T) {
	logger := newLifecycleClickHouseLogger(t, time.Hour)
	flushErr := errors.New("final flush failed")
	closeErr := errors.New("manager close failed")
	manager := newLifecycleClickHouseFlushManager(false)
	manager.flushErrors = []error{flushErr}
	manager.closeErrors = []error{closeErr, nil}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = append(logger.entries, &logging.LogEntry{Message: "failed final snapshot"})
	logger.mu.Unlock()

	if err := logger.Close(context.Background()); !errors.Is(err, flushErr) || !errors.Is(err, closeErr) {
		t.Fatalf("first Close() error = %v, want joined %v and %v", err, flushErr, closeErr)
	}
	if snapshot := manager.snapshot(); snapshot.closeCalls != 1 {
		t.Fatalf("FlushManager.Close calls after failed flush = %d, want 1", snapshot.closeCalls)
	}
	if got := lifecycleClickHouseEntryCount(logger); got != 0 {
		t.Fatalf("entries after failed final flush = %d, want 0", got)
	}

	if err := handleTestInfo(context.Background(), logger, "rejected while manager close is pending"); !errors.Is(err, applogging.ErrLoggerClosed) {
		t.Fatalf("HandleLogEntry() while closing error = %v, want ErrLoggerClosed", err)
	}
	if got := lifecycleClickHouseEntryCount(logger); got != 0 {
		t.Fatalf("entries after log while closed = %d, want 0", got)
	}

	if err := logger.Close(context.Background()); err != nil {
		t.Fatalf("second Close() returned error: %v", err)
	}
	snapshot := manager.snapshot()
	if snapshot.flushCalls != 1 {
		t.Fatalf("Flush calls after second Close = %d, want 1", snapshot.flushCalls)
	}
	if snapshot.closeCalls != 2 {
		t.Fatalf("FlushManager.Close calls after second Close = %d, want 2", snapshot.closeCalls)
	}
}

func TestClickHouseLoggerThresholdFlushReturnsErrorAndDropsBatch(t *testing.T) {
	logger := newLifecycleClickHouseLogger(t, time.Hour)
	flushErr := errors.New("durable handling failed")
	manager := newLifecycleClickHouseFlushManager(false)
	manager.flushErrors = []error{flushErr}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.lastFlushTime = time.Time{}
	logger.mu.Unlock()

	if err := handleTestInfo(context.Background(), logger, "drop failed threshold snapshot"); !errors.Is(err, flushErr) {
		t.Fatalf("HandleLogEntry() error = %v, want %v", err, flushErr)
	}
	if got := lifecycleClickHouseEntryCount(logger); got != 0 {
		t.Fatalf("entries after failed threshold flush = %d, want 0", got)
	}
	if err := logger.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := lifecycleClickHouseEntryCount(logger); got != 0 {
		t.Fatalf("entries after Close = %d, want 0", got)
	}
}

// この試験はFallbackLoggerの外側flushMuから、埋め込まれたnormal loggerの
// flushMuを取得する一方向のlock順でCloseが完了することを確認します。
func TestClickHouseFallbackCloseWaitsForEmbeddedNormalFlushWithoutDeadlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestClickHouseLoggerBaseConfig()
		cfg.FlushInterval = time.Hour
		cfg.MaxEntries = 1
		fallback := newTimerTestClickHouseFallback(t, &recordingClickHouseClient{}, cfg)
		logger := fallback.ClickHouseLogger
		manager := newLifecycleClickHouseFlushManager(true)
		// logger構築時に登録したCloseより先に実行し、失敗時もblockしたflushを解放します。
		t.Cleanup(manager.releaseFlush)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()
		enqueueDone := make(chan struct{})
		go func() {
			defer close(enqueueDone)
			fallback.enqueueProcess(
				context.Background(),
				&logging.LogEntry{Message: "fallback in-flight batch"},
			)
		}()
		waitClickHouseLifecycleSignal(t, manager.started, "fallback flush did not start")

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- fallback.Close(context.Background())
		}()
		// Closeが進める所まで進んだ後に、実行中flushの完了前に戻っていないことを検証します。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("FallbackLogger.Close returned before active flush completed: %v", err)
		default:
		}
		if snapshot := manager.snapshot(); snapshot.closeCalls != 0 {
			t.Fatalf("FlushManager.Close calls during fallback flush = %d, want 0", snapshot.closeCalls)
		}

		manager.releaseFlush()
		select {
		case <-enqueueDone:
		case <-time.After(time.Second):
			t.Fatal("fallback enqueue did not finish")
		}
		waitClickHouseLifecycleError(t, closeDone, "FallbackLogger.Close deadlocked")

		snapshot := manager.snapshot()
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileFlushActive {
			t.Fatal("FlushManager.Close overlapped fallback Flush")
		}
	})
}

func TestDeliveryClickHouseLoggerCloseWaitsForBackgroundFlushAndRejectsNewDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestDeliveryClickHouseLoggerBaseConfig()
		cfg.FlushInterval = time.Hour
		cfg.MaxEntries = 1
		logger := newTestDeliveryClickHouseLoggerWithConfig(t, &recordingClickHouseClient{}, cfg)
		manager := newLifecycleClickHouseFlushManager(true)
		// logger構築時に登録したCloseより先に実行し、失敗時もblockしたflushを解放します。
		t.Cleanup(manager.releaseFlush)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()

		report, err := handleTestDeliveryInfo(context.Background(), logger, "delivery background flush")
		if err != nil {
			t.Fatalf("Delivery HandleLogEntry() error: %v", err)
		}
		if len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliveryEnqueued {
			t.Fatalf("Delivery HandleLogEntry() report = %+v, want enqueued target", report)
		}
		waitClickHouseLifecycleSignal(t, manager.started, "delivery background flush did not start")

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- logger.Close()
		}()
		// Closeは新規受付を止め、実行中のbackground flushの完了を待ちます。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Delivery Close returned before background flush completed: %v", err)
		default:
		}
		if snapshot := manager.snapshot(); snapshot.closeCalls != 0 {
			t.Fatalf("FlushManager.Close calls during background flush = %d, want 0", snapshot.closeCalls)
		}

		rejectedReport, rejectedErr := handleTestDeliveryInfo(context.Background(), logger, "after delivery close started")
		if !errors.Is(rejectedErr, applogging.ErrLoggerClosed) {
			t.Fatalf("Delivery HandleLogEntry() while closing error = %v, want ErrLoggerClosed", rejectedErr)
		}
		if len(rejectedReport.Targets) != 1 ||
			rejectedReport.Targets[0].Status != pkglogger.DeliveryRejected {
			t.Fatalf("Delivery HandleLogEntry() while closing report = %+v, want rejected target", rejectedReport)
		}
		stackReport, stackErr := handleTestDeliveryErrorWithStack(context.Background(), logger, "stack without error after close started", nil)
		if !errors.Is(stackErr, applogging.ErrLoggerClosed) {
			t.Fatalf("Delivery HandleLogEntry(stack, nil error) while closing error = %v, want ErrLoggerClosed", stackErr)
		}
		if len(stackReport.Targets) != 1 || stackReport.Targets[0].Status != pkglogger.DeliveryRejected {
			t.Fatalf("Delivery HandleLogEntry(stack, nil error) while closing report = %+v, want rejected target", stackReport)
		}

		manager.releaseFlush()
		waitClickHouseLifecycleError(t, closeDone, "Delivery Close did not finish")

		snapshot := manager.snapshot()
		if snapshot.flushCalls != 1 {
			t.Fatalf("Flush calls = %d, want 1", snapshot.flushCalls)
		}
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileFlushActive {
			t.Fatal("FlushManager.Close overlapped delivery background Flush")
		}
		if got := lifecycleClickHouseEntryCount(logger.ClickHouseLogger); got != 0 {
			t.Fatalf("entries after Delivery Close = %d, want 0", got)
		}
	})
}

func TestDeliveryClickHouseLoggerBurstUsesSingleCoalescedWakeWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestDeliveryClickHouseLoggerBaseConfig()
		cfg.FlushInterval = time.Hour
		cfg.MaxEntries = 1
		logger := newTestDeliveryClickHouseLoggerWithConfig(t, &recordingClickHouseClient{}, cfg)
		manager := newLifecycleClickHouseFlushManager(true)
		// logger構築時に登録したCloseより先に実行し、失敗時もblockしたflushを解放します。
		t.Cleanup(manager.releaseFlush)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()

		if _, err := handleTestDeliveryInfo(context.Background(), logger, "first"); err != nil {
			t.Fatalf("enqueue first delivery entry: %v", err)
		}
		waitClickHouseLifecycleSignal(t, manager.started, "delivery worker did not start first flush")

		const burstEntries = 100
		for index := 0; index < burstEntries; index++ {
			if _, err := handleTestDeliveryInfo(context.Background(), logger, "burst"); err != nil {
				t.Fatalf("enqueue burst delivery entry %d: %v", index, err)
			}
		}
		// workerが進める所まで進んだ後も、blockしたcallbackの完了前に次のflushは始まりません。
		synctest.Wait()
		if snapshot := manager.snapshot(); snapshot.flushCalls != 1 {
			t.Fatalf("flush calls while first worker callback is blocked = %d, want 1", snapshot.flushCalls)
		}

		manager.releaseFlush()
		// 解放後、合流したwakeで残りのentryを一度だけflushします。
		synctest.Wait()
		if snapshot := manager.snapshot(); snapshot.flushCalls != 2 {
			t.Fatalf("flush calls after release = %d, want 2", snapshot.flushCalls)
		}
		if got := lifecycleClickHouseEntryCount(logger.ClickHouseLogger); got != 0 {
			t.Fatalf("entries after release = %d, want 0", got)
		}
		if snapshot := manager.snapshot(); snapshot.maxActiveFlushes != 1 {
			t.Fatalf("max concurrent delivery flushes = %d, want 1", snapshot.maxActiveFlushes)
		}
		if err := logger.Close(); err != nil {
			t.Fatalf("close Delivery ClickHouse logger: %v", err)
		}
		if snapshot := manager.snapshot(); snapshot.flushCalls != 2 {
			t.Fatalf("flush calls after close = %d, want 2", snapshot.flushCalls)
		}
	})
}

func newLifecycleClickHouseLogger(t *testing.T, flushInterval time.Duration) *ClickHouseLogger {
	t.Helper()
	cfg := newTestClickHouseLoggerBaseConfig()
	cfg.FlushInterval = flushInterval
	return newTestClickHouseLoggerWithConfigAndOptions(t, &recordingClickHouseClient{}, cfg)
}

type lifecycleClickHouseFlushManager struct {
	mu sync.Mutex

	started chan struct{}
	release chan struct{}

	blockFirst   bool
	startOnce    sync.Once
	releaseOnce  sync.Once
	flushCalls   int
	active       int
	maxActive    int
	closeCalls   int
	closeActive  bool
	closeContext context.Context
	flushErrors  []error
	closeErrors  []error
}

func newLifecycleClickHouseFlushManager(blockFirst bool) *lifecycleClickHouseFlushManager {
	return &lifecycleClickHouseFlushManager{
		started:    make(chan struct{}),
		release:    make(chan struct{}),
		blockFirst: blockFirst,
	}
}

// releaseFlushはblockした最初のflushを一度だけ解放します。試験の途中で失敗した場合も
// cleanupから呼び、loggerのCloseが解放待ちのまま止まらないようにします。
func (m *lifecycleClickHouseFlushManager) releaseFlush() {
	m.releaseOnce.Do(func() { close(m.release) })
}

func (m *lifecycleClickHouseFlushManager) Flush(context.Context, []*logging.LogEntry) error {
	m.mu.Lock()
	m.flushCalls++
	call := m.flushCalls
	m.active++
	if m.active > m.maxActive {
		m.maxActive = m.active
	}
	var err error
	if call <= len(m.flushErrors) {
		err = m.flushErrors[call-1]
	}
	block := m.blockFirst && call == 1
	m.mu.Unlock()

	if block {
		m.startOnce.Do(func() { close(m.started) })
		<-m.release
	}

	m.mu.Lock()
	m.active--
	m.mu.Unlock()
	return err
}

func (m *lifecycleClickHouseFlushManager) FlushWithNoFallback(
	ctx context.Context,
	entries []*logging.LogEntry,
) {
	_ = m.Flush(ctx, entries)
}

func (m *lifecycleClickHouseFlushManager) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCalls++
	call := m.closeCalls
	m.closeContext = ctx
	if m.active != 0 {
		m.closeActive = true
	}
	if call <= len(m.closeErrors) {
		return m.closeErrors[call-1]
	}
	return nil
}

type lifecycleClickHouseFlushManagerSnapshot struct {
	flushCalls            int
	maxActiveFlushes      int
	closeCalls            int
	closeWhileFlushActive bool
	closeContext          context.Context
}

func (m *lifecycleClickHouseFlushManager) snapshot() lifecycleClickHouseFlushManagerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return lifecycleClickHouseFlushManagerSnapshot{
		flushCalls:            m.flushCalls,
		maxActiveFlushes:      m.maxActive,
		closeCalls:            m.closeCalls,
		closeWhileFlushActive: m.closeActive,
		closeContext:          m.closeContext,
	}
}

func lifecycleClickHouseEntryCount(logger *ClickHouseLogger) int {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return len(logger.entries)
}

// lifecycleHangTimeoutは、完了するはずの処理がhangしたことを検出するための時間の上限です。
// 処理の完了は通知で待ち、この上限は失敗の検出だけに使います。
const lifecycleHangTimeout = 10 * time.Second

func waitClickHouseLifecycleSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal(message)
	}
}

func waitClickHouseLifecycleError(t *testing.T, result <-chan error, message string) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("%s: %v", message, err)
		}
	case <-time.After(lifecycleHangTimeout):
		t.Fatal(message)
	}
}

// heterolog.Newへsignal.NotifyContextやdeadline付きのctxを渡し、構築直後にcancelされた場合も、
// interval flushとCloseの最終flushは送信を続けます。interval送信のctxは構築時のctxの
// cancelとdeadlineを引き継ぎません（ctxの値の伝播は仕様に含めません）。
func TestClickHouseLoggersFlushAfterConstructorContextCanceled(t *testing.T) {
	const flushInterval = time.Second
	for _, test := range []struct {
		name string
		// newLoggerは構築時のctxで生成したloggerのlog関数とClose関数を返します。
		newLogger func(t *testing.T, ctx context.Context, client Client) (func(string) error, func() error)
	}{
		{
			name: "normal",
			newLogger: func(t *testing.T, ctx context.Context, client Client) (func(string) error, func() error) {
				cfg := newTestClickHouseLoggerBaseConfig()
				cfg.FlushInterval = flushInterval
				logger, err := NewClickHouseLogger(
					ctx,
					NewClickHouseLoggerConfigAdapter(cfg),
					client,
					newTestClickHouseFlushManagerConfig(),
					newTestClickHouseResourceProvider(),
					nil,
					nil,
					nil,
					newTestClickHouseLoggerBase(),
				)
				if err != nil {
					t.Fatalf("NewClickHouseLogger() error: %v", err)
				}
				// 途中のassertionが失敗してもbubble内のworkerを止めます。Closeは冪等です。
				t.Cleanup(func() {
					if err := logger.Close(context.Background()); err != nil {
						t.Errorf("Close() error: %v", err)
					}
				})
				return func(message string) error {
						return handleTestInfo(context.Background(), logger, message)
					}, func() error {
						return logger.Close(context.Background())
					}
			},
		},
		{
			name: "delivery",
			newLogger: func(t *testing.T, ctx context.Context, client Client) (func(string) error, func() error) {
				cfg := newTestDeliveryClickHouseLoggerBaseConfig()
				cfg.FlushInterval = flushInterval
				logger, err := NewDeliveryClickHouseLogger(
					ctx,
					NewDeliveryClickHouseLoggerConfigAdapter(cfg),
					client,
					newTestDeliveryClickHouseFlushManagerConfig(),
					newTestClickHouseResourceProvider(),
					nil,
					nil,
					nil,
					newTestClickHouseLoggerBase(),
				)
				if err != nil {
					t.Fatalf("NewDeliveryClickHouseLogger() error: %v", err)
				}
				// 途中のassertionが失敗してもbubble内のworkerを止めます。Closeは冪等です。
				t.Cleanup(func() {
					if err := logger.Close(); err != nil {
						t.Errorf("Close() error: %v", err)
					}
				})
				return func(message string) error {
					report, err := handleTestDeliveryInfo(context.Background(), logger, message)
					if err == nil && (len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliveryEnqueued) {
						return fmt.Errorf("delivery report = %+v, want one enqueued target", report)
					}
					return err
				}, logger.Close
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &contextRecordingClickHouseClient{}
				constructorCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
				client.constructorDeadline, _ = constructorCtx.Deadline()
				logMessage, closeLogger := test.newLogger(t, constructorCtx, client)
				cancel()

				if err := logMessage("sent by interval"); err != nil {
					t.Fatalf("log after constructor cancel: %v", err)
				}
				synctest.Wait()
				client.requireSends(t)
				time.Sleep(flushInterval)
				synctest.Wait()
				// interval送信のctxは構築時のcancelとdeadlineを引き継ぎません。
				client.requireSends(t, clickHouseExpectedSend{message: "sent by interval"})

				if err := logMessage("sent by Close"); err != nil {
					t.Fatalf("second log after constructor cancel: %v", err)
				}
				if err := closeLogger(); err != nil {
					t.Fatalf("Close() error: %v", err)
				}
				// Closeの最終flushはCloseのctxで送信します。
				client.requireSends(t,
					clickHouseExpectedSend{message: "sent by interval"},
					clickHouseExpectedSend{message: "sent by Close"},
				)
			})
		})
	}
}

// contextRecordingClickHouseClientは送信時点のctxの終了状態と値を、送信recordと一緒に記録します。
type contextRecordingClickHouseClient struct {
	mu           sync.Mutex
	observations []clickHouseSendContextObservation
	// constructorDeadlineは、loggerの構築に渡したctxの期限です。
	constructorDeadline time.Time
}

type clickHouseSendContextObservation struct {
	err      error
	deadline time.Time
	message  string
}

type clickHouseExpectedSend struct {
	message string
}

func (c *contextRecordingClickHouseClient) InsertLogsWithOptions(
	ctx context.Context,
	records []pkgclickhouse.LogRecord,
	_ pkgclickhouse.InsertOptions,
) (*pkgclickhouse.InsertResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range records {
		c.observations = append(c.observations, clickHouseSendContextObservation{
			err:      ctx.Err(),
			deadline: contextDeadline(ctx),
			message:  record.Message,
		})
	}
	return &pkgclickhouse.InsertResponse{StatusCode: 200}, nil
}

func (c *contextRecordingClickHouseClient) requireSends(t *testing.T, want ...clickHouseExpectedSend) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.observations) != len(want) {
		t.Fatalf("ClickHouse sends = %+v, want %+v", c.observations, want)
	}
	for index, observation := range c.observations {
		if observation.message != want[index].message {
			t.Fatalf("send[%d] message = %q, want %q", index, observation.message, want[index].message)
		}
		if observation.err != nil {
			t.Fatalf("send[%d] context error = %v, want an active context", index, observation.err)
		}
		// 送信側が独自の期限を付けることは妨げず、構築時のctxの期限を引き継がないことを確認します。
		if c.constructorDeadline.IsZero() {
			t.Fatal("test setup: the constructor deadline was not recorded")
		}
		if observation.deadline.Equal(c.constructorDeadline) {
			t.Fatalf("send[%d] context deadline = %s, want the constructor deadline not inherited", index, observation.deadline)
		}
	}
}

// 送信やdurable処理が失敗したflush（overload policyのdropなど）も一回の試行として
// interval判定の基準時刻を進め、失敗が続く間もFlushInterval内のlog呼び出しごとに
// 小さなflushを繰り返さないことを確認します。次のintervalでは通常どおりflushします。
func TestClickHouseLoggerFailedFlushStillAdvancesFlushInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const flushInterval = time.Minute
		// helperが登録するcleanupでCloseし、試験中に開始したworkerを停止します。
		logger := newLifecycleClickHouseLogger(t, flushInterval)
		manager := newLifecycleClickHouseFlushManager(false)
		dropErr := fmt.Errorf("clickhouse failure store overloaded: %w", pkglogger.ErrLogDropped)
		manager.flushErrors = []error{dropErr, dropErr}
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()
		ctx := context.Background()

		if err := handleTestInfo(ctx, logger, "dropped by interval flush"); err != nil {
			t.Fatalf("log before interval error = %v, want buffered", err)
		}
		time.Sleep(flushInterval)
		synctest.Wait()
		if got := manager.snapshot().flushCalls; got != 1 {
			t.Fatalf("flush calls after first interval = %d, want one failed flush", got)
		}

		for _, message := range []string{"buffered after failed flush", "still within interval"} {
			if err := handleTestInfo(ctx, logger, message); err != nil {
				t.Fatalf("log after failed flush error = %v, want buffered until next interval", err)
			}
		}
		time.Sleep(flushInterval - time.Nanosecond)
		synctest.Wait()
		if err := handleTestInfo(ctx, logger, "just before next interval"); err != nil {
			t.Fatalf("log just before next interval error = %v, want buffered", err)
		}
		if got := manager.snapshot().flushCalls; got != 1 {
			t.Fatalf("flush calls within interval after failed flush = %d, want 1", got)
		}
		if got := lifecycleClickHouseEntryCount(logger); got != 3 {
			t.Fatalf("buffered entries within interval = %d, want 3", got)
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := manager.snapshot().flushCalls; got != 2 {
			t.Fatalf("flush calls after next interval = %d, want 2", got)
		}
		if got := lifecycleClickHouseEntryCount(logger); got != 0 {
			t.Fatalf("buffered entries after next interval = %d, want 0", got)
		}
	})
}

// contextDeadlineはctxの期限を返します。期限がなければzero値です。
func contextDeadline(ctx context.Context) time.Time {
	deadline, _ := ctx.Deadline()
	return deadline
}

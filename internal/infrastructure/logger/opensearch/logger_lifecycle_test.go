package opensearch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

// 通常終了は送信結果の通知も受理済み処理として待ち、通知先が使用中のmanagerを先に閉じません。
func TestOpenSearchLoggerCloseWaitsForTimerErrorNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.flushInterval = time.Hour
		logger, _ := newTestLogger(t, cfg, nil)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseNotification := func() { releaseOnce.Do(func() { close(release) }) }
		// cleanupは登録と逆順に実行するため、失敗時も先に通知を解放してからCloseし、
		// bubble内のworkerをdeadlockではなく停止させます。
		t.Cleanup(func() {
			if err := logger.Close(context.Background()); err != nil {
				t.Errorf("Close() error: %v", err)
			}
		})
		t.Cleanup(releaseNotification)
		var notificationCtx context.Context
		logger.mu.Lock()
		// managerの欠落を検出した実際のOnError経路を、timerから実行します。
		logger.flushManager = nil
		logger.OnError = func(ctx context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
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

		logger.startBatchFlushWorker(context.Background(), nil, logger.flush)
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
func TestOpenSearchLoggerCloseWaitsForThresholdFlushErrorNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.maxBulkDocument = 1
		cfg.flushInterval = time.Hour
		// clientが未初期化のため、閾値のflushは内部ログのerrorを通知します。
		logger, err := NewOpenSearchLogger(
			context.Background(), cfg, nil, cfg, NewMockResourceProvider(), nil,
			observer.NopTracer{}, observer.NopMeter{}, nil,
		)
		if err != nil {
			t.Fatalf("NewOpenSearchLogger() error: %v", err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseNotification := func() { releaseOnce.Do(func() { close(release) }) }
		// cleanupは登録と逆順に実行するため、失敗時も先に通知を解放してからCloseします。
		t.Cleanup(func() { _ = logger.Close(context.Background()) })
		t.Cleanup(releaseNotification)
		var notificationCtx context.Context
		logger.OnError = func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
			notificationCtx = ctx
			close(entered)
			<-release
		}

		written := make(chan error, 1)
		go func() {
			written <- logger.testInfo(context.Background(), "flushed by the entry threshold")
		}()
		waitLifecycleSignal(t, entered, "the threshold flush did not notify OnError")
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
		waitLifecycleError(t, closed, "Close did not finish after OnError returned")
		if !logger.ShutdownComplete() {
			t.Fatal("logger shutdown did not complete after OnError returned")
		}
	})
}

func TestOpenSearchLoggerCloseStopsFutureTimerCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.flushInterval = time.Second
		logger, _ := newTestLogger(t, cfg, nil)
		calls := 0
		logger.startBatchFlushWorker(context.Background(), nil, func(context.Context) { calls++ })
		// 途中のassertionが失敗してもbubble内のworkerを止めます。Closeは冪等です。
		t.Cleanup(func() {
			if err := logger.Close(context.Background()); err != nil {
				t.Errorf("Close() error: %v", err)
			}
		})
		time.Sleep(cfg.flushInterval)
		synctest.Wait()
		if calls != 1 {
			t.Fatalf("timer callback calls = %d, want 1", calls)
		}
		if err := logger.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * cfg.flushInterval)
		synctest.Wait()
		if calls != 1 {
			t.Fatalf("timer callback calls after Close = %d, want no further callbacks", calls)
		}
	})
}

func TestOpenSearchLoggerCloseWaitsForTimerFlushAndSerializesFlushManager(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.flushInterval = time.Millisecond
		cfg.maxBulkDocument = 100
		logger, _ := newTestLogger(t, cfg, nil)

		manager := newLifecycleOpenSearchFlushManager(true)
		// cleanupは登録と逆順に実行するため、失敗時も先にflushを解放してからCloseし、
		// bubble内のworkerをdeadlockではなく停止させます。Closeは冪等です。
		t.Cleanup(func() { _ = logger.Close(context.Background()) })
		t.Cleanup(manager.releaseFlush)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.entries = append(logger.entries, &logging.LogEntry{Message: "timer batch"})
		logger.mu.Unlock()
		logger.startBatchFlushWorker(context.Background(), nil, logger.flush)

		waitLifecycleSignal(t, manager.started, "timer flush did not start")

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
		// Closeは新規受付を止めて実行中のtimer flushを待ち、並行するdoFlushはflush lockを
		// 待ちます。いずれの待機もbubble内で停止状態として扱われるため、各goroutineが進める
		// 所まで進んだ後に、Closeが戻っていないことを確認できます。
		synctest.Wait()

		if err := logger.testInfo(context.Background(), "after close started"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("Info() while closing error = %v, want ErrLoggerClosed", err)
		}
		// DebugはLogLevelInfo設定では通常filterされるが、Close後はclosed errorを優先する。
		if err := logger.testDebug(context.Background(), "filtered after close started"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("filtered Debug() while closing error = %v, want ErrLoggerClosed", err)
		}
		if err := logger.testErrorWithStack(context.Background(), "stack without error after close started", nil); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("ErrorWithStack(nil) while closing error = %v, want ErrLoggerClosed", err)
		}
		if got := entryCount(logger); got != queuedBeforeClose {
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
		waitLifecycleError(t, concurrentFlushDone, "concurrent doFlush did not finish")
		waitLifecycleError(t, closeDone, "Close did not finish")

		snapshot := manager.snapshot()
		if snapshot.flushCalls != 2 {
			t.Fatalf("Flush calls = %d, want 2", snapshot.flushCalls)
		}
		if snapshot.maxActiveOperations != 1 {
			t.Fatalf("max concurrent Flush calls = %d, want 1", snapshot.maxActiveOperations)
		}
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileOperationActive {
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

func TestOpenSearchLoggerCloseJoinsFlushAndManagerErrorsWithoutResendingSnapshot(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.flushInterval = time.Hour
	logger, _ := newTestLogger(t, cfg, nil)

	flushErr := errors.New("final flush failed")
	closeErr := errors.New("manager close failed")
	manager := newLifecycleOpenSearchFlushManager(false)
	manager.flushErrors = []error{flushErr}
	manager.closeErrors = []error{closeErr, nil}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.entries = append(logger.entries, &logging.LogEntry{Message: "failed final snapshot"})
	logger.mu.Unlock()
	logger.startBatchFlushWorker(context.Background(), nil, logger.flush)

	if err := logger.Close(context.Background()); !errors.Is(err, flushErr) || !errors.Is(err, closeErr) {
		t.Fatalf("first Close() error = %v, want joined %v and %v", err, flushErr, closeErr)
	}
	if snapshot := manager.snapshot(); snapshot.closeCalls != 1 {
		t.Fatalf("FlushManager.Close calls after failed flush = %d, want 1", snapshot.closeCalls)
	}
	if got := entryCount(logger); got != 0 {
		t.Fatalf("entries after failed final flush = %d, want 0", got)
	}

	if err := logger.testInfo(context.Background(), "rejected while manager close is pending"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
		t.Fatalf("Info() while closing error = %v, want ErrLoggerClosed", err)
	}
	if got := entryCount(logger); got != 0 {
		t.Fatalf("entries after log while closing = %d, want 0", got)
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

func TestOpenSearchLoggerThresholdFlushReturnsErrorAndDropsSnapshot(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 1
	cfg.flushInterval = time.Hour
	logger, _ := newTestLogger(t, cfg, nil)
	flushErr := errors.New("durable handling failed")
	manager := newLifecycleOpenSearchFlushManager(false)
	manager.flushErrors = []error{flushErr}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.mu.Unlock()

	if err := logger.testInfo(context.Background(), "drop failed threshold snapshot"); !errors.Is(err, flushErr) {
		t.Fatalf("Info() error = %v, want %v", err, flushErr)
	}
	if got := entryCount(logger); got != 0 {
		t.Fatalf("entries after failed threshold flush = %d, want 0", got)
	}
	if snapshot := manager.snapshot(); snapshot.flushCalls != 1 {
		t.Fatalf("Flush calls after failed threshold flush = %d, want 1", snapshot.flushCalls)
	}
}

func TestOpenSearchLoggerMissingManagerDropsDetachedSnapshot(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.maxBulkDocument = 1
	cfg.flushInterval = time.Hour
	logger, _ := newTestLogger(t, cfg, nil)
	logger.mu.Lock()
	logger.flushManager = nil
	logger.mu.Unlock()

	if err := logger.testInfo(context.Background(), "drop snapshot without manager"); err == nil {
		t.Fatal("Info() without flush manager error = nil, want initialization error")
	}
	if got := entryCount(logger); got != 0 {
		t.Fatalf("entries after missing-manager flush = %d, want 0", got)
	}
}

func TestOpenSearchLoggerCloseWaitsForImmediateIndex(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.noBulkFlushModeEnabled = true
		logger, _ := newTestLogger(t, cfg, nil)
		manager := newLifecycleOpenSearchFlushManager(false)
		manager.blockImmediate = true
		// cleanupは登録と逆順に実行するため、失敗時も先にIndexを解放してからCloseします。
		t.Cleanup(func() { _ = logger.Close(context.Background()) })
		t.Cleanup(manager.releaseImmediate)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()

		indexDone := make(chan error, 1)
		go func() {
			indexDone <- logger.testInfo(context.Background(), "in-flight immediate index")
		}()
		waitLifecycleSignal(t, manager.immediateStarted, "immediate index did not start")

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- logger.Close(context.Background())
		}()
		// Closeは新規受付を止めた後、受付済みの即時Indexの完了を待ちます。
		synctest.Wait()

		afterCloseStartDone := make(chan error, 1)
		go func() {
			afterCloseStartDone <- logger.testInfo(context.Background(), "ignored after close started")
		}()
		waitOpenSearchLoggerClosedError(t, afterCloseStartDone, "Info while closing did not return promptly")
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before immediate index completed: %v", err)
		default:
		}
		if got := manager.snapshot().closeCalls; got != 0 {
			t.Fatalf("FlushManager.Close calls while Index is active = %d, want 0", got)
		}

		manager.releaseImmediate()
		waitLifecycleError(t, indexDone, "immediate index did not finish")
		waitLifecycleError(t, closeDone, "Close did not finish after immediate index")

		snapshot := manager.snapshot()
		if snapshot.immediateCalls != 1 {
			t.Fatalf("Index calls including call after Close started = %d, want 1", snapshot.immediateCalls)
		}
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileOperationActive {
			t.Fatal("FlushManager.Close overlapped immediate Index")
		}
	})
}

func TestOpenSearchLoggerImmediateIndexesRunConcurrently(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	cfg.noBulkFlushModeEnabled = true
	logger, _ := newTestLogger(t, cfg, nil)
	manager := newLifecycleOpenSearchFlushManager(false)
	manager.blockImmediate = true
	t.Cleanup(manager.releaseImmediate)
	logger.mu.Lock()
	logger.flushManager = manager
	logger.mu.Unlock()

	indexDone := make(chan error, 2)
	go func() {
		indexDone <- logger.testInfo(context.Background(), "first immediate index")
	}()
	waitOpenSearchImmediateCallsStarted(t, manager, 1)

	go func() {
		indexDone <- logger.testInfo(context.Background(), "second immediate index")
	}()
	// 一件目を解放する前に二件目もbackendへ渡されることを、開始の通知で確認します。
	waitOpenSearchImmediateCallsStarted(t, manager, 1)

	if got := manager.snapshot().maxActiveOperations; got != 2 {
		t.Fatalf("max concurrent immediate Index calls = %d, want 2", got)
	}

	manager.releaseImmediate()
	waitLifecycleError(t, indexDone, "first immediate index did not finish")
	waitLifecycleError(t, indexDone, "second immediate index did not finish")
}

func TestOpenSearchFallbackLoggerCloseWaitsForInFlightBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.maxBulkDocument = 1
		cfg.flushInterval = time.Hour
		baseLogger, _ := newTestLogger(t, cfg, nil)
		logger := &FallbackLogger{OpenSearchLogger: baseLogger}
		manager := newLifecycleOpenSearchFlushManager(false)
		manager.blockImmediate = true
		// cleanupは登録と逆順に実行するため、失敗時も先にbatchを解放してからCloseします。
		t.Cleanup(func() { _ = logger.Close(context.Background()) })
		t.Cleanup(manager.releaseImmediate)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()
		logger.startFallbackWorker()

		logger.enqueueProcess(&logging.LogEntry{OccurredAt: logger.Clock.Now(), Message: "in-flight fallback batch"})
		waitLifecycleSignal(t, manager.immediateStarted, "fallback batch did not start")

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- logger.Close(context.Background())
		}()
		// Closeは新規受付を止め、実行中のfallback batchの完了を待ちます。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the fallback batch completed: %v", err)
		default:
		}
		if snapshot := manager.snapshot(); snapshot.closeCalls != 0 {
			t.Fatalf("FlushManager.Close calls during fallback batch = %d, want 0", snapshot.closeCalls)
		}

		enqueueWhileClosingDone := make(chan struct{})
		go func() {
			logger.enqueueProcess(&logging.LogEntry{OccurredAt: logger.Clock.Now(), Message: "ignored after close started"})
			close(enqueueWhileClosingDone)
		}()
		waitLifecycleSignal(t, enqueueWhileClosingDone, "fallback enqueue while closing did not return")
		synctest.Wait()
		if got := manager.snapshot().immediateCalls; got != 1 {
			t.Fatalf("fallback batch calls while closing = %d, want 1", got)
		}

		manager.releaseImmediate()
		waitLifecycleError(t, closeDone, "Close did not finish after fallback batch")

		snapshot := manager.snapshot()
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileOperationActive {
			t.Fatal("FlushManager.Close overlapped fallback batch")
		}
	})
}

func TestDeliveryOpenSearchLoggerCloseWaitsForInFlightIndex(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		indexer := &lifecycleDeliveryOpenSearchIndexer{
			started: make(chan struct{}),
			release: make(chan struct{}),
		}
		// 失敗時もbubble内のgoroutineを停止させるため、待機中のindexを解放します。
		t.Cleanup(indexer.releaseIndex)
		logger := &DeliveryOpenSearchLogger{
			LoggerBase: &LoggerBase{Clock: fixedOpenSearchOptionClock{t: time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)}},
			indexer:    indexer,
		}

		type deliveryResult struct {
			report pkglogger.DeliveryReport
			err    error
		}
		indexDone := make(chan deliveryResult, 1)
		go func() {
			report, err := logger.testInfo(context.Background(), "in-flight delivery index")
			indexDone <- deliveryResult{report: report, err: err}
		}()
		waitLifecycleSignal(t, indexer.started, "delivery index did not start")

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- logger.Close()
		}()
		// Closeは新規受付を止めた後、受付済みのindexの完了を待ちます。その待機はbubble内で
		// 停止状態として扱われるため、Closeが進める所まで進んだ後に戻っていないことを確認できます。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Delivery OpenSearch Close returned before index completion: %v", err)
		default:
		}
		if got := indexer.closeCallCount(); got != 0 {
			t.Fatalf("indexer Close calls before index completion = %d, want 0", got)
		}

		rejectedReport, rejectedErr := logger.testInfo(context.Background(), "delivery after close started")
		if !errors.Is(rejectedErr, pkglogger.ErrLoggerClosed) {
			t.Fatalf("Delivery Info() while closing error = %v, want ErrLoggerClosed", rejectedErr)
		}
		if len(rejectedReport.Targets) != 1 || rejectedReport.Targets[0].Status != pkglogger.DeliveryRejected {
			t.Fatalf("Delivery Info() while closing report = %+v, want rejected target", rejectedReport)
		}

		indexer.releaseIndex()
		select {
		case result := <-indexDone:
			if result.err != nil {
				t.Fatalf("Delivery Info() error: %v", result.err)
			}
			if len(result.report.Targets) != 1 ||
				result.report.Targets[0].Status != pkglogger.DeliverySucceeded {
				t.Fatalf("Delivery Info() report = %+v, want succeeded target", result.report)
			}
		case <-time.After(time.Second):
			t.Fatal("delivery index did not finish")
		}
		waitLifecycleError(t, closeDone, "Delivery OpenSearch Close did not finish after index completion")

		if got := indexer.callCount(); got != 1 {
			t.Fatalf("IndexWithReport calls = %d, want 1", got)
		}
		if got := indexer.closeCallCount(); got != 1 {
			t.Fatalf("indexer Close calls = %d, want 1", got)
		}

		report, err := logger.testInfo(context.Background(), "delivery after close")
		if !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("Delivery Info() after Close error = %v, want ErrLoggerClosed", err)
		}
		if len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliveryRejected {
			t.Fatalf("Delivery Info() after Close report = %+v, want rejected target", report)
		}
		stackReport, stackErr := logger.testErrorWithStack(context.Background(), "stack without error after delivery close", nil)
		if !errors.Is(stackErr, pkglogger.ErrLoggerClosed) {
			t.Fatalf("Delivery ErrorWithStack(nil) after Close error = %v, want ErrLoggerClosed", stackErr)
		}
		if len(stackReport.Targets) != 1 || stackReport.Targets[0].Status != pkglogger.DeliveryRejected {
			t.Fatalf("Delivery ErrorWithStack(nil) after Close report = %+v, want rejected target", stackReport)
		}
	})
}

type lifecycleOpenSearchFlushManager struct {
	mu sync.Mutex

	started          chan struct{}
	release          chan struct{}
	immediateStarted chan struct{}
	immediateRelease chan struct{}

	blockFirst     bool
	blockImmediate bool
	startOnce      sync.Once
	immediateOnce  sync.Once
	releaseOnce    sync.Once
	// immediateReleaseOnceはimmediateReleaseを一度だけ閉じます。
	immediateReleaseOnce sync.Once
	// immediateCallStartedは即時Indexの開始ごとに通知します。
	immediateCallStarted chan struct{}
	flushCalls           int
	immediateCalls       int
	active               int
	maxActive            int
	closeCalls           int
	closeActive          bool
	closeContext         context.Context
	flushErrors          []error
	closeErrors          []error
}

func newLifecycleOpenSearchFlushManager(blockFirst bool) *lifecycleOpenSearchFlushManager {
	return &lifecycleOpenSearchFlushManager{
		started:              make(chan struct{}),
		release:              make(chan struct{}),
		immediateStarted:     make(chan struct{}),
		immediateRelease:     make(chan struct{}),
		immediateCallStarted: make(chan struct{}, 16),
		blockFirst:           blockFirst,
	}
}

// releaseFlushは、待機中の一件目のFlushを一度だけ解放します。
func (m *lifecycleOpenSearchFlushManager) releaseFlush() {
	m.releaseOnce.Do(func() { close(m.release) })
}

// releaseImmediateは、待機中の即時Indexを一度だけ解放します。
func (m *lifecycleOpenSearchFlushManager) releaseImmediate() {
	m.immediateReleaseOnce.Do(func() { close(m.immediateRelease) })
}

func (m *lifecycleOpenSearchFlushManager) Flush(
	context.Context,
	[]*logging.LogEntry,
) error {
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

func (m *lifecycleOpenSearchFlushManager) Index(
	context.Context,
	*logging.LogEntry,
) error {
	m.mu.Lock()
	m.immediateCalls++
	m.active++
	if m.active > m.maxActive {
		m.maxActive = m.active
	}
	block := m.blockImmediate
	m.mu.Unlock()
	select {
	case m.immediateCallStarted <- struct{}{}:
	default:
	}

	if block {
		m.immediateOnce.Do(func() { close(m.immediateStarted) })
		<-m.immediateRelease
	}

	m.mu.Lock()
	m.active--
	m.mu.Unlock()
	return nil
}

func (m *lifecycleOpenSearchFlushManager) IndexWithReport(
	context.Context,
	*logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	return pkglogger.DeliveryReport{}, nil
}

func (m *lifecycleOpenSearchFlushManager) FlushWithNoFallback(
	ctx context.Context,
	entries []*logging.LogEntry,
) {
	if len(entries) > 0 {
		_ = m.Index(ctx, entries[0])
	}
}

func (m *lifecycleOpenSearchFlushManager) Close(ctx context.Context) error {
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

func (m *lifecycleOpenSearchFlushManager) Config() OpenSearchFlushManagerRuntimeConfigProvider {
	return nil
}

func (m *lifecycleOpenSearchFlushManager) LogEntryToDocumentConverter() LogEntryToDocumentConverter {
	return nil
}

func (m *lifecycleOpenSearchFlushManager) Resource() resource.ResourceProvider {
	return nil
}

type lifecycleOpenSearchFlushManagerSnapshot struct {
	flushCalls                int
	immediateCalls            int
	maxActiveOperations       int
	closeCalls                int
	closeWhileOperationActive bool
	closeContext              context.Context
}

func (m *lifecycleOpenSearchFlushManager) snapshot() lifecycleOpenSearchFlushManagerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return lifecycleOpenSearchFlushManagerSnapshot{
		flushCalls:                m.flushCalls,
		immediateCalls:            m.immediateCalls,
		maxActiveOperations:       m.maxActive,
		closeCalls:                m.closeCalls,
		closeWhileOperationActive: m.closeActive,
		closeContext:              m.closeContext,
	}
}

// waitOpenSearchImmediateCallsStartedは、即時Indexの開始通知をcount件受け取るまで待ちます。
// 時間の上限はhangの検出だけに使います。
func waitOpenSearchImmediateCallsStarted(
	t *testing.T,
	manager *lifecycleOpenSearchFlushManager,
	count int,
) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-manager.immediateCallStarted:
		case <-time.After(lifecycleHangTimeout):
			t.Fatalf("immediate Index call %d did not start", i+1)
		}
	}
}

// lifecycleHangTimeoutは、完了するはずの処理がhangしたことを検出するための時間の上限です。
// 処理の完了は通知で待ち、この上限は失敗の検出だけに使います。
const lifecycleHangTimeout = 10 * time.Second

func waitLifecycleSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal(message)
	}
}

func waitLifecycleError(t *testing.T, result <-chan error, message string) {
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

func waitOpenSearchLoggerClosedError(t *testing.T, result <-chan error, message string) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("%s: error = %v, want ErrLoggerClosed", message, err)
		}
	case <-time.After(lifecycleHangTimeout):
		t.Fatal(message)
	}
}

type lifecycleDeliveryOpenSearchIndexer struct {
	mu sync.Mutex

	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	calls       int
	closes      int
}

// releaseIndexは、待機中の一件目のIndexWithReportを一度だけ解放します。
func (i *lifecycleDeliveryOpenSearchIndexer) releaseIndex() {
	i.releaseOnce.Do(func() { close(i.release) })
}

func (i *lifecycleDeliveryOpenSearchIndexer) IndexWithReport(
	context.Context,
	*logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	i.mu.Lock()
	i.calls++
	call := i.calls
	i.mu.Unlock()

	if call == 1 {
		close(i.started)
		<-i.release
	}
	return pkglogger.NewTargetDeliveryReport(openSearchDeliverySucceededResult()), nil
}

func (i *lifecycleDeliveryOpenSearchIndexer) Close(context.Context) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.closes++
	return nil
}

func (i *lifecycleDeliveryOpenSearchIndexer) callCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.calls
}

func (i *lifecycleDeliveryOpenSearchIndexer) closeCallCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.closes
}

// heterolog.Newへsignal.NotifyContextやdeadline付きのctxを渡し、構築直後にcancelされた場合も、
// interval flushとCloseの最終flushは送信を続けます。interval送信のctxは構築時のctxの
// cancelとdeadlineを引き継ぎません（ctxの値の伝播は仕様に含めません）。
func TestOpenSearchLoggerFlushesAfterConstructorContextCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := NewMockOpenSearchConfigProvider()
		cfg.flushInterval = time.Second
		cfg.maxBulkDocument = 100
		client := &contextRecordingOpenSearchClient{MockOpenSearchClient: NewMockOpenSearchClient()}
		constructorCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
		client.constructorDeadline, _ = constructorCtx.Deadline()
		tracer, meter := observer.NopTracer{}, observer.NopMeter{}
		logger, err := NewOpenSearchLogger(
			constructorCtx,
			cfg,
			client,
			cfg,
			NewMockResourceProvider(),
			nil,
			tracer,
			meter,
			&LoggerBase{Tracer: tracer, Meter: meter},
		)
		if err != nil {
			t.Fatalf("NewOpenSearchLogger() error: %v", err)
		}
		// 途中のassertionが失敗してもbubble内のworkerを止め、deadlockではなく失敗として報告します。
		// Closeは完了後の再呼び出しで何もしません。
		t.Cleanup(func() {
			if err := logger.Close(context.Background()); err != nil {
				t.Errorf("Close() error: %v", err)
			}
		})
		cancel()

		if err := logger.testInfo(context.Background(), "sent by interval"); err != nil {
			t.Fatalf("log after constructor cancel: %v", err)
		}
		synctest.Wait()
		if got := client.sentMessages(); len(got) != 0 {
			t.Fatalf("messages before interval = %v, want none below size triggers", got)
		}
		time.Sleep(cfg.flushInterval)
		synctest.Wait()
		// interval送信のctxは構築時のcancelとdeadlineを引き継ぎません。
		client.requireSends(t, openSearchExpectedSend{message: "sent by interval"})

		if err := logger.testInfo(context.Background(), "sent by Close"); err != nil {
			t.Fatalf("second log after constructor cancel: %v", err)
		}
		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		// Closeの最終flushはCloseへ渡したctxで送信します。
		client.requireSends(t,
			openSearchExpectedSend{message: "sent by interval"},
			openSearchExpectedSend{message: "sent by Close"},
		)
	})
}

// contextRecordingOpenSearchClientは送信時点のctxの終了状態と値を、送信documentと一緒に記録します。
type contextRecordingOpenSearchClient struct {
	*MockOpenSearchClient

	observeMu    sync.Mutex
	observations []openSearchSendContextObservation
	// constructorDeadlineは、loggerの構築に渡したctxの期限です。
	constructorDeadline time.Time
}

type openSearchSendContextObservation struct {
	err      error
	deadline time.Time
	message  any
}

func (c *contextRecordingOpenSearchClient) IndexDocument(
	ctx context.Context,
	doc pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.IndexResponse, error) {
	c.observe(ctx, doc)
	return c.MockOpenSearchClient.IndexDocument(ctx, doc)
}

func (c *contextRecordingOpenSearchClient) IndexDocuments(
	ctx context.Context,
	docs []pkgopensearch.DocumentWriteRequest,
) (*pkgopensearch.BulkResponse, error) {
	c.observe(ctx, docs...)
	return c.MockOpenSearchClient.IndexDocuments(ctx, docs)
}

func (c *contextRecordingOpenSearchClient) observe(ctx context.Context, docs ...pkgopensearch.DocumentWriteRequest) {
	c.observeMu.Lock()
	defer c.observeMu.Unlock()
	for _, doc := range docs {
		c.observations = append(c.observations, openSearchSendContextObservation{
			err:      ctx.Err(),
			deadline: contextDeadline(ctx),
			message:  doc.Doc["message"],
		})
	}
}

func (c *contextRecordingOpenSearchClient) sentMessages() []any {
	c.observeMu.Lock()
	defer c.observeMu.Unlock()
	messages := make([]any, 0, len(c.observations))
	for _, observation := range c.observations {
		messages = append(messages, observation.message)
	}
	return messages
}

type openSearchExpectedSend struct {
	message string
}

func (c *contextRecordingOpenSearchClient) requireSends(t *testing.T, want ...openSearchExpectedSend) {
	t.Helper()
	c.observeMu.Lock()
	defer c.observeMu.Unlock()
	if len(c.observations) != len(want) {
		t.Fatalf("OpenSearch sends = %+v, want %+v", c.observations, want)
	}
	for index, observation := range c.observations {
		if observation.message != want[index].message {
			t.Fatalf("send[%d] message = %v, want %q", index, observation.message, want[index].message)
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
func TestOpenSearchLoggerFailedFlushStillAdvancesFlushInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const flushInterval = time.Minute
		cfg := NewMockOpenSearchConfigProvider()
		cfg.flushInterval = flushInterval
		cfg.maxBulkDocument = 100
		logger, _ := newTestLogger(t, cfg, nil)
		manager := newLifecycleOpenSearchFlushManager(false)
		dropErr := fmt.Errorf("opensearch failure store overloaded: %w", pkglogger.ErrLogDropped)
		manager.flushErrors = []error{dropErr, dropErr}
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()
		logger.startBatchFlushWorker(context.Background(), nil, logger.flush)
		t.Cleanup(func() {
			if err := logger.Close(context.Background()); err != nil {
				t.Errorf("Close() error = %v", err)
			}
		})
		ctx := context.Background()

		if err := logger.testInfo(ctx, "dropped by interval flush"); err != nil {
			t.Fatalf("log before interval error = %v, want buffered", err)
		}
		time.Sleep(flushInterval)
		synctest.Wait()
		if got := manager.snapshot().flushCalls; got != 1 {
			t.Fatalf("flush calls after first interval = %d, want one failed flush", got)
		}

		for _, message := range []string{"buffered after failed flush", "still within interval"} {
			if err := logger.testInfo(ctx, message); err != nil {
				t.Fatalf("log after failed flush error = %v, want buffered until next interval", err)
			}
		}
		time.Sleep(flushInterval - time.Nanosecond)
		synctest.Wait()
		if err := logger.testInfo(ctx, "just before next interval"); err != nil {
			t.Fatalf("log just before next interval error = %v, want buffered", err)
		}
		if got := manager.snapshot().flushCalls; got != 1 {
			t.Fatalf("flush calls within interval after failed flush = %d, want 1", got)
		}
		if got := entryCount(logger); got != 3 {
			t.Fatalf("buffered entries within interval = %d, want 3", got)
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := manager.snapshot().flushCalls; got != 2 {
			t.Fatalf("flush calls after next interval = %d, want 2", got)
		}
		if got := entryCount(logger); got != 0 {
			t.Fatalf("buffered entries after next interval = %d, want 0", got)
		}
	})
}

// contextDeadlineはctxの期限を返します。期限がなければzero値です。
func contextDeadline(ctx context.Context) time.Time {
	deadline, _ := ctx.Deadline()
	return deadline
}

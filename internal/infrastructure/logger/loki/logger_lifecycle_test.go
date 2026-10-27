package loki

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// 通常終了は送信結果の通知も受理済み処理として待ち、通知先が使用中のmanagerを先に閉じません。
func TestLokiLoggerCloseWaitsForTimerErrorNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := newLifecycleLokiLogger(t, time.Hour)
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

		logger.mu.Unlock()
		appendLifecycleLokiEntry(t, logger, "timer failure")

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
func TestLokiLoggerCloseWaitsForThresholdFlushErrorNotification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// clientが未初期化のため、閾値のflushは内部ログのerrorを通知します。
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
			t.Fatalf("NewLokiLogger() error: %v", err)
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
		waitLokiLifecycleSignal(t, entered, "the threshold flush did not notify OnError")
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
		waitLokiLifecycleError(t, closed, "Close did not finish after OnError returned")
		if !logger.ShutdownComplete() {
			t.Fatal("logger shutdown did not complete after OnError returned")
		}
	})
}

func TestLokiLoggerCloseWaitsForTimerFlushAndSerializesFlushManager(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger := newLifecycleLokiLogger(t, time.Millisecond)
		manager := newLifecycleLokiFlushManager(true)
		// cleanupは登録と逆順に実行するため、失敗時も先にflushを解放してからCloseし、
		// bubble内のworkerをdeadlockではなく停止させます。Closeは冪等です。
		t.Cleanup(func() { _ = logger.Close(context.Background()) })
		t.Cleanup(manager.releaseFlush)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()
		appendLifecycleLokiEntry(t, logger, "timer batch")

		waitLokiLifecycleSignal(t, manager.started, "timer flush did not start")

		appendLifecycleLokiEntry(t, logger, "close batch")
		logger.mu.Lock()
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

		if err := logger.testInfo(context.Background(), "after close started"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("Info() while closing error = %v, want ErrLoggerClosed", err)
		}
		if err := logger.testErrorWithStack(context.Background(), "stack without error after close started", nil); !errors.Is(err, pkglogger.ErrLoggerClosed) {
			t.Fatalf("ErrorWithStack(nil) while closing error = %v, want ErrLoggerClosed", err)
		}
		if got := lifecycleLokiEntryCount(logger); got != queuedBeforeClose {
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
		waitLokiLifecycleError(t, concurrentFlushDone, "concurrent doFlush did not finish")
		waitLokiLifecycleError(t, closeDone, "Close did not finish")

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

func TestLokiLoggerCloseJoinsFlushAndManagerErrorsWithoutResendingSnapshot(t *testing.T) {
	logger := newLifecycleLokiLogger(t, time.Hour)
	flushErr := errors.New("final flush failed")
	closeErr := errors.New("manager close failed")
	manager := newLifecycleLokiFlushManager(false)
	manager.flushErrors = []error{flushErr}
	manager.closeErrors = []error{closeErr, nil}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.mu.Unlock()
	appendLifecycleLokiEntry(t, logger, "failed final snapshot")

	if err := logger.Close(context.Background()); !errors.Is(err, flushErr) || !errors.Is(err, closeErr) {
		t.Fatalf("first Close() error = %v, want joined %v and %v", err, flushErr, closeErr)
	}
	if snapshot := manager.snapshot(); snapshot.closeCalls != 1 {
		t.Fatalf("FlushManager.Close calls after failed flush = %d, want 1", snapshot.closeCalls)
	}
	if got := lifecycleLokiEntryCount(logger); got != 0 {
		t.Fatalf("entries after failed final flush = %d, want 0", got)
	}
	bufferedEntries, bufferedEntryBytes, bufferedBytes := lifecycleLokiBufferState(logger)
	if bufferedEntries != 0 || bufferedEntryBytes != 0 || bufferedBytes != 0 {
		t.Fatalf(
			"buffer state after failed final flush = (entries=%d, entryBytes=%d, bytes=%d), want zero",
			bufferedEntries,
			bufferedEntryBytes,
			bufferedBytes,
		)
	}

	if err := logger.testInfo(context.Background(), "rejected while manager close is pending"); !errors.Is(err, pkglogger.ErrLoggerClosed) {
		t.Fatalf("Info() while closing error = %v, want ErrLoggerClosed", err)
	}
	if got := lifecycleLokiEntryCount(logger); got != 0 {
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
	bufferedEntries, bufferedEntryBytes, bufferedBytes = lifecycleLokiBufferState(logger)
	if bufferedEntries != 0 || bufferedEntryBytes != 0 || bufferedBytes != 0 {
		t.Fatalf(
			"buffer state after second Close = (entries=%d, entryBytes=%d, bytes=%d), want zero",
			bufferedEntries,
			bufferedEntryBytes,
			bufferedBytes,
		)
	}
}

func TestLokiLoggerThresholdFlushReturnsErrorAndDropsSnapshot(t *testing.T) {
	logger := newLifecycleLokiLogger(t, time.Hour)
	flushErr := errors.New("durable handling failed")
	manager := newLifecycleLokiFlushManager(false)
	manager.flushErrors = []error{flushErr}
	logger.mu.Lock()
	logger.flushManager = manager
	logger.lastFlushTime = time.Time{}
	logger.mu.Unlock()

	if err := logger.testInfo(context.Background(), "drop failed threshold snapshot"); !errors.Is(err, flushErr) {
		t.Fatalf("Info() error = %v, want %v", err, flushErr)
	}
	if got := lifecycleLokiEntryCount(logger); got != 0 {
		t.Fatalf("entries after failed threshold flush = %d, want 0", got)
	}
	bufferedEntries, bufferedEntryBytes, bufferedBytes := lifecycleLokiBufferState(logger)
	if bufferedEntries != 0 || bufferedEntryBytes != 0 || bufferedBytes != 0 {
		t.Fatalf(
			"buffer state after failed threshold flush = (entries=%d, entryBytes=%d, bytes=%d), want zero",
			bufferedEntries,
			bufferedEntryBytes,
			bufferedBytes,
		)
	}
	if snapshot := manager.snapshot(); snapshot.flushCalls != 1 {
		t.Fatalf("Flush calls after failed threshold flush = %d, want 1", snapshot.flushCalls)
	}
}

func TestLokiLoggerCloseWaitsForImmediatePush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, err := NewLokiLogger(
			context.Background(),
			newTestLokiLoggerConfig(100, time.Hour, true),
			&recordingFlushManagerLokiClient{},
			newTestLokiFlushManagerConfig(),
			newTestLokiResourceProvider(),
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
			&LoggerBase{},
		)
		if err != nil {
			t.Fatalf("NewLokiLogger() error: %v", err)
		}
		manager := newLifecycleLokiFlushManager(false)
		manager.blockImmediate = true
		// cleanupは登録と逆順に実行するため、失敗時も先にPushを解放してからCloseします。
		t.Cleanup(func() { _ = logger.Close(context.Background()) })
		t.Cleanup(manager.releaseImmediate)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()

		pushDone := make(chan error, 1)
		go func() {
			pushDone <- logger.testInfo(context.Background(), "in-flight immediate push")
		}()
		waitLokiLifecycleSignal(t, manager.immediateStarted, "immediate push did not start")

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- logger.Close(context.Background())
		}()
		// Closeは新規受付を止めた後、受付済みの即時Pushの完了を待ちます。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before immediate push completed: %v", err)
		default:
		}

		pushWhileClosingDone := make(chan error, 1)
		go func() {
			pushWhileClosingDone <- logger.testInfo(context.Background(), "ignored after close started")
		}()
		waitLokiLoggerClosedError(t, pushWhileClosingDone, "immediate push while closing did not return")
		if got := manager.snapshot().immediateCalls; got != 1 {
			t.Fatalf("Push calls while closing = %d, want 1", got)
		}

		manager.releaseImmediate()
		waitLokiLifecycleError(t, pushDone, "immediate push did not finish")
		waitLokiLifecycleError(t, closeDone, "Close did not finish after immediate push")

		snapshot := manager.snapshot()
		if snapshot.immediateCalls != 1 {
			t.Fatalf("Push calls = %d, want 1", snapshot.immediateCalls)
		}
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileOperationActive {
			t.Fatal("FlushManager.Close overlapped immediate Push")
		}
	})
}

func TestLokiLoggerImmediatePushAllowsConcurrentCalls(t *testing.T) {
	logger, err := NewLokiLogger(
		context.Background(),
		newTestLokiLoggerConfig(100, time.Hour, true),
		&recordingFlushManagerLokiClient{},
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
	)
	if err != nil {
		t.Fatalf("NewLokiLogger() error: %v", err)
	}
	manager := newLifecycleLokiFlushManager(false)
	manager.blockImmediate = true
	logger.mu.Lock()
	logger.flushManager = manager
	logger.mu.Unlock()

	release := manager.releaseImmediate
	t.Cleanup(release)

	pushDone := make(chan error, 2)
	go func() {
		pushDone <- logger.testInfo(context.Background(), "first concurrent push")
	}()
	waitLokiLifecycleSignal(t, manager.immediateStarted, "first immediate push did not start")
	go func() {
		pushDone <- logger.testInfo(context.Background(), "second concurrent push")
	}()

	// 一件目を解放する前に二件目もbackendへ渡されることを、開始の通知で確認します。
	waitLokiImmediateCallsStarted(t, manager, 2)
	snapshot := manager.snapshot()
	if snapshot.maxActiveFlushes != 2 {
		t.Fatalf("max concurrent Push calls = %d, want 2", snapshot.maxActiveFlushes)
	}

	release()
	waitLokiLifecycleError(t, pushDone, "first immediate push did not finish")
	waitLokiLifecycleError(t, pushDone, "second immediate push did not finish")
	if err := logger.Close(context.Background()); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestDeliveryLokiLoggerCloseWaitsForPushWithReportAndRejectsNewDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base := newLifecycleLokiLogger(t, time.Hour)
		logger := &DeliveryLokiLogger{LokiLogger: base}
		manager := newLifecycleLokiFlushManager(false)
		manager.blockImmediate = true
		// cleanupは登録と逆順に実行するため、失敗時も先にPushを解放してからCloseします。
		t.Cleanup(func() { _ = logger.Close() })
		t.Cleanup(manager.releaseImmediate)
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()

		type deliveryResult struct {
			report pkglogger.DeliveryReport
			err    error
		}
		pushDone := make(chan deliveryResult, 1)
		go func() {
			report, err := logger.testInfo(context.Background(), "in-flight delivery push")
			pushDone <- deliveryResult{report: report, err: err}
		}()
		waitLokiLifecycleSignal(t, manager.immediateStarted, "delivery PushWithReport did not start")

		closeDone := make(chan error, 1)
		go func() {
			closeDone <- logger.Close()
		}()
		// Closeは新規受付を止めた後、受付済みのPushWithReportの完了を待ちます。
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before delivery push completed: %v", err)
		default:
		}
		if snapshot := manager.snapshot(); snapshot.closeCalls != 0 {
			t.Fatalf("FlushManager.Close calls during delivery push = %d, want 0", snapshot.closeCalls)
		}

		rejectedDone := make(chan deliveryResult, 1)
		go func() {
			report, err := logger.testInfo(context.Background(), "rejected after close started")
			rejectedDone <- deliveryResult{report: report, err: err}
		}()
		select {
		case result := <-rejectedDone:
			if !errors.Is(result.err, pkglogger.ErrLoggerClosed) {
				t.Fatalf("Delivery Info() while closing error = %v, want ErrLoggerClosed", result.err)
			}
			if len(result.report.Targets) != 1 ||
				result.report.Targets[0].Status != pkglogger.DeliveryRejected {
				t.Fatalf("Delivery Info() while closing report = %+v, want rejected target", result.report)
			}
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("Delivery Info() while closing did not return")
		}
		if got := manager.snapshot().immediateCalls; got != 1 {
			t.Fatalf("PushWithReport calls while closing = %d, want 1", got)
		}

		manager.releaseImmediate()
		select {
		case result := <-pushDone:
			if result.err != nil {
				t.Fatalf("Delivery Info() error: %v", result.err)
			}
			if len(result.report.Targets) != 1 ||
				result.report.Targets[0].Status != pkglogger.DeliverySucceeded {
				t.Fatalf("Delivery Info() report = %+v, want succeeded target", result.report)
			}
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("delivery push did not finish")
		}
		waitLokiLifecycleError(t, closeDone, "Delivery Close did not finish")

		report, err := logger.testInfo(context.Background(), "after delivery close")
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
		snapshot := manager.snapshot()
		if snapshot.immediateCalls != 1 {
			t.Fatalf("PushWithReport calls = %d, want 1", snapshot.immediateCalls)
		}
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileOperationActive {
			t.Fatal("FlushManager.Close overlapped PushWithReport")
		}
	})
}

func TestDeliveryLokiLoggerPushWithReportAllowsConcurrentCalls(t *testing.T) {
	logger := &DeliveryLokiLogger{LokiLogger: newLifecycleLokiLogger(t, time.Hour)}
	manager := newLifecycleLokiFlushManager(false)
	manager.blockImmediate = true
	logger.mu.Lock()
	logger.flushManager = manager
	logger.mu.Unlock()

	release := manager.releaseImmediate
	t.Cleanup(release)

	type deliveryResult struct {
		report pkglogger.DeliveryReport
		err    error
	}
	pushDone := make(chan deliveryResult, 2)
	go func() {
		report, err := logger.testInfo(context.Background(), "first concurrent delivery push")
		pushDone <- deliveryResult{report: report, err: err}
	}()
	waitLokiLifecycleSignal(t, manager.immediateStarted, "first delivery PushWithReport did not start")
	go func() {
		report, err := logger.testInfo(context.Background(), "second concurrent delivery push")
		pushDone <- deliveryResult{report: report, err: err}
	}()

	// 一件目を解放する前に二件目もbackendへ渡されることを、開始の通知で確認します。
	waitLokiImmediateCallsStarted(t, manager, 2)
	snapshot := manager.snapshot()
	if snapshot.maxActiveFlushes != 2 {
		t.Fatalf("max concurrent PushWithReport calls = %d, want 2", snapshot.maxActiveFlushes)
	}

	release()
	for index := 0; index < 2; index++ {
		select {
		case result := <-pushDone:
			if result.err != nil {
				t.Fatalf("Delivery Info() error: %v", result.err)
			}
			if len(result.report.Targets) != 1 ||
				result.report.Targets[0].Status != pkglogger.DeliverySucceeded {
				t.Fatalf("Delivery Info() report = %+v, want succeeded target", result.report)
			}
		case <-time.After(lifecycleHangTimeout):
			t.Fatal("concurrent delivery push did not finish")
		}
	}
	if err := logger.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
}

func TestLokiFallbackLoggerCloseWaitsForInFlightBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newTestLokiLoggerConfig(1, time.Hour, false)
		manager := newLifecycleLokiFlushManager(false)
		manager.blockImmediate = true
		logger := newLokiFallbackBatchTestLogger(cfg, manager)
		// cleanupは登録と逆順に実行するため、失敗時も先にbatchを解放してからCloseします。
		t.Cleanup(func() { _ = logger.Close(context.Background()) })
		t.Cleanup(manager.releaseImmediate)
		logger.startFallbackWorker()

		logger.enqueueProcess(newTestLokiLogEntry(pkglogger.LogLevelInfo, "in-flight fallback batch"))
		waitLokiLifecycleSignal(t, manager.immediateStarted, "fallback batch did not start")

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
			logger.enqueueProcess(newTestLokiLogEntry(pkglogger.LogLevelInfo, "ignored after close started"))
			close(enqueueWhileClosingDone)
		}()
		waitLokiLifecycleSignal(t, enqueueWhileClosingDone, "fallback enqueue while closing did not return")
		synctest.Wait()
		if got := manager.snapshot().immediateCalls; got != 1 {
			t.Fatalf("fallback batch calls while closing = %d, want 1", got)
		}

		manager.releaseImmediate()
		waitLokiLifecycleError(t, closeDone, "Close did not finish after fallback batch")

		snapshot := manager.snapshot()
		if snapshot.closeCalls != 1 {
			t.Fatalf("FlushManager.Close calls = %d, want 1", snapshot.closeCalls)
		}
		if snapshot.closeWhileOperationActive {
			t.Fatal("FlushManager.Close overlapped fallback batch")
		}
	})
}

func TestLokiFallbackFactoryStartsOwnedBatchWorker(t *testing.T) {
	flushIntervalGeneratorCalls := 0
	logger, err := newFallbackLogger(
		context.Background(),
		newTestLokiLoggerConfig(100, time.Second, false),
		&pkgloki.Config{Endpoint: "http://127.0.0.1:3100"},
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		WithFlushIntervalGenerator(func(time.Duration) *time.Ticker {
			flushIntervalGeneratorCalls++
			return time.NewTicker(time.Hour)
		}),
	)
	if err != nil {
		t.Fatalf("newFallbackLogger() error: %v", err)
	}
	if flushIntervalGeneratorCalls != 1 {
		t.Fatalf("fallback worker interval generator calls = %d, want 1", flushIntervalGeneratorCalls)
	}
	if logger.ticker == nil {
		t.Fatal("fallback worker ticker is nil")
	}
	if logger.stopCh == nil {
		t.Fatal("fallback worker stopCh is nil")
	}
	if err := logger.Close(context.Background()); err != nil {
		t.Fatalf("fallback logger Close() error: %v", err)
	}
}

func newLifecycleLokiLogger(t *testing.T, flushInterval time.Duration) *LokiLogger {
	t.Helper()
	logger, err := NewLokiLogger(
		context.Background(),
		newTestLokiLoggerConfig(100, flushInterval, false),
		&recordingFlushManagerLokiClient{},
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
		observer.NopTracer{},
		observer.NopMeter{},
		&LoggerBase{},
	)
	if err != nil {
		t.Fatalf("NewLokiLogger() error: %v", err)
	}
	return logger
}

type lifecycleLokiFlushManager struct {
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
	// immediateCallStartedは即時送信の開始ごとに通知します。
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

func newLifecycleLokiFlushManager(blockFirst bool) *lifecycleLokiFlushManager {
	return &lifecycleLokiFlushManager{
		started:              make(chan struct{}),
		release:              make(chan struct{}),
		immediateStarted:     make(chan struct{}),
		immediateRelease:     make(chan struct{}),
		immediateCallStarted: make(chan struct{}, 16),
		blockFirst:           blockFirst,
	}
}

// releaseFlushは、待機中の一件目のFlushを一度だけ解放します。
func (m *lifecycleLokiFlushManager) releaseFlush() {
	m.releaseOnce.Do(func() { close(m.release) })
}

// releaseImmediateは、待機中の即時送信を一度だけ解放します。
func (m *lifecycleLokiFlushManager) releaseImmediate() {
	m.immediateReleaseOnce.Do(func() { close(m.immediateRelease) })
}

func (m *lifecycleLokiFlushManager) Flush(context.Context, []*logging.LogEntry) error {
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

func (m *lifecycleLokiFlushManager) Push(context.Context, *logging.LogEntry) error {
	m.runImmediate()
	return nil
}

func (m *lifecycleLokiFlushManager) runImmediate() {
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
}

func (m *lifecycleLokiFlushManager) PushWithReport(
	context.Context,
	*logging.LogEntry,
) (pkglogger.DeliveryReport, error) {
	m.runImmediate()
	return pkglogger.NewTargetDeliveryReport(lokiDeliverySucceededResult()), nil
}

func (m *lifecycleLokiFlushManager) FlushWithNoFallback(
	context.Context,
	[]*logging.LogEntry,
) {
	m.runImmediate()
}

func (m *lifecycleLokiFlushManager) Close(ctx context.Context) error {
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

type lifecycleLokiFlushManagerSnapshot struct {
	flushCalls                int
	immediateCalls            int
	maxActiveFlushes          int
	closeCalls                int
	closeWhileOperationActive bool
	closeContext              context.Context
}

func (m *lifecycleLokiFlushManager) snapshot() lifecycleLokiFlushManagerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return lifecycleLokiFlushManagerSnapshot{
		flushCalls:                m.flushCalls,
		immediateCalls:            m.immediateCalls,
		maxActiveFlushes:          m.maxActive,
		closeCalls:                m.closeCalls,
		closeWhileOperationActive: m.closeActive,
		closeContext:              m.closeContext,
	}
}

func lifecycleLokiEntryCount(logger *LokiLogger) int {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return len(logger.entries)
}

func lifecycleLokiBufferState(logger *LokiLogger) (entries, entryBytes, bufferBytes int) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return len(logger.entries), len(logger.entryBytes), logger.bufferBytes
}

func appendLifecycleLokiEntry(t *testing.T, logger *LokiLogger, message string) {
	t.Helper()
	entry := newTestLokiLogEntry(pkglogger.LogLevelInfo, message)
	entryBytes, err := logger.measureBufferedEntryBytes(entry)
	if err != nil {
		t.Fatalf("measure lifecycle Loki entry: %v", err)
	}

	logger.mu.Lock()
	logger.entries = append(logger.entries, entry)
	logger.entryBytes = append(logger.entryBytes, entryBytes)
	logger.bufferBytes += entryBytes
	logger.mu.Unlock()
}

// lifecycleHangTimeoutは、完了するはずの処理がhangしたことを検出するための時間の上限です。
// 処理の完了は通知で待ち、この上限は失敗の検出だけに使います。
const lifecycleHangTimeout = 10 * time.Second

func waitLokiLifecycleSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(lifecycleHangTimeout):
		t.Fatal(message)
	}
}

func waitLokiLifecycleError(t *testing.T, result <-chan error, message string) {
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

func waitLokiLoggerClosedError(t *testing.T, result <-chan error, message string) {
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

// waitLokiImmediateCallsStartedは、即時送信の開始通知をcount件受け取るまで待ちます。
// 時間の上限はhangの検出だけに使います。
func waitLokiImmediateCallsStarted(t *testing.T, manager *lifecycleLokiFlushManager, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-manager.immediateCallStarted:
		case <-time.After(lifecycleHangTimeout):
			t.Fatalf("immediate call %d did not start", i+1)
		}
	}
}

// heterolog.Newへsignal.NotifyContextやdeadline付きのctxを渡し、構築直後にcancelされた場合も、
// interval flushとCloseの最終flushは送信を続けます。interval送信のctxは構築時のctxの
// cancelとdeadlineを引き継ぎません（ctxの値の伝播は仕様に含めません）。
func TestLokiLoggerFlushesAfterConstructorContextCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const flushInterval = time.Second
		client := &contextRecordingLokiClient{}
		constructorCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
		client.constructorDeadline, _ = constructorCtx.Deadline()
		logger, err := NewLokiLogger(
			constructorCtx,
			newTestLokiLoggerConfig(100, flushInterval, false),
			client,
			newTestLokiFlushManagerConfig(),
			newTestLokiResourceProvider(),
			nil,
			observer.NopTracer{},
			observer.NopMeter{},
			&LoggerBase{},
		)
		if err != nil {
			t.Fatalf("NewLokiLogger() error: %v", err)
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
		client.requireSends(t)
		time.Sleep(flushInterval)
		synctest.Wait()
		// interval送信のctxは構築時のcancelとdeadlineを引き継ぎません。
		client.requireSends(t, lokiExpectedSend{message: "sent by interval"})

		if err := logger.testInfo(context.Background(), "sent by Close"); err != nil {
			t.Fatalf("second log after constructor cancel: %v", err)
		}
		if err := logger.Close(context.Background()); err != nil {
			t.Fatalf("Close() error: %v", err)
		}
		// Closeの最終flushはCloseへ渡したctxで送信します。
		client.requireSends(t,
			lokiExpectedSend{message: "sent by interval"},
			lokiExpectedSend{message: "sent by Close"},
		)
	})
}

// contextRecordingLokiClientは送信時点のctxの終了状態と値を、送信lineと一緒に記録します。
type contextRecordingLokiClient struct {
	mu           sync.Mutex
	observations []lokiSendContextObservation
	// constructorDeadlineは、loggerの構築に渡したctxの期限です。
	constructorDeadline time.Time
}

type lokiSendContextObservation struct {
	err      error
	deadline time.Time
	line     string
}

type lokiExpectedSend struct {
	message string
}

func (c *contextRecordingLokiClient) Push(ctx context.Context, streams []pkgloki.Stream) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, stream := range streams {
		for _, entry := range stream.Entries {
			c.observations = append(c.observations, lokiSendContextObservation{
				err:      ctx.Err(),
				deadline: contextDeadline(ctx),
				line:     entry.Line,
			})
		}
	}
	return nil
}

func (c *contextRecordingLokiClient) requireSends(t *testing.T, want ...lokiExpectedSend) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.observations) != len(want) {
		t.Fatalf("Loki sends = %+v, want %+v", c.observations, want)
	}
	for index, observation := range c.observations {
		if !strings.Contains(observation.line, want[index].message) {
			t.Fatalf("send[%d] line = %q, want %q", index, observation.line, want[index].message)
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
func TestLokiLoggerFailedFlushStillAdvancesFlushInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const flushInterval = time.Minute
		logger := newLifecycleLokiLogger(t, flushInterval)
		manager := newLifecycleLokiFlushManager(false)
		dropErr := fmt.Errorf("loki failure store overloaded: %w", pkglogger.ErrLogDropped)
		manager.flushErrors = []error{dropErr, dropErr}
		logger.mu.Lock()
		logger.flushManager = manager
		logger.mu.Unlock()
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
		if got := lifecycleLokiEntryCount(logger); got != 3 {
			t.Fatalf("buffered entries within interval = %d, want 3", got)
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if got := manager.snapshot().flushCalls; got != 2 {
			t.Fatalf("flush calls after next interval = %d, want 2", got)
		}
		if got := lifecycleLokiEntryCount(logger); got != 0 {
			t.Fatalf("buffered entries after next interval = %d, want 0", got)
		}
	})
}

// contextDeadlineはctxの期限を返します。期限がなければzero値です。
func contextDeadline(ctx context.Context) time.Time {
	deadline, _ := ctx.Deadline()
	return deadline
}

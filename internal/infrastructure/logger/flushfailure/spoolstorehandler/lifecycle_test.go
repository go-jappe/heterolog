package spoolstorehandler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
)

// 受理済みのHandleFailureが統計scanの前で止まっている間も、Shutdownはその完了を待ちます。
func TestSpoolStoreHandlerShutdownWaitsForAcceptedHandleFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
		monitor := newGatedMemoryMonitor()
		handler := newTestHandlerWithOverloadHandler(
			t,
			now,
			testOverloadPolicyConfig{useMemoryPressureForOverload: true},
			monitor,
			nil,
			InternalLogHandlers{},
		)
		// 試験が途中で失敗しても、止めた判定を再開してから片付けます。
		t.Cleanup(monitor.open)

		handleDone := make(chan error, 1)
		go func() {
			handleDone <- handler.HandleFailure(
				context.Background(),
				testRetryableFailureResult(now, "accepted-before-shutdown"),
			)
		}()

		select {
		case <-monitor.entered:
		case err := <-handleDone:
			t.Fatalf("HandleFailure returned before the memory overload check: %v", err)
		}

		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- handler.Shutdown(context.Background())
		}()
		synctest.Wait()
		select {
		case err := <-shutdownDone:
			t.Fatalf("Shutdown returned before accepted HandleFailure completed: %v", err)
		default:
		}

		monitor.open()
		if err := <-handleDone; err != nil {
			t.Fatalf("accepted HandleFailure error = %v", err)
		}
		if err := <-shutdownDone; err != nil {
			t.Fatalf("Shutdown error = %v", err)
		}
		if handler.shutdownState != shutdownStateCompleted {
			t.Fatalf("shutdownState = %d, want Completed", handler.shutdownState)
		}
		if err := handler.HandleFailure(
			context.Background(),
			testRetryableFailureResult(now, "rejected-after-shutdown"),
		); !errors.Is(err, ErrSpoolStoreHandlerClosing) {
			t.Fatalf("HandleFailure after Shutdown error = %v, want ErrSpoolStoreHandlerClosing", err)
		}
	})
}

// gatedMemoryMonitorは、最初のmemory overload判定をopenまで止め、逼迫なしを返します。
type gatedMemoryMonitor struct {
	recordingMemoryMonitor
	entered   chan struct{}
	gate      chan struct{}
	enterOnce sync.Once
	openOnce  sync.Once
}

func newGatedMemoryMonitor() *gatedMemoryMonitor {
	return &gatedMemoryMonitor{
		entered: make(chan struct{}),
		gate:    make(chan struct{}),
	}
}

func (m *gatedMemoryMonitor) IsCritical(context.Context) (bool, error) {
	first := false
	m.enterOnce.Do(func() { first = true })
	if first {
		close(m.entered)
		<-m.gate
	}
	return false, nil
}

func (m *gatedMemoryMonitor) open() {
	m.openOnce.Do(func() { close(m.gate) })
}

func TestSpoolStoreHandlerShutdownTracksAsyncRefreshAndCanRetryAfterTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
		scanStarted := make(chan struct{})
		allowScan := make(chan struct{})
		var allowScanOnce sync.Once
		releaseScan := func() { allowScanOnce.Do(func() { close(allowScan) }) }
		handler := newTestHandlerWithDirStatsScanner(
			t,
			now,
			testOverloadPolicyConfig{statsScanInterval: time.Second},
			func(spool.StateDir) (DirStats, error) {
				close(scanStarted)
				<-allowScan
				return DirStats{}, nil
			},
		)
		// 試験が途中で失敗しても、止めたscanを再開してから片付けます。
		t.Cleanup(releaseScan)
		handler.overloadCache.mu.Lock()
		handler.overloadCache.snapshot = dirStatsSnapshot{
			ScannedAt: now.Add(-2 * time.Second),
		}
		handler.overloadCache.mu.Unlock()

		if err := handler.HandleFailure(
			context.Background(),
			testRetryableFailureResult(now, "async-refresh"),
		); err != nil {
			t.Fatalf("HandleFailure error = %v", err)
		}
		<-scanStarted

		// scanを止めている間、呼び出し元の期限で待機だけが終わります。
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := handler.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first Shutdown error = %v, want DeadlineExceeded", err)
		}
		if handler.shutdownState != shutdownStateStarted {
			t.Fatalf("shutdownState = %d, want Started after timeout", handler.shutdownState)
		}
		if err := handler.HandleFailure(
			context.Background(),
			testRetryableFailureResult(now, "rejected-while-shutdown-incomplete"),
		); !errors.Is(err, ErrSpoolStoreHandlerClosing) {
			t.Fatalf("HandleFailure during incomplete Shutdown error = %v, want ErrSpoolStoreHandlerClosing", err)
		}

		releaseScan()
		if err := handler.Shutdown(context.Background()); err != nil {
			t.Fatalf("retry Shutdown error = %v", err)
		}
		if handler.shutdownState != shutdownStateCompleted {
			t.Fatalf("shutdownState = %d, want Completed after retry", handler.shutdownState)
		}
		if err := handler.Shutdown(context.Background()); err != nil {
			t.Fatalf("idempotent Shutdown error = %v", err)
		}
	})
}

func TestSpoolStoreHandlerReusesCacheWhileSingleAsyncDirStatsScanIsRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
		scanStarted := make(chan struct{})
		allowScan := make(chan struct{})
		var allowScanOnce sync.Once
		releaseScan := func() { allowScanOnce.Do(func() { close(allowScan) }) }
		var scanCalls atomic.Int32
		handler := newTestHandlerWithDirStatsScanner(
			t,
			now,
			testOverloadPolicyConfig{statsScanInterval: time.Second},
			func(spool.StateDir) (DirStats, error) {
				scanCalls.Add(1)
				close(scanStarted)
				<-allowScan
				return DirStats{}, nil
			},
		)
		// 試験が途中で失敗しても、止めたscanを再開してから片付けます。
		t.Cleanup(releaseScan)
		handler.overloadCache.mu.Lock()
		handler.overloadCache.snapshot = dirStatsSnapshot{
			ScannedAt: now.Add(-2 * time.Second),
		}
		handler.overloadCache.mu.Unlock()

		if err := handler.HandleFailure(
			context.Background(),
			testRetryableFailureResult(now, "starts-async-refresh"),
		); err != nil {
			t.Fatalf("first HandleFailure error = %v", err)
		}
		select {
		case <-scanStarted:
		case <-time.After(time.Second):
			t.Fatal("first HandleFailure did not start the asynchronous refresh scan")
		}

		handler.overloadCache.mu.RLock()
		firstScanDone := handler.overloadCache.activeScanDone
		handler.overloadCache.mu.RUnlock()
		if firstScanDone == nil {
			t.Fatal("activeScanDone is nil while asynchronous refresh scan is running")
		}

		if err := handler.HandleFailure(
			context.Background(),
			testRetryableFailureResult(now, "uses-cache-during-refresh"),
		); err != nil {
			t.Fatalf("second HandleFailure error = %v", err)
		}
		handler.overloadCache.mu.RLock()
		secondScanDone := handler.overloadCache.activeScanDone
		handler.overloadCache.mu.RUnlock()
		if secondScanDone != firstScanDone {
			t.Fatal("second HandleFailure replaced the in-progress directory scan")
		}
		if got := scanCalls.Load(); got != 1 {
			t.Fatalf("directory scan calls = %d, want 1", got)
		}

		releaseScan()
		if err := handler.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown error = %v", err)
		}
		handler.overloadCache.mu.RLock()
		remainingScanDone := handler.overloadCache.activeScanDone
		handler.overloadCache.mu.RUnlock()
		if remainingScanDone != nil {
			t.Fatal("activeScanDone remains set after asynchronous refresh scan completed")
		}
	})
}

func TestSpoolStoreHandlerShutdownHonorsCanceledContextAndRemainsRetryable(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	handler := newTestHandler(t, now, testOverloadPolicyConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handler.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown error = %v, want context.Canceled", err)
	}
	if handler.shutdownState != shutdownStateStarted {
		t.Fatalf("shutdownState = %d, want Started after canceled Shutdown", handler.shutdownState)
	}
	if err := handler.HandleFailure(
		context.Background(),
		testRetryableFailureResult(now, "rejected-after-canceled-shutdown"),
	); !errors.Is(err, ErrSpoolStoreHandlerClosing) {
		t.Fatalf("HandleFailure after canceled Shutdown error = %v, want ErrSpoolStoreHandlerClosing", err)
	}
	if err := shutdownSpoolHandlerForTest(handler); err != nil {
		t.Fatalf("retry Shutdown error = %v", err)
	}
	if handler.shutdownState != shutdownStateCompleted {
		t.Fatalf("shutdownState = %d, want Completed after retry", handler.shutdownState)
	}
}

func TestSpoolStoreHandlerShutdownDeadlineOnlyReleasesWaiterDuringAcceptedWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
		handler := newTestHandlerWithDirStatsScanner(t, now, testOverloadPolicyConfig{}, func(spool.StateDir) (DirStats, error) {
			close(entered)
			<-release
			return DirStats{}, nil
		})
		// 試験が途中で失敗しても、止めた保存を再開してから片付けます。
		t.Cleanup(releaseWrite)
		acceptedCtx, cancelAccepted := context.WithCancel(context.Background())
		defer cancelAccepted()
		handled := make(chan error, 1)
		go func() {
			handled <- handler.HandleFailure(acceptedCtx, testRetryableFailureResult(now, "accepted-write"))
		}()
		<-entered
		waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
		defer cancelWait()
		if err := handler.Shutdown(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown error = %v, want waiter deadline", err)
		}
		if err := acceptedCtx.Err(); err != nil {
			t.Fatalf("Shutdown canceled accepted write: %v", err)
		}
		if err := handler.HandleFailure(context.Background(), testRetryableFailureResult(now, "late-write")); !errors.Is(err, ErrSpoolStoreHandlerClosing) {
			t.Fatalf("new write after timed-out shutdown = %v, want closing", err)
		}
		finished := make(chan error, 1)
		go func() { finished <- handler.Shutdown(context.Background()) }()
		synctest.Wait()
		select {
		case err := <-finished:
			t.Fatalf("normal shutdown returned before accepted write completed: %v", err)
		default:
		}
		releaseWrite()
		if err := <-handled; err != nil {
			t.Fatalf("accepted write failed after waiter deadline: %v", err)
		}
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		if err := handler.Shutdown(context.Background()); err != nil {
			t.Fatalf("idempotent shutdown: %v", err)
		}
	})
}

// spoolHandlerHangTimeoutは、終了処理が止まったことを検出する上限です。完了は通知で確認し、
// この時間は停止の検出だけに使います。
const spoolHandlerHangTimeout = 10 * time.Second

// shutdownSpoolHandlerForTestは、停止の検出だけに使う期限付きでShutdownを呼びます。
func shutdownSpoolHandlerForTest(handler *SpoolStoreHandler) error {
	ctx, cancel := context.WithTimeout(context.Background(), spoolHandlerHangTimeout)
	defer cancel()
	return handler.Shutdown(ctx)
}

func newTestHandlerWithDirStatsScanner(
	t *testing.T,
	now time.Time,
	overload testOverloadPolicyConfig,
	scanner func(spool.StateDir) (DirStats, error),
) *SpoolStoreHandler {
	t.Helper()
	handler := newTestHandler(t, now, overload)
	handler.dirStatsScanner = scanner
	return handler
}

func testRetryableFailureResult(now time.Time, requestID string) loggerfailure.SendResult {
	return loggerfailure.SendResult{
		Meta: loggerfailure.Meta{
			Backend:     "opensearch",
			RequestID:   requestID,
			AttemptedAt: now,
			InputCount:  1,
		},
		RetryableRecords: []loggerfailure.FailedRecord{
			failedRecord(requestID, "retryable", now),
		},
	}
}

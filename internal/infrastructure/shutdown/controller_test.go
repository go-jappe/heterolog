package shutdown

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

func TestControllerGracefulCloseWaitsAndSharesTerminalResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Controller
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		terminalErr := errors.New("cleanup completed with a reported error")
		var calls atomic.Int32
		attempt := func() (bool, error) {
			if calls.Add(1) == 1 {
				close(entered)
			}
			<-release
			return true, terminalErr
		}
		results := make(chan error, 8)
		for range cap(results) {
			go func() { results <- c.Close("accepted cleanup", attempt) }()
		}
		<-entered
		// 通常終了には内部期限を設けず、進捗表示を繰り返しても処理完了まで待ちます。
		time.Sleep(3 * ReportInterval)
		synctest.Wait()
		if len(results) != 0 || c.IsComplete() || c.IsForced() {
			t.Fatal("normal Close stopped waiting for accepted cleanup")
		}
		if calls.Load() != 1 {
			t.Fatalf("concurrent cleanup calls=%d, want 1", calls.Load())
		}
		unblock()
		for range cap(results) {
			if err := <-results; !errors.Is(err, terminalErr) {
				t.Fatal(err)
			}
		}
		<-c.Done()
		if !c.IsComplete() {
			t.Fatal("cleanup completed without recording completion")
		}
		if err := c.Close("already completed", func() (bool, error) { t.Error("cleanup repeated"); return true, nil }); !errors.Is(err, terminalErr) {
			t.Fatalf("repeated Close lost terminal error: %v", err)
		}
	})
}

// ForceCloseは全てのClose呼び出しの待機を直ちに解除しますが、実行中の後片付けは中止せず、
// その完了までDoneを閉じません。待機はchannelの通知で確かめ、上限（boundedwait.HangTimeout）は
// 停止の検出だけに使います。
func TestControllerForceReleasesCallersButNotRunningCleanup(t *testing.T) {
	var c Controller
	entered, release, aborted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseCleanup := sync.OnceFunc(func() { close(release) })
	// 途中の確認が失敗しても、受理済みの後片付けを解放し、終了workerを残しません。
	t.Cleanup(releaseCleanup)
	var calls atomic.Int32
	attempt := func() (bool, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return true, nil
	}
	results := make(chan error, 8)
	for range 8 {
		go func() { results <- c.Close("test blocked I/O", attempt) }()
	}
	boundedwait.Receive(t, entered, "the cleanup attempt")
	if err := c.ForceClose(func() { close(aborted) }); !errors.Is(err, ErrForcedClose) {
		t.Fatal(err)
	}
	for range 8 {
		if err := boundedwait.Receive(t, results, "a Close wait released by ForceClose"); !errors.Is(err, ErrForcedClose) {
			t.Fatal(err)
		}
	}
	select {
	case <-c.Done():
		t.Fatal("active cleanup was not awaited")
	default:
	}
	boundedwait.Receive(t, aborted, "the abort callback")
	releaseCleanup()
	boundedwait.Receive(t, c.Done(), "the cleanup after its I/O returned")
	if calls.Load() != 1 {
		t.Fatalf("cleanup calls=%d", calls.Load())
	}
}

func TestControllerRetriesIncompleteCleanup(t *testing.T) {
	// 再試行の待機は実時間ではなく、synctestの時計で進めます。
	synctest.Test(t, func(t *testing.T) {
		var c Controller
		var calls atomic.Int32
		var attempts []time.Time
		if err := c.Close("retry", func() (bool, error) {
			attempts = append(attempts, time.Now())
			if calls.Add(1) == 1 {
				return false, errors.New("temporary failure")
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatalf("calls=%d", calls.Load())
		}
		// 未完了の処理はReportInterval以上の間隔を空けて再試行し、連続して呼び出しません。
		// 間隔の上限は定めないため、下限だけを確認します。
		if gap := attempts[1].Sub(attempts[0]); gap < ReportInterval {
			t.Fatalf("retried incomplete cleanup after %s, want at least %s: attempts=%v", gap, ReportInterval, attempts)
		}
		// attemptは終了workerのgoroutineで実行されるため、t.Fatalではなくt.Errorで記録します。
		if err := c.Close("completed", func() (bool, error) { t.Error("duplicate cleanup"); return true, nil }); err != nil {
			t.Fatal(err)
		}
	})
}

func TestControllerCallerCancellationDoesNotAbortCleanup(t *testing.T) {
	var c Controller
	entered, release := make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.CloseContext(ctx, "context", func() (bool, error) { close(entered); <-release; return true, nil })
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if c.IsForced() {
		t.Fatal("waiting context forced owner shutdown")
	}
	select {
	case <-c.Done():
		t.Fatal("caller cancellation completed cleanup while its operation was still running")
	default:
	}
	close(release)
	<-c.Done()
}

func TestControllerForceBeforeCloseStillCompletesResourceCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var c Controller
		if err := c.ForceClose(nil); !errors.Is(err, ErrForcedClose) {
			t.Fatal(err)
		}
		var calls atomic.Int32
		if err := c.Close("forced", func() (bool, error) {
			if calls.Add(1) == 1 {
				return false, errors.New("unfinished")
			}
			return true, nil
		}); !errors.Is(err, ErrForcedClose) {
			t.Fatal(err)
		}
		<-c.Done()
		if calls.Load() != 2 {
			t.Fatalf("calls=%d", calls.Load())
		}
	})
}

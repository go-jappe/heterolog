package boundedwait

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// recordingTBは、helperが試験を失敗させた内容を記録します。Fatalfは実際の試験を
// 失敗させず、helperを実行したgoroutineだけを終了します。
type recordingTB struct {
	testing.TB
	failed   bool
	skipped  bool
	fatal    chan string
	errors   []string
	cleanups []func()
}

func newRecordingTB(t *testing.T) *recordingTB {
	return &recordingTB{TB: t, fatal: make(chan string, 1)}
}

func (r *recordingTB) Helper()       {}
func (r *recordingTB) Failed() bool  { return r.failed }
func (r *recordingTB) Skipped() bool { return r.skipped }

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.fatal <- fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Cleanup(f func()) { r.cleanups = append(r.cleanups, f) }

// runHelperは、Fatalfでgoroutineが終了してもよいように、helperを別goroutineで実行します。
// 戻り値はhelperが失敗させた場合のmessageで、正常に戻った場合は空です。
func runHelper(r *recordingTB, helper func()) string {
	done := make(chan struct{})
	go func() {
		defer close(done)
		helper()
	}()
	<-done
	select {
	case message := <-r.fatal:
		return message
	default:
		return ""
	}
}

func TestReceiveWithinReturnsValueOrFailsAtBubbleDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ch := make(chan int, 1)
		ch <- 7
		recorder := newRecordingTB(t)
		var got int
		if message := runHelper(recorder, func() { got = ReceiveWithin(recorder, ch, time.Minute, "ready value") }); message != "" {
			t.Fatalf("available value failed: %s", message)
		}
		if got != 7 {
			t.Fatalf("value = %d, want 7", got)
		}

		start := time.Now()
		message := runHelper(recorder, func() { ReceiveWithin(recorder, ch, time.Minute, "missing value") })
		if !strings.Contains(message, "missing value did not complete within 1m0s") {
			t.Fatalf("failure message = %q, want the description and bound", message)
		}
		if elapsed := time.Since(start); elapsed != time.Minute {
			t.Fatalf("failed after %s of bubble time, want exactly the 1m bound", elapsed)
		}
	})
}

func TestCloseWithinReturnsCloseErrorWithoutForcing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		closeErr := errors.New("close failed")
		recorder := newRecordingTB(t)
		forced := false
		var got error
		message := runHelper(recorder, func() {
			got = CloseWithin(recorder, time.Minute, "store.Close()", func() error { return closeErr }, func() error {
				forced = true
				return nil
			})
		})
		if message != "" || !errors.Is(got, closeErr) || forced {
			t.Fatalf("message=%q err=%v forced=%t, want the close error without ForceClose", message, got, forced)
		}
	})
}

func TestCloseWithinForcesAndFailsWhenCloseDoesNotReturn(t *testing.T) {
	for _, returnsAfterForce := range []bool{true, false} {
		t.Run(fmt.Sprintf("returns_after_force=%t", returnsAfterForce), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				stop := make(chan struct{})
				defer close(stop)
				forcedAt := time.Duration(-1)
				start := time.Now()
				closeFunc := func() error {
					select {
					case <-release:
						return errors.New("forced close")
					case <-stop:
						return nil
					}
				}
				force := func() error {
					forcedAt = time.Since(start)
					if returnsAfterForce {
						close(release)
					}
					return nil
				}
				recorder := newRecordingTB(t)
				message := runHelper(recorder, func() { CloseWithin(recorder, time.Minute, "manager.Close()", closeFunc, force) })
				if forcedAt != time.Minute {
					t.Fatalf("ForceClose requested after %s, want the 1m bound", forcedAt)
				}
				want := "manager.Close() did not return within 1m0s, even after ForceClose"
				if returnsAfterForce {
					want = "manager.Close() did not return within 1m0s; it returned after ForceClose: forced close"
				}
				if message != want {
					t.Fatalf("failure message = %q, want %q", message, want)
				}
			})
		})
	}
}

// forceがnilなら中止を通知できないため、期限で試験を失敗させます。Closeが戻らないまま
// errorなしとして返すと、止まった終了処理が成功した試験に見えます。
func TestCloseWithinFailsAtBoundWithoutForce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stop := make(chan struct{})
		defer close(stop)
		start := time.Now()
		recorder := newRecordingTB(t)
		message := runHelper(recorder, func() {
			CloseWithin(recorder, time.Minute, "route.Close()", func() error {
				<-stop
				return nil
			}, nil)
		})
		if want := "route.Close() did not return within 1m0s"; message != want {
			t.Fatalf("failure message = %q, want %q", message, want)
		}
		if elapsed := time.Since(start); elapsed != time.Minute {
			t.Fatalf("failed after %s of bubble time, want exactly the 1m bound", elapsed)
		}
	})
}

func TestCloseOnCleanupForcesFailedOrSkippedTestBeforeClosing(t *testing.T) {
	for _, state := range []string{"passed", "failed", "skipped"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				recorder := newRecordingTB(t)
				recorder.failed = state == "failed"
				recorder.skipped = state == "skipped"
				var calls []string
				closeErr := errors.New("close result")
				CloseOnCleanup(recorder, time.Minute, "lifecycle.Close()", func() error {
					calls = append(calls, "close")
					return closeErr
				}, func() error {
					calls = append(calls, "force")
					return nil
				}, NoError)
				if len(recorder.cleanups) != 1 {
					t.Fatalf("registered %d cleanups, want 1", len(recorder.cleanups))
				}
				if message := runHelper(recorder, recorder.cleanups[0]); message != "" {
					t.Fatalf("cleanup failed: %s", message)
				}
				want := []string{"close"}
				if state != "passed" {
					// 失敗・skipした試験では障害注入が残り得るため中止を先に通知し、
					// 終了処理のerrorを失敗理由に重ねません。
					want = []string{"force", "close"}
				}
				if fmt.Sprint(calls) != fmt.Sprint(want) {
					t.Fatalf("calls = %v, want %v", calls, want)
				}
				if (state == "passed") != (len(recorder.errors) == 1) {
					t.Fatalf("reported errors = %v; only a successful test must report the close error", recorder.errors)
				}
			})
		})
	}
}

// 期限切れの失敗の理由には、cleanupを登録した試験側の位置を含めます。testingはbubble内の
// cleanupの失敗を登録位置へ辿れないため、同じ説明の終了処理が複数あっても特定できるようにします。
func TestCloseOnCleanupReportsRegistrationSiteWhenCloseDoesNotReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stop := make(chan struct{})
		defer close(stop)
		recorder := newRecordingTB(t)
		_, file, line, _ := runtime.Caller(0)
		CloseOnCleanup(recorder, time.Minute, "store.Close()", func() error { <-stop; return nil }, nil, NoError)
		if len(recorder.cleanups) != 1 {
			t.Fatalf("registered %d cleanups, want 1", len(recorder.cleanups))
		}
		message := runHelper(recorder, recorder.cleanups[0])
		want := fmt.Sprintf("store.Close() did not return within 1m0s (cleanup registered at %s:%d", filepath.Base(file), line+1)
		if !strings.HasPrefix(message, want) {
			t.Fatalf("failure message = %q, want the bound and the registration site %q", message, want)
		}
	})
}

// errControllerForcedは、controllerLikeCloserのForceClose後のCloseが返すerrorです。
var errControllerForced = errors.New("controller close forced")

// controllerLikeCloserは、shutdown.Controllerと同じく、ForceCloseが呼び出し元の待機だけを解除し、
// 後片付けの再試行を別goroutineで仮想時間の間隔で続ける所有者を模します。ForceClose後のCloseは
// 後片付けの完了を待たずにerrControllerForcedを返し、DoneはそのgoroutineがStopすると閉じます。
type controllerLikeCloser struct {
	mu       sync.Mutex
	started  bool
	retries  int
	stuck    chan struct{}
	forced   chan struct{}
	finished chan struct{}
}

func newControllerLikeCloser(retries int, stuck chan struct{}) *controllerLikeCloser {
	return &controllerLikeCloser{retries: retries, stuck: stuck, forced: make(chan struct{}), finished: make(chan struct{})}
}

func (c *controllerLikeCloser) startLocked() {
	if c.started {
		return
	}
	c.started = true
	go func() {
		defer close(c.finished)
		for range c.retries {
			time.Sleep(time.Second)
		}
		if c.stuck != nil {
			<-c.stuck
		}
	}()
}

func (c *controllerLikeCloser) Close() error {
	c.mu.Lock()
	c.startLocked()
	c.mu.Unlock()
	select {
	case <-c.finished:
		return nil
	case <-c.forced:
		return errControllerForced
	}
}

func (c *controllerLikeCloser) ForceClose() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.forced:
	default:
		close(c.forced)
	}
	c.startLocked()
	return errControllerForced
}

func (c *controllerLikeCloser) Done() <-chan struct{} { return c.finished }

// 失敗・skipした試験の終了処理では先にForceCloseします。ForceClose後のCloseが後片付けを待たずに
// 戻る所有者でも、CloseOnCleanupUntilDoneは後片付けのgoroutineが実際に戻るまで同じ上限で待ちます。
// 待たずに戻ると、bubble内では主goroutineの終了後に残った再試行がdeadlockのpanicになります。
func TestCloseOnCleanupUntilDoneWaitsForBackgroundShutdown(t *testing.T) {
	for _, state := range []string{"passed", "failed", "skipped"} {
		t.Run(state, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				recorder := newRecordingTB(t)
				recorder.failed = state == "failed"
				recorder.skipped = state == "skipped"
				closer := newControllerLikeCloser(3, nil)
				start := time.Now()
				var checked []error
				CloseOnCleanupUntilDone(recorder, time.Minute, "logger.Close()", closer.Close, closer.ForceClose, closer.Done,
					func(_ testing.TB, err error) { checked = append(checked, err) })
				if message := runHelper(recorder, recorder.cleanups[0]); message != "" {
					t.Fatalf("cleanup failed: %s", message)
				}
				select {
				case <-closer.Done():
				default:
					t.Errorf("the cleanup returned while the background shutdown was still retrying")
					<-closer.Done()
				}
				if elapsed := time.Since(start); elapsed != 3*time.Second {
					t.Fatalf("the cleanup returned after %s of bubble time, want the 3s background shutdown", elapsed)
				}
				want := "[<nil>]"
				if state != "passed" {
					// 失敗・skipした試験では終了処理のerrorを失敗理由に重ねません。
					want = "[]"
				}
				if fmt.Sprint(checked) != want {
					t.Fatalf("checked close errors = %v, want %s", checked, want)
				}
			})
		})
	}
}

// 後片付けが上限内に終わらなければ、CloseOnCleanupUntilDoneは登録位置とともに試験を失敗させます。
func TestCloseOnCleanupUntilDoneFailsWhenBackgroundShutdownDoesNotFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stuck := make(chan struct{})
		closer := newControllerLikeCloser(0, stuck)
		defer func() {
			close(stuck)
			<-closer.Done()
		}()
		recorder := newRecordingTB(t)
		recorder.failed = true
		_, file, line, _ := runtime.Caller(0)
		CloseOnCleanupUntilDone(recorder, time.Minute, "logger.Close()", closer.Close, closer.ForceClose, closer.Done, nil)
		message := runHelper(recorder, recorder.cleanups[0])
		want := fmt.Sprintf("logger.Close(): background shutdown did not finish within 1m0s (cleanup registered at %s:%d", filepath.Base(file), line+1)
		if !strings.HasPrefix(message, want) {
			t.Fatalf("failure message = %q, want %q", message, want)
		}
	})
}

func TestSendOrReportReportsOnlyTheFirstUnexpectedValue(t *testing.T) {
	recorder := newRecordingTB(t)
	ch := make(chan int, 1)
	SendOrReport(recorder, ch, 1, "notification")
	if len(recorder.errors) != 0 {
		t.Fatalf("expected value was reported: %v", recorder.errors)
	}
	for value := 2; value <= 4; value++ {
		SendOrReport(recorder, ch, value, "notification")
	}
	want := []string{"unexpected additional notification: 2 (later ones are not reported)"}
	if fmt.Sprint(recorder.errors) != fmt.Sprint(want) {
		t.Fatalf("reported errors = %q, want %q", recorder.errors, want)
	}
	if got := <-ch; got != 1 {
		t.Fatalf("received %d, want the expected value 1", got)
	}
	// 別のchannelの予定外の値は、それぞれ最初の1件を報告します。
	other := make(chan int)
	SendOrReport(recorder, other, 5, "retry error")
	if len(recorder.errors) != 2 || recorder.errors[1] != "unexpected additional retry error: 5 (later ones are not reported)" {
		t.Fatalf("reported errors = %q, want the first value on the second channel", recorder.errors)
	}
}

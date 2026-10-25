// Package boundedwaitは、試験が処理の完了を待つ時間に上限を設ける共通helperです。
// 上限は処理の停止（hang）を試験の失敗として検出するためだけに使い、正常な実行時間の
// 測定には使いません。
//
// testing/synctestのbubble内ではtime packageの時計が仮想時間になるため、同じhelperの
// 上限もbubble時間で数えます。bubble内では再試行のtimerが仮想時間を進め続けても上限で
// 失敗させ、仮想時間の空転や大量の待機ログで試験が終わらなくなることを防ぎます。
// bubble外では実時間の上限で失敗させます。
package boundedwait

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	// HangTimeoutは、bubble外の試験が実時間で待つ上限です。正常な実行では到達しません。
	// 共有fixtureの終了処理では、bubble内でも同じ値を仮想時間の上限として使います。
	HangTimeout = 10 * time.Second
	// BubbleTimeoutは、bubble内の試験が仮想時間で待つ既定の上限です。本番の既定の再試行間隔
	// （5秒）より十分長く、再試行のtimerが仮想時間を進め続けても試験を短い実時間で終わらせます。
	// これより長い仮想時間を意図して待つ試験は、ReceiveWithin・CloseWithinで上限を指定します。
	BubbleTimeout = 10 * time.Minute
)

// Receiveは、bubble外でchから値を受け取るまでHangTimeoutだけ実時間で待ちます。
// 期限を過ぎたら、descriptionの処理が止まったものとして試験を失敗させます。
func Receive[T any](t testing.TB, ch <-chan T, description string) T {
	t.Helper()
	return ReceiveWithin(t, ch, HangTimeout, description)
}

// ReceiveInBubbleは、synctestのbubble内でchから値を受け取るまでBubbleTimeoutだけ仮想時間で待ちます。
func ReceiveInBubble[T any](t testing.TB, ch <-chan T, description string) T {
	t.Helper()
	return ReceiveWithin(t, ch, BubbleTimeout, description)
}

// ReceiveWithinは、chから値を受け取るまでtimeoutだけ待ちます。bubble内では仮想時間で数えます。
// 期限を過ぎたら、descriptionの処理が止まったものとして試験を失敗させます。
func ReceiveWithin[T any](t testing.TB, ch <-chan T, timeout time.Duration, description string) T {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatalf("%s did not complete within %s", description, timeout)
		var zero T
		return zero
	}
}

// Closeは、bubble外でcloseFuncを上限HangTimeoutで実行します。動作はCloseWithinと同じです。
func Close(t testing.TB, description string, closeFunc, force func() error) error {
	t.Helper()
	return CloseWithin(t, HangTimeout, description, closeFunc, force)
}

// CloseInBubbleは、synctestのbubble内でcloseFuncを上限BubbleTimeoutで実行します。
// 動作はCloseWithinと同じです。
func CloseInBubble(t testing.TB, description string, closeFunc, force func() error) error {
	t.Helper()
	return CloseWithin(t, BubbleTimeout, description, closeFunc, force)
}

// CloseWithinは、closeFuncを別goroutineで実行し、timeoutだけ待ってその戻り値を返します。
// 本番のCloseは受理済み処理の完了を期限なく待つ場合があるため、期限を過ぎたらforce
// （通常はForceClose）で中止を通知し、さらにtimeoutだけ待ってから試験を失敗させます。
// forceがnilなら中止を通知せずに失敗させます。
func CloseWithin(t testing.TB, timeout time.Duration, description string, closeFunc, force func() error) error {
	t.Helper()
	failure, err := closeWithin(timeout, description, closeFunc, force)
	if failure != "" {
		t.Fatalf("%s", failure)
	}
	return err
}

// closeWithinはCloseWithinの本体です。期限内に戻らなかった場合は失敗の理由を返し、試験を
// 失敗させるかどうかは呼び出し元が決めます。
func closeWithin(timeout time.Duration, description string, closeFunc, force func() error) (failure string, err error) {
	closed := make(chan error, 1)
	go func() { closed <- closeFunc() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-closed:
		return "", err
	case <-timer.C:
	}
	if force == nil {
		return fmt.Sprintf("%s did not return within %s", description, timeout), nil
	}
	_ = force()
	forced := time.NewTimer(timeout)
	defer forced.Stop()
	select {
	case err := <-closed:
		return fmt.Sprintf("%s did not return within %s; it returned after ForceClose: %v", description, timeout, err), err
	case <-forced.C:
		return fmt.Sprintf("%s did not return within %s, even after ForceClose", description, timeout), nil
	}
}

// waitBackgroundShutdownは、doneが返すchannelが閉じるまでtimeoutだけ待ちます。期限内に
// 閉じなければ失敗の理由を返します。nilのchannelは完了の通知がないものとして待ちません。
func waitBackgroundShutdown(timeout time.Duration, description string, done func() <-chan struct{}) string {
	if done == nil {
		return ""
	}
	finished := done()
	if finished == nil {
		return ""
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-finished:
		return ""
	case <-timer.C:
		return fmt.Sprintf("%s: background shutdown did not finish within %s", description, timeout)
	}
}

// CloseOnCleanupは、試験の終了時にcloseFuncを上限timeoutで実行するcleanupを登録します。
// 試験が失敗またはskipした場合は、障害注入が残っていても終了処理で止まらないよう先にforceを呼び、
// 終了処理のerrorは失敗理由に重ねません。成功した試験ではcheckに終了処理のerrorを渡します。
// checkがnilならerrorを確認しません。いずれの場合も期限内に戻らなければ試験を失敗させます。
//
// forceの後のcloseFuncが後片付けの完了を待たずに戻る所有者（shutdown.Controllerを使う所有者は
// ForceClose後のCloseが直ちにErrForcedCloseを返し、後片付けの再試行を別goroutineで続けます）には
// 使わず、CloseOnCleanupUntilDoneで完了の通知を渡してください。synctestのbubble内では、残った
// 再試行のgoroutineが主goroutineの終了後の仮想時刻を待ち続け、bubbleがdeadlockのpanicで終わります。
//
// 期限切れの失敗の理由には、このcleanupを登録した位置（直接の呼び出し元とその呼び出し元）を
// 加えます。testingはcleanupの失敗を登録位置へ辿れず、特にsynctestのbubble内ではcleanupの
// closureをhelperにしてもtestingの内部の行を表示するため、位置は理由の文字列で示します。
func CloseOnCleanup(t testing.TB, timeout time.Duration, description string, closeFunc, force func() error, check func(testing.TB, error)) {
	t.Helper()
	registerCloseCleanup(t, timeout, description, registrationSites(), closeFunc, force, nil, check)
}

// CloseOnCleanupUntilDoneは、CloseOnCleanupの終了処理の後に、doneが返すchannelが閉じるまで
// 同じ上限timeoutで待ちます。試験が失敗またはskipしてforceを先に呼んだ場合も、所有者の後片付けが
// 実際に終わるまで待ち、bubble内にgoroutineを残しません。期限内に閉じなければ試験を失敗させます。
// doneには、終了処理のgoroutineが実際に戻ると閉じる通知（shutdown.Controller.Doneなど）を渡します。
func CloseOnCleanupUntilDone(t testing.TB, timeout time.Duration, description string, closeFunc, force func() error, done func() <-chan struct{}, check func(testing.TB, error)) {
	t.Helper()
	registerCloseCleanup(t, timeout, description, registrationSites(), closeFunc, force, done, check)
}

// registerCloseCleanupは、CloseOnCleanupとCloseOnCleanupUntilDoneの共通の本体です。
func registerCloseCleanup(t testing.TB, timeout time.Duration, description, site string, closeFunc, force func() error, done func() <-chan struct{}, check func(testing.TB, error)) {
	t.Cleanup(func() {
		aborting := t.Failed() || t.Skipped()
		if aborting && force != nil {
			_ = force()
		}
		failure, err := closeWithin(timeout, description, closeFunc, force)
		// 期限切れでforceした場合も、後片付けの完了を待ってからgoroutineの残りを報告します。
		if pending := waitBackgroundShutdown(timeout, description, done); pending != "" {
			failure = strings.TrimPrefix(failure+"; "+pending, "; ")
		}
		if failure != "" {
			t.Fatalf("%s (cleanup registered at %s)", failure, site)
		}
		if !aborting && check != nil {
			check(t, err)
		}
	})
}

// registrationSitesは、CloseOnCleanup・CloseOnCleanupUntilDoneを呼んだ試験側の位置を、直接の
// 呼び出し元から二件まで「file:line <- file:line」の形で返します。fixtureから呼ばれた場合は、
// fixtureとそれを呼んだ試験の位置になります。testing・runtimeなど標準libraryの内部のframeは
// 含めません。
func registrationSites() string {
	var pcs [16]uintptr
	// runtime.Callers・registrationSites・公開している登録関数の三つを飛ばします。
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	sites := make([]string, 0, 2)
	for len(sites) < 2 {
		frame, more := frames.Next()
		if frame.File != "" && !standardLibraryFrame(frame.Function) {
			sites = append(sites, filepath.Base(frame.File)+":"+strconv.Itoa(frame.Line))
		}
		if !more {
			break
		}
	}
	if len(sites) == 0 {
		return "unknown location"
	}
	return strings.Join(sites, " <- ")
}

// standardLibraryFrameは、関数名が試験の実行を担う標準libraryのものかを返します。
func standardLibraryFrame(function string) bool {
	for _, prefix := range []string{"testing.", "testing/", "runtime.", "internal/"} {
		if strings.HasPrefix(function, prefix) {
			return true
		}
	}
	return false
}

// NoErrorは、CloseOnCleanupのcheckとして使い、終了処理がerrorを返さなかったことを確認します。
func NoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("unexpected close error: %v", err)
	}
}

// ForceCloserOfは、vがForceCloseを持てばそのmethodを返し、持たなければnilを返します。
// interface型のStoreなど、ForceCloseの有無が型で決まらない値をCloseWithinへ渡すために使います。
func ForceCloserOf(v any) func() error {
	if forcer, ok := v.(interface{ ForceClose() error }); ok {
		return forcer.ForceClose
	}
	return nil
}

// reportedExtraSendsは、SendOrReportが予定外の値を報告済みのchannelを記録します。
var reportedExtraSends sync.Map

// SendOrReportは、chへvalueを待たずに送ります。試験の受け取り手が予定の数だけ受け取った後の
// 予定外の値は、そのchannelで最初の1件だけを試験の失敗として記録し、以降は捨てます。
// 再試行が止まらない退行でも、送り手のgoroutine（多くは本番のcallback）を止めず、
// 失敗のlogも再試行の回数だけ増やしません。
func SendOrReport[T any](t testing.TB, ch chan<- T, value T, description string) {
	select {
	case ch <- value:
		return
	default:
	}
	if _, reported := reportedExtraSends.LoadOrStore(ch, struct{}{}); reported {
		return
	}
	t.Errorf("unexpected additional %s: %+v (later ones are not reported)", description, value)
}

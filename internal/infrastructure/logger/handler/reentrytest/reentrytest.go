// Package reentrytestは、内部ログの通知先（OnError・OnWarn・OnInfo・OnDebug）が同じloggerへ
// 書き込む再入の試験に共通する、書き込み方法と通知の数え方を提供します。試験からだけ使います。
package reentrytest

import (
	"context"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"
)

// HangTimeoutは、別のgoroutineへ渡した書き込みの停止（hang）を検出するまで待つ上限です。
// 正常な処理に必要な時間の見積もりではなく、失敗した試験を待ち続けないための上限です。
const HangTimeout = 10 * time.Second

// Modeは、通知先が同じloggerへ書き込む方法です。
type Mode struct {
	Name string
	// PropagateContextは、通知先が受け取ったcontextで書き込むかです。falseなら
	// context.Backgroundで書き込みます（slogのlogger.Error(msg)などと同じです）。
	PropagateContext bool
	// HandOffは、書き込みを別のgoroutineへ渡して完了を同期的に待つかです。
	HandOff bool
}

// Modesは、再入を判別できるcontextの引き継ぎとgoroutineの組み合わせです。
// 書き込みを別のgoroutineへ渡し、さらにcontextも捨てる組み合わせは含めません。contextの印でも
// 同じgoroutineでも判別できず、設計上その通知は再帰するためです（公開APIでは作れない形です）。
var Modes = []Mode{
	{Name: "same_goroutine/propagate_context", PropagateContext: true},
	{Name: "same_goroutine/drop_context"},
	{Name: "hand_off/propagate_context", PropagateContext: true, HandOff: true},
}

// HandlerNestingDepthは、通知先が同じloggerへ書き込んだ場合の通知の入れ子の深さです。再入した
// 書き込みの内部ログは通知しないため、modeによらず1です。同じ理由で、通知の回数も元の書き込みが
// 同期的に発生させた内部ログの件数と同じです。
const HandlerNestingDepth int32 = 1

// Writeは通知先のgoroutineから呼び、modeに従ったcontextでwriteを実行します。HandOffなら
// 別のgoroutineでwriteを実行して完了を同期的に待ち、handOffTimeout以内に戻らなければ試験を
// 失敗させて待機をやめます。試験が別の理由で先に終わった場合は、報告せずに待機をやめます。
func (m Mode) Write(t testing.TB, received context.Context, write func(context.Context)) {
	if t.Context().Err() != nil {
		// 試験が既に終わっていれば（停止を検出して失敗した後など）、書き込みを続けません。
		return
	}
	ctx := received
	if !m.PropagateContext {
		ctx = context.Background()
	}
	if !m.HandOff {
		write(ctx)
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		write(ctx)
	}()
	timer := time.NewTimer(handOffTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		if t.Context().Err() == nil {
			t.Errorf("the handed-off write did not finish within %s", handOffTimeout)
		}
	case <-t.Context().Done():
	}
}

// handOffTimeoutは、別のgoroutineへ渡した書き込みを待つ上限です。test goroutineが
// CompleteWithinで同じ停止を先に検出し、試験の終了後にこのgoroutineから報告しないよう、
// HangTimeoutより長くします。
const handOffTimeout = 2 * HangTimeout

// Probeは、通知先の呼び出し回数と、同じloggerへの書き込みを挟んだ通知の入れ子の深さを
// 数えます。深さがLimitを超えたら試験を失敗させ、以後の通知では書き込みを止めさせます。
// 再入の判定が壊れていても、無限再帰でtest binaryを落とさずに失敗として報告します。
// 試験が終わった後に届いた通知でも、書き込みを止めさせます。
type Probe struct {
	t        testing.TB
	depth    atomic.Int32
	maxDepth atomic.Int32
	calls    atomic.Int32
	exceeded atomic.Bool
}

func NewProbe(t testing.TB) *Probe {
	return &Probe{t: t}
}

// Limitは、試験を失敗させる入れ子の深さです。正しい判定では入れ子は1段です。判定が壊れた
// 場合の再帰は、通知が分岐してもこの深さで止まるよう小さくします。
const Limit = 3

// Enterは通知先の開始を記録し、入れ子の深さと、同じloggerへ書き込んでよいかを返します。
// 呼び出し元はdeferでExitを呼びます。
func (p *Probe) Enter() (int32, bool) {
	p.calls.Add(1)
	depth := p.depth.Add(1)
	for {
		maxDepth := p.maxDepth.Load()
		if depth <= maxDepth || p.maxDepth.CompareAndSwap(maxDepth, depth) {
			break
		}
	}
	if p.t.Context().Err() != nil {
		// 試験の終了後（Closeの最終送信など）に届いた通知では、書き込まず報告もしません。
		return depth, false
	}
	if depth > Limit || p.exceeded.Load() {
		if p.exceeded.CompareAndSwap(false, true) {
			p.t.Errorf("handler nesting depth %d exceeded %d: writes from the handler kept dispatching", depth, Limit)
		}
		return depth, false
	}
	return depth, true
}

func (p *Probe) Exit() {
	p.depth.Add(-1)
}

func (p *Probe) Calls() int32 {
	return p.calls.Load()
}

func (p *Probe) MaxDepth() int32 {
	return p.maxDepth.Load()
}

// RequireCountsは、元の書き込みeventsPerWrite件の内部ログに対する通知回数と入れ子の深さを確認します。
// 再入した書き込みの内部ログは通知しないため、modeによらず通知回数はeventsPerWrite、入れ子の深さは
// HandlerNestingDepthです。modeは失敗の報告に使います。
func (p *Probe) RequireCounts(t testing.TB, mode Mode, eventsPerWrite int) {
	t.Helper()
	if got, want := p.MaxDepth(), HandlerNestingDepth; got != want {
		t.Fatalf("%s: handler nesting depth = %d, want %d", mode.Name, got, want)
	}
	if got, want := p.Calls(), int32(eventsPerWrite); got != want {
		t.Fatalf("%s: handler calls = %d, want %d", mode.Name, got, want)
	}
}

// CompleteWithinは、fを別のgoroutineで実行し、HangTimeout以内に戻ればその結果とtrueを返します。
// 戻らなければ試験を失敗させてfalseを返します。falseの場合、fは停止したまま残るため、
// 呼び出し元はfが保持するlockを待つ後片付け（Closeなど）を行ってはいけません。
func CompleteWithin(t testing.TB, what string, f func() error) (error, bool) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- f() }()
	timer := time.NewTimer(HangTimeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err, true
	case <-timer.C:
		t.Errorf("%s did not finish within %s", what, HangTimeout)
		return nil, false
	}
}

// DiscardStandardLogは、直接の書き込みの失敗を標準loggerへ残す経路の出力を、試験の間だけ
// 捨てます。標準loggerは全体で共有されるため、t.Parallelの試験では使いません。
func DiscardStandardLog(t testing.TB) {
	t.Helper()
	previous := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(previous) })
}

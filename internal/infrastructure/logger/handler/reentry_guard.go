package handler

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ReentryGuardは、一つのlogger instanceが内部ログ通知の中から再び書き込まれた場合を
// 判別します。
//
// Dispatchは通知先へ渡すcontextへこのinstance固有の印を付け、通知を実行するgoroutineを
// 記録します。Reenteredは、次のどちらかに当たる書き込みを再入と判定します。
//  1. contextの印: 通知先が受け取ったcontext（またはその派生）で書き込んだ場合です。
//     通知先が別のgoroutineで書き込む場合（組み込みのfan-outなど）も判別できます。
//  2. 同じgoroutine: 通知先がcontextを捨て（context.Backgroundなど）、通知を実行している
//     goroutineで書き込んだ場合です。
//
// どちらにも当たらない書き込みは、同時に実行中の通知がいくつあっても再入とはみなさず、
// 内部ログを通常どおり通知します。判別できないのは、独自の通知先がcontextを捨て、さらに
// 書き込みを別のgoroutineへ渡してその完了を同期的に待つ場合だけです。この場合は通知が
// 再帰します。公開APIの通知先は組み込みのものだけで、この形を作れません。
//
// 呼び出し元は、再入と判定した書き込みで同期的に発生した内部ログを同じ通知先へ再び渡さず、
// loggerの出力へ直接記録します。別のloggerの印・goroutineとは区別します。
//
// zero値のまま使用できます。最初の使用後はcopyしてはいけません。
type ReentryGuard struct {
	goroutines goroutineDepths
}

// reentryMarkKeyは、guardごとに異なるcontextのkeyです。
type reentryMarkKey struct {
	guard *ReentryGuard
}

// anyReentryMarkKeyは、いずれかのguardのDispatch中であることを表すcontextのkeyです。
type anyReentryMarkKey struct{}

// Dispatchはdispatchを、このguardの印を付けたcontextで呼び出します。dispatchが
// 戻るまでは、呼び出し元goroutineを通知の実行中として記録します。dispatchがpanicした
// 場合も記録を戻します。ctxがnilの場合はcontext.Backgroundに印を付けます。
func (g *ReentryGuard) Dispatch(ctx context.Context, dispatch func(context.Context)) {
	if ctx == nil {
		ctx = context.Background()
	}
	if id, ok := currentGoroutineID(); ok {
		g.goroutines.enter(id)
		defer g.goroutines.leave(id)
		dispatchingGoroutines.enter(id)
		defer dispatchingGoroutines.leave(id)
	}
	if !g.marked(ctx) {
		ctx = context.WithValue(ctx, reentryMarkKey{guard: g}, struct{}{})
	}
	if ctx.Value(anyReentryMarkKey{}) == nil {
		ctx = context.WithValue(ctx, anyReentryMarkKey{}, struct{}{})
	}
	dispatch(ctx)
}

// Reenteredは、ctxで行う書き込みが同期的に発生させた内部ログを、通知せずloggerの出力へ
// 直接記録すべきかを返します。ctxがこのguardのDispatch中に通知先へ渡したcontext
// （またはその派生）である場合と、呼び出し元goroutineがこのguardのDispatchを実行中の場合に
// trueです。実行中の通知の数では判定しません。
// このguardの通知が一件も実行中でなければgoroutine idを取得しません。
func (g *ReentryGuard) Reentered(ctx context.Context) bool {
	if g.marked(ctx) || g.goroutines.containsCurrent() {
		reentryDetections.Add(1)
		return true
	}
	return false
}

// reentryDetectionsは、プロセス全体でReenteredとInInternalLogDispatchがtrueを返した回数です。
// 再入を検出した場合だけ増やすため、通常の書き込みと通知には追加の処理はありません。
var reentryDetections atomic.Uint64

// ReentryDetectionsは、プロセス全体でReenteredとInInternalLogDispatchが再入を検出した
// （trueを返した）回数の累計を返します。試験の観測専用です。本番の配線で構築した通知先は
// 通知元loggerの通常の書き込み経路へ戻らないため、その構成ではこの値が増えないことを
// 試験で確認します。
func ReentryDetections() uint64 {
	return reentryDetections.Load()
}

func (g *ReentryGuard) marked(ctx context.Context) bool {
	return ctx != nil && ctx.Value(reentryMarkKey{guard: g}) != nil
}

// InInternalLogDispatchは、ctxがいずれかのloggerのDispatch中に通知先へ渡したcontext
// （またはその派生）であるか、呼び出し元goroutineがいずれかのloggerのDispatchを実行中かを
// 返します。内部ログ通知の中から行われた書き込みを、loggerの外側（非同期loggerの同時実行数の
// 待機など）で判別するために使います。どの通知も実行中でなければgoroutine idを取得しません。
func InInternalLogDispatch(ctx context.Context) bool {
	if (ctx != nil && ctx.Value(anyReentryMarkKey{}) != nil) || dispatchingGoroutines.containsCurrent() {
		reentryDetections.Add(1)
		return true
	}
	return false
}

// deferredNotificationsKeyは、guardごとに異なる保留中の通知のcontextのkeyです。
type deferredNotificationsKey struct {
	guard *ReentryGuard
}

// deferredNotificationsは、lockの保持中に発生した通知を発生順に保留します。
type deferredNotifications struct {
	mu      sync.Mutex
	closed  bool
	pending []func()
}

// enqueueはnotificationを保留します。runが保留を終えた後はfalseを返し、保留しません。
func (d *deferredNotifications) enqueue(notification func()) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return false
	}
	d.pending = append(d.pending, notification)
	return true
}

// runは保留した通知を発生順に実行し、実行中に保留された通知も続けて実行します。保留が
// なくなった時点で保留を終え、以後の通知はその場で実行させます。
func (d *deferredNotifications) run() {
	for {
		d.mu.Lock()
		if len(d.pending) == 0 {
			d.closed = true
			d.mu.Unlock()
			return
		}
		next := d.pending[0]
		d.pending[0] = nil
		d.pending = d.pending[1:]
		d.mu.Unlock()
		next()
	}
}

// DeferNotificationsは、このguardのCall*Handlerを返したcontextで呼んだ通知を、その場では
// 実行せず発生順に保留します。呼び出し元はlockを解放した後で返した関数を一度だけ呼び、
// 保留した通知を発生順に実行します。lockを保持したまま通知すると、通知の中から同じloggerへの
// 書き込みが同じlockを待って停止するため、lockの範囲で発生する通知に使います。
func (g *ReentryGuard) DeferNotifications(ctx context.Context) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	deferred := &deferredNotifications{}
	return context.WithValue(ctx, deferredNotificationsKey{guard: g}, deferred), deferred.run
}

// callは、ctxにこのguardの保留があれば通知を保留し、なければその場で通知します。
func (g *ReentryGuard) call(
	ctx context.Context,
	entry *logging.LogEntry,
	direct DirectInternalLogFunc,
	dispatch func(context.Context),
) {
	if ctx != nil {
		if deferred, ok := ctx.Value(deferredNotificationsKey{guard: g}).(*deferredNotifications); ok &&
			deferred.enqueue(func() { g.callNow(ctx, entry, direct, dispatch) }) {
			return
		}
	}
	g.callNow(ctx, entry, direct, dispatch)
}

func (g *ReentryGuard) callNow(
	ctx context.Context,
	entry *logging.LogEntry,
	direct DirectInternalLogFunc,
	dispatch func(context.Context),
) {
	if g.Reentered(ctx) {
		writeInternalLogDirectly(direct, ctx, entry)
		return
	}
	g.Dispatch(ctx, dispatch)
}

// DirectInternalLogFuncは、再入した書き込みが発生させた内部ログを、通知先を呼ばずに
// loggerの出力へ直接書き込みます。直接の書き込みは内部ログを通知してはいけません。
type DirectInternalLogFunc func(context.Context, *logging.LogEntry)

// CallErrorHandlerは、ctxがこのguardの再入でなければ、DispatchしたcontextでhandlerへOnErrorの
// 通知を行います。handlerがnilなら再入も起きないため、判定もgoroutine idの取得もせず、
// CallErrorHandlerIfPresentと同じく診断を一度だけ残します。
// 再入ならhandlerを呼ばず、directへentryを渡します。directがnilなら記録しません。
// ctxがDeferNotificationsのcontextなら、判定と通知を保留の実行時まで遅らせます。
// 書き込み処理の中から同期的に呼ばれる通知と、background処理の通知の両方で使えます。
func (g *ReentryGuard) CallErrorHandler(
	handler func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter),
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	direct DirectInternalLogFunc,
) {
	if handler == nil {
		// 通知先がなければ再入も起きないため、判定せず従来どおりの診断だけを残します。
		CallErrorHandlerIfPresent(handler, ctx, err, entry, tracer, meter)
		return
	}
	g.call(ctx, entry, direct, func(ctx context.Context) {
		CallErrorHandlerIfPresent(handler, ctx, err, entry, tracer, meter)
	})
}

// CallWarnHandlerは、CallErrorHandlerと同じ規則でOnWarnへ通知します。
func (g *ReentryGuard) CallWarnHandler(
	handler func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter),
	ctx context.Context,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	direct DirectInternalLogFunc,
) {
	if handler == nil {
		CallWarnHandlerIfPresent(handler, ctx, entry, tracer, meter)
		return
	}
	g.call(ctx, entry, direct, func(ctx context.Context) {
		CallWarnHandlerIfPresent(handler, ctx, entry, tracer, meter)
	})
}

// CallInfoHandlerは、CallErrorHandlerと同じ規則でOnInfoへ通知します。
func (g *ReentryGuard) CallInfoHandler(
	handler func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter),
	ctx context.Context,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	direct DirectInternalLogFunc,
) {
	if handler == nil {
		CallInfoHandlerIfPresent(handler, ctx, entry, tracer, meter)
		return
	}
	g.call(ctx, entry, direct, func(ctx context.Context) {
		CallInfoHandlerIfPresent(handler, ctx, entry, tracer, meter)
	})
}

// CallDebugHandlerは、CallErrorHandlerと同じ規則でOnDebugへ通知します。
func (g *ReentryGuard) CallDebugHandler(
	handler func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter),
	ctx context.Context,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
	isDebugMode bool,
	direct DirectInternalLogFunc,
) {
	if handler == nil {
		CallDebugHandlerIfPresent(handler, ctx, entry, tracer, meter, isDebugMode)
		return
	}
	g.call(ctx, entry, direct, func(ctx context.Context) {
		CallDebugHandlerIfPresent(handler, ctx, entry, tracer, meter, isDebugMode)
	})
}

func writeInternalLogDirectly(direct DirectInternalLogFunc, ctx context.Context, entry *logging.LogEntry) {
	if direct == nil || entry == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	direct(ctx, entry)
}

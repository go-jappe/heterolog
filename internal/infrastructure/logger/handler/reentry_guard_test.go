package handler

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type reentryTestKey struct{}

// reentryHangTimeoutは、別goroutineの処理の停止（hang）を検出するまで待つ上限です。
const reentryHangTimeout = 10 * time.Second

// runOnOtherGoroutineは、fを別のgoroutineで実行し、その結果を同期的に待ちます。
// reentryHangTimeout以内に戻らなければfalseを返します。
func runOnOtherGoroutine[T any](f func() T) (T, bool) {
	result := make(chan T, 1)
	go func() { result <- f() }()
	select {
	case value := <-result:
		return value, true
	case <-time.After(reentryHangTimeout):
		var zero T
		return zero, false
	}
}

// onOtherGoroutineは、test goroutineからrunOnOtherGoroutineを呼び、停止を試験の失敗にします。
func onOtherGoroutine[T any](t *testing.T, f func() T) T {
	t.Helper()
	value, ok := runOnOtherGoroutine(f)
	if !ok {
		t.Fatalf("the other goroutine did not finish within %s", reentryHangTimeout)
	}
	return value
}

func TestReentryGuardMarksDispatchContextAndItsDescendants(t *testing.T) {
	var guard, other ReentryGuard
	parent := context.WithValue(context.Background(), reentryTestKey{}, "parent value")
	if guard.Reentered(parent) || guard.Reentered(nil) {
		t.Fatal("a context outside Dispatch must not be reported as reentered")
	}

	called := false
	guard.Dispatch(parent, func(ctx context.Context) {
		called = true
		if got := ctx.Value(reentryTestKey{}); got != "parent value" {
			t.Fatalf("Dispatch dropped the caller's context value: got %v", got)
		}
		cancelable, cancel := context.WithCancel(ctx)
		defer cancel()
		derived := []context.Context{
			ctx,
			cancelable,
			context.WithoutCancel(ctx),
			context.WithValue(ctx, reentryTestKey{}, "child value"),
		}
		for i, derivedCtx := range derived {
			// 別のgoroutineで判定し、印だけで再入と判別できることを確認します。
			if !onOtherGoroutine(t, func() bool { return guard.Reentered(derivedCtx) }) {
				t.Fatalf("derived context %d lost the dispatch mark on another goroutine", i)
			}
			if !onOtherGoroutine(t, func() bool { return InInternalLogDispatch(derivedCtx) }) {
				t.Fatalf("derived context %d lost the internal-log dispatch mark", i)
			}
		}
		if onOtherGoroutine(t, func() bool { return guard.Reentered(parent) }) {
			t.Fatal("an unmarked context on another goroutine must not be reported as reentered")
		}
		// 別のloggerのguardは、このguardの印・goroutineを自身の再入とみなしません。
		if other.Reentered(ctx) {
			t.Fatal("a mark of another guard must not be reported as reentered")
		}
		other.Dispatch(ctx, func(nested context.Context) {
			if !guard.Reentered(nested) || !other.Reentered(nested) {
				t.Fatal("nested dispatch of another guard must keep both marks")
			}
		})
	})
	if !called {
		t.Fatal("Dispatch did not call dispatch")
	}
	if guard.Reentered(parent) || InInternalLogDispatch(parent) {
		t.Fatal("the caller's context and goroutine must not be reported after Dispatch returned")
	}

	guard.Dispatch(nil, func(ctx context.Context) {
		if ctx == nil || !onOtherGoroutine(t, func() bool { return guard.Reentered(ctx) }) {
			t.Fatal("Dispatch with a nil context must pass a marked context")
		}
	})
}

func TestReentryGuardDetectsTheDispatchingGoroutineWithoutTheContext(t *testing.T) {
	var guard, other ReentryGuard
	guard.Dispatch(context.Background(), func(context.Context) {
		if !guard.Reentered(context.Background()) {
			t.Fatal("a write on the dispatching goroutine must be reported as reentered without the mark")
		}
		if !InInternalLogDispatch(context.Background()) {
			t.Fatal("the dispatching goroutine must be reported as inside an internal-log dispatch")
		}
		if other.Reentered(context.Background()) {
			t.Fatal("the goroutine of another guard's dispatch must not be reported as reentered")
		}
		if onOtherGoroutine(t, func() bool { return guard.Reentered(context.Background()) }) {
			t.Fatal("an independent goroutine must not be reported as reentered")
		}
		if onOtherGoroutine(t, func() bool { return InInternalLogDispatch(context.Background()) }) {
			t.Fatal("an independent goroutine must not be reported as inside an internal-log dispatch")
		}
	})
	if guard.Reentered(context.Background()) || InInternalLogDispatch(context.Background()) {
		t.Fatal("the goroutine must not be reported after Dispatch returned")
	}
}

// TestReentryGuardBookkeepingSurvivesPanicsAndConcurrencyは、panicした通知の記録が残らないこと、
// 多数の独立した通知が同時に実行中でも、各goroutineは自身の通知だけを再入と判定し、通知の外の
// goroutineの書き込みは再入と判定されないこと（通知の数では判定しないこと）を確認します。
func TestReentryGuardBookkeepingSurvivesPanicsAndConcurrency(t *testing.T) {
	var guard ReentryGuard
	for range 16 {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("Dispatch must propagate the handler panic")
				}
			}()
			guard.Dispatch(context.Background(), func(context.Context) {
				guard.Dispatch(context.Background(), func(context.Context) { panic("handler panic") })
			})
		}()
	}
	if guard.goroutines.active.Load() != 0 || dispatchingGoroutines.active.Load() != 0 || guard.Reentered(context.Background()) {
		t.Fatal("panicking dispatches must not stay recorded")
	}

	// 全ての通知が同時に実行中になってから判定します。
	const workers = 32
	entered, release := make(chan struct{}, workers), make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			guard.Dispatch(context.Background(), func(context.Context) {
				entered <- struct{}{}
				<-release
				runtime.Gosched()
				if !guard.Reentered(context.Background()) {
					results <- errors.New("a dispatching goroutine lost its own record")
					return
				}
				results <- nil
			})
		}()
	}
	for range workers {
		select {
		case <-entered:
		case <-time.After(reentryHangTimeout):
			t.Fatal("concurrent dispatches did not start")
		}
	}
	if guard.Reentered(context.Background()) || InInternalLogDispatch(context.Background()) {
		close(release)
		t.Fatalf("a goroutine outside %d concurrent dispatches must not be reported as reentered", workers)
	}
	close(release)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if guard.goroutines.active.Load() != 0 || dispatchingGoroutines.active.Load() != 0 {
		t.Fatal("finished concurrent dispatches must not stay recorded")
	}
}

func TestReentryGuardDeferredNotificationsRunInOrderAfterRelease(t *testing.T) {
	var guard ReentryGuard
	ctx, run := guard.DeferNotifications(context.Background())
	var order []string
	record := func(name string) func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
		return func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
			order = append(order, name)
		}
	}
	entry := &logging.LogEntry{Level: pkglogger.LogLevelWarn, Message: "internal log", Internal: true}
	guard.CallWarnHandler(func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
		order = append(order, "first")
		// 保留を実行している間に同じcontextで発生した通知も、発生順に続けて実行します。
		guard.CallInfoHandler(record("raised by first"), ctx, entry, nil, nil, nil)
	}, ctx, entry, nil, nil, nil)
	guard.CallErrorHandler(func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
		order = append(order, "second")
	}, ctx, errors.New("internal error"), entry, nil, nil, nil)
	if len(order) != 0 {
		t.Fatalf("notifications ran before release: %v", order)
	}
	run()
	want := []string{"first", "second", "raised by first"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("deferred notification order = %v, want %v", order, want)
	}
	// 保留の実行後は、同じcontextの通知もその場で実行します。
	guard.CallDebugHandler(record("after release"), ctx, entry, nil, nil, false, nil)
	if order[len(order)-1] != "after release" {
		t.Fatalf("a notification after release was not run immediately: %v", order)
	}
}

func TestReentryGuardCallHandlersWriteReenteredInternalLogsDirectly(t *testing.T) {
	type call func(guard *ReentryGuard, ctx context.Context, record func(string, context.Context), direct DirectInternalLogFunc)
	entry := &logging.LogEntry{Level: pkglogger.LogLevelError, Message: "internal log", Internal: true}
	for name, invoke := range map[string]call{
		"error": func(guard *ReentryGuard, ctx context.Context, record func(string, context.Context), direct DirectInternalLogFunc) {
			guard.CallErrorHandler(func(ctx context.Context, _ error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				record("handler", ctx)
			}, ctx, errors.New("internal error"), entry, nil, nil, direct)
		},
		"warn": func(guard *ReentryGuard, ctx context.Context, record func(string, context.Context), direct DirectInternalLogFunc) {
			guard.CallWarnHandler(func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				record("handler", ctx)
			}, ctx, entry, nil, nil, direct)
		},
		"info": func(guard *ReentryGuard, ctx context.Context, record func(string, context.Context), direct DirectInternalLogFunc) {
			guard.CallInfoHandler(func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				record("handler", ctx)
			}, ctx, entry, nil, nil, direct)
		},
		"debug": func(guard *ReentryGuard, ctx context.Context, record func(string, context.Context), direct DirectInternalLogFunc) {
			guard.CallDebugHandler(func(ctx context.Context, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				record("handler", ctx)
			}, ctx, entry, nil, nil, false, direct)
		},
	} {
		t.Run(name, func(t *testing.T) {
			var guard ReentryGuard
			var calls []string
			var handlerCtx context.Context
			record := func(kind string, ctx context.Context) {
				calls = append(calls, kind)
				if kind == "handler" {
					handlerCtx = ctx
				}
			}
			direct := func(_ context.Context, got *logging.LogEntry) {
				if got != entry {
					t.Fatalf("direct writer received %v, want the internal log entry", got)
				}
				record("direct", nil)
			}

			invoke(&guard, context.Background(), record, direct)
			if len(calls) != 1 || calls[0] != "handler" || !guard.marked(handlerCtx) {
				t.Fatalf("calls outside a dispatch = %v, want one handler call with a marked context", calls)
			}
			// handlerが受け取ったcontextでの内部ログは、handlerを呼ばず直接書き込みます。
			invoke(&guard, handlerCtx, record, direct)
			if len(calls) != 2 || calls[1] != "direct" {
				t.Fatalf("calls with the handler's context = %v, want a direct write", calls)
			}
			// 直接の書き込み先がない場合は記録せず、handlerも呼びません。
			invoke(&guard, handlerCtx, record, nil)
			if len(calls) != 2 {
				t.Fatalf("calls without a direct writer = %v, want no additional call", calls)
			}
		})
	}
}

// TestReentryDetectionsCountsOnlyDetectedReentryは、ReentryDetectionsがReenteredと
// InInternalLogDispatchがtrueを返した場合だけ増え、再入でない判定では増えないことを確認します。
// 本番の配線の試験は、この値が変わらないことで通知先が再入しないことを確認します。
func TestReentryDetectionsCountsOnlyDetectedReentry(t *testing.T) {
	var guard ReentryGuard
	before := ReentryDetections()
	if guard.Reentered(context.Background()) || InInternalLogDispatch(context.Background()) {
		t.Fatal("no dispatch is in flight, so nothing may be detected as re-entry")
	}
	if got := ReentryDetections(); got != before {
		t.Fatalf("ReentryDetections() = %d after decisions without re-entry, want %d", got, before)
	}

	guard.Dispatch(context.Background(), func(ctx context.Context) {
		if !guard.Reentered(ctx) {
			t.Error("a write with the dispatch's context must be detected as re-entry")
		}
		if !InInternalLogDispatch(ctx) {
			t.Error("a write with the dispatch's context must be detected as inside an internal log dispatch")
		}
	})
	if got, want := ReentryDetections(), before+2; got != want {
		t.Fatalf("ReentryDetections() = %d after two detections, want %d", got, want)
	}
}

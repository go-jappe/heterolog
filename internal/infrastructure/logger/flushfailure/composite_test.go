package flushfailure

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestNewCompositeFailureHandlerRejectsInvalidMaxConcurrent(t *testing.T) {
	for _, maxConcurrent := range []int{0, -1} {
		if _, err := NewCompositeFailureHandler(nil, maxConcurrent); err == nil {
			t.Fatalf("NewCompositeFailureHandler(maxConcurrent=%d) error = nil", maxConcurrent)
		}
	}
}

func TestNewReportingCompositeFailureHandlerRejectsInvalidMaxConcurrent(t *testing.T) {
	for _, maxConcurrent := range []int{0, -1} {
		if _, err := NewReportingCompositeFailureHandler(nil, maxConcurrent); err == nil {
			t.Fatalf(
				"NewReportingCompositeFailureHandler(maxConcurrent=%d) error = nil",
				maxConcurrent,
			)
		}
	}
}

func mustNewCompositeFailureHandler(
	t *testing.T,
	handlers []FailureHandler,
	maxConcurrent int,
) *CompositeFailureHandler {
	t.Helper()
	handler, err := NewCompositeFailureHandler(handlers, maxConcurrent)
	if err != nil {
		t.Fatalf("NewCompositeFailureHandler() error = %v", err)
	}
	return handler
}

func mustNewReportingCompositeFailureHandler(
	t *testing.T,
	handlers []FailureHandlerWithReport,
	maxConcurrent int,
) *ReportingCompositeFailureHandler {
	t.Helper()
	handler, err := NewReportingCompositeFailureHandler(handlers, maxConcurrent)
	if err != nil {
		t.Fatalf("NewReportingCompositeFailureHandler() error = %v", err)
	}
	return handler
}

// failureHandlerHangTimeoutは、終了処理が止まったことを検出する上限です。完了は通知で確認し、
// この時間は停止の検出だけに使います。
const failureHandlerHangTimeout = 10 * time.Second

// drainWithinHangTimeoutForTestは、停止の検出だけに使う期限付きでDrainを呼びます。
func drainWithinHangTimeoutForTest(handler interface{ Drain(context.Context) error }) error {
	ctx, cancel := context.WithTimeout(context.Background(), failureHandlerHangTimeout)
	defer cancel()
	return handler.Drain(ctx)
}

// shutdownWithinHangTimeoutForTestは、停止の検出だけに使う期限付きでShutdownを呼びます。
func shutdownWithinHangTimeoutForTest(handler interface{ Shutdown(context.Context) error }) error {
	ctx, cancel := context.WithTimeout(context.Background(), failureHandlerHangTimeout)
	defer cancel()
	return handler.Shutdown(ctx)
}

// newReleaseForTestは、試験が止めた処理を再開させるchannelと再開関数を返します。
// 試験が途中で失敗しても止めた処理が終わるよう、再開関数をt.Cleanupへ登録します。
func newReleaseForTest(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	t.Cleanup(resume)
	return release, resume
}

type failureHandlerFunc func(context.Context, loggerfailure.SendResult) error

func (f failureHandlerFunc) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	return f(ctx, result)
}

type lifecycleFailureHandler struct {
	shutdown func(context.Context) error
}

func (h lifecycleFailureHandler) HandleFailure(
	context.Context,
	loggerfailure.SendResult,
) error {
	return nil
}

func (h lifecycleFailureHandler) Shutdown(ctx context.Context) error {
	return h.shutdown(ctx)
}

type trackedLifecycleFailureHandler struct {
	handle   func(context.Context, loggerfailure.SendResult) error
	shutdown func(context.Context) error
}

func (h trackedLifecycleFailureHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	return h.handle(ctx, result)
}

func (h trackedLifecycleFailureHandler) Shutdown(ctx context.Context) error {
	return h.shutdown(ctx)
}

func TestCompositeFailureHandlerDrainShutsDownLifecycleChildrenWithoutDispatch(t *testing.T) {
	var mu sync.Mutex
	shutdownCalls := 0
	composite := mustNewCompositeFailureHandler(t,
		[]FailureHandler{lifecycleFailureHandler{
			shutdown: func(context.Context) error {
				mu.Lock()
				shutdownCalls++
				mu.Unlock()
				return nil
			},
		}},
		1,
	)

	if err := drainWithinHangTimeoutForTest(composite); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if shutdownCalls != 1 {
		t.Fatalf("Shutdown calls = %d, want 1", shutdownCalls)
	}
}

func TestCompositeFailureHandlerDrainErrorIdentifiesHandlerType(t *testing.T) {
	shutdownErr := errors.New("shutdown failed")
	handler := lifecycleFailureHandler{
		shutdown: func(context.Context) error {
			return shutdownErr
		},
	}
	composite := mustNewCompositeFailureHandler(t, []FailureHandler{handler}, 1)

	err := drainWithinHangTimeoutForTest(composite)
	if !errors.Is(err, shutdownErr) {
		t.Fatalf("Drain() error = %v, want %v", err, shutdownErr)
	}
	requireFailureHandlerError(t, err, 0, handler, false, FailureHandlerOperationShutdown)
}

func TestCompositeFailureHandlerWaitsForChildrenAndJoinsErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseChildren := func() { releaseOnce.Do(func() { close(release) }) }
		// 試験が途中で失敗しても、止めたchildを再開してから終了します。
		t.Cleanup(releaseChildren)
		started := make(chan struct{}, 2)
		firstErr := errors.New("first failed")
		secondErr := errors.New("second failed")

		newHandler := func(err error) FailureHandler {
			return failureHandlerFunc(func(context.Context, loggerfailure.SendResult) error {
				started <- struct{}{}
				<-release
				return err
			})
		}
		composite := mustNewCompositeFailureHandler(t,
			[]FailureHandler{newHandler(firstErr), newHandler(secondErr)},
			2,
		)

		done := make(chan error, 1)
		go func() {
			done <- composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
		}()

		// 二つのchildが並行に開始し、どちらも完了していない間はHandleFailureが戻りません。
		synctest.Wait()
		if got := len(started); got != 2 {
			t.Fatalf("started child handlers = %d, want 2 in parallel", got)
		}
		select {
		case err := <-done:
			t.Fatalf("HandleFailure returned before child completion: %v", err)
		default:
		}

		releaseChildren()
		err := <-done
		if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
			t.Fatalf("HandleFailure error = %v, want both child errors", err)
		}
	})
}

// 子handlerのpanicは、位置・型・処理と回復した値を持つFailureHandlerErrorとして返します。
// 文面の書式はTestFailureHandlerErrorFormatで確認します。
func TestCompositeFailureHandlerPanicIdentifiesHandlerType(t *testing.T) {
	handler := failureHandlerFunc(func(context.Context, loggerfailure.SendResult) error {
		panic("handler panic")
	})
	composite := mustNewCompositeFailureHandler(t, []FailureHandler{handler}, 1)

	err := composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
	handlerErr := requireFailureHandlerError(t, err, 0, handler, false, FailureHandlerOperationPanic)
	if handlerErr.PanicValue != "handler panic" {
		t.Fatalf("PanicValue = %v, want handler panic", handlerErr.PanicValue)
	}
}

func TestCompositeFailureHandlerDrainWaitsForAcceptedHandleFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release, releaseChild := newReleaseForTest(t)
		composite := mustNewCompositeFailureHandler(t,
			[]FailureHandler{failureHandlerFunc(func(
				context.Context,
				loggerfailure.SendResult,
			) error {
				close(started)
				<-release
				return nil
			})},
			1,
		)

		handled := make(chan error, 1)
		go func() {
			handled <- composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("failure handler did not start")
		}

		drained := make(chan error, 1)
		go func() {
			drained <- composite.Drain(context.Background())
		}()
		// Drainが受理済みchildの完了待ちで停止するまで進めてから確認します。
		synctest.Wait()
		select {
		case err := <-drained:
			t.Fatalf("Drain returned before child completion: %v", err)
		default:
		}

		releaseChild()
		if err := <-handled; err != nil {
			t.Fatalf("HandleFailure() error = %v", err)
		}
		select {
		case err := <-drained:
			if err != nil {
				t.Fatalf("Drain() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Drain did not finish after child completion")
		}
	})
}

func TestCompositeFailureHandlerDrainPreservesCallerContextForShutdown(t *testing.T) {
	// callerの期限は仮想時間で進め、Drainがshutdownへ渡す前に期限切れにしません。
	synctest.Test(t, func(t *testing.T) {
		type contextKey struct{}
		const contextValue = "close-context"

		receivedValue := make(chan any, 1)
		receivedDeadline := make(chan time.Time, 1)
		composite := mustNewCompositeFailureHandler(t,
			[]FailureHandler{lifecycleFailureHandler{
				shutdown: func(ctx context.Context) error {
					receivedValue <- ctx.Value(contextKey{})
					deadline, ok := ctx.Deadline()
					if !ok {
						return errors.New("shutdown context has no deadline")
					}
					receivedDeadline <- deadline
					<-ctx.Done()
					return ctx.Err()
				},
			}},
			1,
		)
		composite.Timeout = time.Second

		parent := context.WithValue(context.Background(), contextKey{}, contextValue)
		ctx, cancel := context.WithTimeout(parent, 20*time.Millisecond)
		callerDeadline, _ := ctx.Deadline()
		defer cancel()

		err := composite.Drain(ctx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Drain() error = %v, want caller DeadlineExceeded", err)
		}
		select {
		case got := <-receivedValue:
			if got != contextValue {
				t.Fatalf("shutdown context value = %v, want %q", got, contextValue)
			}
		default:
			t.Fatal("Drain returned without passing the caller context to shutdown")
		}
		if got := <-receivedDeadline; !got.Equal(callerDeadline) {
			t.Fatalf("shutdown deadline = %s, want caller deadline %s", got, callerDeadline)
		}
	})
}

func TestCompositeFailureHandlerDrainRejectsCanceledContextBeforeShutdown(t *testing.T) {
	shutdownCalls := 0
	composite := mustNewCompositeFailureHandler(t,
		[]FailureHandler{lifecycleFailureHandler{
			shutdown: func(context.Context) error {
				shutdownCalls++
				return nil
			},
		}},
		1,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := composite.Drain(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain() error = %v, want context.Canceled", err)
	}
	if shutdownCalls != 0 {
		t.Fatalf("Shutdown calls = %d, want 0", shutdownCalls)
	}
}

func TestCompositeFailureHandlerDrainCanRetryAfterTimeout(t *testing.T) {
	// 処理とDrainの期限は仮想時間で進め、Drainの開始前に期限切れにしません。
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release, releaseChild := newReleaseForTest(t)
		composite := mustNewCompositeFailureHandler(t,
			[]FailureHandler{failureHandlerFunc(func(
				context.Context,
				loggerfailure.SendResult,
			) error {
				close(started)
				<-release
				return nil
			})},
			1,
		)
		composite.Timeout = 20 * time.Millisecond

		handled := make(chan error, 1)
		go func() {
			handled <- composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("failure handler did not start")
		}
		drainCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := composite.Drain(drainCtx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Drain() error = %v, want DeadlineExceeded", err)
		}
		if !errors.Is(err, ErrFailureHandlingDrainIncomplete) {
			t.Fatalf("Drain() error = %v, want timeout while waiting for accepted failure handling", err)
		}
		if err := composite.HandleFailure(context.Background(), loggerfailure.SendResult{}); !errors.Is(err, ErrFailureHandlerNotAccepting) {
			t.Fatalf("HandleFailure() after timed-out Drain error = %v, want ErrFailureHandlerNotAccepting", err)
		}

		releaseChild()
		<-handled
		if err := composite.Drain(context.Background()); err != nil {
			t.Fatalf("retry Drain() error = %v", err)
		}
	})
}

func TestCompositeFailureHandlerDrainDoesNotUseHandlingTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release, releaseShutdown := newReleaseForTest(t)
		entered := make(chan struct{})
		composite := mustNewCompositeFailureHandler(t, []FailureHandler{lifecycleFailureHandler{
			shutdown: func(ctx context.Context) error {
				if _, exists := ctx.Deadline(); exists {
					t.Error("normal shutdown inherited the failure-handling timeout")
				}
				close(entered)
				<-release
				return ctx.Err()
			},
		}}, 1)
		composite.Timeout = time.Second
		drained := make(chan error, 1)
		go func() { drained <- composite.Drain(context.Background()) }()
		<-entered
		time.Sleep(3 * composite.Timeout)
		synctest.Wait()
		select {
		case err := <-drained:
			t.Fatalf("Drain stopped waiting: %v", err)
		default:
		}
		releaseShutdown()
		if err := <-drained; err != nil {
			t.Fatal(err)
		}
	})
}

func TestCompositeFailureHandlerRejectsNewWorkAfterDrainStarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		shutdownStarted := make(chan struct{})
		releaseShutdown, resumeShutdown := newReleaseForTest(t)
		var mu sync.Mutex
		handleCalls := 0
		handler := trackedLifecycleFailureHandler{
			handle: func(context.Context, loggerfailure.SendResult) error {
				mu.Lock()
				handleCalls++
				mu.Unlock()
				return nil
			},
			shutdown: func(context.Context) error {
				close(shutdownStarted)
				<-releaseShutdown
				return nil
			},
		}
		composite := mustNewCompositeFailureHandler(t, []FailureHandler{handler}, 1)

		drained := make(chan error, 1)
		go func() {
			drained <- composite.Drain(context.Background())
		}()
		select {
		case <-shutdownStarted:
		case err := <-drained:
			t.Fatalf("Drain returned before child shutdown started: %v", err)
		}

		err := composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
		if !errors.Is(err, ErrFailureHandlerNotAccepting) {
			t.Fatalf("HandleFailure() during Drain error = %v, want ErrFailureHandlerNotAccepting", err)
		}
		mu.Lock()
		if handleCalls != 0 {
			t.Fatalf("child HandleFailure calls = %d, want 0", handleCalls)
		}
		mu.Unlock()

		resumeShutdown()
		if err := <-drained; err != nil {
			t.Fatalf("Drain() error = %v", err)
		}
		if err := composite.HandleFailure(context.Background(), loggerfailure.SendResult{}); !errors.Is(err, ErrFailureHandlerNotAccepting) {
			t.Fatalf("HandleFailure() after Drain error = %v, want ErrFailureHandlerNotAccepting", err)
		}
	})
}

func TestCompositeFailureHandlerConcurrentHandleAndDrainNeverStartsChildAfterShutdown(t *testing.T) {
	// 子の処理時間は仮想時間で進め、実時間の待機なしでDrainとの並行実行を重ねます。
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		shutdownStarted := false
		handledAfterShutdown := 0
		handler := trackedLifecycleFailureHandler{
			handle: func(context.Context, loggerfailure.SendResult) error {
				mu.Lock()
				if shutdownStarted {
					handledAfterShutdown++
				}
				mu.Unlock()
				time.Sleep(time.Millisecond)
				return nil
			},
			shutdown: func(context.Context) error {
				mu.Lock()
				shutdownStarted = true
				mu.Unlock()
				return nil
			},
		}
		composite := mustNewCompositeFailureHandler(t, []FailureHandler{handler}, 8)

		const calls = 64
		start := make(chan struct{})
		results := make(chan error, calls)
		var callers sync.WaitGroup
		callers.Add(calls)
		for range calls {
			go func() {
				defer callers.Done()
				<-start
				results <- composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
			}()
		}
		drained := make(chan error, 1)
		go func() {
			<-start
			drained <- composite.Drain(context.Background())
		}()
		close(start)

		callers.Wait()
		close(results)
		for err := range results {
			if err != nil && !errors.Is(err, ErrFailureHandlerNotAccepting) {
				t.Fatalf("concurrent HandleFailure() error = %v", err)
			}
		}
		if err := <-drained; err != nil {
			t.Fatalf("Drain() error = %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if handledAfterShutdown != 0 {
			t.Fatalf("child handlers started after Shutdown = %d, want 0", handledAfterShutdown)
		}
	})
}

type reportFailureHandler struct {
	handle func(context.Context, loggerfailure.SendResult) error
	report func(context.Context, loggerfailure.SendResult) (pkglogger.DeliveryReport, error)
}

func (h reportFailureHandler) HandleFailure(
	ctx context.Context,
	result loggerfailure.SendResult,
) error {
	if h.handle == nil {
		return nil
	}
	return h.handle(ctx, result)
}

func (h reportFailureHandler) HandleFailureWithReport(
	ctx context.Context,
	result loggerfailure.SendResult,
) (pkglogger.DeliveryReport, error) {
	return h.report(ctx, result)
}

// report対応の子handlerのpanicも、Report=trueのFailureHandlerErrorとして位置・型・値を返します。
// 文面の書式はTestFailureHandlerErrorFormatで確認します。
func TestReportingCompositeFailureHandlerPanicIdentifiesHandlerType(t *testing.T) {
	handler := reportFailureHandler{
		report: func(
			context.Context,
			loggerfailure.SendResult,
		) (pkglogger.DeliveryReport, error) {
			panic("report handler panic")
		},
	}
	composite := mustNewReportingCompositeFailureHandler(t,
		[]FailureHandlerWithReport{handler},
		1,
	)

	_, err := composite.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{},
	)
	handlerErr := requireFailureHandlerError(t, err, 0, handler, true, FailureHandlerOperationPanic)
	if handlerErr.PanicValue != "report handler panic" {
		t.Fatalf("PanicValue = %v, want report handler panic", handlerErr.PanicValue)
	}
}

func TestCompositeFailureReportAggregatesOnlyDurablesAndEvacuations(t *testing.T) {
	reportErr := errors.New("evacuation failed")
	composite := mustNewReportingCompositeFailureHandler(t, []FailureHandlerWithReport{
		reportFailureHandler{report: func(
			context.Context,
			loggerfailure.SendResult,
		) (pkglogger.DeliveryReport, error) {
			return pkglogger.DeliveryReport{
				Targets: []pkglogger.DeliveryResult{{
					Backend: "must-not-be-aggregated",
					Status:  pkglogger.DeliverySucceeded,
				}},
				Durables: []pkglogger.DurableResult{{
					Backend: "opensearch",
					Kind:    pkglogger.DurableArchive,
					Status:  pkglogger.DurableSucceeded,
				}},
			}, nil
		}},
		reportFailureHandler{report: func(
			context.Context,
			loggerfailure.SendResult,
		) (pkglogger.DeliveryReport, error) {
			return pkglogger.DeliveryReport{
				Evacuations: []pkglogger.EvacuationResult{{
					Kind:   pkglogger.EvacuationSpilled,
					Status: pkglogger.EvacuationFailed,
					Err:    reportErr,
				}},
			}, reportErr
		}},
	}, 2)

	report, err := composite.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{},
	)
	if !errors.Is(err, reportErr) {
		t.Fatalf("HandleFailureWithReport error = %v, want %v", err, reportErr)
	}
	if len(report.Targets) != 0 {
		t.Fatalf("Targets = %+v, want empty", report.Targets)
	}
	if len(report.Durables) != 1 {
		t.Fatalf("Durables = %+v, want one result", report.Durables)
	}
	if len(report.Evacuations) != 1 {
		t.Fatalf("Evacuations = %+v, want one result", report.Evacuations)
	}
}

func TestCompositeFailureReportPreservesCompletedResultsWhenSiblingTimesOut(t *testing.T) {
	// timeoutは仮想時間で進め、先に完了したchildの結果を必ず回収できるようにします。
	synctest.Test(t, func(t *testing.T) {
		completed := make(chan struct{})
		releaseSlow, releaseSlowHandler := newReleaseForTest(t)
		slowFinished := make(chan struct{})
		composite := mustNewReportingCompositeFailureHandler(t, []FailureHandlerWithReport{
			reportFailureHandler{report: func(
				context.Context,
				loggerfailure.SendResult,
			) (pkglogger.DeliveryReport, error) {
				close(completed)
				return pkglogger.DeliveryReport{
					Durables: []pkglogger.DurableResult{{
						Backend: "opensearch",
						Kind:    pkglogger.DurableArchive,
						Status:  pkglogger.DurableSucceeded,
					}},
					Evacuations: []pkglogger.EvacuationResult{{
						Kind:   pkglogger.EvacuationSpilled,
						Status: pkglogger.EvacuationSucceeded,
					}},
				}, nil
			}},
			reportFailureHandler{report: func(
				ctx context.Context,
				_ loggerfailure.SendResult,
			) (pkglogger.DeliveryReport, error) {
				defer close(slowFinished)
				<-releaseSlow
				return pkglogger.DeliveryReport{}, ctx.Err()
			}},
		}, 2)
		composite.Timeout = 30 * time.Millisecond

		report, err := composite.HandleFailureWithReport(
			context.Background(),
			loggerfailure.SendResult{},
		)

		select {
		case <-completed:
		default:
			t.Fatal("fast failure report handler did not complete")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("HandleFailureWithReport error = %v, want DeadlineExceeded", err)
		}
		if len(report.Durables) != 1 {
			t.Fatalf("Durables = %+v, want completed durable result", report.Durables)
		}
		if len(report.Evacuations) != 1 {
			t.Fatalf("Evacuations = %+v, want completed evacuation result", report.Evacuations)
		}

		releaseSlowHandler()
		select {
		case <-slowFinished:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for slow failure report handler cleanup")
		}
	})
}

func TestOnlyReportingCompositeImplementsFailureReportHandler(t *testing.T) {
	if _, ok := any(mustNewCompositeFailureHandler(t, nil, 1)).(FailureReportHandler); ok {
		t.Fatal("CompositeFailureHandler unexpectedly implements FailureReportHandler")
	}
	if _, ok := any(mustNewReportingCompositeFailureHandler(t, nil, 1)).(FailureReportHandler); !ok {
		t.Fatal("ReportingCompositeFailureHandler does not implement FailureReportHandler")
	}
}

func TestReportingCompositeFailureHandlerRejectsMissingHandlers(t *testing.T) {
	var nilComposite *ReportingCompositeFailureHandler
	if _, err := nilComposite.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{},
	); !errors.Is(err, ErrNoFailureHandler) {
		t.Fatalf("nil HandleFailureWithReport() error = %v, want ErrNoFailureHandler", err)
	}

	composite := mustNewReportingCompositeFailureHandler(t, nil, 1)
	if _, err := composite.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{},
	); !errors.Is(err, ErrNoFailureHandler) {
		t.Fatalf("empty HandleFailureWithReport() error = %v, want ErrNoFailureHandler", err)
	}
}

func TestReportingCompositeFailureHandlerRejectsNewWorkAfterDrain(t *testing.T) {
	reportCalls := 0
	composite := mustNewReportingCompositeFailureHandler(t,
		[]FailureHandlerWithReport{reportFailureHandler{
			report: func(
				context.Context,
				loggerfailure.SendResult,
			) (pkglogger.DeliveryReport, error) {
				reportCalls++
				return pkglogger.DeliveryReport{}, nil
			},
		}},
		1,
	)

	if err := drainWithinHangTimeoutForTest(composite); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	report, err := composite.HandleFailureWithReport(
		context.Background(),
		loggerfailure.SendResult{},
	)
	if !errors.Is(err, ErrFailureHandlerNotAccepting) {
		t.Fatalf("HandleFailureWithReport() error = %v, want ErrFailureHandlerNotAccepting", err)
	}
	if reportCalls != 0 {
		t.Fatalf("child HandleFailureWithReport calls = %d, want 0", reportCalls)
	}
	if len(report.Targets) != 0 || len(report.Durables) != 0 || len(report.Evacuations) != 0 {
		t.Fatalf("HandleFailureWithReport() report = %+v, want empty", report)
	}
}

func TestCompositeFailureHandlerPreservesCompletedErrorWhenSiblingTimesOut(t *testing.T) {
	// timeoutは仮想時間で進め、先に完了したchildの結果を必ず回収できるようにします。
	synctest.Test(t, func(t *testing.T) {
		completedErr := errors.New("completed child failed")
		completed := make(chan struct{})
		releaseSlow, releaseSlowHandler := newReleaseForTest(t)
		slowFinished := make(chan struct{})
		composite := mustNewCompositeFailureHandler(t, []FailureHandler{
			failureHandlerFunc(func(context.Context, loggerfailure.SendResult) error {
				close(completed)
				return completedErr
			}),
			failureHandlerFunc(func(context.Context, loggerfailure.SendResult) error {
				defer close(slowFinished)
				<-completed
				<-releaseSlow
				return nil
			}),
		}, 2)
		composite.Timeout = 30 * time.Millisecond

		err := composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
		if !errors.Is(err, completedErr) {
			t.Fatalf("HandleFailure error = %v, want completed child error", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("HandleFailure error = %v, want DeadlineExceeded", err)
		}

		releaseSlowHandler()
		select {
		case <-slowFinished:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for slow failure handler cleanup")
		}
	})
}

func TestCompositeFailureHandlerReturnsTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		composite := mustNewCompositeFailureHandler(t, []FailureHandler{
			failureHandlerFunc(func(ctx context.Context, _ loggerfailure.SendResult) error {
				<-ctx.Done()
				return ctx.Err()
			}),
		}, 1)
		composite.Timeout = 20 * time.Millisecond

		started := time.Now()
		err := composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("HandleFailure error = %v, want DeadlineExceeded", err)
		}
		if elapsed := time.Since(started); elapsed != composite.Timeout {
			t.Fatalf("HandleFailure returned after %v, want the composite timeout %v", elapsed, composite.Timeout)
		}
	})
}

func TestCompositeFailureHandlerRejectsMissingHandlers(t *testing.T) {
	var nilComposite *CompositeFailureHandler
	if err := nilComposite.HandleFailure(context.Background(), loggerfailure.SendResult{}); !errors.Is(err, ErrNoFailureHandler) {
		t.Fatalf("nil HandleFailure error = %v, want ErrNoFailureHandler", err)
	}

	composite := mustNewCompositeFailureHandler(t, nil, 1)
	if err := composite.HandleFailure(context.Background(), loggerfailure.SendResult{}); !errors.Is(err, ErrNoFailureHandler) {
		t.Fatalf("empty HandleFailure error = %v, want ErrNoFailureHandler", err)
	}
}

func TestCompositeFailureHandlerRespectsMaxConcurrent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		active := 0
		maxActive := 0
		release, releaseChildren := newReleaseForTest(t)
		started := make(chan struct{}, 2)

		handler := failureHandlerFunc(func(context.Context, loggerfailure.SendResult) error {
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			started <- struct{}{}

			<-release

			mu.Lock()
			active--
			mu.Unlock()
			return nil
		})
		composite := mustNewCompositeFailureHandler(t, []FailureHandler{handler, handler}, 1)

		done := make(chan error, 1)
		go func() {
			done <- composite.HandleFailure(context.Background(), loggerfailure.SendResult{})
		}()

		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("first handler did not start")
		}
		// 二つ目のchildがsemaphore待ちで停止するまで進めてから確認します。
		synctest.Wait()
		select {
		case <-started:
			t.Fatal("second handler started before semaphore release")
		default:
		}

		releaseChildren()
		if err := <-done; err != nil {
			t.Fatalf("HandleFailure error = %v", err)
		}
		if maxActive != 1 {
			t.Fatalf("max active handlers = %d, want 1", maxActive)
		}
	})
}

// requireFailureHandlerErrorは、errが子handlerの位置・型・処理を識別するFailureHandlerErrorであることを確認します。
func requireFailureHandlerError(
	t *testing.T,
	err error,
	index int,
	handler any,
	report bool,
	operation FailureHandlerOperation,
) *FailureHandlerError {
	t.Helper()
	var handlerErr *FailureHandlerError
	if !errors.As(err, &handlerErr) {
		t.Fatalf("error = %v, want FailureHandlerError", err)
	}
	if handlerErr.Index != index || reflect.TypeOf(handlerErr.Handler) != reflect.TypeOf(handler) ||
		handlerErr.Report != report || handlerErr.Operation != operation {
		t.Fatalf("FailureHandlerError = %+v, want index=%d handler=%T report=%t operation=%q",
			handlerErr, index, handler, report, operation)
	}
	return handlerErr
}

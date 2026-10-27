package loggerruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

type blockingRuntimeCloser struct {
	calls   atomic.Int32
	stopped atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (c *blockingRuntimeCloser) StopAccepting() { c.stopped.Store(true) }
func (c *blockingRuntimeCloser) Close() error {
	if c.calls.Add(1) == 1 {
		close(c.started)
	}
	<-c.release
	return nil
}

func TestShutdownDeadlineContinuesCleanupAndJoinsConcurrentClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		closer := &blockingRuntimeCloser{started: make(chan struct{}), release: make(chan struct{})}
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(closer.release) }) }
		defer release()
		owner := &Runtime{lifecycle: &lifecycle{closer: closer}}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if err := owner.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown = %v", err)
		}
		if !closer.stopped.Load() {
			t.Fatal("shutdown did not stop accepting")
		}
		<-closer.started
		results := make(chan error, 8)
		for range cap(results) {
			go func() { results <- owner.Close() }()
		}
		// 後片付けが止まっている間は、どの呼び出し元も待ち続ける必要があります。すべての
		// goroutineが止まるまで待つことで、この確認をschedulingに左右されないようにします。
		synctest.Wait()
		if len(results) != 0 {
			t.Fatal("Close reported completion while cleanup was still in progress")
		}
		release()
		for range cap(results) {
			select {
			case err := <-results:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not join cleanup")
			}
		}
		if got := closer.calls.Load(); got != 1 {
			t.Fatalf("Close invoked %d times", got)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		if got := closer.calls.Load(); got != 1 {
			t.Fatalf("completed Close invoked %d times", got)
		}
	})
}

type forceRuntimeCloser struct {
	*blockingRuntimeCloser
	releaseOnce sync.Once
	forceCalls  int
	forceErr    error
}

func (c *forceRuntimeCloser) finish() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *forceRuntimeCloser) ForceClose() error {
	c.forceCalls++
	c.finish()
	return c.forceErr
}

func TestForceCloseForwardsDuringCleanupAndPreservesErrors(t *testing.T) {
	for _, kind := range []string{"sync", "async", "delivery"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cause := errors.New("recovery force failure")
				closer := &forceRuntimeCloser{
					blockingRuntimeCloser: &blockingRuntimeCloser{started: make(chan struct{}), release: make(chan struct{})},
					forceErr:              errors.Join(pkgshutdown.ErrForcedClose, cause),
				}
				defer closer.finish()
				state := &lifecycle{closer: closer}
				var owner interface {
					Close() error
					ForceClose() error
				}
				switch kind {
				case "sync":
					owner = &Runtime{lifecycle: state}
				case "async":
					owner = &AsyncRuntime{lifecycle: state}
				case "delivery":
					owner = &DeliveryRuntime{lifecycle: state}
				}
				closed := make(chan error, 1)
				go func() { closed <- owner.Close() }()
				<-closer.started
				if err := owner.ForceClose(); !errors.Is(err, pkgshutdown.ErrForcedClose) || !errors.Is(err, cause) {
					t.Fatalf("ForceClose lost its result: %v", err)
				}
				if closer.forceCalls != 1 {
					t.Fatalf("underlying ForceClose calls = %d, want 1", closer.forceCalls)
				}
				if err := <-closed; err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

type retryRuntimeCloser struct{ calls int }

func (c *retryRuntimeCloser) Close() error {
	c.calls++
	if c.calls == 1 {
		return errors.New("cleanup incomplete")
	}
	return nil
}

func TestShutdownRetriesIncompleteCleanup(t *testing.T) {
	closer := &retryRuntimeCloser{}
	owner := &lifecycle{closer: closer}
	if err := owner.Close(); err == nil {
		t.Fatal("first close unexpectedly succeeded")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if closer.calls != 2 {
		t.Fatalf("Close calls = %d", closer.calls)
	}
}

package multi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	loggercommon "github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type asyncBestEffortConfigStub struct {
	timeout          time.Duration
	maxParallel      int
	maxLogEntryBytes int
}

func (c asyncBestEffortConfigStub) Timeout() time.Duration { return c.timeout }
func (c asyncBestEffortConfigStub) MaxParallel() int       { return c.maxParallel }
func (c asyncBestEffortConfigStub) MaxLogEntryBytes() int {
	if c.maxLogEntryBytes != 0 {
		return c.maxLogEntryBytes
	}
	return pkglogger.DefaultMaxLogEntryBytes
}

func TestAsyncBestEffortMultiLoggerPassesTimeoutContextToHandler(t *testing.T) {
	// handlerへ渡すcontextの期限はsynctestの仮想時刻で進めます。time.Afterはhang検出用です。
	synctest.Test(t, func(t *testing.T) {
		const timeout = 20 * time.Millisecond
		observed := make(chan error, 1)
		child := &syncHandlerStub{handleFn: func(ctx context.Context, _ *infralogging.LogEntry) error {
			<-ctx.Done()
			observed <- ctx.Err()
			return ctx.Err()
		}}
		m, err := NewAsyncBestEffortMultiLogger(
			asyncBestEffortConfigStub{timeout: timeout, maxParallel: 1},
			child,
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = m.Close() })

		started := time.Now()
		m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "blocked async"))
		select {
		case err := <-observed:
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.GreaterOrEqual(t, time.Since(started), timeout, "context must not expire before the configured timeout")
		case <-time.After(time.Minute):
			t.Fatal("child handler did not observe async timeout")
		}
		require.NoError(t, m.Close())
	})
}

func TestAsyncBestEffortMultiLoggerRecoversAndObservesHandlerPanic(t *testing.T) {
	var output bytes.Buffer
	previousOutput := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	defer func() {
		log.SetOutput(previousOutput)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	}()

	var attempts atomic.Int32
	child := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
		if attempts.Add(1) == 1 {
			panic("async child panic")
		}
		return nil
	}}
	m, err := NewAsyncBestEffortMultiLogger(
		asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
		child,
	)
	require.NoError(t, err)
	m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "panic"))
	m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "after panic"))
	require.NoError(t, m.Close())
	require.Equal(t, int32(2), child.handleCalls.Load())
	// best-effortのpanicはerrorとして返さず標準loggerへ記録するため、利用者が読む
	// 診断行にchildの型とpanic値が出力されることを確認します。
	require.Contains(t, output.String(), fmt.Sprintf("async handler=%T", child))
	require.Contains(t, output.String(), "logger panicked: async child panic")
}

func TestAsyncBestEffortMultiLoggerDiscardsReturnedHandlerError(t *testing.T) {
	childErr := errors.New("best effort child error")
	child := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
		return childErr
	}}
	m, err := NewAsyncBestEffortMultiLogger(
		asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
		child,
	)
	require.NoError(t, err)
	m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "ignored error"))
	require.NoError(t, m.Close())
}

func TestAsyncBestEffortMultiLoggerCloseWaitsForAcceptedEntries(t *testing.T) {
	// 「まだ戻らない」ことはsynctest.Waitで全goroutineの停止を待ってから確認します。
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseFirst := sync.OnceFunc(func() { close(release) })
		t.Cleanup(releaseFirst)
		firstStarted := make(chan struct{})
		first := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
			close(firstStarted)
			<-release
			return nil
		}}
		second := &syncHandlerStub{}
		third := &syncHandlerStub{}
		m, err := NewAsyncBestEffortMultiLogger(
			asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
			first,
			second,
			third,
		)
		require.NoError(t, err)

		dispatchDone := make(chan struct{})
		go func() {
			m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "accepted before close"))
			close(dispatchDone)
		}()
		<-firstStarted

		closeDone := make(chan error, 1)
		go func() { closeDone <- m.Close() }()
		synctest.Wait()
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before the accepted call completed: %v", err)
		default:
		}

		releaseFirst()
		<-dispatchDone
		require.NoError(t, <-closeDone)
		require.Equal(t, int32(1), first.handleCalls.Load())
		require.Equal(t, int32(1), second.handleCalls.Load())
		require.Equal(t, int32(1), third.handleCalls.Load())
		require.Equal(t, int32(1), first.closeCalls.Load())
		require.Equal(t, int32(1), second.closeCalls.Load())
		require.Equal(t, int32(1), third.closeCalls.Load())

		// Close開始後のentryは新規要求として受理しません。受理していれば、Wait後には
		// childの呼出しが記録されています。
		m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "after close"))
		synctest.Wait()
		require.Equal(t, int32(1), first.handleCalls.Load())
		require.Equal(t, int32(1), second.handleCalls.Load())
		require.Equal(t, int32(1), third.handleCalls.Load())
	})
}

func TestAsyncBestEffortMultiLoggerCloseRetriesFailedChildAndKeepsIntakeClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		closeErr := errors.New("first async close failed")
		var mu sync.Mutex
		failedOnce := false
		failedFirst := &syncHandlerStub{closeFn: func() error {
			mu.Lock()
			defer mu.Unlock()
			if !failedOnce {
				failedOnce = true
				return closeErr
			}
			return nil
		}}
		succeededFirst := &syncHandlerStub{}
		m, err := NewAsyncBestEffortMultiLogger(
			asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 2},
			failedFirst,
			succeededFirst,
		)
		require.NoError(t, err)

		require.ErrorIs(t, m.Close(), closeErr)
		require.Equal(t, loggercommon.LoggerCloseStateStarted, m.closeState)
		// 失敗したClose後も受付は再開しません。受理していればWait後に呼出しが記録されます。
		m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "after failed close"))
		synctest.Wait()
		require.Equal(t, int32(0), failedFirst.handleCalls.Load())
		require.Equal(t, int32(0), succeededFirst.handleCalls.Load())

		require.NoError(t, m.Close())
		require.Equal(t, loggercommon.LoggerCloseStateCompleted, m.closeState)
		require.Equal(t, int32(2), failedFirst.closeCalls.Load())
		require.Equal(t, int32(1), succeededFirst.closeCalls.Load())
	})
}

func TestAsyncBestEffortMultiLoggerWaitsForCapacityAndRunsEveryChild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseFirst := sync.OnceFunc(func() { close(release) })
		firstStarted := make(chan struct{})
		first := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
			close(firstStarted)
			<-release
			return nil
		}}
		second := &syncHandlerStub{}
		third := &syncHandlerStub{}
		m, err := NewAsyncBestEffortMultiLogger(
			asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 1},
			first,
			second,
			third,
		)
		require.NoError(t, err)
		// Cleanupは後に登録したものから実行されます。Closeは解放待ちのchildを待つため、
		// 試験が途中で失敗しても先にchildを解放するよう、解放をCloseより後に登録します。
		t.Cleanup(func() { _ = m.Close() })
		t.Cleanup(releaseFirst)

		dispatchDone := make(chan struct{})
		go func() {
			m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "all children"))
			close(dispatchDone)
		}()

		<-firstStarted
		synctest.Wait()
		select {
		case <-dispatchDone:
			t.Fatal("runAll returned while the only child execution slot was occupied")
		default:
		}

		releaseFirst()
		<-dispatchDone
		require.NoError(t, m.Close())
		require.Equal(t, int32(1), first.handleCalls.Load())
		require.Equal(t, int32(1), second.handleCalls.Load())
		require.Equal(t, int32(1), third.handleCalls.Load())
	})
}

func TestAsyncBestEffortMultiLoggerLimitsConcurrentChildHandling(t *testing.T) {
	const (
		maxParallel = 2
		entryCount  = 4
	)

	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseChildren := sync.OnceFunc(func() { close(release) })
		started := make(chan struct{}, entryCount)
		var inFlight atomic.Int32
		var peak atomic.Int32
		child := &syncHandlerStub{handleFn: func(context.Context, *infralogging.LogEntry) error {
			current := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				previous := peak.Load()
				if current <= previous || peak.CompareAndSwap(previous, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			return nil
		}}
		m, err := NewAsyncBestEffortMultiLogger(
			asyncBestEffortConfigStub{timeout: time.Second, maxParallel: maxParallel},
			child,
		)
		require.NoError(t, err)
		// 解放をCloseより後に登録し、試験が途中で失敗してもCloseより先に解放します。
		t.Cleanup(func() { _ = m.Close() })
		t.Cleanup(releaseChildren)

		var callers sync.WaitGroup
		for index := range entryCount {
			callers.Add(1)
			go func() {
				defer callers.Done()
				m.runAll(
					context.Background(),
					testLogEntry(pkglogger.LogLevelInfo, fmt.Sprintf("entry-%d", index)),
				)
			}()
		}
		for range maxParallel {
			<-started
		}
		// 上限まで開始したchildが解放待ちで止まった後も、追加のchildは開始しません。
		synctest.Wait()
		select {
		case <-started:
			t.Fatal("more child handlers ran concurrently than MaxParallel allows")
		default:
		}

		releaseChildren()
		callers.Wait()
		require.NoError(t, m.Close())
		require.Equal(t, int32(entryCount), child.handleCalls.Load())
		require.Equal(t, int32(maxParallel), peak.Load())
	})
}

// 受付とCloseが競合しても、受理済みのentryはchildのClose前に処理を終え、Close後に
// childを呼びません。どのentryが受理されるかは実行順に依存するため件数は上限だけを確認し、
// Close前後の順序とClose完了後の受付拒否を検証します。
func TestAsyncBestEffortMultiLoggerConcurrentHandleAndCloseIsRaceSafe(t *testing.T) {
	const callers = 64
	var childClosed atomic.Bool
	var handledAfterClose atomic.Int32
	child := &syncHandlerStub{
		handleFn: func(context.Context, *infralogging.LogEntry) error {
			if childClosed.Load() {
				handledAfterClose.Add(1)
			}
			return nil
		},
		closeFn: func() error {
			childClosed.Store(true)
			return nil
		},
	}
	m, err := NewAsyncBestEffortMultiLogger(
		asyncBestEffortConfigStub{timeout: time.Second, maxParallel: 8},
		child,
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "race"))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = m.Close()
	}()
	wg.Wait()
	require.NoError(t, m.Close())

	require.Zero(t, handledAfterClose.Load(), "Close must finish accepted entries before closing the child")
	require.Equal(t, int32(1), child.closeCalls.Load(), "repeated Close must not close the child again")
	handled := child.handleCalls.Load()
	require.LessOrEqual(t, handled, int32(callers))

	m.runAll(context.Background(), testLogEntry(pkglogger.LogLevelInfo, "after close"))
	m.Info(context.Background(), "after close")
	require.Equal(t, handled, child.handleCalls.Load(), "closed logger must not accept new entries")
}

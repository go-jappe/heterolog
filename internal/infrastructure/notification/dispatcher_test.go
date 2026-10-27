package notification_test

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/notification"
)

func TestDispatcherCloseAfterDrainStopsAcceptanceAndDoneWaitsForCallbacks(t *testing.T) {
	// CloseAfterDrainは受付停止の要求です。所有者の正常終了はDoneまで待つ必要があります。
	dispatcher := notification.NewDispatcher()
	t.Cleanup(dispatcher.CloseAfterDrain)
	started := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var calls []int
	dispatcher.Enqueue(func() {
		close(started)
		<-release
		calls = append(calls, 1)
	})
	waitForNotificationSignal(t, started)
	dispatcher.Enqueue(nil)
	dispatcher.Enqueue(func() {
		calls = append(calls, 2)
		panic("one failed observer must not stop later notifications")
	})
	dispatcher.Enqueue(func() { calls = append(calls, 3) })

	closed := make(chan struct{})
	go func() {
		dispatcher.CloseAfterDrain()
		close(closed)
	}()
	waitForNotificationSignal(t, closed)
	select {
	case <-dispatcher.Done():
		t.Fatal("worker stopped before the blocked callback completed")
	default:
	}
	dispatcher.Enqueue(func() { calls = append(calls, 4) })
	unblock()
	waitForNotificationSignal(t, dispatcher.Done())
	if !slices.Equal(calls, []int{1, 2, 3}) {
		t.Fatalf("calls = %v, want accepted notifications in FIFO order [1 2 3]", calls)
	}
	dispatcher.CloseAfterDrain()
}

func TestDispatcherCallbackCanCloseAndDrainRemainingNotifications(t *testing.T) {
	dispatcher := notification.NewDispatcher()
	t.Cleanup(dispatcher.CloseAfterDrain)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var calls []int
	dispatcher.Enqueue(func() {
		<-release
		dispatcher.CloseAfterDrain()
		dispatcher.CloseAfterDrain()
		dispatcher.Enqueue(func() { calls = append(calls, 3) })
		calls = append(calls, 1)
	})
	dispatcher.Enqueue(func() { calls = append(calls, 2) })
	unblock()
	waitForNotificationSignal(t, dispatcher.Done())
	if !slices.Equal(calls, []int{1, 2}) {
		t.Fatalf("calls = %v, want notifications accepted before reentrant close [1 2]", calls)
	}
}

func TestDispatcherNilReceiverDispatchesSynchronouslyAndIsolatesPanic(t *testing.T) {
	var dispatcher *notification.Dispatcher
	dispatcher.Enqueue(nil)
	calls := 0
	dispatcher.Enqueue(func() {
		calls++
		panic("failed observer")
	})
	if calls != 1 {
		t.Fatalf("calls = %d, want synchronous callback", calls)
	}
	dispatcher.Enqueue(func() { calls++ })
	if calls != 2 {
		t.Fatalf("calls = %d, want subsequent synchronous callback", calls)
	}
	dispatcher.CloseAfterDrain()
}

func TestDispatcherConcurrentEnqueueAndClose(t *testing.T) {
	dispatcher := notification.NewDispatcher()
	t.Cleanup(dispatcher.CloseAfterDrain)
	const producers = 8
	const notificationsPerProducer = 128
	var closing atomic.Bool
	guaranteed := make([]bool, producers*notificationsPerProducer)
	calls := make([]int, len(guaranteed))
	start := make(chan struct{})
	ready := make(chan struct{}, producers)
	var publishers sync.WaitGroup
	for producer := range producers {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			<-start
			for notification := range notificationsPerProducer {
				id := producer*notificationsPerProducer + notification
				dispatcher.Enqueue(func() { calls[id]++ })
				// この読み取りがfalseならCloseはまだ始まっていないため、Enqueueは
				// この通知をすでに受理しているはずです。
				guaranteed[id] = !closing.Load()
				if notification == 0 {
					ready <- struct{}{}
				}
			}
		}()
	}
	// close前に受理したgateでworkerを止め、close後の通知が受付を拒否されたかを、
	// workerが受理済み通知を排出できる間に確認します。
	gateEntered := make(chan struct{})
	gateRelease := make(chan struct{})
	releaseGate := sync.OnceFunc(func() { close(gateRelease) })
	t.Cleanup(releaseGate)
	closed := make(chan struct{})
	go func() {
		for range producers {
			<-ready
		}
		dispatcher.Enqueue(func() {
			close(gateEntered)
			<-gateRelease
		})
		closing.Store(true)
		dispatcher.CloseAfterDrain()
		close(closed)
	}()
	close(start)
	published := make(chan struct{})
	go func() {
		publishers.Wait()
		close(published)
	}()
	waitForNotificationSignal(t, published)
	waitForNotificationSignal(t, closed)
	waitForNotificationSignal(t, gateEntered)
	calledAfterClose := false
	dispatcher.Enqueue(func() { calledAfterClose = true })
	releaseGate()
	waitForNotificationSignal(t, dispatcher.Done())
	for id, count := range calls {
		if count > 1 || (guaranteed[id] && count != 1) {
			t.Fatalf("notification %d: calls = %d, accepted before close = %v", id, count, guaranteed[id])
		}
	}
	if calledAfterClose {
		t.Fatal("dispatcher accepted a notification after close")
	}
}

func waitForNotificationSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("notification dispatcher did not make progress")
	}
}

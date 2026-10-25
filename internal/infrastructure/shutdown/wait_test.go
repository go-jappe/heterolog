package shutdown

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestWaitReportsUntilCompletionWithoutDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		done := make(chan struct{})
		returned := make(chan struct{})
		var elapsed []time.Duration
		go func() {
			_ = wait(context.Background(), "checkpoint source", done, func(description string, duration time.Duration) {
				if description != "checkpoint source" {
					t.Errorf("description = %q", description)
				}
				elapsed = append(elapsed, duration)
			})
			close(returned)
		}()
		synctest.Wait()
		for range 3 {
			time.Sleep(ReportInterval)
			synctest.Wait()
		}
		if len(elapsed) != 3 {
			t.Fatalf("reports = %v", elapsed)
		}
		select {
		case <-returned:
			t.Fatal("wait returned before completion")
		default:
		}
		close(done)
		<-returned
	})
}

func TestWaitCompletedWorkDoesNotReport(t *testing.T) {
	done := make(chan struct{})
	close(done)
	// 完了と通知の契機が同時に準備済みの状態です。selectは準備済みのcaseを無作為に
	// 選ぶため、通知の契機を選んだ場合も完了を優先することを繰り返し確認します。
	for range 64 {
		ticks := make(chan time.Time, 1)
		ticks <- time.Time{}
		if err := waitTicks(context.Background(), "complete", done, ticks, func(string, time.Duration) {
			t.Fatal("reported completed work")
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := wait(context.Background(), "absent", nil, func(string, time.Duration) { t.Fatal("reported absent work") }); err != nil {
		t.Fatal(err)
	}
}

func TestWaitContextCancellationOnlyEndsWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		done := make(chan struct{})
		defer close(done)
		canceled, cancelBeforeWait := context.WithCancel(context.Background())
		cancelBeforeWait()
		public := make(chan error, 1)
		go func() { public <- WaitContext(canceled, "explicitly canceled wait", done) }()
		synctest.Wait()
		select {
		case err := <-public:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("WaitContext error = %v", err)
			}
		default:
			t.Fatal("WaitContext ignored an explicit cancellation")
		}

		ctx, cancel := context.WithCancel(context.Background())
		reports := 0
		returned := make(chan error, 1)
		go func() {
			returned <- wait(ctx, "explicitly canceled wait", done, func(string, time.Duration) { reports++ })
		}()
		synctest.Wait()
		time.Sleep(ReportInterval)
		synctest.Wait()
		if reports != 1 {
			t.Fatalf("reports before cancellation = %d, want 1", reports)
		}
		cancel()
		synctest.Wait()
		select {
		case err := <-returned:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		default:
			t.Fatal("cancellation did not end the wait while the operation was pending")
		}
		// 中止したのは待機だけです。処理が残っていても、その後は待機状況を通知しません。
		time.Sleep(3 * ReportInterval)
		synctest.Wait()
		if reports != 1 {
			t.Fatalf("reports after cancellation = %d, want 1", reports)
		}
	})
}

func TestWaitGroupWaitsForAcceptedWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var group sync.WaitGroup
		group.Add(1)
		returned := make(chan struct{})
		go func() {
			WaitGroup("accepted tasks", &group)
			close(returned)
		}()
		synctest.Wait()
		select {
		case <-returned:
			t.Fatal("wait returned while work remained")
		default:
		}
		group.Done()
		<-returned
	})
}

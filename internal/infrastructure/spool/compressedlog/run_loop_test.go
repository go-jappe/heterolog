package compressedlog

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestReplayLoopWaitsForFirstIntervalAndCompletedReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		completed := make(chan error, 1)
		defer func() {
			cancel()
			if err := <-completed; !errors.Is(err, context.Canceled) {
				t.Errorf("loop error = %v, want context.Canceled", err)
			}
		}()
		started := time.Now()
		replays := make(chan time.Time, 2)
		go func() {
			first := true
			completed <- runReplayAndPurgeLoop(ctx, 10*time.Second, func() {
				replays <- time.Now()
				if first {
					first = false
					// Upload and reporting together may exceed several intervals.
					time.Sleep(35 * time.Second)
				}
			}, 0, nil)
		}()
		synctest.Wait()
		time.Sleep(9 * time.Second)
		synctest.Wait()
		if len(replays) != 0 {
			t.Fatalf("replay count before its first interval = %d, want 0", len(replays))
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(replays) != 1 {
			t.Fatalf("replay count at first interval = %d, want 1", len(replays))
		}
		time.Sleep(35 * time.Second)
		synctest.Wait()
		if len(replays) != 1 {
			t.Fatalf("replay count immediately after slow work = %d, want 1", len(replays))
		}
		time.Sleep(9 * time.Second)
		synctest.Wait()
		if len(replays) != 1 {
			t.Fatalf("replay count before the completion interval elapsed = %d, want 1", len(replays))
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(replays) != 2 {
			t.Fatalf("replay count after the completion interval = %d, want 2", len(replays))
		}
		if first, second := <-replays, <-replays; first != started.Add(10*time.Second) || second != started.Add(55*time.Second) {
			t.Fatalf("replay times = %v, %v, want first at interval and second at completion + interval", first, second)
		}
	})
}

func TestReplayLoopKeepsIndependentPurgeInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		completed := make(chan error, 1)
		defer func() {
			cancel()
			<-completed
		}()
		var replays, purges atomic.Int32
		go func() {
			completed <- runReplayAndPurgeLoop(ctx, 10*time.Second, func() {
				replays.Add(1)
			}, 3*time.Second, func() {
				purges.Add(1)
			})
		}()
		synctest.Wait()
		time.Sleep(9 * time.Second)
		synctest.Wait()
		if replays.Load() != 0 || purges.Load() != 3 {
			t.Fatalf("before replay: replays/purges = %d/%d, want 0/3", replays.Load(), purges.Load())
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if replays.Load() != 1 || purges.Load() != 3 {
			t.Fatalf("first replay: replays/purges = %d/%d, want 1/3", replays.Load(), purges.Load())
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if replays.Load() != 1 || purges.Load() != 4 {
			t.Fatalf("after replay: replays/purges = %d/%d, want 1/4", replays.Load(), purges.Load())
		}
	})
}

func TestReplayLoopSerializesReplayAndPurge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		completed := make(chan error, 1)
		defer func() {
			cancel()
			<-completed
		}()
		releaseReplay := make(chan struct{})
		releasePurge := make(chan struct{})
		var replays, purges atomic.Int32
		go func() {
			completed <- runReplayAndPurgeLoop(ctx, 5*time.Second, func() {
				if replays.Add(1) == 1 {
					select {
					case <-releaseReplay:
					case <-ctx.Done():
					}
				}
			}, 7*time.Second, func() {
				if purges.Add(1) == 1 {
					select {
					case <-releasePurge:
					case <-ctx.Done():
					}
				}
			})
		}()
		synctest.Wait()
		time.Sleep(8 * time.Second)
		synctest.Wait()
		if replays.Load() != 1 || purges.Load() != 0 {
			t.Fatalf("while replay blocked: replays/purges = %d/%d, want 1/0", replays.Load(), purges.Load())
		}
		close(releaseReplay)
		synctest.Wait()
		if replays.Load() != 1 || purges.Load() != 1 {
			t.Fatalf("after replay: replays/purges = %d/%d, want 1/1", replays.Load(), purges.Load())
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if replays.Load() != 1 || purges.Load() != 1 {
			t.Fatalf("while purge blocked: replays/purges = %d/%d, want 1/1", replays.Load(), purges.Load())
		}
		close(releasePurge)
		synctest.Wait()
		if replays.Load() != 2 || purges.Load() != 1 {
			t.Fatalf("after purge: replays/purges = %d/%d, want 2/1", replays.Load(), purges.Load())
		}
	})
}

func TestReplayLoopCancellationSkipsPendingPurge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		completed := make(chan error, 1)
		replays, purges := 0, 0
		go func() {
			completed <- runReplayAndPurgeLoop(ctx, time.Second, func() {
				replays++
				time.Sleep(3 * time.Second)
				cancel()
			}, 2*time.Second, func() {
				purges++
			})
		}()
		if err := <-completed; !errors.Is(err, context.Canceled) {
			t.Fatalf("loop error = %v, want context.Canceled", err)
		}
		if replays != 1 || purges != 0 {
			t.Fatalf("canceled replay loop: replays/purges = %d/%d, want 1/0", replays, purges)
		}
	})
}

func TestReplayLoopRejectsInvalidIntervalsAndCanceledContext(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		if err := runReplayAndPurgeLoop(context.Background(), interval, nil, 0, nil); !errors.Is(err, ErrInvalidRunnerInterval) {
			t.Errorf("replay interval %v: error = %v, want ErrInvalidRunnerInterval", interval, err)
		}
		if err := runReplayAndPurgeLoop(context.Background(), time.Second, nil, interval, func() {}); !errors.Is(err, ErrInvalidCompressedLogFileUploadSpoolConfig) {
			t.Errorf("purge interval %v: error = %v, want ErrInvalidCompressedLogFileUploadSpoolConfig", interval, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runReplayAndPurgeLoop(ctx, time.Second, func() {
		t.Error("replay ran with an already canceled context")
	}, 0, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("canceled loop error = %v, want context.Canceled", err)
	}
}

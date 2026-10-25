package slack

import (
	"context"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestSlackIdleResetIgnoresAlreadyFiredCallback(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		for _, concurrentClose := range []bool{false, true} {
			name := slackResetKind(fallback) + "/next timer"
			if concurrentClose {
				name = slackResetKind(fallback) + "/concurrent close"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					client := &slackResetHTTPClient{}
					fixture := newSlackResetFixture(t, fallback, client)
					fixture.enqueue(t, "initial")
					synctest.Wait()
					fixture.enqueue(t, "old pending entry")

					// Hold a real expired callback immediately before its ownership
					// check, modelling a callback descheduled while reset runs.
					// Stop must return false; the callback still owns its WG.Done.
					entered, release := make(chan struct{}), make(chan struct{})
					var releaseOnce sync.Once
					t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
					schedule := &slackBatchSchedule{}
					fixture.base.mu.Lock()
					fixture.base.stopBatchTimerLocked()
					fixture.base.timerWG.Add(1)
					schedule.timer = time.AfterFunc(2*time.Hour, func() {
						defer fixture.base.timerWG.Done()
						close(entered)
						<-release
						fixture.base.runScheduledBatch(schedule, func(logs []*logging.LogEntry) {
							if fixture.fallback != nil {
								fixture.fallback.sendLogs(logs)
							} else {
								_ = fixture.base.sendLogs(context.Background(), logs)
							}
						})
					})
					fixture.base.timer = schedule.timer
					fixture.base.mu.Unlock()
					<-entered
					if schedule.timer.Stop() {
						t.Fatal("test did not reach an already-fired callback")
					}

					fixture.enqueue(t, "reset entry")
					synctest.Wait()
					client.requireCalls(t, 2, time.Now(), "old pending entry", "reset entry")
					fixture.enqueue(t, "following batch")
					var closed chan error
					if concurrentClose {
						closed = make(chan error, 1)
						go func() { closed <- fixture.close() }()
						synctest.Wait()
						select {
						case err := <-closed:
							t.Fatalf("Close returned before the fired callback completed: %v", err)
						default:
						}
					}
					releaseOnce.Do(func() { close(release) })
					synctest.Wait()
					if concurrentClose {
						if err := <-closed; err != nil {
							t.Fatal(err)
						}
					} else {
						// An obsolete callback cannot drain the following batch early
						// or cancel the timer which owns it.
						client.requireCalls(t, 2, time.Now(), "reset entry")
						time.Sleep(2 * time.Hour)
						synctest.Wait()
					}
					client.requireCalls(t, 3, time.Now(), "following batch")
					if err := fixture.close(); err != nil {
						t.Fatal(err)
					}
					time.Sleep(10 * time.Hour)
					synctest.Wait()
					client.requireExactlyOnce(t, "initial", "old pending entry", "reset entry", "following batch")
				})
			})
		}
	}
}

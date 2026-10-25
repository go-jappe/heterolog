package slack

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestSlackNotificationStateStartsImmediateThenConsumesPositiveIntervals(t *testing.T) {
	intervals := []time.Duration{time.Minute, 5 * time.Minute}
	index := 0
	immediate := true

	delay, normalizedIndex := slackNotificationDelay(intervals, index, immediate)
	if delay != 0 || normalizedIndex != 0 {
		t.Fatalf("initial delay state = (%s, %d), want (0s, 0)", delay, normalizedIndex)
	}

	index, immediate = nextSlackNotificationState(intervals, normalizedIndex, immediate)
	if index != 0 || immediate {
		t.Fatalf("state after immediate batch = (%d, %t), want (0, false)", index, immediate)
	}

	delay, index = slackNotificationDelay(intervals, index, immediate)
	if delay != time.Minute || index != 0 {
		t.Fatalf("first configured delay state = (%s, %d), want (1m, 0)", delay, index)
	}

	index, immediate = nextSlackNotificationState(intervals, index, immediate)
	delay, index = slackNotificationDelay(intervals, index, immediate)
	if delay != 5*time.Minute || index != 1 {
		t.Fatalf("second configured delay state = (%s, %d), want (5m, 1)", delay, index)
	}

	index, immediate = nextSlackNotificationState(intervals, index, immediate)
	delay, index = slackNotificationDelay(intervals, index, immediate)
	if delay != 5*time.Minute || index != 1 || immediate {
		t.Fatalf("terminal delay state = (%s, %d, %t), want (5m, 1, false)", delay, index, immediate)
	}
}

// 状態fieldを手で設定せず、実際のenqueueがMaxRestThresholdを超えた休止後に通知状態を
// resetすることを送信時刻で確認します。予約中timerがない状態からのresetを対象にします。
func TestSlackNotificationResetStateIsImmediateAtFirstInterval(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(slackResetKind(fallback), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &slackResetHTTPClient{}
				// Intervalsは2時間、3時間、MaxRestThresholdは1時間です。
				fixture := newSlackResetFixture(t, fallback, client)
				start := time.Now()
				fixture.enqueue(t, "initial")
				synctest.Wait()
				client.requireCalls(t, 1, start, "initial")

				// Intervals[0]で送信した後は、次の通知がIntervals[1]（3時間）へ進みます。
				fixture.enqueue(t, "first interval")
				time.Sleep(2 * time.Hour)
				synctest.Wait()
				client.requireCalls(t, 2, start.Add(2*time.Hour), "first interval")

				// 最後のlogから閾値を超えて休止した後のlogは、Intervalsを待たず即時送信します。
				resetAt := time.Now()
				fixture.enqueue(t, "after idle reset")
				synctest.Wait()
				client.requireCalls(t, 3, resetAt, "after idle reset")

				// reset後の次のbatchは、Intervals[1]ではなくIntervals[0]を待ちます。
				fixture.enqueue(t, "first interval after reset")
				time.Sleep(2*time.Hour - time.Nanosecond)
				synctest.Wait()
				client.requireCalls(t, 3, resetAt, "after idle reset")
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				client.requireCalls(t, 4, resetAt.Add(2*time.Hour), "first interval after reset")
			})
		})
	}
}

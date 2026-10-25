package file

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileSeriesUploadRetryScheduleImmediateReservationIsClaimedOnlyOnce(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule fileSeriesUploadRetrySchedule

	require.False(t, schedule.isRetryDue(now, interval))
	require.False(t, schedule.claimForAttempt(now, interval), "an unscheduled retry cannot be claimed")

	schedule.scheduleImmediateRetry()
	require.True(t, schedule.isRetryDue(now, interval))
	require.True(t, schedule.claimForAttempt(now, interval))

	require.False(t, schedule.isRetryDue(now, interval), "claiming the retry removes its reservation")
	require.False(t, schedule.claimForAttempt(now, interval), "the same reservation cannot be claimed twice")
}

func TestFileSeriesUploadRetryScheduleKeepsEarlierFailureDeadline(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.FixedZone("JST", 9*60*60))
	interval := 15 * time.Minute
	var schedule fileSeriesUploadRetrySchedule

	schedule.scheduleRetry(now, interval)
	schedule.scheduleRetry(now.Add(5*time.Minute), interval)

	scheduled, retryAt := fileSeriesUploadRetryScheduleValue(&schedule)
	require.True(t, scheduled)
	require.Equal(t, now.Add(interval), retryAt)
	require.False(t, schedule.isRetryDue(now.Add(interval-time.Nanosecond), interval))
	require.True(t, schedule.isRetryDue(now.Add(interval), interval))
}

func TestFileSeriesUploadRetryScheduleReplacesBootstrapWithFailureBackoff(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 30, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule fileSeriesUploadRetrySchedule

	schedule.scheduleImmediateRetry()
	require.True(t, schedule.isRetryDue(now, interval))

	// startup bootstrapの発見前にrotation uploadが失敗した場合は、
	// 直後のwriteでの即時再試行を避け、失敗時点からTTL後へ送ります。
	schedule.scheduleRetry(now, interval)
	scheduled, retryAt := fileSeriesUploadRetryScheduleValue(&schedule)
	require.True(t, scheduled)
	require.Equal(t, now.Add(interval), retryAt)
	require.False(t, schedule.isRetryDue(now, interval))
}

func TestFileSeriesUploadRetryScheduleRecoveryPreservesExistingSchedule(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 45, 0, 0, time.FixedZone("JST", 9*60*60))
	interval := 15 * time.Minute
	var schedule fileSeriesUploadRetrySchedule

	schedule.scheduleImmediateRetry()
	schedule.scheduleRetryIfUnscheduled(now, interval)
	scheduled, retryAt := fileSeriesUploadRetryScheduleValue(&schedule)
	require.True(t, scheduled)
	require.True(t, retryAt.IsZero(), "recovery must preserve an immediate bootstrap schedule")

	require.True(t, schedule.claimForAttempt(now, interval))
	schedule.scheduleRetryIfUnscheduled(now, interval)
	scheduled, retryAt = fileSeriesUploadRetryScheduleValue(&schedule)
	require.True(t, scheduled)
	require.Equal(t, now.Add(interval), retryAt)
}

func TestFileSeriesUploadRetryScheduleTreatsClockRollbackAsDue(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule fileSeriesUploadRetrySchedule
	schedule.scheduleRetry(now, interval)

	require.True(t, schedule.isRetryDue(now.Add(-time.Second), interval))
}

func TestFileSeriesUploadRetryScheduleClaimDoesNotClearLaterSchedule(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 30, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule fileSeriesUploadRetrySchedule
	schedule.scheduleRetry(now.Add(-interval), interval)

	// snapshot成功後にremote I/Oへ出る前にscheduleをclaimします。
	require.True(t, schedule.claimForAttempt(now, interval))
	scheduled, _ := fileSeriesUploadRetryScheduleValue(&schedule)
	require.False(t, scheduled)

	// old retryがremote I/O中に別rotationが失敗した場合、成功側は
	// 後処理を行わないため新しいscheduleがそのまま残ります。
	schedule.scheduleRetry(now.Add(time.Minute), interval)
	scheduled, retryAt := fileSeriesUploadRetryScheduleValue(&schedule)
	require.True(t, scheduled)
	require.Equal(t, now.Add(time.Minute+interval), retryAt)
}

func fileSeriesUploadRetryScheduleValue(
	schedule *fileSeriesUploadRetrySchedule,
) (bool, time.Time) {
	schedule.mu.Lock()
	defer schedule.mu.Unlock()
	if schedule.retryAt == nil {
		return false, time.Time{}
	}
	return true, *schedule.retryAt
}

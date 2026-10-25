package fsarchivehandler

import (
	"testing"
	"time"
)

func TestArchiveUploadRetryScheduleKeepsEarlierFailureDeadline(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule archiveUploadRetrySchedule

	schedule.arm(now, interval)
	schedule.arm(now.Add(5*time.Minute), interval)

	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&schedule)
	if !scheduled || !retryAt.Equal(now.Add(interval)) {
		t.Fatalf(
			"retry schedule = (%t, %v), want (true, %v)",
			scheduled,
			retryAt,
			now.Add(interval),
		)
	}
	if schedule.due(now.Add(interval-time.Nanosecond), interval) {
		t.Fatal("retry schedule became due before its deadline")
	}
	if !schedule.due(now.Add(interval), interval) {
		t.Fatal("retry schedule was not due at its deadline")
	}
}

func TestArchiveUploadRetryScheduleReplacesBootstrapWithFailureBackoff(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 30, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule archiveUploadRetrySchedule

	schedule.armImmediate()
	if !schedule.due(now, interval) {
		t.Fatal("bootstrap schedule is not immediately due")
	}

	// bootstrapの発見前にrotation uploadが失敗した場合は、直後のwriteで
	// 即時再試行せず、実際の失敗時点からTTL後へ置き換えます。
	schedule.arm(now, interval)
	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&schedule)
	if !scheduled || !retryAt.Equal(now.Add(interval)) {
		t.Fatalf(
			"failure schedule = (%t, %v), want (true, %v)",
			scheduled,
			retryAt,
			now.Add(interval),
		)
	}
	if schedule.due(now, interval) {
		t.Fatal("failure schedule remained immediately due after bootstrap replacement")
	}
}

func TestArchiveUploadRetryScheduleRecoveryPreservesExistingSchedule(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 45, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule archiveUploadRetrySchedule

	schedule.armImmediate()
	schedule.armIfUnscheduled(now, interval)
	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&schedule)
	if !scheduled || !retryAt.IsZero() {
		t.Fatalf("schedule after recovery discovery = (%t, %v), want (true, zero)", scheduled, retryAt)
	}

	if !schedule.claimForAttempt(now, interval) {
		t.Fatal("failed to consume immediate bootstrap schedule")
	}
	schedule.armIfUnscheduled(now, interval)
	scheduled, retryAt = archiveUploadRetryScheduleSnapshot(&schedule)
	if !scheduled || !retryAt.Equal(now.Add(interval)) {
		t.Fatalf(
			"schedule after unscheduled recovery = (%t, %v), want (true, %v)",
			scheduled,
			retryAt,
			now.Add(interval),
		)
	}
}

func TestArchiveUploadRetryScheduleTreatsClockRollbackAsDue(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule archiveUploadRetrySchedule
	schedule.arm(now, interval)

	if !schedule.due(now.Add(-time.Second), interval) {
		t.Fatal("clock rollback unnecessarily extended the retry wait")
	}
}

func TestArchiveUploadRetryScheduleBacksOffFailedTargetScan(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 15, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule archiveUploadRetrySchedule
	schedule.armImmediate()

	schedule.backoffDue(now, interval)
	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&schedule)
	if !scheduled || !retryAt.Equal(now.Add(interval)) {
		t.Fatalf(
			"target scan backoff = (%t, %v), want (true, %v)",
			scheduled,
			retryAt,
			now.Add(interval),
		)
	}
	if schedule.due(now, interval) {
		t.Fatal("failed target scan was immediately retried")
	}
}

func TestArchiveUploadRetryScheduleClaimPreservesLaterArm(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 30, 0, 0, time.UTC)
	interval := 15 * time.Minute
	var schedule archiveUploadRetrySchedule
	schedule.arm(now.Add(-interval), interval)

	if !schedule.claimForAttempt(now, interval) {
		t.Fatal("failed to claim due retry schedule")
	}
	if scheduled, _ := archiveUploadRetryScheduleSnapshot(&schedule); scheduled {
		t.Fatal("claimed retry schedule remained armed")
	}

	// old retryのremote I/O中に別rotationが失敗した場合、old retry成功側は
	// 後処理でclearしないため、新しいscheduleがそのまま残ります。
	schedule.arm(now.Add(time.Minute), interval)
	scheduled, retryAt := archiveUploadRetryScheduleSnapshot(&schedule)
	if !scheduled || !retryAt.Equal(now.Add(time.Minute+interval)) {
		t.Fatalf(
			"later rotation schedule = (%t, %v), want (true, %v)",
			scheduled,
			retryAt,
			now.Add(time.Minute+interval),
		)
	}
}

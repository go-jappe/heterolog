package directorysize

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTrackerStartsInitialObservationAndKeepsOneInFlight(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	require.False(t, tracker.TryStartObservation(now, config, true))

	tracker.FinishObservation(50)
	require.False(t, tracker.TryStartObservation(now.Add(time.Minute), config, false))
}

func TestTrackerDropsBlockedCallerRequest(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	require.False(t, tracker.TryStartObservation(now, config, true))
	tracker.FinishObservation(50)
	require.False(t, tracker.TryStartObservation(now.Add(time.Minute), config, false),
		"a caller blocked by the active observation must not force another scan")
}

func TestTrackerStartsWhenEstimateStrictlyExceedsCurrentLimit(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.FinishObservation(100)
	require.False(t, tracker.TryStartObservation(now.Add(time.Second), config, false))

	tracker.RecordGrowth(1)
	require.True(t, tracker.TryStartObservation(now.Add(2*time.Second), config, false))
}

func TestTrackerUsesCurrentConfigValuesForEachObservationDecision(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := &directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.FinishObservation(50)

	config.maxDirSize = 40
	require.True(t, tracker.TryStartObservation(now.Add(time.Second), config, false))
	tracker.FinishObservation(50)

	config.maxDirSize = 100
	config.usageCacheTTL = 2 * time.Second
	require.True(t, tracker.TryStartObservation(now.Add(3*time.Second), config, false))
}

func TestTrackerStartsAtUsageCacheTTLBoundaryAndAfterClockRollback(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.FinishObservation(10)
	require.False(t, tracker.TryStartObservation(now.Add(time.Hour-time.Nanosecond), config, false))
	require.True(t, tracker.TryStartObservation(now.Add(time.Hour), config, false))
	tracker.FinishObservation(10)
	require.True(t, tracker.TryStartObservation(now.Add(-time.Hour), config, false))
}

func TestTrackerMeasuresUsageCacheTTLFromObservationStartInstant(t *testing.T) {
	tracker := NewTracker()
	location := time.FixedZone("test-local", 9*60*60)
	localTime := time.Date(2026, time.August, 26, 21, 0, 0, 0, location)
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	// 開始時刻を地方時で渡しても、TTLは同じ瞬間から数えます。判定に使う時刻の表記に依存しません。
	require.True(t, tracker.TryStartObservation(localTime, config, false))
	tracker.FinishObservation(10)
	require.False(t, tracker.TryStartObservation(localTime.Add(time.Hour-time.Nanosecond).UTC(), config, false))
	otherZone := time.FixedZone("test-other", -5*60*60)
	require.True(t, tracker.TryStartObservation(localTime.Add(time.Hour).In(otherZone), config, false))
}

func TestTrackerAbortAllowsImmediateRetry(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.AbortObservation()
	require.True(t, tracker.TryStartObservation(now, config, false))
}

func TestTrackerDisabledLimitRequiresForce(t *testing.T) {
	tracker := NewTracker()
	config := directorySizeTestConfig{usageCacheTTL: time.Hour}

	tracker.RecordGrowth(math.MaxUint64)
	require.False(t, tracker.TryStartObservation(directorySizeTestTime(), config, false))
	require.True(t, tracker.TryStartObservation(directorySizeTestTime(), config, true))
}

func TestTrackerGrowthSaturates(t *testing.T) {
	tracker := NewTracker()
	config := directorySizeTestConfig{maxDirSize: math.MaxUint64 - 1, usageCacheTTL: time.Hour}

	tracker.RecordGrowth(math.MaxUint64 - 5)
	tracker.RecordGrowth(10)
	require.True(t, tracker.TryStartObservation(directorySizeTestTime(), config, false))
}

func TestTrackerAddsObservedGrowthAverageToSnapshot(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 1_000, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.RecordGrowth(10)
	tracker.RecordGrowth(20)
	tracker.RecordGrowth(30)
	tracker.FinishObservation(100)

	requireEstimatedBytes(t, tracker, now.Add(time.Minute), 120)
	require.False(t, tracker.TryStartObservation(now.Add(time.Minute), config, false))
}

func TestTrackerObservedGrowthAverageKeepsOnlyLatestTenSamples(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 1_000, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.RecordGrowth(1_000) // 11件目でringから外れる。
	for range observationGrowthSampleCapacity {
		tracker.RecordGrowth(20)
	}
	tracker.FinishObservation(100)

	requireEstimatedBytes(t, tracker, now.Add(time.Minute), 120)
}

func TestTrackerGrowthOutsideObservationIsNotUsedAsSample(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 200, usageCacheTTL: time.Hour}

	tracker.RecordGrowth(80)
	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.FinishObservation(100)

	requireEstimatedBytes(t, tracker, now.Add(time.Minute), 100)
}

func TestTrackerObservedGrowthAverageSaturates(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: math.MaxUint64, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.RecordGrowth(math.MaxUint64)
	tracker.FinishObservation(math.MaxUint64)

	requireEstimatedBytes(t, tracker, now.Add(time.Minute), math.MaxUint64)
}

func TestTrackerAbortDiscardsObservedGrowthSamples(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.RecordGrowth(90)
	tracker.AbortObservation()

	require.True(t, tracker.TryStartObservation(now.Add(time.Minute), config, true))
	tracker.FinishObservation(10)
	requireEstimatedBytes(t, tracker, now.Add(2*time.Minute), 10)
	require.False(t, tracker.TryStartObservation(now.Add(2*time.Minute), config, false))
}

func TestTrackerInvalidateForcesNextObservationInsideTTL(t *testing.T) {
	for _, test := range []struct {
		name  string
		usage uint64
	}{
		{name: "below capacity limit", usage: 60},
		{name: "over capacity limit", usage: 101},
	} {
		t.Run(test.name, func(t *testing.T) {
			tracker := NewTracker()
			now := directorySizeTestTime()
			config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

			require.True(t, tracker.TryStartObservation(now, config, false))
			tracker.FinishObservation(test.usage)
			require.False(t, tracker.TryStartObservation(now.Add(time.Minute), config, false))

			tracker.Invalidate()
			require.True(t, tracker.TryStartObservation(now.Add(2*time.Minute), config, false))
		})
	}
}

func TestTrackerInvalidateDuringObservationSurvivesFinish(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.Invalidate()
	tracker.FinishObservation(60)
	require.True(t, tracker.TryStartObservation(now.Add(time.Minute), config, false))
}

func TestTrackerBacksOffOverLimitObservationUntilUsageCacheTTL(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 100, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.FinishObservation(101)
	require.False(t, tracker.TryStartObservation(now.Add(time.Minute), config, false),
		"unchanged over-limit state must not be rescanned on every write")
	require.False(t, tracker.TryStartObservation(now.Add(time.Hour-time.Nanosecond), config, false))
	require.True(t, tracker.TryStartObservation(now.Add(time.Hour), config, false))
}

func TestTrackerObservedGrowthAverageUsesLatestSamplesWithFloorAndResets(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 1_000, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	for value := uint64(1); value <= observationGrowthSampleCapacity+1; value++ {
		tracker.RecordGrowth(value)
	}
	tracker.FinishObservation(100)
	// 直近10件(2..11)の平均を切り捨てた6を、観測値へ一度だけ上乗せします。
	requireEstimatedBytes(t, tracker, now.Add(time.Minute), 106)

	// 次の観測は前回のsampleを持ち越しません。
	require.True(t, tracker.TryStartObservation(now.Add(2*time.Minute), config, true))
	tracker.FinishObservation(50)
	requireEstimatedBytes(t, tracker, now.Add(3*time.Minute), 50)
}

func TestTrackerObservedGrowthAverageDoesNotOverflow(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: math.MaxUint64, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.RecordGrowth(math.MaxUint64)
	tracker.RecordGrowth(math.MaxUint64)
	// sampleの合計がuint64を超えても、平均は最大値のまま計算します。
	tracker.FinishObservation(0)
	requireEstimatedBytes(t, tracker, now.Add(time.Minute), math.MaxUint64)
}

func TestTrackerFinishAndAbortWithoutObservationDoNothing(t *testing.T) {
	tracker := NewTracker()
	tracker.RecordGrowth(10)

	tracker.FinishObservation(20)
	tracker.AbortObservation()

	require.True(t, tracker.TryStartObservation(
		directorySizeTestTime(),
		directorySizeTestConfig{maxDirSize: 5, usageCacheTTL: time.Hour},
		false,
	))
}

func TestTrackerConcurrentGrowthIsSerialized(t *testing.T) {
	tracker := NewTracker()
	now := directorySizeTestTime()
	config := directorySizeTestConfig{maxDirSize: 3_199, usageCacheTTL: time.Hour}

	require.True(t, tracker.TryStartObservation(now, config, false))
	tracker.FinishObservation(0)

	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				tracker.RecordGrowth(1)
			}
		}()
	}
	wait.Wait()

	require.True(t, tracker.TryStartObservation(now.Add(time.Second), config, false))
}

// requireEstimatedBytesは、TTL内の通常判定が概算値と現在の上限の比較だけで決まることを使い、
// 概算値がwantであることを観測開始の可否から確かめます。直前に観測を完了し、
// 上限超過のbackoffがない状態で呼び出してください。上限wantでは開始せず、want-1では開始します。
// 開始した観測は中止し、概算値と成功観測時刻を変えずに戻します。
func requireEstimatedBytes(t *testing.T, tracker *Tracker, now time.Time, want uint64) {
	t.Helper()
	require.False(t, tracker.TryStartObservation(now, directorySizeTestConfig{maxDirSize: want, usageCacheTTL: time.Hour}, false),
		"estimate exceeds %d", want)
	if want == 0 {
		return
	}
	require.True(t, tracker.TryStartObservation(now, directorySizeTestConfig{maxDirSize: want - 1, usageCacheTTL: time.Hour}, false),
		"estimate does not exceed %d", want-1)
	tracker.AbortObservation()
}

func directorySizeTestTime() time.Time {
	return time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
}

type directorySizeTestConfig struct {
	maxDirSize    uint64
	usageCacheTTL time.Duration
}

func (c directorySizeTestConfig) MaxDirSize() uint64 {
	return c.maxDirSize
}

func (c directorySizeTestConfig) UsageCacheTTL() time.Duration {
	return c.usageCacheTTL
}

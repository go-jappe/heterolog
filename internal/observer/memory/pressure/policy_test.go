package pressure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/memorypressure"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pollReadings は readings を一回ずつ順に取得し、各取得後の snapshot を返します。
func pollReadings(
	t *testing.T,
	policy memorypressure.CriticalPolicy,
	readings ...memorypressure.MemoryPressure,
) []memorypressure.MonitorSnapshot {
	t.Helper()
	created, err := New(&sequenceMemoryPressureReader{values: readings}, policy, time.Hour)
	require.NoError(t, err)
	monitor := created.(*memoryPressureMonitor)
	snapshots := make([]memorypressure.MonitorSnapshot, 0, len(readings))
	for range readings {
		require.NoError(t, monitor.pollOnce(context.Background()))
		snapshots = append(snapshots, monitor.Last())
	}
	return snapshots
}

// monitor の判定は、同じ観測列を pkg/memorypressure.CriticalState で評価した結果と
// 一致することを確認する。CriticalSince は critical 候補の連続が始まった観測の時刻です。
func TestMonitorFollowsPublicCriticalStateRules(t *testing.T) {
	critical := memorypressure.MemoryPressure{Ratio: 0.95, Reliable: true}
	healthy := memorypressure.MemoryPressure{Ratio: 0.50, Reliable: true}
	unreliable := memorypressure.MemoryPressure{Ratio: 0.95, Reliable: false}
	readings := []memorypressure.MemoryPressure{
		critical, critical, healthy, critical, unreliable, critical, critical, critical,
	}
	for _, hits := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("consecutive hits %d", hits), func(t *testing.T) {
			policy := memorypressure.CriticalPolicy{CriticalRatio: 0.90, ConsecutiveCriticalHits: hits}
			var reference memorypressure.CriticalState
			var streakStart time.Time
			for index, snapshot := range pollReadings(t, policy, readings...) {
				wantCritical := reference.Evaluate(readings[index], policy)
				switch reference.ConsecutiveHits {
				case 0:
					streakStart = time.Time{}
				case 1:
					streakStart = snapshot.LastReadAt
				}
				assert.Equal(t, wantCritical, snapshot.LastCritical, "reading %d", index)
				assert.Equal(t, reference.ConsecutiveHits, snapshot.ConsecutiveHits, "reading %d", index)
				assert.Equal(t, streakStart, snapshot.CriticalSince, "reading %d", index)
			}
		})
	}
}

// 連続回数を指定しない比率だけの policy は、pkg/memorypressure と同じく
// 最初の critical 候補で critical と判定されることを確認する。
func TestMonitorRatioOnlyPolicyIsCriticalOnFirstCandidate(t *testing.T) {
	snapshots := pollReadings(t, memorypressure.CriticalPolicy{CriticalRatio: 0.90},
		memorypressure.MemoryPressure{Ratio: 1.0, Reliable: true})

	assert.True(t, snapshots[0].LastCritical)
	assert.Equal(t, 1, snapshots[0].ConsecutiveHits)
}

// 連続して critical なメモリ状態が観測された場合、
// ConsecutiveCriticalHits に達したタイミングで critical と判定されることを確認する。
func TestMonitorEvaluateByConsecutiveHits(t *testing.T) {
	// Ratio が CriticalRatio を超えているため、pressure 自体は critical 候補。
	pressure := memorypressure.MemoryPressure{Ratio: 0.95, Reliable: true}
	snapshots := pollReadings(t, memorypressure.CriticalPolicy{
		CriticalRatio:           0.90,
		ConsecutiveCriticalHits: 2,
	}, pressure, pressure)

	// 1 回目の critical 候補では、まだ ConsecutiveCriticalHits に達していない。
	assert.False(t, snapshots[0].LastCritical)
	assert.Equal(t, 1, snapshots[0].ConsecutiveHits)
	// CriticalSince には、最初に critical 候補となった観測の時刻が記録される。
	assert.Equal(t, snapshots[0].LastReadAt, snapshots[0].CriticalSince)

	// 2 回目の critical 候補で ConsecutiveCriticalHits に達するため、critical と判定される。
	assert.True(t, snapshots[1].LastCritical)
	assert.Equal(t, 2, snapshots[1].ConsecutiveHits)
	// CriticalSince は最初の critical 候補の時刻のまま維持される。
	assert.Equal(t, snapshots[0].LastReadAt, snapshots[1].CriticalSince)
}

// AvailableBytes が MinAvailableBytes 以下の場合、
// メモリ不足として critical と判定されることを確認する。
func TestMonitorUsesAvailableBytesThreshold(t *testing.T) {
	// AvailableBytes が閾値と同じ 128 のため critical と判定される。
	snapshots := pollReadings(t, memorypressure.CriticalPolicy{
		MinAvailableBytes:       128,
		ConsecutiveCriticalHits: 1,
	}, memorypressure.MemoryPressure{AvailableBytes: 128, Reliable: true})

	assert.True(t, snapshots[0].LastCritical)
}

// AvailableBytes が 0 の場合は、
// 「空きメモリが 0 バイト」ではなく「値が取れていない」とみなし、
// MinAvailableBytes の判定対象にしないことを確認する。
func TestMonitorIgnoresZeroAvailableBytes(t *testing.T) {
	snapshots := pollReadings(t, memorypressure.CriticalPolicy{
		MinAvailableBytes:       128,
		ConsecutiveCriticalHits: 1,
	}, memorypressure.MemoryPressure{AvailableBytes: 0, Reliable: true})

	assert.False(t, snapshots[0].LastCritical)
	// critical 候補として扱われないため、連続回数は増えず、開始時刻も記録されない。
	assert.Equal(t, 0, snapshots[0].ConsecutiveHits)
	assert.True(t, snapshots[0].CriticalSince.IsZero())
}

// pressure が unreliable、または critical 候補ではない状態になった場合、
// 連続回数と開始時刻がリセットされることを確認する。
func TestMonitorResetsOnUnreliableOrNonCriticalPressure(t *testing.T) {
	criticalPressure := memorypressure.MemoryPressure{Ratio: 0.95, Reliable: true}
	snapshots := pollReadings(t, memorypressure.CriticalPolicy{
		CriticalRatio:           0.90,
		ConsecutiveCriticalHits: 3,
	},
		criticalPressure,
		// Reliable が false の pressure は判定に使わないため、critical 候補ではない。
		memorypressure.MemoryPressure{Ratio: 0.95, Reliable: false},
		criticalPressure,
		// Reliable ではあるが Ratio が CriticalRatio 未満のため critical 候補ではない。
		memorypressure.MemoryPressure{Ratio: 0.50, Reliable: true},
	)

	for index, wantHits := range []int{1, 0, 1, 0} {
		assert.False(t, snapshots[index].LastCritical, "reading %d", index)
		assert.Equal(t, wantHits, snapshots[index].ConsecutiveHits, "reading %d", index)
		assert.Equal(t, wantHits == 0, snapshots[index].CriticalSince.IsZero(), "reading %d", index)
	}
	// リセット後の critical 候補は、新しい連続として開始時刻を記録し直す。
	assert.Equal(t, snapshots[2].LastReadAt, snapshots[2].CriticalSince)
}

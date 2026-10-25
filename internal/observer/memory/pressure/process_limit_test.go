package pressure

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

func TestProcessLimitMonitorCountsOneObservationAndResetsOnRecovery(t *testing.T) {
	processHigh := memorypressure.MemoryLimitUsage{UsedBytes: 95, LimitBytes: 100, Reliable: true}
	reader := &sequenceMemoryPressureReader{values: []memorypressure.MemoryPressure{
		{Ratio: .95, Reliable: true, ProcessLimit: processHigh},
		// 全体値が不明になっても、取得できたprocess側が危険なら継続する。
		{ProcessLimit: processHigh},
		{ProcessLimit: memorypressure.MemoryLimitUsage{UsedBytes: 10, LimitBytes: 100, Reliable: true}},
		{ProcessLimit: processHigh},
		{},
	}}
	created, err := New(reader, memorypressure.CriticalPolicy{
		CriticalRatio: .9, ConsecutiveCriticalHits: 2,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	monitor := created.(*memoryPressureMonitor)
	for i, wantHits := range []int{1, 2, 0, 1, 0} {
		if err := monitor.pollOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		snapshot := monitor.Last()
		if snapshot.ConsecutiveHits != wantHits || snapshot.LastCritical != (wantHits >= 2) {
			t.Fatalf("observation %d: %+v", i, snapshot)
		}
		// snapshotの利用者による変更がmonitor内部へ戻らないことも確認する。
		snapshot.Pressure.ProcessLimit.UsedBytes = 999
		if monitor.Last().Pressure.ProcessLimit.UsedBytes == 999 {
			t.Fatal("snapshot shares mutable process observation")
		}
	}
}

func TestProcessLimitPolicyUsesRemainingBytes(t *testing.T) {
	const mib = 1024 * 1024
	// Jobの使用率は閾値未満で全体残量も十分ですが、process上限の残量は閾値に達しています。
	high := memorypressure.MemoryPressure{Ratio: .8, AvailableBytes: 2048 * mib, Reliable: true,
		ProcessLimit: memorypressure.MemoryLimitUsage{UsedBytes: 768 * mib, LimitBytes: 1024 * mib, Reliable: true}}
	recovered := high
	recovered.ProcessLimit.UsedBytes = 0
	snapshots := pollReadings(t, memorypressure.CriticalPolicy{
		CriticalRatio: .9, MinAvailableBytes: 256 * mib, ConsecutiveCriticalHits: 2,
	}, high, high, recovered)
	if snapshots[0].LastCritical || snapshots[0].ConsecutiveHits != 1 {
		t.Fatalf("consecutive hits should not be met at first observation: %+v", snapshots[0])
	}
	if !snapshots[1].LastCritical {
		t.Fatal("process remaining capacity was ignored because Job ratio was higher")
	}
	if snapshots[2].LastCritical || snapshots[2].ConsecutiveHits != 0 || !snapshots[2].CriticalSince.IsZero() {
		t.Fatalf("process recovery did not reset the critical streak: %+v", snapshots[2])
	}
}

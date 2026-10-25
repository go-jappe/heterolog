package memorypressure

import "testing"

func TestCgroupLimitThresholdsRemainIndependent(t *testing.T) {
	for _, tc := range []struct {
		name      string
		limit     MemoryLimitUsage
		ratio     float64
		available uint64
		want      bool
	}{
		{"ratio reached", MemoryLimitUsage{95, 100, true}, .9, 0, true},
		{"remaining reached", MemoryLimitUsage{80, 100, true}, .9, 20, true},
		{"remaining above threshold", MemoryLimitUsage{79, 100, true}, .9, 20, false},
		{"zero remaining measured", MemoryLimitUsage{100, 100, true}, 0, 1, true},
		{"usage above limit", MemoryLimitUsage{101, 100, true}, 0, 1, true},
		{"zero usage measured", MemoryLimitUsage{0, 100, true}, 0, 100, true},
		{"unknown usage", MemoryLimitUsage{100, 100, false}, .9, 1, false},
		{"known zero limit ratio", MemoryLimitUsage{0, 0, true}, .9, 0, true},
		{"known zero limit remaining", MemoryLimitUsage{0, 0, true}, 0, 1, true},
		{"unknown zero limit", MemoryLimitUsage{0, 0, false}, .9, 1, false},
		{"both thresholds disabled", MemoryLimitUsage{100, 100, true}, 0, 0, false},
		{"zero value", MemoryLimitUsage{}, .9, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A known cgroup constraint also works when the primary observation is unknown.
			p := MemoryPressure{CgroupLimit: tc.limit}
			if got := MeetsCriticalThresholds(p, tc.ratio, tc.available); got != tc.want {
				t.Fatalf("MeetsCriticalThresholds(%+v) = %t, want %t", p, got, tc.want)
			}
		})
	}
}

// CgroupLimitを追加しても、既存の全体観測とprocess上限の判定は変わらず、cgroup上限の観測も
// 一回ずつ数えます。monitorが返すsnapshotと内部状態の独立性は、
// internal/observer/memory/pressureのTestProcessLimitMonitorCountsOneObservationAndResetsOnRecoveryが
// monitor.Last()で確認します。
func TestCgroupLimitKeepsExistingObservationsAndCountsOnce(t *testing.T) {
	p := MemoryPressure{CgroupLimit: MemoryLimitUsage{95, 100, true}}
	// The new zero-value field must not suppress host or Windows process limits.
	for _, existing := range []MemoryPressure{
		{Ratio: .95, Reliable: true},
		{ProcessLimit: MemoryLimitUsage{95, 100, true}},
	} {
		if !IsCriticalOverload(existing, .9) || !MeetsCriticalThresholds(existing, .9, 0) {
			t.Fatalf("existing observation changed: %+v", existing)
		}
	}
	var state CriticalState
	if !state.Evaluate(p, CriticalPolicy{CriticalRatio: .9, ConsecutiveCriticalHits: 1}) || state.ConsecutiveHits != 1 {
		t.Fatalf("cgroup observation did not count once: %+v", state)
	}
	recovered := p
	recovered.CgroupLimit.UsedBytes = 10
	if state.Evaluate(recovered, CriticalPolicy{CriticalRatio: .9, ConsecutiveCriticalHits: 1}) || state.ConsecutiveHits != 0 {
		t.Fatalf("recovered cgroup observation did not reset: %+v", state)
	}
}

func TestCriticalThresholdsEvaluateBothLimitsIndependently(t *testing.T) {
	const mib = 1024 * 1024
	job := MemoryPressure{Ratio: .8, UsedBytes: 8192 * mib, LimitBytes: 10240 * mib,
		AvailableBytes: 2048 * mib, Reliable: true}
	for _, tt := range []struct {
		name      string
		primary   MemoryPressure
		process   MemoryLimitUsage
		ratio     float64
		available uint64
		want      bool
	}{
		{"process ratio", job, MemoryLimitUsage{950, 1000, true}, .9, 0, true},
		// Job使用率80%よりprocess使用率75%の方が低くても、processの残量不足は検出する。
		{"process remaining bytes", job, MemoryLimitUsage{768 * mib, 1024 * mib, true}, .9, 256 * mib, true},
		{"primary ratio", job, MemoryLimitUsage{10, 1000, true}, .8, 0, true},
		{"primary remaining bytes", job, MemoryLimitUsage{0, 4096 * mib, true}, 0, 2048 * mib, true},
		{"unknown primary known process", MemoryPressure{}, MemoryLimitUsage{950, 1000, true}, .9, 0, true},
		{"known primary unknown process", job, MemoryLimitUsage{1000, 1000, false}, .8, 0, true},
		{"unknown process ignored", job, MemoryLimitUsage{1000, 1000, false}, .9, 1, false},
		{"both unknown", MemoryPressure{}, MemoryLimitUsage{1000, 1000, false}, .9, 1, false},
		{"process exhausted", job, MemoryLimitUsage{1000, 1000, true}, 0, 1, true},
		{"usage exceeds limit", job, MemoryLimitUsage{1001, 1000, true}, 0, 1, true},
		{"zero usage measured", job, MemoryLimitUsage{0, 1000, true}, .9, 1, false},
		{"zero limit unknown", job, MemoryLimitUsage{1000, 0, true}, .9, 1, false},
		{"no thresholds", job, MemoryLimitUsage{1000, 1000, true}, 0, 0, false},
		{"legacy zero available ignored", MemoryPressure{Reliable: true}, MemoryLimitUsage{}, 0, 1, false},
		{"single scope unchanged", job, MemoryLimitUsage{}, .9, 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.primary
			p.ProcessLimit = tt.process
			if got := MeetsCriticalThresholds(p, tt.ratio, tt.available); got != tt.want {
				t.Fatalf("critical=%v, want %v: %+v", got, tt.want, p)
			}
			state := CriticalState{}
			policy := CriticalPolicy{CriticalRatio: tt.ratio, MinAvailableBytes: tt.available}
			if got := state.Evaluate(p, policy); got != tt.want {
				t.Fatalf("state critical=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestCriticalStateCountsBothLimitsOncePerObservation(t *testing.T) {
	policy := CriticalPolicy{CriticalRatio: .9, ConsecutiveCriticalHits: 2}
	state := CriticalState{}
	p := MemoryPressure{Ratio: .95, Reliable: true, ProcessLimit: MemoryLimitUsage{95, 100, true}}
	if state.Evaluate(p, policy) || state.ConsecutiveHits != 1 {
		t.Fatalf("both limits counted more than once: %+v", state)
	}
	p.Reliable = false
	if !state.Evaluate(p, policy) || state.ConsecutiveHits != 2 {
		t.Fatalf("known process pressure lost with unknown Job: %+v", state)
	}
	p.ProcessLimit.UsedBytes = 10
	if state.Evaluate(p, policy) || state.ConsecutiveHits != 0 {
		t.Fatalf("recovery did not reset: %+v", state)
	}
	p.ProcessLimit.UsedBytes = 95
	state.Evaluate(p, policy)
	p.ProcessLimit.Reliable = false
	if state.Evaluate(p, policy) || state.ConsecutiveHits != 0 {
		t.Fatalf("unknown observation carried previous pressure: %+v", state)
	}
}

func TestIsCriticalOverloadRetainsRatioOnlyThresholdContract(t *testing.T) {
	for _, p := range []MemoryPressure{
		{Reliable: true},
		{ProcessLimit: MemoryLimitUsage{0, 100, true}},
	} {
		if !IsCriticalOverload(p, 0) || IsCriticalOverload(p, .1) {
			t.Fatalf("ratio-only threshold changed: %+v", p)
		}
	}
}

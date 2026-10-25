package pressure

import (
	"context"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/memorypressure"
)

// providerが測定不能をerrorではなくReliable=falseで返したとき、
// monitorが前回のcritical判定を保持しないことを確認します。観測値はWindows providerの
// 返し方を模していますが、判定はOSに依存しないため全platformで実行します。
func TestUnknownMemoryMeasurementClearsMonitorCriticalState(t *testing.T) {
	reader := &sequenceMemoryPressureReader{values: []memorypressure.MemoryPressure{
		{Source: memorypressure.SourceWindowsAPI, Ratio: .95, Reliable: true},
		{Source: memorypressure.SourceWindowsAPI, Reliable: false},
		{Source: memorypressure.SourceWindowsAPI, Ratio: .4, Reliable: true},
	}}
	created, err := New(reader, memorypressure.CriticalPolicy{CriticalRatio: .9, ConsecutiveCriticalHits: 1}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	monitor := created.(*memoryPressureMonitor)
	for index, wantCritical := range []bool{true, false, false} {
		if err := monitor.pollOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		critical, err := monitor.IsCritical(context.Background())
		if err != nil || critical != wantCritical {
			t.Fatalf("sample %d: critical=%t error=%v, want %t", index, critical, err, wantCritical)
		}
		if !wantCritical {
			snapshot := monitor.Last()
			if snapshot.ConsecutiveHits != 0 || !snapshot.CriticalSince.IsZero() {
				t.Fatalf("sample %d retained critical history: %+v", index, snapshot)
			}
		}
	}
}

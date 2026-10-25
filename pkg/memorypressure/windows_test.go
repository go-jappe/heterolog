//go:build windows

package memorypressure

import (
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestWindowsMemoryPressureUsesCurrentMeasurement(t *testing.T) {
	base := MemoryPressure{LimitBytes: 100, Source: SourceWindowsAPI, Container: true,
		RSSBytes: 7, RSSReliable: true}
	for _, tt := range []struct {
		name      string
		used      uint64
		reliable  bool
		wantUsed  uint64
		wantAvail uint64
		wantRatio float64
	}{
		{name: "current usage", used: 40, reliable: true, wantUsed: 40, wantAvail: 60, wantRatio: .4},
		{name: "measured zero", reliable: true, wantAvail: 100},
		{name: "over limit", used: 120, reliable: true, wantUsed: 100, wantRatio: 1},
		{name: "failed measurement is not a peak fallback", used: 90},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := windowsMemoryPressureForUsage(base, tt.used, tt.reliable)
			if p.UsedBytes != tt.wantUsed || p.AvailableBytes != tt.wantAvail ||
				p.Ratio != tt.wantRatio || p.Reliable != tt.reliable {
				t.Fatalf("pressure = %+v, want used=%d available=%d ratio=%f reliable=%t",
					p, tt.wantUsed, tt.wantAvail, tt.wantRatio, tt.reliable)
			}
			if p.Source != base.Source || !p.Container || p.DirectHost ||
				p.RSSBytes != base.RSSBytes || p.RSSReliable != base.RSSReliable {
				t.Fatalf("scope or auxiliary RSS changed: %+v", p)
			}
			if err := p.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// ContainerはLinuxと同じく制約付きscopeの観測だけを表します。Job上限ではなく
// host全体を観測した場合はDirectHostで、Containerではありません。
func TestWindowsHostMemoryPressureIsNotContainerScoped(t *testing.T) {
	p := readWindowsHostMemoryPressure(7, true)
	if p.Source != SourceWindowsAPI || !p.DirectHost || p.Container || p.RSSBytes != 7 || !p.RSSReliable {
		t.Fatalf("host fallback must identify its scope: %+v", p)
	}
}

func TestWindowsMemoryPressureDoesNotCarryOldCriticalUsage(t *testing.T) {
	p := windowsMemoryPressureForUsage(MemoryPressure{LimitBytes: 100}, 95, true)
	if !IsCriticalOverload(p, .9) {
		t.Fatal("high current usage was not critical")
	}
	unknown := windowsMemoryPressureForUsage(p, 95, false)
	if unknown.Reliable || unknown.UsedBytes != 0 || unknown.Ratio != 0 ||
		unknown.AvailableBytes != 0 || IsCriticalOverload(unknown, .9) {
		t.Fatalf("failed measurement kept old critical usage: %+v", unknown)
	}
	recovered := windowsMemoryPressureForUsage(unknown, 30, true)
	if !recovered.Reliable || recovered.UsedBytes != 30 || IsCriticalOverload(recovered, .9) {
		t.Fatalf("measurement did not recover: %+v", recovered)
	}
	if p := windowsMemoryPressureForUsage(MemoryPressure{}, 10, true); p.Reliable || p.Ratio != 0 {
		t.Fatalf("missing limit became a reliable measurement: %+v", p)
	}
}

func TestWindowsMemoryPressureABISizes(t *testing.T) {
	wantProcess, wantExtended := uintptr(80), uintptr(144)
	if unsafe.Sizeof(uintptr(0)) == 4 {
		wantProcess, wantExtended = 44, 112
	}
	if size := unsafe.Sizeof(processMemoryCountersEx{}); size != wantProcess {
		t.Fatalf("process memory counters size = %d, want %d", size, wantProcess)
	}
	if size := unsafe.Sizeof(windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}); size != wantExtended {
		t.Fatalf("extended job information size = %d, want %d", size, wantExtended)
	}
	if size := unsafe.Sizeof(jobObjectMemoryUsageInformation{}); size != 16 {
		t.Fatalf("job current usage information size = %d, want 16", size)
	}
}

//go:build windows

package memorypressure

import (
	"context"
	"unsafe"

	"golang.org/x/sys/windows"
)

// memoryStatusExはGlobalMemoryStatusExが返すhost全体の現在の物理メモリ情報です。
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// processMemoryCountersExはGetProcessMemoryInfoのABIです。
// PrivateUsageは現在のprivate commit量で、process memory limitとの比較に使います。
// WorkingSetSizeは物理メモリ常駐量で、RSSの補助指標としてだけ使用します。
// Peak等の未参照fieldもWindowsの構造体layoutを維持するため必要です。
type processMemoryCountersEx struct {
	CB                         uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
	PrivateUsage               uintptr
}

// jobObjectMemoryUsageInformationはMicrosoft hcsshimが使用するclass 28のABIです。
// JobMemoryが現在のcommit量です。PeakJobMemoryUsedはlayout維持用で、判定には使いません。
// https://github.com/microsoft/hcsshim/blob/main/internal/winapi/jobobject.go
type jobObjectMemoryUsageInformation struct {
	JobMemory         uint64
	PeakJobMemoryUsed uint64
}

const jobObjectMemoryUsageInformationClass = 28

var (
	modkernel32 = windows.NewLazySystemDLL("kernel32.dll")
	modpsapi    = windows.NewLazySystemDLL("psapi.dll")

	procGlobalMemoryStatusEx   = modkernel32.NewProc("GlobalMemoryStatusEx")
	procIsProcessInJob         = modkernel32.NewProc("IsProcessInJob")
	procGetProcessMemoryInfo   = modpsapi.NewProc("GetProcessMemoryInfo")
	procQueryInformationJobObj = modkernel32.NewProc("QueryInformationJobObject")
)

func (defaultProvider) ReadMemoryPressure(ctx context.Context) (MemoryPressure, error) {
	_ = ctx

	// 一度の取得結果からprivate commitと補助RSSを取り出します。
	process, processReliable := readWindowsProcessMemoryBestEffort()
	p, handled := readWindowsJobMemoryPressureBestEffort(process, processReliable)
	if !handled {
		p = readWindowsHostMemoryPressure(uint64(process.WorkingSetSize), processReliable)
	}
	return p, p.Validate()
}

// readWindowsJobMemoryPressureBestEffortのboolは、この観測をJob側で扱ったことを表します。
// Job非所属、または照会したJobにmemory limitがない場合だけfalseでhostへ委ねます。
// 取得失敗したscopeはReliable=falseにし、hostやpeakで代用しません。
// Job現在量が不明でも、別に取得できたprocess上限・現在量は判定に残します。
// errorだけ返すと共通monitorが前回のcritical判定を保持するため、不明な観測で更新します。
//
// hJob=NULLの照会対象は現在のprocessが属する即時Jobです。祖先Jobまでは列挙しません。
// Job上限があれば既存の全体観測に入れ、process上限もあれば追加制約として保持します。
func readWindowsJobMemoryPressureBestEffort(
	process processMemoryCountersEx,
	processReliable bool,
) (MemoryPressure, bool) {
	p := MemoryPressure{
		Source:      SourceWindowsAPI,
		RSSBytes:    uint64(process.WorkingSetSize),
		RSSReliable: processReliable,
	}
	inJob, membershipKnown := isCurrentProcessInJob()
	if !membershipKnown {
		return p, true
	}
	if !inJob {
		return MemoryPressure{}, false
	}
	// Containerは従来どおり、Jobの制約付き実行単位を表す値として使用します。
	p.Container = true

	// x/sysの定義はWindows/386に必要なpaddingも含みます。
	var info windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	r1, _, _ := procQueryInformationJobObj.Call(
		0, uintptr(windows.JobObjectExtendedLimitInformation),
		uintptr(unsafe.Pointer(&info)), unsafe.Sizeof(info), 0,
	)
	if r1 == 0 {
		return p, true
	}

	flags := info.BasicLimitInformation.LimitFlags
	var used uint64
	var usageReliable bool
	switch {
	case flags&windows.JOB_OBJECT_LIMIT_JOB_MEMORY != 0:
		p.LimitBytes = uint64(info.JobMemoryLimit)
		used, usageReliable = readWindowsJobCurrentMemoryBestEffort()
		if flags&windows.JOB_OBJECT_LIMIT_PROCESS_MEMORY != 0 {
			p.ProcessLimit = MemoryLimitUsage{
				LimitBytes: uint64(info.ProcessMemoryLimit),
				UsedBytes:  uint64(process.PrivateUsage),
				Reliable:   processReliable,
			}
		}
	case flags&windows.JOB_OBJECT_LIMIT_PROCESS_MEMORY != 0:
		p.LimitBytes = uint64(info.ProcessMemoryLimit)
		used = uint64(process.PrivateUsage)
		usageReliable = processReliable
	default:
		return MemoryPressure{}, false
	}
	return windowsMemoryPressureForUsage(p, used, usageReliable), true
}

// readWindowsJobCurrentMemoryBestEffortは現在のJob commit量だけを返します。
// class 28は全Windows環境での利用を前提にせず、未対応・照会失敗なら不明扱いにします。
func readWindowsJobCurrentMemoryBestEffort() (uint64, bool) {
	var usage jobObjectMemoryUsageInformation
	r1, _, _ := procQueryInformationJobObj.Call(
		0, jobObjectMemoryUsageInformationClass,
		uintptr(unsafe.Pointer(&usage)), unsafe.Sizeof(usage), 0,
	)
	if r1 == 0 {
		return 0, false
	}
	return usage.JobMemory, true
}

// windowsMemoryPressureForUsageは同じscopeの上限と現在量から判定用の値を作ります。
// 現在量0の正常観測と取得失敗を区別し、取得不能時は古い値を持ち越しません。
func windowsMemoryPressureForUsage(p MemoryPressure, used uint64, usageReliable bool) MemoryPressure {
	p.UsedBytes, p.AvailableBytes, p.Ratio, p.Reliable = 0, 0, 0, false
	if !usageReliable || p.LimitBytes == 0 {
		return p
	}
	if used > p.LimitBytes {
		used = p.LimitBytes
	}
	p.UsedBytes = used
	p.AvailableBytes = p.LimitBytes - used
	p.Ratio = float64(used) / float64(p.LimitBytes)
	p.Reliable = true
	return p
}

// readWindowsHostMemoryPressureはJob非所属、または照会したJobにmemory limitがない
// 場合のhost物理メモリ観測です。API失敗も不明な観測として返し、前回の判定を更新します。
func readWindowsHostMemoryPressure(rssBytes uint64, rssReliable bool) MemoryPressure {
	p := MemoryPressure{
		Source:      SourceWindowsAPI,
		DirectHost:  true,
		RSSBytes:    rssBytes,
		RSSReliable: rssReliable,
	}
	var st memoryStatusEx
	st.Length = uint32(unsafe.Sizeof(st))
	r1, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&st)))
	if r1 == 0 || st.TotalPhys == 0 {
		return p
	}
	if st.AvailPhys > st.TotalPhys {
		st.AvailPhys = st.TotalPhys
	}
	p.LimitBytes = st.TotalPhys
	return windowsMemoryPressureForUsage(p, st.TotalPhys-st.AvailPhys, true)
}

// isCurrentProcessInJobはJobへの所属と、その照会が成功したかを返します。
// JobHandle=NULLは任意のJobへの所属を確認する指定です。
func isCurrentProcessInJob() (bool, bool) {
	var result int32
	r1, _, _ := procIsProcessInJob.Call(
		uintptr(windows.CurrentProcess()), 0, uintptr(unsafe.Pointer(&result)),
	)
	if r1 == 0 {
		return false, false
	}
	return result != 0, true
}

func readWindowsProcessMemoryBestEffort() (processMemoryCountersEx, bool) {
	var counters processMemoryCountersEx
	counters.CB = uint32(unsafe.Sizeof(counters))
	r1, _, _ := procGetProcessMemoryInfo.Call(
		uintptr(windows.CurrentProcess()), uintptr(unsafe.Pointer(&counters)), uintptr(counters.CB),
	)
	if r1 == 0 {
		return processMemoryCountersEx{}, false
	}
	return counters, true
}

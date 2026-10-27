package memorypressure

import (
	"context"
	"errors"
	"time"
)

type PressureSource string

const (
	SourceUnknown       PressureSource = "unknown"
	SourceLinuxCgroupV2 PressureSource = "linux_cgroup_v2"
	SourceLinuxProcMem  PressureSource = "linux_proc_meminfo"
	SourceWindowsAPI    PressureSource = "windows_globalmemorystatusex"
	SourceMacOSVMStat   PressureSource = "macos_vm_stat"
	SourceUnixSysctl    PressureSource = "unix_sysctl"
)

// MonitorSnapshot is a snapshot for reading the latest state that a monitor
// holds.
//
// It is used to show the internal state to the outside as it is, without
// calling the provider again.
type MonitorSnapshot struct {
	// Pressure is the most recently read pressure.
	Pressure MemoryPressure

	// LastReadAt is the time of the most recent read. It is the zero time if
	// nothing has been read yet.
	LastReadAt time.Time

	// LastCritical is the result of the most recent evaluation.
	LastCritical bool

	// ConsecutiveHits is how many times in a row the most recent readings were
	// in a dangerous state.
	ConsecutiveHits int

	// CriticalSince is the time when the current dangerous state began. It is
	// the zero time when memory is not in a dangerous state.
	CriticalSince time.Time
}

// MemoryLimitUsage is an explicit memory limit and the current usage that
// corresponds to it. Reliable reports that the usage was obtained, which
// distinguishes a correctly observed 0 bytes from a failure to obtain it.
type MemoryLimitUsage struct {
	UsedBytes  uint64
	LimitBytes uint64
	Reliable   bool
}

type MemoryPressure struct {
	// ProcessLimit is an additional constraint on the process alone, imposed
	// separately from the overall limit. It is used on Windows when there are
	// both a limit for the whole Job and a limit for the single process. The
	// existing Ratio and related fields represent the whole Job, and the
	// evaluation also checks this additional constraint independently. When
	// there is only a process limit, the existing fields represent it, so this
	// field is the zero value. It is a value type, so the snapshot of a
	// monitor also gets a copy of the value as it was observed.
	ProcessLimit MemoryLimitUsage

	// CgroupLimit is the observation with the least remaining memory among the
	// finite cgroup constraints read on Linux. The primary observation in
	// Ratio and related fields is the scope with the highest usage ratio, so
	// it can differ from this constraint. CgroupLimit is set even when it is
	// the same scope as the primary observation, so that a remaining amount of
	// 0 that was read is distinguished from one that was not read. In this
	// field, LimitBytes=0 with Reliable=true is also a valid limit of 0 (0
	// remaining, usage ratio 1). This does not affect treating LimitBytes=0 in
	// ProcessLimit as not read. It is the zero value outside Linux, when the
	// constraints cannot be read, and when there is no finite constraint. It
	// is a value type, so its copy in a snapshot is independent.
	CgroupLimit MemoryLimitUsage

	// Ratio is the memory pressure ratio.
	//
	// The expected range is 0.0 - 1.0. 0.0 leans toward no pressure, and 1.0
	// leans toward reaching the limit.
	//
	// It is basically the normalized value of UsedBytes / LimitBytes. However,
	// the source differs by OS and execution environment, so the same 0.90
	// does not always mean exactly the same thing.
	//
	// When Reliable=true, the value is expected to be usable for overload
	// evaluation.
	Ratio float64

	// AvailableBytes is the amount of available memory in bytes.
	//
	// It corresponds to MemAvailable on Linux bare metal, to the amount
	// remaining below the cgroup limit in a Linux container, and on Windows to
	// the amount remaining below the Job or process limit, or to the free
	// physical memory when there is no limit. On macOS and other Unix systems
	// it can be an approximation.
	//
	// It is 0 when it cannot be obtained. A 0 does not always mean that there
	// is really no free memory; it also includes "not obtained".
	AvailableBytes uint64

	// LimitBytes is the limit used for evaluation, in bytes.
	//
	// It holds the limit used as the denominator of Ratio. It is expected to
	// be memory.max of the cgroup in a Linux container, and the equivalent of
	// the total physical memory on bare metal.
	//
	// It is 0 when it cannot be obtained. A 0 that was not obtained makes
	// Ratio unusable for evaluation, so Reliable is false. When memory.max=0
	// is actually observed on Linux, Ratio=1 and CgroupLimit represent the
	// known limit that has been reached, and Reliable is true if the usage was
	// also obtained.
	LimitBytes uint64

	// UsedBytes is the current usage in bytes.
	//
	// It is basically LimitBytes - AvailableBytes, or the usage that the OS
	// returns directly.
	//
	// It is 0 when it cannot be obtained. A 0 may mean "not obtained" rather
	// than "unused".
	UsedBytes uint64

	// Source is where the values were obtained from.
	//
	// For example:
	//   - linux_cgroup_v2
	//   - linux_proc_meminfo
	//   - windows_globalmemorystatusex
	//   - macos_vm_stat
	//
	// Callers can use this value to tell from which environment's point of
	// view the values are.
	Source PressureSource

	// Reliable reports whether the values are reliable enough to be used for
	// overload evaluation.
	//
	// true:
	//   the values were computed from a valid source for the environment
	// false:
	//   only an approximation was obtained, or required information is missing
	//
	// When it is false, this overall observation is not used for evaluation.
	// The reliability of the additional constraints is independent.
	Reliable bool

	// DirectHost reports whether the memory values are from the point of view
	// of the whole host.
	//
	// true:
	//   bare metal, a host-view API, procfs, and so on
	// false:
	//   the view of a constrained execution environment such as a container
	//   or a cgroup, or not determined
	DirectHost bool

	// Container reports whether the memory view is confined to an execution
	// unit, such as a container or a cgroup.
	//
	// true:
	//   the limit and usage under container constraints are used. This is the
	//   case when a finite cgroup v2 limit is observed on Linux, or on Windows
	//   when the process is in a Job object that has a memory limit or whose
	//   limits cannot be read. It shows whether the scope is a constrained one
	//   that is not the whole host, not whether a container runtime exists.
	// false:
	//   the view of the whole host, or not determined to be a container
	Container bool

	RSSBytes    uint64
	RSSReliable bool
}

func (p MemoryPressure) Validate() error {
	if p.Ratio < 0 || p.Ratio > 1 {
		return errors.New("invalid memory pressure ratio")
	}
	return nil
}

type Provider interface {
	ReadMemoryPressure(ctx context.Context) (MemoryPressure, error)
}

type defaultProvider struct{}

func New() Provider {
	return defaultProvider{}
}

func IsCriticalOverload(p MemoryPressure, criticalRatio float64) bool {
	if p.Reliable && p.Ratio >= criticalRatio {
		return true
	}
	limit := p.ProcessLimit
	if limit.Reliable && limit.LimitBytes > 0 &&
		float64(min(limit.UsedBytes, limit.LimitBytes))/float64(limit.LimitBytes) >= criticalRatio {
		return true
	}
	limit = p.CgroupLimit
	if !limit.Reliable {
		return false
	}
	if limit.LimitBytes == 0 {
		return criticalRatio <= 1
	}
	return float64(min(limit.UsedBytes, limit.LimitBytes))/float64(limit.LimitBytes) >= criticalRatio
}

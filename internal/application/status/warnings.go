package status

import (
	"fmt"
	"slices"
	"time"
)

// Thresholdsは、注意すべき状態の判定の閾値です。
type Thresholds struct {
	// DiskFreeWarningRatioとDiskFreeCriticalRatioは、file systemの空きの割合（0から1）で、
	// 下回るとwarning・criticalです。
	DiskFreeWarningRatio  float64
	DiskFreeCriticalRatio float64
	// DirectorySizeWarningRatioは、directoryの上限に対する使用の割合で、超えるとwarning
	// です。上限を超えるとcriticalです。
	DirectorySizeWarningRatio float64
	// FailureRateWarningRatioは、出力先へ送ったrecordのうち送れなかった割合で、超えると
	// warningです。
	FailureRateWarningRatio float64
	// SlowRequestは、出力先への送信の平均の所要時間で、超えるとwarningです。
	SlowRequest time.Duration
}

// DefaultThresholdsは、既定の閾値です。
func DefaultThresholds() Thresholds {
	return Thresholds{
		DiskFreeWarningRatio:      0.10,
		DiskFreeCriticalRatio:     0.05,
		DirectorySizeWarningRatio: 0.90,
		FailureRateWarningRatio:   0.10,
		SlowRequest:               5 * time.Second,
	}
}

// Warningの種類です。
const (
	CodeDiskFreeLow         = "disk_free_low"
	CodeDiskFreeCritical    = "disk_free_critical"
	CodeDiskBelowMinFree    = "disk_below_min_free"
	CodeDirectoryNearLimit  = "directory_size_near_limit"
	CodeDirectoryOverLimit  = "directory_size_over_limit"
	CodeBackendSlow         = "backend_slow"
	CodeBackendFailures     = "backend_failures"
	CodeCircuitBreakerOpen  = "circuit_breaker_open"
	CodeSpoolReplayStopped  = "spool_replay_stopped"
	CodeSpoolFailedRecords  = "spool_failed_records"
	CodeSpoolMemoryCritical = "spool_memory_critical"
)

// 判定で使う、送り直すrunnerの状態とspoolの状態の名前です。
const (
	replayStateRunning   = "running"
	spoolStateDead       = "dead"
	spoolStateQuarantine = "quarantine"
)

// Evaluateは、報告から注意すべき状態を判定し、criticalを先に、同じ重さならTargetと
// Codeの順に並べて返します。
func Evaluate(report *Report, th Thresholds) []Warning {
	warnings := []Warning{}
	add := func(severity, code, target, message string) {
		warnings = append(warnings, Warning{Severity: severity, Code: code, Target: target, Message: message})
	}
	seenDisk := map[string]bool{}
	for _, directory := range report.Directories {
		target := directory.Path
		if directory.Disk != nil && directory.Disk.TotalBytes > 0 && !seenDisk[target] {
			seenDisk[target] = true
			ratio := float64(directory.Disk.FreeBytes) / float64(directory.Disk.TotalBytes)
			switch {
			case ratio < th.DiskFreeCriticalRatio:
				add(SeverityCritical, CodeDiskFreeCritical, target, fmt.Sprintf("only %.1f%% of the disk is free", ratio*100))
			case ratio < th.DiskFreeWarningRatio:
				add(SeverityWarning, CodeDiskFreeLow, target, fmt.Sprintf("only %.1f%% of the disk is free", ratio*100))
			}
			if directory.MinFreeBytes > 0 && directory.Disk.FreeBytes < directory.MinFreeBytes {
				add(SeverityCritical, CodeDiskBelowMinFree, target,
					fmt.Sprintf("the free disk space (%d bytes) is below the configured minimum (%d bytes); writes are refused", directory.Disk.FreeBytes, directory.MinFreeBytes))
			}
		}
		if directory.LimitBytes > 0 && !directory.Usage.ScannedAt.IsZero() {
			ratio := float64(directory.Usage.Bytes) / float64(directory.LimitBytes)
			switch {
			case ratio > 1:
				add(SeverityCritical, CodeDirectoryOverLimit, target,
					fmt.Sprintf("the directory uses %d bytes, over its limit of %d bytes", directory.Usage.Bytes, directory.LimitBytes))
			case ratio >= th.DirectorySizeWarningRatio:
				add(SeverityWarning, CodeDirectoryNearLimit, target,
					fmt.Sprintf("the directory uses %.0f%% of its limit of %d bytes", ratio*100, directory.LimitBytes))
			}
		}
	}
	for name, member := range report.Members {
		for _, backend := range member.Backends {
			target := name + "/" + backend.Name
			if r := backend.Requests; r != nil && r.Count > 0 && th.SlowRequest > 0 && r.AverageSeconds > th.SlowRequest.Seconds() {
				add(SeverityWarning, CodeBackendSlow, target, fmt.Sprintf("requests take %.1fs on average", r.AverageSeconds))
			}
			failed := backend.Records.Retryable + backend.Records.Dead + backend.Records.Quarantine + backend.Records.Failure
			if total := failed + backend.Records.Success; total > 0 && th.FailureRateWarningRatio > 0 {
				if ratio := float64(failed) / float64(total); ratio > th.FailureRateWarningRatio {
					add(SeverityWarning, CodeBackendFailures, target, fmt.Sprintf("%.0f%% of the records could not be sent", ratio*100))
				}
			}
			if backend.CircuitBreaker != nil && backend.CircuitBreaker.Open {
				add(SeverityCritical, CodeCircuitBreakerOpen, target, "the circuit breaker is open; requests are not sent")
			}
			if spool := backend.Spool; spool != nil {
				if active := spool.States[SpoolStateActive]; active.Files > 0 && spool.Replay.State != replayStateRunning {
					add(SeverityCritical, CodeSpoolReplayStopped, target,
						fmt.Sprintf("%d spooled files wait but the resend is %s", active.Files, spool.Replay.State))
				}
				if failed := spool.States[spoolStateDead].Files + spool.States[spoolStateQuarantine].Files; failed > 0 {
					add(SeverityWarning, CodeSpoolFailedRecords, target,
						fmt.Sprintf("%d spool files are dead or quarantined and need a person to check them", failed))
				}
				if spool.MemoryCritical != nil && *spool.MemoryCritical {
					add(SeverityCritical, CodeSpoolMemoryCritical, target, "memory pressure is critical; the overload policy applies")
				}
			}
		}
	}
	slices.SortStableFunc(warnings, func(a, b Warning) int {
		if a.Severity != b.Severity {
			if a.Severity == SeverityCritical {
				return -1
			}
			return 1
		}
		if a.Target != b.Target {
			if a.Target < b.Target {
				return -1
			}
			return 1
		}
		switch {
		case a.Code < b.Code:
			return -1
		case a.Code > b.Code:
			return 1
		}
		return 0
	})
	return warnings
}

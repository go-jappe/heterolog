// Package spoollifecycle holds the settings that control the retention of
// finalized spool records.
package spoollifecycle

import "time"

// LifecycleConfig controls the retention of finalized spool records: done
// records were replayed, dead records cannot be delivered, and quarantined
// records need inspection before any resend, because their outcome is unknown,
// the backend rejected their content, or they could not be restored or
// validated. Records of each kind are deleted only when purging that kind is
// enabled.
type LifecycleConfig struct {
	EnablePurgeDone bool
	// DoneMaxFileAge must be positive when EnablePurgeDone is true.
	DoneMaxFileAge int
	// DoneMaxDirSize must be positive when EnablePurgeDone is true.
	DoneMaxDirSize uint64

	EnablePurgeDead bool
	// DeadMaxFileAge must be positive when EnablePurgeDead is true.
	DeadMaxFileAge int
	// DeadMaxDirSize must be positive when EnablePurgeDead is true.
	DeadMaxDirSize uint64

	EnablePurgeQuarantine bool
	// QuarantineMaxFileAge must be positive when EnablePurgeQuarantine is true.
	QuarantineMaxFileAge int
	// QuarantineMaxDirSize must be positive when EnablePurgeQuarantine is true.
	QuarantineMaxDirSize uint64

	// SizePurgeInterval is the minimum interval between purges by size. It must
	// be positive.
	SizePurgeInterval time.Duration
	// EnableSizeEstimateCache decides purges by size from an estimated size. A
	// full scan is skipped only while the estimate stays within the maximum
	// directory size; when a purge may be needed, the directory is always fully
	// scanned and the measured total is used.
	EnableSizeEstimateCache bool
	// SizeFullScanEveryNChecks is the number of size checks between full scans;
	// 1 scans on every check. It must be positive when EnableSizeEstimateCache
	// is true.
	SizeFullScanEveryNChecks int
	// SizeFullScanMaxInterval is the longest time between full scans. It must be
	// positive when EnableSizeEstimateCache is true.
	SizeFullScanMaxInterval time.Duration
}

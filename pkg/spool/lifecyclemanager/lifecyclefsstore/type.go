package lifecyclefsstore

import "time"

type LifeCycleFsstoreConfig struct {
	SpoolPath string
	Target    string
	Dir       string

	EnablePurgeDone       bool
	EnablePurgeDead       bool
	EnablePurgeQuarantine bool

	DoneMaxFileAge int
	DoneMaxDirSize uint64

	DeadMaxFileAge int
	DeadMaxDirSize uint64

	QuarantineMaxFileAge int
	QuarantineMaxDirSize uint64

	SizePurgeInterval time.Duration

	EnableSizeEstimateCache bool

	SizeFullScanEveryNChecks int

	SizeFullScanMaxInterval time.Duration
}

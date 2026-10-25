package objectstoragespool

import "time"

// PurgeConfigProviderはterminal stateの保持設定を取得する契約です。
// DonePurge・DeadPurge・QuarantinePurgeはnilではないproviderを返します。
type PurgeConfigProvider interface {
	DonePurge() StatePurgeConfigProvider
	DeadPurge() StatePurgeConfigProvider
	QuarantinePurge() StatePurgeConfigProvider
	CapacityPurgeMinimumFileAge() time.Duration
}

// StatePurgeConfigProviderは一つのstateの削除条件を提供します。
type StatePurgeConfigProvider interface {
	Enabled() bool
	MaxFileAgeDays() int
	MaxDirSize() uint64
}

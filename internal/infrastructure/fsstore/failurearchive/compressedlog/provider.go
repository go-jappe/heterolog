package compressedlogarchive

// PurgeConfigProviderは失敗分類ごとの保持期限と最大容量を提供します。
// RetryablePurge・PermanentPurgeはnilではないproviderを返します。
type PurgeConfigProvider interface {
	RetryablePurge() FailureCategoryPurgeConfigProvider
	PermanentPurge() FailureCategoryPurgeConfigProvider
}

// PurgeConfigAdapterは構築時の設定値を保持し、正規化や既定値の補完を行いません。
type PurgeConfigAdapter struct {
	config PurgeConfig
}

var _ PurgeConfigProvider = PurgeConfigAdapter{}

func NewPurgeConfigAdapter(config PurgeConfig) PurgeConfigAdapter {
	return PurgeConfigAdapter{config: config}
}

func (a PurgeConfigAdapter) RetryablePurge() FailureCategoryPurgeConfigProvider {
	return NewFailureCategoryPurgeConfigAdapter(a.config.Retryable)
}

func (a PurgeConfigAdapter) PermanentPurge() FailureCategoryPurgeConfigProvider {
	return NewFailureCategoryPurgeConfigAdapter(a.config.Permanent)
}

// FailureCategoryPurgeConfigProviderは一つの失敗分類の削除条件を提供します。
type FailureCategoryPurgeConfigProvider interface {
	Enabled() bool
	MaxFileAgeDays() int
	MaxDirSize() uint64
}

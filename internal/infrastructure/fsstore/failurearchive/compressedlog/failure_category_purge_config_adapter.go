package compressedlogarchive

import pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"

// FailureCategoryPurgeConfigAdapterは一つの削除対象ディレクトリの保持設定を値として保持します。
// 正規化や既定値の補完は行わず、検証はpurgerの構築時に行います。
type FailureCategoryPurgeConfigAdapter struct {
	config pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig
}

var _ FailureCategoryPurgeConfigProvider = FailureCategoryPurgeConfigAdapter{}

func NewFailureCategoryPurgeConfigAdapter(config pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig) FailureCategoryPurgeConfigAdapter {
	return FailureCategoryPurgeConfigAdapter{config: config}
}

func (a FailureCategoryPurgeConfigAdapter) Enabled() bool       { return a.config.Enabled }
func (a FailureCategoryPurgeConfigAdapter) MaxFileAgeDays() int { return a.config.MaxFileAgeDays }
func (a FailureCategoryPurgeConfigAdapter) MaxDirSize() uint64  { return a.config.MaxDirSize }

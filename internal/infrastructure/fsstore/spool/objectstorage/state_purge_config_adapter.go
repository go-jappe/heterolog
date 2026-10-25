package objectstoragespool

import pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"

// StatePurgeConfigAdapterは一つの削除対象ディレクトリの保持設定を値として保持します。
// 正規化や既定値の補完は行わず、検証はpurgerの構築時に行います。
type StatePurgeConfigAdapter struct {
	config pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig
}

var _ StatePurgeConfigProvider = StatePurgeConfigAdapter{}

func NewStatePurgeConfigAdapter(config pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig) StatePurgeConfigAdapter {
	return StatePurgeConfigAdapter{config: config}
}

func (a StatePurgeConfigAdapter) Enabled() bool       { return a.config.Enabled }
func (a StatePurgeConfigAdapter) MaxFileAgeDays() int { return a.config.MaxFileAgeDays }
func (a StatePurgeConfigAdapter) MaxDirSize() uint64  { return a.config.MaxDirSize }

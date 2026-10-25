package compressedlogarchive

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

var (
	// ErrNilPurgeConfigは、保持設定のproviderがnilであることを表します。
	ErrNilPurgeConfig = errors.New("compressed log file upload failure archive purge config is nil")
	// ErrNilFailureCategoryPurgeConfigは、失敗分類の保持設定providerがnilであることを表します。
	ErrNilFailureCategoryPurgeConfig = errors.New("purge config provider is nil")
	// ErrInvalidPurgeMaxFileAgeDaysは、有効な分類の保持日数が正でないことを表します。
	ErrInvalidPurgeMaxFileAgeDays = errors.New("max file age days must be positive")
	// ErrInvalidPurgeMaxDirSizeは、有効な分類の最大容量が0であることを表します。
	ErrInvalidPurgeMaxDirSize = errors.New("max directory size must be positive")
	// ErrArchiveByteCountOverflowは、分類内のarchiveの合計byte数がuint64を超えることを表します。
	ErrArchiveByteCountOverflow = errors.New("total byte count overflows uint64")
)

// FailureCategoryPurgeConfigErrorは、失敗分類ごとの保持設定の不正を示します。
// Errは不正の種類を表すsentinelです。
type FailureCategoryPurgeConfigError struct {
	FailureCategory uploader.FailureCategory
	Err             error
	// MaxFileAgeDaysは、ErrInvalidPurgeMaxFileAgeDaysの場合に拒否した保持日数です。
	MaxFileAgeDays int
}

func (e *FailureCategoryPurgeConfigError) Error() string {
	if errors.Is(e.Err, ErrInvalidPurgeMaxFileAgeDays) {
		return fmt.Sprintf("%s %v: %d", e.FailureCategory, e.Err, e.MaxFileAgeDays)
	}
	return fmt.Sprintf("%s %v", e.FailureCategory, e.Err)
}

func (e *FailureCategoryPurgeConfigError) Unwrap() error {
	return e.Err
}

// FailureCategoryPurgeErrorは、一つの失敗分類のarchiveを整理できなかったことを示します。
type FailureCategoryPurgeError struct {
	FailureCategory uploader.FailureCategory
	Err             error
}

func (e *FailureCategoryPurgeError) Error() string {
	return fmt.Sprintf("purge compressed log file upload failure archive %s: %v", e.FailureCategory, e.Err)
}

func (e *FailureCategoryPurgeError) Unwrap() error {
	return e.Err
}

// ArchiveEntryErrorは、分類directory内のentryを削除候補として検査できなかったことを示します。
// Nameは分類directory直下のentry名です。
type ArchiveEntryError struct {
	Name string
	Err  error
}

func (e *ArchiveEntryError) Error() string {
	return fmt.Sprintf("inspect compressed log file archive entry %q: %v", e.Name, e.Err)
}

func (e *ArchiveEntryError) Unwrap() error {
	return e.Err
}

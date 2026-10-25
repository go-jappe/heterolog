package objectstoragespool

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

var (
	// ErrNilTerminalPurgeConfigは、terminal purgeの設定providerがnilであることを表します。
	ErrNilTerminalPurgeConfig = errors.New("object storage compressed log file terminal purge config provider is nil")
	// ErrNegativeCapacityPurgeMinimumFileAgeは、容量purgeの最小経過時間が負であることを表します。
	ErrNegativeCapacityPurgeMinimumFileAge = errors.New("capacity purge minimum file age must not be negative")
	// ErrNilStatePurgeConfigは、stateの保持設定providerがnilであることを表します。
	ErrNilStatePurgeConfig = errors.New("purge config provider is nil")
	// ErrInvalidPurgeMaxFileAgeDaysは、有効なstateの保持日数が正でないことを表します。
	ErrInvalidPurgeMaxFileAgeDays = errors.New("max file age days must be positive")
	// ErrInvalidPurgeMaxDirSizeは、有効なstateの最大容量が0であることを表します。
	ErrInvalidPurgeMaxDirSize = errors.New("max directory size must be positive")
)

// StatePurgeConfigErrorは、terminal stateごとの保持設定の不正を示します。
// Errは不正の種類を表すsentinelです。
type StatePurgeConfigError struct {
	State pkgspool.State
	Err   error
	// MaxFileAgeDaysは、ErrInvalidPurgeMaxFileAgeDaysの場合に拒否した保持日数です。
	MaxFileAgeDays int
}

func (e *StatePurgeConfigError) Error() string {
	if errors.Is(e.Err, ErrInvalidPurgeMaxFileAgeDays) {
		return fmt.Sprintf("%s %v: %d", e.State, e.Err, e.MaxFileAgeDays)
	}
	return fmt.Sprintf("%s %v", e.State, e.Err)
}

func (e *StatePurgeConfigError) Unwrap() error {
	return e.Err
}

// UnexpectedStagingEntryErrorは、staging directoryに専用名以外のentryがあることを示します。
// このentryは削除せず、管理者の確認へ残します。
type UnexpectedStagingEntryError struct {
	Path string
}

func (e *UnexpectedStagingEntryError) Error() string {
	return fmt.Sprintf("unexpected compressed log file staging entry %q", e.Path)
}

// InvalidEntryPreservationWarningは、不正名や非通常fileのentryを.preservedへ隔離したことを示す警告です。
// Errは隔離した理由で、ErrInvalidCompressedLogFileNameやErrInvalidCompressedLogPathなどをwrapします。
type InvalidEntryPreservationWarning struct {
	// SourcePathは隔離前のentryのpathです。
	SourcePath string
	// PreservedPathは.preserved内の隔離先です。
	PreservedPath string
	Destination   objectstorage.DestinationID
	Err           error
}

func (w *InvalidEntryPreservationWarning) Error() string {
	return fmt.Sprintf(
		"preserved invalid spool entry %q as %q (destination %q); administrator review required: %v",
		w.SourcePath, w.PreservedPath, w.Destination, w.Err,
	)
}

func (w *InvalidEntryPreservationWarning) Unwrap() error {
	return w.Err
}

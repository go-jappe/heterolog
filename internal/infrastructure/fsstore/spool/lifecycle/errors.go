package lifecycle

import (
	"errors"
	"fmt"

	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

var (
	// ErrInvalidSizePurgeIntervalは、容量purgeの間隔が正でないことを表します。
	ErrInvalidSizePurgeInterval = errors.New("spool lifecycle size purge interval must be positive")
	// ErrInvalidSizeFullScanChecksは、容量推定cacheの全走査までの確認回数が正でないことを表します。
	ErrInvalidSizeFullScanChecks = errors.New("spool lifecycle size full scan checks must be positive")
	// ErrInvalidSizeFullScanMaxIntervalは、容量推定cacheの全走査の最大間隔が正でないことを表します。
	ErrInvalidSizeFullScanMaxInterval = errors.New("spool lifecycle size full scan max interval must be positive")
	// ErrInvalidStateMaxFileAgeは、purgeを有効にしたstateの保持期間が正でないことを表します。
	ErrInvalidStateMaxFileAge = errors.New("max file age must be positive")
	// ErrInvalidStateMaxDirSizeは、purgeを有効にしたstateの最大容量が0であることを表します。
	ErrInvalidStateMaxDirSize = errors.New("max dir size must be positive")
)

// StatePurgeConfigErrorは、stateごとのpurge設定の不正を示します。Errは不正の種類を表す
// ErrInvalidStateMaxFileAgeまたはErrInvalidStateMaxDirSizeです。
type StatePurgeConfigError struct {
	State pkgspool.State
	Err   error
	// MaxFileAgeは、ErrInvalidStateMaxFileAgeの場合に拒否した値です。
	MaxFileAge int
	// MaxDirSizeは、ErrInvalidStateMaxDirSizeの場合に拒否した値です。
	MaxDirSize uint64
}

func (e *StatePurgeConfigError) Error() string {
	if errors.Is(e.Err, ErrInvalidStateMaxDirSize) {
		return fmt.Sprintf("spool lifecycle %s %v: %d", e.State, e.Err, e.MaxDirSize)
	}
	return fmt.Sprintf("spool lifecycle %s %v: %d", e.State, e.Err, e.MaxFileAge)
}

func (e *StatePurgeConfigError) Unwrap() error {
	return e.Err
}

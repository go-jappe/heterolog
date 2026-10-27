package config

import (
	"errors"

	"github.com/go-jappe/heterolog/internal/config/configerror"
	configruntime "github.com/go-jappe/heterolog/internal/config/runtime"
)

var (
	// ErrInvalidMemoryCriticalRatioは、memory使用率の危険判定の割合が0より大きく1以下でないことを表します。
	ErrInvalidMemoryCriticalRatio = errors.New(
		"runtime memory critical ratio must be greater than zero and at most one",
	)
	// ErrInvalidMemoryMinAvailableBytesは、危険判定に使う最小空きmemory量が0であることを表します。
	ErrInvalidMemoryMinAvailableBytes = errors.New(
		"runtime memory minimum available bytes must be greater than zero",
	)
	// ErrInvalidMemoryCriticalConsecutiveHitsは、危険と判定するまでの連続観測回数が0以下であることを表します。
	ErrInvalidMemoryCriticalConsecutiveHits = errors.New(
		"runtime memory critical consecutive hits must be greater than zero",
	)
)

// validateRuntimeMemoryHealthConfigは、memory逼迫を過負荷の判定に使うspoolが、監視を
// 渡されなかった場合に使う判定の閾値を検証します。環境設定では各判定条件をすべて
// 有効にするため、0を無効化sentinelとして扱いません。
func validateRuntimeMemoryHealthConfig(cfg configruntime.MemoryHealthConfig) error {
	var errs []error

	if !(cfg.CriticalRatio > 0 && cfg.CriticalRatio <= 1) {
		errs = append(errs, configerror.New(
			ErrInvalidMemoryCriticalRatio,
			"runtime memory critical ratio must be greater than zero and at most one: %v",
			cfg.CriticalRatio,
		))
	}
	if cfg.MinAvailableBytes == 0 {
		errs = append(errs, ErrInvalidMemoryMinAvailableBytes)
	}
	if cfg.CriticalConsecutiveHits <= 0 {
		errs = append(errs, configerror.New(
			ErrInvalidMemoryCriticalConsecutiveHits,
			"runtime memory critical consecutive hits must be greater than zero: %d",
			cfg.CriticalConsecutiveHits,
		))
	}

	return errors.Join(errs...)
}

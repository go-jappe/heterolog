package multi

import (
	"errors"
	"time"
)

// top-level loggerの設定検証で返すsentinel errorです。呼び出し側はerrors.Isで種類を判定
// します。errorの文字列には、契約種別と受理できなかった値を含めます。
var (
	// ErrConfigRequiredは設定providerがnilであることを表します。
	ErrConfigRequired = errors.New("config is nil")
	// ErrInvalidTimeoutはtimeoutが正の値でないことを表します。
	ErrInvalidTimeout = errors.New("timeout must be positive")
	// ErrInvalidMaxParallelはasync loggerの同時実行数が正の値でないことを表します。
	ErrInvalidMaxParallel = errors.New("max parallel must be positive")
)

// 一件のlog entry上限が正の値でない場合は、pkg/loggerのValidateMaxLogEntryBytesが返す
// pkglogger.ErrInvalidMaxLogEntryBytesをそのまま返します。

type MultiLoggerConfigProvider interface {
	Timeout() time.Duration
	MaxLogEntryBytes() int
}

type AsyncBestEffortMultiLoggerConfigProvider interface {
	Timeout() time.Duration
	MaxParallel() int
	MaxLogEntryBytes() int
}

type DeliveryMultiLoggerConfigProvider interface {
	Timeout() time.Duration
	MaxLogEntryBytes() int
}

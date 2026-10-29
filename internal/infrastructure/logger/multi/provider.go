package multi

import (
	"errors"
	"time"
)

// ErrConfigRequiredは、top-level loggerの設定providerがnilであることを表します。
// 呼び出し側はerrors.Isで種類を判定します。
var ErrConfigRequired = errors.New("config is nil")

// 正でないtimeoutと非同期loggerの同時実行数は、pkg/loggerのValidateTimeoutと
// ValidateMaxParallelが返す、ErrNonPositiveSettingのConfigFieldErrorをそのまま返します。
// sourceは契約種別の表示名です。一件のlog entry上限が正の値でない場合は、pkg/loggerの
// ValidateMaxLogEntryBytesが返すpkglogger.ErrInvalidMaxLogEntryBytesをそのまま返します。

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

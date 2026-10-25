package logger

import (
	"errors"
	"fmt"
)

// ErrInvalidMaxLogEntryBytesは、一件のlogを受理するbyte上限が正の値でないことを表します。
var ErrInvalidMaxLogEntryBytes = errors.New("max log entry bytes must be positive")

// ValidateMaxLogEntryBytesは、application loggingの共通入口で一件のlogを受理する
// byte上限が正の値であることを検証します。0を無効値や無制限としては扱いません。
// 不正な値はErrInvalidMaxLogEntryBytesとerrors.Isで判別できます。
func ValidateMaxLogEntryBytes(maxLogEntryBytes int) error {
	if maxLogEntryBytes <= 0 {
		return fmt.Errorf("%w: %d", ErrInvalidMaxLogEntryBytes, maxLogEntryBytes)
	}
	return nil
}

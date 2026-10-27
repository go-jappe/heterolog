package logger

import (
	"errors"
	"fmt"
)

// ErrInvalidMaxLogEntryBytes reports that the byte limit for accepting a single
// log entry is not positive.
var ErrInvalidMaxLogEntryBytes = errors.New("max log entry bytes must be positive")

// ValidateMaxLogEntryBytes validates that the byte limit for accepting a single
// log entry at the common entry point of application logging is positive. It
// treats 0 neither as a value that disables the limit nor as unlimited. For an
// invalid value, errors.Is matches the returned error with
// ErrInvalidMaxLogEntryBytes.
func ValidateMaxLogEntryBytes(maxLogEntryBytes int) error {
	if maxLogEntryBytes <= 0 {
		return fmt.Errorf("%w: %d", ErrInvalidMaxLogEntryBytes, maxLogEntryBytes)
	}
	return nil
}

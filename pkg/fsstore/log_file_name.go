package fsstore

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidLogFileNameComponent reports that a setting value that makes up a
// log file name contains a colon. The *LogFileNameComponentError that
// ValidateLogFileNameValue returns wraps this sentinel.
var ErrInvalidLogFileNameComponent = errors.New("log file name component must not contain ':'")

// LogFileNameComponentError reports a setting value that cannot be used in a
// log file name. errors.Is identifies it as ErrInvalidLogFileNameComponent, and
// errors.As retrieves the setting name and the value.
type LogFileNameComponentError struct {
	// Field is the setting name passed to ValidateLogFileNameValue.
	Field string
	// Value is the rejected setting value.
	Value string
}

// Error returns the message of the validation error, which includes the
// setting name and the rejected value.
func (e *LogFileNameComponentError) Error() string {
	return fmt.Sprintf("%s must not contain ':' in log file names: %q", e.Field, e.Value)
}

// Unwrap returns ErrInvalidLogFileNameComponent.
func (e *LogFileNameComponentError) Unwrap() error {
	return ErrInvalidLogFileNameComponent
}

// ValidateLogFileNameValue rejects colons in setting values that make up log
// file names. To prevent writing to a separate NTFS stream instead of the
// regular file on Windows, it applies the same restriction on all operating
// systems, including Linux. Required values and formats are validated by the
// existing validator of each setting. Destination paths are out of scope: this
// validation is not applied to drive specifications such as C:\logs. When it
// rejects a value, it returns a *LogFileNameComponentError.
func ValidateLogFileNameValue(fieldName, value string) error {
	if strings.ContainsRune(value, ':') {
		return &LogFileNameComponentError{Field: fieldName, Value: value}
	}
	return nil
}

package env

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidValue reports that the parse function passed to ParseEnv cannot
	// convert the value of an environment variable.
	ErrInvalidValue = errors.New("environment variable has an invalid value")
	// ErrNotInteger reports that a value cannot be interpreted as the required
	// integer. This includes unsigned integers, durations with a unit, and
	// comma-separated lists of integers.
	ErrNotInteger = errors.New("environment variable value is not an integer")
	// ErrNotPositive reports that a value of zero or less was set for an
	// environment variable that requires a positive value.
	ErrNotPositive = errors.New("environment variable value must be positive")
	// ErrOutOfRange reports that a value, or a value after applying a unit or a
	// multiplier, exceeds the range that can be represented.
	ErrOutOfRange = errors.New("environment variable value is out of range")
	// ErrNotBoolean reports that a value cannot be interpreted as a boolean.
	ErrNotBoolean = errors.New("environment variable value is not a boolean")
	// ErrNotNumber reports that a value cannot be interpreted as a
	// floating-point number.
	ErrNotNumber = errors.New("environment variable value is not a number")
	// ErrNotFinite reports that a floating-point value is NaN or infinite.
	ErrNotFinite = errors.New("environment variable value is not finite")
	// ErrInvalidJSON reports that a value cannot be converted as a single valid
	// JSON value.
	ErrInvalidJSON = errors.New("environment variable value is not valid JSON")
	// ErrInvalidDurationUnit reports that the duration unit that the caller
	// specified is not positive.
	ErrInvalidDurationUnit = errors.New("environment variable duration unit must be positive")
)

// VariableError reports that the value of an environment variable cannot be
// accepted as a configuration value. When it is retrieved with errors.As, Key
// and Value identify the invalid environment variable, and errors.Is can check
// the sentinel in Err. When parsing failed, errors.Is and errors.As can also
// reach its cause.
type VariableError struct {
	// Key is the name of the environment variable. It is empty for validation
	// that does not look up an environment variable, such as ParseBool.
	Key string
	// Value is the value that was not accepted. It is empty when the default for
	// the unset case cannot be converted.
	Value string
	// Err is the sentinel for the kind of invalidity.
	Err error

	cause   error
	message string
}

// Error returns a description of the invalid environment variable and what is
// wrong with it.
func (e *VariableError) Error() string { return e.message }

// Unwrap returns the kind of invalidity and, when parsing failed, its cause.
func (e *VariableError) Unwrap() []error {
	if e.cause == nil {
		return []error{e.Err}
	}
	return []error{e.Err, e.cause}
}

func newVariableError(key, value string, kind, cause error, format string, args ...any) error {
	return &VariableError{
		Key:     key,
		Value:   value,
		Err:     kind,
		cause:   cause,
		message: fmt.Sprintf(format, args...),
	}
}

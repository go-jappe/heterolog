package logger

import (
	"errors"
	"fmt"
)

var (
	// ErrLoggerClosed reports an entry passed to a synchronous or Delivery logger
	// after Close started. It is returned from the moment the logger stops
	// accepting entries, even before Close completes.
	ErrLoggerClosed = errors.New("logger is closed")

	// ErrLogEntryTooLarge reports a logical entry from the application that exceeds
	// the common limit checked before fan-out to the backends.
	ErrLogEntryTooLarge = errors.New("log entry is too large")
)

// LogEntryTooLargeError holds the actual size in bytes of a logical entry that
// exceeded the limit, and the limit that was applied. errors.Is matches
// ErrLogEntryTooLarge.
type LogEntryTooLargeError struct {
	ActualBytes int
	MaxBytes    int
}

func (e *LogEntryTooLargeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf(
		"%v: entry has %d bytes, limit is %d",
		ErrLogEntryTooLarge,
		e.ActualBytes,
		e.MaxBytes,
	)
}

func (e *LogEntryTooLargeError) Unwrap() error {
	return ErrLogEntryTooLarge
}

// LoggerFailure describes a failure of a single physical handler.
type LoggerFailure struct {
	HandlerType string // concrete type name of the handler that failed
	Op          string // operation, such as "multi.Info"
	Err         error  // cause, possibly wrapped
}

func (f LoggerFailure) Error() string {
	return fmt.Sprintf("%s: handler=%s: %v", f.Op, f.HandlerType, f.Err)
}

// Unwrap returns the cause, so errors.Is and errors.As can reach it while the
// LoggerFailure still identifies the handler that failed.
func (f LoggerFailure) Unwrap() error {
	return f.Err
}

// MultiLoggerError reports that one or more physical handlers failed.
// errors.As finds it, and Unwrap returns each LoggerFailure, so errors.Is and
// errors.As can also reach each failure and its cause.
type MultiLoggerError struct {
	Op       string
	Failures []LoggerFailure
}

func (e *MultiLoggerError) Unwrap() []error {
	errs := make([]error, 0, len(e.Failures))
	for _, f := range e.Failures {
		// ここは「元エラー」でも「f」でも良いが、どのloggerか追えるよう f を入れる
		errs = append(errs, f)
	}
	return errs
}

func (e *MultiLoggerError) Error() string {
	if e == nil {
		return "<nil>"
	}

	message := fmt.Sprintf("%s: %d handler(s) failed", e.Op, len(e.Failures))
	for index, failure := range e.Failures {
		message += fmt.Sprintf(
			"; failure[%d]: handler=%s: %v",
			index,
			failure.HandlerType,
			failure.Err,
		)
	}
	return message
}

package errors

import "fmt"

type DefaultLoggerInternalError struct {
	Op  string
	Err error
}

func (e *DefaultLoggerInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *DefaultLoggerInternalError) Unwrap() error {
	return e.Err
}

type ErrorLoggerInternalError struct {
	Op  string
	Err error
}

func (e *ErrorLoggerInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *ErrorLoggerInternalError) Unwrap() error {
	return e.Err
}

type DebugLoggerInternalError struct {
	Op  string
	Err error
}

func (e *DebugLoggerInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *DebugLoggerInternalError) Unwrap() error {
	return e.Err
}

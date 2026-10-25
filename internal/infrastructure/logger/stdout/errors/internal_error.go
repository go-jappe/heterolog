package errors

import "fmt"

type StdoutLoggerInternalError struct {
	Op  string
	Err error
}

func (e *StdoutLoggerInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *StdoutLoggerInternalError) Unwrap() error {
	return e.Err
}

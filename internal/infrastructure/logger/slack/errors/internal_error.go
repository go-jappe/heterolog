package errors

import "fmt"

type SlackLoggerInternalError struct {
	Op  string
	Err error
}

func (e *SlackLoggerInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *SlackLoggerInternalError) Unwrap() error {
	return e.Err
}

package errors

import "fmt"

type LokiLoggerInternalError struct {
	Op  string
	Err error
}

func (e *LokiLoggerInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *LokiLoggerInternalError) Unwrap() error {
	return e.Err
}

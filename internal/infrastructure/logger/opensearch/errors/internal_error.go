package errors

import "fmt"

type OpenSearchLoggerInternalError struct {
	Op  string
	Err error
}

func (e *OpenSearchLoggerInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *OpenSearchLoggerInternalError) Unwrap() error {
	return e.Err
}

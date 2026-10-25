package errors

import "fmt"

type CloudWatchLogsLoggerInternalError struct {
	Op  string
	Err error
}

func (e *CloudWatchLogsLoggerInternalError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, e.Err)
}

func (e *CloudWatchLogsLoggerInternalError) Unwrap() error {
	return e.Err
}

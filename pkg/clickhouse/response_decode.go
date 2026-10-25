package clickhouse

import "fmt"

// ResponseDecodeError means a successful HTTP response could not be inspected
// for a ClickHouse exception, so the INSERT outcome remains unknown.
type ResponseDecodeError struct {
	Err error
}

func (e *ResponseDecodeError) Error() string {
	return fmt.Sprintf("failed to read clickhouse successful response: %v", e.Err)
}

func (e *ResponseDecodeError) Unwrap() error { return e.Err }

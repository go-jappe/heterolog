package errors

import "fmt"

type ClickHouseLoggerInternalError struct {
	Op  string
	Err error
}

func (e *ClickHouseLoggerInternalError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Op == "" {
		return fmt.Sprintf("clickhouse logger internal error: %v", e.Err)
	}
	return fmt.Sprintf("clickhouse logger internal error: op=%s err=%v", e.Op, e.Err)
}

func (e *ClickHouseLoggerInternalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

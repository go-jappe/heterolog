package clickhouse

import (
	"errors"
	"fmt"
)

var (
	// ErrNegativeTimeout reports that Config.Timeout is negative.
	ErrNegativeTimeout = errors.New("clickhouse timeout must not be negative")
	// ErrEndpointRequired reports that the endpoint is empty or contains only
	// whitespace.
	ErrEndpointRequired = errors.New("clickhouse endpoint is required")
	// ErrInvalidEndpoint reports that the endpoint cannot be parsed as a URL or
	// does not meet the constraints on its scheme, host, user info, query or
	// fragment.
	ErrInvalidEndpoint = errors.New("invalid clickhouse endpoint")
	// ErrTableRequired reports that the table to INSERT into is empty or contains
	// only whitespace.
	ErrTableRequired = errors.New("clickhouse table is required")
)

// validationErrorは、従来のmessageを保ったまま、errors.Isで種類を判別できる検証errorです。
// causeがあれば、それもerrors.Isやerrors.Asで辿れます。
type validationError struct {
	kind    error
	cause   error
	message string
}

func (e *validationError) Error() string { return e.message }

func (e *validationError) Unwrap() []error {
	if e.cause == nil {
		return []error{e.kind}
	}
	return []error{e.kind, e.cause}
}

func newValidationError(kind, cause error, format string, args ...any) error {
	return &validationError{kind: kind, cause: cause, message: fmt.Sprintf(format, args...)}
}

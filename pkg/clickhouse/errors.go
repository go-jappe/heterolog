package clickhouse

import (
	"errors"
	"fmt"
)

var (
	// ErrNegativeTimeoutは、Config.Timeoutが負値であることを表します。
	ErrNegativeTimeout = errors.New("clickhouse timeout must not be negative")
	// ErrEndpointRequiredは、endpointが空または空白だけであることを表します。
	ErrEndpointRequired = errors.New("clickhouse endpoint is required")
	// ErrInvalidEndpointは、endpointをURLとして解釈できないか、scheme、host、user info、
	// query、fragmentの制約を満たさないことを表します。
	ErrInvalidEndpoint = errors.New("invalid clickhouse endpoint")
	// ErrTableRequiredは、INSERT先のtableが空または空白だけであることを表します。
	ErrTableRequired = errors.New("clickhouse table is required")
	// ErrTooManyRedirectsは、HTTPClientにCheckRedirectがなく、redirectが既定の
	// 10回を超えたことを表します。
	ErrTooManyRedirects = errors.New("stopped after 10 redirects")
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

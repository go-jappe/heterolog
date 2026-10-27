package loki

import (
	"errors"
	"fmt"
)

var (
	// ErrNegativeTimeout reports that Config.Timeout is negative.
	ErrNegativeTimeout = errors.New("loki timeout must not be negative")
	// ErrConflictingAuth reports that both Basic authentication and a bearer token
	// were specified.
	ErrConflictingAuth = errors.New("loki basic auth and bearer token cannot be used together")
	// ErrInvalidTenantID reports that the tenant ID does not meet the Loki tenant
	// ID constraints.
	ErrInvalidTenantID = errors.New("invalid loki tenant id")
	// ErrEndpointRequired reports that the endpoint is empty or blank.
	ErrEndpointRequired = errors.New("loki endpoint is required")
	// ErrInvalidEndpoint reports that the endpoint cannot be parsed as a URL or
	// does not meet the constraints on its scheme, host, user info, query, fragment
	// or path.
	ErrInvalidEndpoint = errors.New("invalid loki endpoint")
	// ErrLabelsRequired reports that a stream has no labels.
	ErrLabelsRequired = errors.New("loki stream labels are required")
	// ErrInvalidLabelName reports that a label name is not in a form that Loki
	// accepts. The invalid label name can be obtained from InvalidLabelNameError.
	ErrInvalidLabelName = errors.New("invalid loki label name")
	// ErrStreamWithoutEntries reports that a stream to be sent has no entries.
	ErrStreamWithoutEntries = errors.New("loki stream has no entries")
	// ErrTooManyRedirects reports that a request stopped after 10 redirects, the
	// default limit, because the HTTP client has no CheckRedirect. This applies to
	// the default client and to Config.HTTPClient.
	ErrTooManyRedirects = errors.New("stopped after 10 redirects")
)

// InvalidLabelNameError reports that a label name is not in the Prometheus form
// that Loki accepts. errors.Is matches ErrInvalidLabelName.
type InvalidLabelNameError struct {
	// Name is the invalid label name.
	Name string
}

// Error returns a description that includes the invalid label name.
func (e *InvalidLabelNameError) Error() string {
	return fmt.Sprintf("invalid label name %q", e.Name)
}

// Unwrap returns ErrInvalidLabelName.
func (e *InvalidLabelNameError) Unwrap() error { return ErrInvalidLabelName }

// validationErrorは、従来のmessageを保ったまま、errors.Isで種類を判別できる検証errorです。
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

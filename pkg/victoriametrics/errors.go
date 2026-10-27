package victoriametrics

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidBatchSize reports that the batch size given with WithBatchSize is
	// negative.
	ErrInvalidBatchSize = errors.New("batch size must be positive")
	// ErrInvalidRetryCount reports that the retry count given with WithRetry is
	// negative.
	ErrInvalidRetryCount = errors.New("retry count must not be negative")
	// ErrInvalidWorkerCount reports that SendAsync does not accept metrics because
	// the number of asynchronous workers is 0 or less.
	ErrInvalidWorkerCount = errors.New("async worker count must be positive")
	// ErrClientClosed reports that SendAsync was called on a client after Close.
	ErrClientClosed = errors.New("client is closed")
	// ErrAsyncChannelFull reports that SendAsync did not accept metrics because the
	// asynchronous send queue is full.
	ErrAsyncChannelFull = errors.New("async channel is full")
	// ErrTooManyRedirects reports that a request sending metrics stopped after 10
	// redirects, the default limit, because the HTTP client has no CheckRedirect.
	// This applies to the default client and to a client given with
	// WithHTTPClient.
	ErrTooManyRedirects = errors.New("stopped after 10 redirects")

	// ErrEmptyMetricName reports that Metric.Name is empty.
	ErrEmptyMetricName = errors.New("metric name cannot be empty")
	// ErrInvalidMetricName reports that Metric.Name does not follow the Prometheus
	// rules for metric names.
	ErrInvalidMetricName = errors.New("invalid metric name")
	// ErrInvalidLabelName reports that a label name in Metric.Labels does not
	// follow the Prometheus rules for label names or is a reserved label name.
	ErrInvalidLabelName = errors.New("invalid label name")
)

// validationErrorは、従来のmessageを保ったまま、errors.Isで種類を判別できる検証errorです。
type validationError struct {
	kind    error
	message string
}

func (e *validationError) Error() string { return e.message }

func (e *validationError) Unwrap() error { return e.kind }

func newValidationError(kind error, format string, args ...any) error {
	return &validationError{kind: kind, message: fmt.Sprintf(format, args...)}
}

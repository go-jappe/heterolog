package victoriametrics

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidBatchSizeは、WithBatchSizeで指定したbatch sizeが0以下であることを表します。
	ErrInvalidBatchSize = errors.New("batch size must be positive")
	// ErrInvalidWorkerCountは、非同期workerが0以下のためSendAsyncがmetricsを受け付けない
	// ことを表します。
	ErrInvalidWorkerCount = errors.New("async worker count must be positive")
	// ErrClientClosedは、Close後のclientへSendAsyncしたことを表します。
	ErrClientClosed = errors.New("client is closed")
	// ErrAsyncChannelFullは、非同期送信queueが満杯で、SendAsyncがmetricsを受け付けなかった
	// ことを表します。
	ErrAsyncChannelFull = errors.New("async channel is full")
	// ErrTooManyRedirectsは、WithHTTPClientのclientにCheckRedirectがなく、redirectが既定の
	// 10回を超えたことを表します。
	ErrTooManyRedirects = errors.New("stopped after 10 redirects")

	// ErrEmptyMetricNameは、Metric.Nameが空であることを表します。
	ErrEmptyMetricName = errors.New("metric name cannot be empty")
	// ErrInvalidMetricNameは、Metric.NameがPrometheusのmetric名の規則を満たさないことを表します。
	ErrInvalidMetricName = errors.New("invalid metric name")
	// ErrInvalidLabelNameは、Metric.Labelsのlabel名がPrometheusのlabel名の規則を満たさないか、
	// 予約されたlabel名であることを表します。
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

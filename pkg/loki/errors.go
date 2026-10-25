package loki

import (
	"errors"
	"fmt"
)

var (
	// ErrNegativeTimeoutは、Config.Timeoutが負値であることを表します。
	ErrNegativeTimeout = errors.New("loki timeout must not be negative")
	// ErrConflictingAuthは、Basic認証とbearer tokenが同時に指定されたことを表します。
	ErrConflictingAuth = errors.New("loki basic auth and bearer token cannot be used together")
	// ErrInvalidTenantIDは、tenant IDがLokiのTenant ID制約を満たさないことを表します。
	ErrInvalidTenantID = errors.New("invalid loki tenant id")
	// ErrEndpointRequiredは、endpointが空であることを表します。
	ErrEndpointRequired = errors.New("loki endpoint is required")
	// ErrInvalidEndpointは、endpointをURLとして解釈できないか、scheme、host、
	// user info、query、fragment、pathの制約を満たさないことを表します。
	ErrInvalidEndpoint = errors.New("invalid loki endpoint")
	// ErrLabelsRequiredは、streamにlabelが一つもないことを表します。
	ErrLabelsRequired = errors.New("loki stream labels are required")
	// ErrInvalidLabelNameは、label名がLokiの受け付ける形式ではないことを表します。
	// 不正なlabel名はInvalidLabelNameErrorから取り出せます。
	ErrInvalidLabelName = errors.New("invalid loki label name")
	// ErrStreamWithoutEntriesは、送信するstreamにentryが一つもないことを表します。
	ErrStreamWithoutEntries = errors.New("loki stream has no entries")
	// ErrTooManyRedirectsは、HTTPClientにCheckRedirectがなく、redirectが既定の
	// 10回を超えたことを表します。
	ErrTooManyRedirects = errors.New("stopped after 10 redirects")
)

// InvalidLabelNameErrorは、label名がLokiの受け付けるPrometheus形式ではないことを表します。
// errors.Isでは、ErrInvalidLabelNameと一致します。
type InvalidLabelNameError struct {
	// Nameは不正なlabel名です。
	Name string
}

// Errorは、不正なlabel名を含む説明を返します。
func (e *InvalidLabelNameError) Error() string {
	return fmt.Sprintf("invalid label name %q", e.Name)
}

// Unwrapは、ErrInvalidLabelNameを返します。
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

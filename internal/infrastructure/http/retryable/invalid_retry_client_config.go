package retryable

import (
	"errors"
	"fmt"
)

// ErrInvalidRetryClientConfigは、retry wrapperを組み立てる設定が不正なことを表します。
// 実際に返すerrorはInvalidRetryClientConfigErrorです。
var ErrInvalidRetryClientConfig = errors.New("retry client config is invalid")

// InvalidRetryClientConfigErrorは、backendのretry wrapperを組み立てる設定が不正なことを表します。
// errors.Is(err, ErrInvalidRetryClientConfig)で判定でき、設定検証のerrorもerrors.Is・errors.Asで辿れます。
type InvalidRetryClientConfigError struct {
	// Backendは、設定を検証したbackendの表示名です。
	Backend string
	// Errは、設定検証のerrorです。
	Err error
}

func (e *InvalidRetryClientConfigError) Error() string {
	return fmt.Sprintf("%s retry client config is invalid: %v", e.Backend, e.Err)
}

// Isは、ErrInvalidRetryClientConfigと一致します。
func (e *InvalidRetryClientConfigError) Is(target error) bool {
	return target == ErrInvalidRetryClientConfig
}

// Unwrapは設定検証のerrorを返します。
func (e *InvalidRetryClientConfigError) Unwrap() error { return e.Err }

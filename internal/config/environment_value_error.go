package config

import (
	"errors"
	"fmt"

	pkgenv "github.com/go-jappe/heterolog/pkg/env"
)

// newInvalidEnvironmentValueErrorは、表示文formatを保ったInvalidEnvironmentValueErrorを作ります。
func newInvalidEnvironmentValueError(key, value string, err error, format string, args ...any) error {
	return pkgenv.NewInvalidEnvironmentValueError(key, value, err, fmt.Sprintf(format, args...))
}

// ErrUnsupportedRetryClientModeは、retry client modeの値が対応していないmodeであることを表します。
var ErrUnsupportedRetryClientMode = errors.New("unsupported retry client mode")

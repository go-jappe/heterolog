package config

import (
	"errors"
	"fmt"
)

// InvalidEnvironmentValueErrorは、環境変数keyの値をLoadConfigが受け付けないことを表します。
// Keyは値を拒否したkey名、Valueは読み込んだ値の表示、Errは拒否の種類を表すsentinelまたは
// 拒否の原因です。認証情報を含み得る値ではValueを空にします。pkg/envが型変換で拒否した値はenv.VariableErrorで表し、このerrorは
// loaderが値の意味を解釈して拒否した場合に使います。ただし認証情報を含み得る値（*_S3_DESTINATIONSと
// HTTP headerのJSON）は、型変換で拒否した場合もenv.VariableErrorを包まずにこのerrorで表し、
// Errにはsentinel（env.ErrInvalidJSONなど）だけを持たせます。LoadConfigは複数の設定errorを
// errors.Joinでまとめるため、errors.Asで取り出してください。
type InvalidEnvironmentValueError struct {
	Key   string
	Value string
	Err   error

	message string
}

// newInvalidEnvironmentValueErrorは、表示文formatを保ったInvalidEnvironmentValueErrorを作ります。
func newInvalidEnvironmentValueError(key, value string, err error, format string, args ...any) error {
	return &InvalidEnvironmentValueError{Key: key, Value: value, Err: err, message: fmt.Sprintf(format, args...)}
}

func (e *InvalidEnvironmentValueError) Error() string {
	if e.message != "" {
		return e.message
	}
	return fmt.Sprintf("invalid %s %s: %v", e.Key, e.Value, e.Err)
}

func (e *InvalidEnvironmentValueError) Unwrap() error { return e.Err }

// ErrUnsupportedRetryClientModeは、retry client modeの値が対応していないmodeであることを表します。
var ErrUnsupportedRetryClientMode = errors.New("unsupported retry client mode")

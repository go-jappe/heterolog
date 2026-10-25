package env

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidValueは、ParseEnvへ渡したparseが環境変数の値を変換できないことを表します。
	ErrInvalidValue = errors.New("environment variable has an invalid value")
	// ErrNotIntegerは、値を要求された整数として解釈できないことを表します。
	// 符号なし整数、単位付きのduration、comma区切りの整数一覧も含みます。
	ErrNotInteger = errors.New("environment variable value is not an integer")
	// ErrNotPositiveは、正の値が必要な環境変数に0以下の値が指定されたことを表します。
	ErrNotPositive = errors.New("environment variable value must be positive")
	// ErrOutOfRangeは、値または単位・倍率を適用した値が表現できる範囲を超えることを表します。
	ErrOutOfRange = errors.New("environment variable value is out of range")
	// ErrNotBooleanは、値をboolean値として解釈できないことを表します。
	ErrNotBoolean = errors.New("environment variable value is not a boolean")
	// ErrNotNumberは、値を浮動小数点数として解釈できないことを表します。
	ErrNotNumber = errors.New("environment variable value is not a number")
	// ErrNotFiniteは、浮動小数点数の値がNaNまたは無限大であることを表します。
	ErrNotFinite = errors.New("environment variable value is not finite")
	// ErrInvalidJSONは、値が一つの妥当なJSON値として変換できないことを表します。
	ErrInvalidJSON = errors.New("environment variable value is not valid JSON")
	// ErrInvalidDurationUnitは、呼び出し側が指定したduration単位が正の値でないことを表します。
	ErrInvalidDurationUnit = errors.New("environment variable duration unit must be positive")
)

// VariableErrorは、環境変数の値を設定値として受け付けられないことを表します。
// errors.Asで取り出すとKeyとValueで不正な環境変数を特定でき、Errのsentinelは
// errors.Isで判定できます。parseに失敗した場合は、その原因もerrors.Is/Asで辿れます。
type VariableError struct {
	// Keyは環境変数名です。ParseBoolのように環境変数を参照しない検証では空です。
	Key string
	// Valueは受け付けられなかった値です。未設定時のdefaultを変換できない場合は空です。
	Value string
	// Errは不正の種類を表すsentinelです。
	Err error

	cause   error
	message string
}

// Errorは、不正な環境変数と内容の説明を返します。
func (e *VariableError) Error() string { return e.message }

// Unwrapは不正の種類と、parseに失敗した場合はその原因を返します。
func (e *VariableError) Unwrap() []error {
	if e.cause == nil {
		return []error{e.Err}
	}
	return []error{e.Err, e.cause}
}

func newVariableError(key, value string, kind, cause error, format string, args ...any) error {
	return &VariableError{
		Key:     key,
		Value:   value,
		Err:     kind,
		cause:   cause,
		message: fmt.Sprintf(format, args...),
	}
}

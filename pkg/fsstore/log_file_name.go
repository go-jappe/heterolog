package fsstore

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidLogFileNameComponentは、ログファイル名を構成する設定値にcolonが含まれることを表します。
// ValidateLogFileNameValueが返す*LogFileNameComponentErrorはこのsentinelをwrapします。
var ErrInvalidLogFileNameComponent = errors.New("log file name component must not contain ':'")

// LogFileNameComponentErrorは、ログファイル名に使用できない設定値を示します。
// errors.IsでErrInvalidLogFileNameComponentと判別でき、errors.Asで設定名と値を取得できます。
type LogFileNameComponentError struct {
	// Fieldは、ValidateLogFileNameValueに渡した設定名です。
	Field string
	// Valueは、拒否した設定値です。
	Value string
}

// Errorは、設定名と拒否した値を含む検証エラーの文言を返します。
func (e *LogFileNameComponentError) Error() string {
	return fmt.Sprintf("%s must not contain ':' in log file names: %q", e.Field, e.Value)
}

// Unwrapは、ErrInvalidLogFileNameComponentを返します。
func (e *LogFileNameComponentError) Unwrap() error {
	return ErrInvalidLogFileNameComponent
}

// ValidateLogFileNameValueは、ログファイル名を構成する設定値のcolonを拒否します。
// Windowsで通常fileではなくNTFSの別streamへ書き込むことを防ぐため、Linuxを含む
// 全OSで同じ制約を適用します。必須値や形式の検証は各設定の既存validatorが担当します。
// 保存先pathは対象外です。C:\logsのドライブ指定などにこの検証を適用しません。
// 拒否した場合は*LogFileNameComponentErrorを返します。
func ValidateLogFileNameValue(fieldName, value string) error {
	if strings.ContainsRune(value, ':') {
		return &LogFileNameComponentError{Field: fieldName, Value: value}
	}
	return nil
}

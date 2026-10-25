package fsstore

import (
	"errors"
	"strings"
)

var (
	// ErrEmptyLogDirectoryは、通常ログの保存先サブディレクトリ名が空であることを表します。
	ErrEmptyLogDirectory = errors.New("log directory must not be empty")
	// ErrEmptyDebugLogDirectoryは、debug directory分離が有効なのに保存先がないことを表します。
	ErrEmptyDebugLogDirectory = errors.New("debug log directory must not be empty")
)

// ValidateLogDirectoryは、通常ログの保存先サブディレクトリ名LogDirが
// 空文字・空白だけでないことを検証し、空ならErrEmptyLogDirectoryを返します。
// 呼び出し元は既定値と設定継承を適用した後、file出力で実際に使用する設定だけを渡してください。
// ディレクトリの実在確認や作成、権限の検査は行いません。
func ValidateLogDirectory(directory string) error {
	if strings.TrimSpace(directory) == "" {
		return ErrEmptyLogDirectory
	}
	return nil
}

// ValidateDebugLogDirectoryは、debug directory分離が有効な場合に保存先が指定されているかを検証し、
// 指定がなければErrEmptyDebugLogDirectoryを返します。
// directory分離が無効な場合は、以前の保存先が残っていても検証しません。
func ValidateDebugLogDirectory(enabled bool, directory string) error {
	if enabled && strings.TrimSpace(directory) == "" {
		return ErrEmptyDebugLogDirectory
	}
	return nil
}

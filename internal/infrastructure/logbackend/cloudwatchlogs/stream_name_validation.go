package cloudwatchlogs

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
)

// ValidateApplicationLogStreamNameが検出する、applicationの運用上の制約への違反です。
// 空のstream名とAWS APIの制約への違反は、pkg/cloudwatchlogsのErrLogStreamNameRequired、
// ErrLogStreamNameTooLong、ErrLogStreamNameInvalidCharacterで判定できます。
var (
	// ErrLogStreamNameInvalidUTF8は、stream名が不正なUTF-8を含むことを表します。
	ErrLogStreamNameInvalidUTF8 = errors.New("cloudwatch logs log stream name must be valid UTF-8")
	// ErrLogStreamNameControlCharacterは、stream名が改行などの制御文字を含むことを表します。
	ErrLogStreamNameControlCharacter = errors.New("cloudwatch logs log stream name must not contain control character")
	// ErrLogStreamNameFormatCharacterは、stream名が不可視のUnicode書式制御文字を含むことを表します。
	ErrLogStreamNameFormatCharacter = errors.New("cloudwatch logs log stream name must not contain Unicode format character")
	// ErrLogStreamNameNonASCIIWhitespaceは、stream名がASCII space以外の空白を含むことを表します。
	ErrLogStreamNameNonASCIIWhitespace = errors.New("cloudwatch logs log stream name must not contain non-ASCII whitespace")
	// ErrLogStreamNameSurroundingWhitespaceは、stream名の先頭または末尾に空白があることを表します。
	ErrLogStreamNameSurroundingWhitespace = errors.New("cloudwatch logs log stream name must not have leading or trailing whitespace")
)

// LogStreamNameCharacterErrorは、stream名に運用上許可しない文字が含まれることを表します。
// Kindは、ErrLogStreamNameControlCharacter、ErrLogStreamNameFormatCharacter、
// ErrLogStreamNameNonASCIIWhitespaceのいずれかで、errors.Isで判定できます。
type LogStreamNameCharacterError struct {
	Kind error
	// Characterは、最初に見つかった許可しない文字です。
	Character rune
}

func (e *LogStreamNameCharacterError) Error() string {
	return fmt.Sprintf("%v U+%04X", e.Kind, e.Character)
}

func (e *LogStreamNameCharacterError) Unwrap() error { return e.Kind }

// ValidateApplicationLogStreamNameは、AWS APIのstream名制約に加え、このapplicationで
// 設定・表示・検索しやすい識別子として扱うための運用上の制約を検証します。
//
// 通常のUnicode文字と名前中のASCII spaceは許可します。一方、JSON encoding時に値が
// 置換される不正UTF-8、改行を含む制御文字、改行として表示されるUnicode空白、不可視の
// 書式制御文字、見落としやすい先頭・末尾の空白は拒否します。
func ValidateApplicationLogStreamName(name string) error {
	if !utf8.ValidString(name) {
		return ErrLogStreamNameInvalidUTF8
	}
	if strings.TrimSpace(name) == "" {
		return pkgcloudwatchlogs.ErrLogStreamNameRequired
	}
	if err := pkgcloudwatchlogs.ValidateLogStreamName(name); err != nil {
		return err
	}

	for _, character := range name {
		switch {
		case unicode.IsControl(character):
			return &LogStreamNameCharacterError{Kind: ErrLogStreamNameControlCharacter, Character: character}
		case unicode.Is(unicode.Cf, character):
			return &LogStreamNameCharacterError{Kind: ErrLogStreamNameFormatCharacter, Character: character}
		case unicode.IsSpace(character) && character != ' ':
			return &LogStreamNameCharacterError{Kind: ErrLogStreamNameNonASCIIWhitespace, Character: character}
		}
	}

	if name != strings.TrimSpace(name) {
		return ErrLogStreamNameSurroundingWhitespace
	}
	return nil
}

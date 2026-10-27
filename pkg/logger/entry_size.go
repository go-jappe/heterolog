package logger

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// ErrLogErrorTextPanicked reports that the Error method of an error passed to a
// log call panicked, so its text could not be read. The panic does not reach the
// caller: it is returned as this error, with the recovered value in the message.
var ErrLogErrorTextPanicked = errors.New("error method panicked")

const (
	canonicalEntryMessagePrefix = `{"message":`
	canonicalEntryErrorPrefix   = `,"error":`
	canonicalEntryFieldsPrefix  = `,"fields":[`
	canonicalFieldKeyPrefix     = `{"key":`
	canonicalFieldValuePrefix   = `,"value":`
)

// MeasureLogEntrySize returns the byte length of the application logging API's
// canonical logical-entry representation:
//
//	{"message":<JSON string>,"error":<JSON string>,"fields":[
//	  {"key":<JSON string>,"value":<JSON value>}
//	]}
//
// error is omitted when nil, fields is omitted when no defined Field is present,
// and fields otherwise retain call order and duplicate keys. Keeping fields as an
// ordered sequence makes the input budget account for every value the caller asks
// each backend to inspect, even though a backend may later normalize duplicate keys.
// Values passed to Any use the immutable JSON snapshot that Any captured.
//
// This representation intentionally excludes timestamps, resource attributes and
// generated stack frames. Those are backend-specific expansion and remain subject
// to each backend's encoded-line/document/request validation.
func MeasureLogEntrySize(
	message string,
	errorMessage error,
	fields ...Field,
) (int, error) {
	var size uint64
	if err := addLogicalEntrySize(&size, uint64(len(canonicalEntryMessagePrefix))); err != nil {
		return 0, err
	}
	if err := addLogicalEntrySize(&size, canonicalJSONStringSize(message)); err != nil {
		return 0, err
	}

	if errorMessage != nil {
		errorText, err := logErrorText(errorMessage)
		if err != nil {
			return 0, err
		}
		if err := addLogicalEntrySize(&size, uint64(len(canonicalEntryErrorPrefix))); err != nil {
			return 0, err
		}
		if err := addLogicalEntrySize(&size, canonicalJSONStringSize(errorText)); err != nil {
			return 0, err
		}
	}

	definedFields := 0
	for _, field := range fields {
		if field.IsDefined() {
			definedFields++
		}
	}
	if definedFields > 0 {
		if err := addLogicalEntrySize(&size, uint64(len(canonicalEntryFieldsPrefix))); err != nil {
			return 0, err
		}

		fieldIndex := 0
		for _, field := range fields {
			if !field.IsDefined() {
				continue
			}
			if fieldIndex > 0 {
				if err := addLogicalEntrySize(&size, 1); err != nil { // カンマ
					return 0, err
				}
			}
			fieldIndex++

			if err := addLogicalEntrySize(&size, uint64(len(canonicalFieldKeyPrefix))); err != nil {
				return 0, err
			}
			if err := addLogicalEntrySize(&size, canonicalJSONStringSize(field.key)); err != nil {
				return 0, err
			}
			if err := addLogicalEntrySize(&size, uint64(len(canonicalFieldValuePrefix))); err != nil {
				return 0, err
			}

			valueSize, err := canonicalFieldValueSize(field.value)
			if err != nil {
				return 0, fmt.Errorf("measure log field %q: %w", field.key, err)
			}
			if err := addLogicalEntrySize(&size, valueSize); err != nil {
				return 0, err
			}
			if err := addLogicalEntrySize(&size, 1); err != nil { // fieldのobjectを閉じる括弧
				return 0, err
			}
		}

		if err := addLogicalEntrySize(&size, 1); err != nil { // fieldsの配列を閉じる括弧
			return 0, err
		}
	}

	if err := addLogicalEntrySize(&size, 1); err != nil { // entryのobjectを閉じる括弧
		return 0, err
	}
	return int(size), nil
}

// ValidateLogEntrySize returns a *LogEntryTooLargeError only when the canonical
// size of a logical entry is strictly larger than maxBytes. An entry whose
// canonical size equals maxBytes is accepted.
func ValidateLogEntrySize(
	maxBytes int,
	message string,
	errorMessage error,
	fields ...Field,
) error {
	if maxBytes <= 0 {
		return fmt.Errorf("%w: %d", ErrInvalidMaxLogEntryBytes, maxBytes)
	}

	actualBytes, err := MeasureLogEntrySize(message, errorMessage, fields...)
	if err != nil {
		return err
	}
	if actualBytes > maxBytes {
		return &LogEntryTooLargeError{
			ActualBytes: actualBytes,
			MaxBytes:    maxBytes,
		}
	}
	return nil
}

func canonicalFieldValueSize(value any) (uint64, error) {
	switch typed := value.(type) {
	case jsonSnapshot:
		return uint64(len(typed)), nil
	case string:
		return canonicalJSONStringSize(typed), nil
	case bool:
		if typed {
			return uint64(len("true")), nil
		}
		return uint64(len("false")), nil
	case int:
		return uint64(len(strconv.FormatInt(int64(typed), 10))), nil
	case int64:
		return uint64(len(strconv.FormatInt(typed, 10))), nil
	case uint64:
		return uint64(len(strconv.FormatUint(typed, 10))), nil
	case float64:
		// Fieldのconstructorは、NaNとInfを拒否します。小さなscalarをencoding/jsonに
		// 任せ、指数の書式の規則をそのまま保ちます。
		encoded, err := json.Marshal(typed)
		if err != nil {
			return 0, err
		}
		return uint64(len(encoded)), nil
	default:
		// Fieldのfieldは非公開で、constructorは値を上の場合に限っています。今後Fieldの
		// 値の型が増えた場合に備えて、この経路を残します。
		encoded, err := json.Marshal(typed)
		if err != nil {
			return 0, err
		}
		return uint64(len(encoded)), nil
	}
}

// canonicalJSONStringSizeは、正規の文字列の符号化でのvalueの長さを返します。
// '\\'・'"'・\b・\f・\n・\r・\tは短いescape、そのほかの制御文字・'<'・'>'・'&'・
// U+2028・U+2029は6byteの\u escape、不正なUTF-8の各byteは6byteの\ufffdのescapeとして
// 数えます。これはencoding/jsonのescapeの規則です。置換文字をそのまま書くencoder
// （Go 1.27以降のencoding/json）は、これより短くなることはあっても長くはならないため、
// この大きさは上限になります。大きすぎるmessageの写しを、もう一つ作らずに済みます。
func canonicalJSONStringSize(value string) uint64 {
	size := uint64(2) // 開きと閉じの引用符
	for index := 0; index < len(value); {
		character := value[index]
		if character < utf8.RuneSelf {
			switch character {
			case '\\', '"', '\b', '\f', '\n', '\r', '\t':
				size += 2
			case '<', '>', '&':
				size += 6
			default:
				if character < 0x20 {
					size += 6
				} else {
					size++
				}
			}
			index++
			continue
		}

		runeValue, runeBytes := utf8.DecodeRuneInString(value[index:])
		if runeValue == utf8.RuneError && runeBytes == 1 {
			size += 6 // \ufffd
			index++
			continue
		}
		if runeValue == '\u2028' || runeValue == '\u2029' {
			size += 6
		} else {
			size += uint64(runeBytes)
		}
		index += runeBytes
	}
	return size
}

func logErrorText(err error) (text string, resultErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			resultErr = fmt.Errorf("read log error text: %w: %v", ErrLogErrorTextPanicked, recovered)
		}
	}()
	return err.Error(), nil
}

func addLogicalEntrySize(total *uint64, additional uint64) error {
	maxInt := uint64(^uint(0) >> 1)
	if additional > maxInt-*total {
		return fmt.Errorf("log entry size exceeds int range")
	}
	*total += additional
	return nil
}

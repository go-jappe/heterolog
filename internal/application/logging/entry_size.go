package logging

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// ErrLogErrorTextPanickedは、logへ渡されたerrorのError methodがpanicしたため、
// そのerror文を読み取れなかったことを表します。panicはlog呼び出し元へ伝えずにこの
// errorとして返し、回復した値をmessageに含めます。
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
// Any values use the immutable JSON snapshot captured by Field.Any.
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
				if err := addLogicalEntrySize(&size, 1); err != nil { // comma
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
			if err := addLogicalEntrySize(&size, 1); err != nil { // field object closing brace
				return 0, err
			}
		}

		if err := addLogicalEntrySize(&size, 1); err != nil { // fields array closing bracket
			return 0, err
		}
	}

	if err := addLogicalEntrySize(&size, 1); err != nil { // entry object closing brace
		return 0, err
	}
	return int(size), nil
}

// ValidateLogEntrySize rejects a logical entry only when it is strictly larger
// than maxBytes. An entry whose canonical size equals maxBytes is accepted.
func ValidateLogEntrySize(
	maxBytes int,
	message string,
	errorMessage error,
	fields ...Field,
) error {
	if maxBytes <= 0 {
		return fmt.Errorf("%w: %d", pkglogger.ErrInvalidMaxLogEntryBytes, maxBytes)
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
		// Field constructors reject NaN and Inf. Delegating the small scalar to
		// encoding/json preserves its exponent-formatting contract exactly.
		encoded, err := json.Marshal(typed)
		if err != nil {
			return 0, err
		}
		return uint64(len(encoded)), nil
	default:
		// Field's fields are private and constructors currently restrict values to
		// the cases above. Keep this defensive path for future Field value types.
		encoded, err := json.Marshal(typed)
		if err != nil {
			return 0, err
		}
		return uint64(len(encoded)), nil
	}
}

// canonicalJSONStringSize returns the length of value in the canonical string
// encoding: short escapes for '\\', '"', \b, \f, \n, \r and \t, six-byte \u escapes
// for other control characters, '<', '>', '&', U+2028 and U+2029, and a six-byte
// \ufffd escape for each invalid UTF-8 byte. These are encoding/json's escaping
// rules; an encoder that writes the replacement character raw (as encoding/json
// does since Go 1.27) produces fewer bytes, never more, so the size is an upper
// bound. It avoids allocating a second copy of an oversized message.
func canonicalJSONStringSize(value string) uint64 {
	size := uint64(2) // opening and closing quotes
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

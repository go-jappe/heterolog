package logger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrFieldKeyInvalid reports a string that cannot be the key of a structured
	// attribute because it is empty, is not valid UTF-8, or contains whitespace or
	// control characters.
	ErrFieldKeyInvalid = errors.New("log field key is invalid")
	// ErrFieldValueNotJSON reports that a value passed to Any cannot be represented
	// as JSON, or cannot be kept and restored as a JSON snapshot, for example
	// because it nests deeper than MaxFieldNestingDepth.
	ErrFieldValueNotJSON = errors.New("log field value is not JSON-encodable")
	// ErrFieldFloatNotFinite reports a NaN or infinite value, which JSON cannot
	// represent as a number.
	ErrFieldFloatNotFinite = errors.New("log field float value must be finite")
)

// MaxFieldNestingDepth is the deepest nesting of arrays and objects that Any
// and slog attributes accept: a scalar is depth 0 and one enclosing array is
// depth 1.
//
// Spool records, the JSON written to stdout and files, and the payload of each
// backend add a few levels around each value. The limit leaves 1,000 of the
// 10,000 levels that JSON encoding and decoding handle for those levels, and a
// deeper value is rejected when it is created, before it could be stored and
// then fail to be restored.
const MaxFieldNestingDepth = 9000

// Field is one immutable structured attribute that an application passes to a
// Logger.
//
// Its fields are unexported, so a valid Field can only be built with the typed
// constructors of this package. They keep the type of the application's value,
// validate the key, and snapshot values passed to Any. The zero value is an
// undefined Field, which loggers ignore.
type Field struct {
	key   string
	value any // primitive値、またはAnyが生成したjsonSnapshot
	// slogは空のattribute keyを認めます。この印を立てるのはNewSlogFieldだけで、
	// ほかのconstructorは厳密なkeyの検証を保ちます。
	emptyKeyDefined bool
}

// FieldKey is a key validated as the name of a structured attribute. Once it
// is created with NewFieldKey or MustFieldKey, it builds string, bool and
// integer fields repeatedly without errors. The zero value is invalid, and its
// methods panic.
type FieldKey struct {
	value string
}

// jsonSnapshotは、Anyへ渡された値を呼出時点で固定したJSONです。
// privateな識別型としてvalueへ格納し、Valueからbyte slice自体を外部へ公開しません。
type jsonSnapshot []byte

var requestIDFieldKey = MustFieldKey("request_id")

// NewFieldKey validates key and returns a FieldKey for building fields
// repeatedly. Use it for keys that are not constants in the source code, such as
// keys from runtime configuration.
func NewFieldKey(key string) (FieldKey, error) {
	if err := validateFieldKey(key); err != nil {
		return FieldKey{}, err
	}
	return FieldKey{value: key}, nil
}

// MustFieldKey returns a validated FieldKey and panics if key is invalid. Use it
// only for constant keys, such as package variables, that are known when the
// program is written.
func MustFieldKey(key string) FieldKey {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		panic(err)
	}
	return fieldKey
}

// WithRequestID returns a Field that sets the request ID of a single log call.
// An empty id sets no request ID.
func WithRequestID(id string) Field {
	return requestIDFieldKey.String(id)
}

// String returns a Field holding a string value. It returns an error instead of
// a Field when key is invalid.
func String(key, value string) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.String(value), nil
}

// Bool returns a Field holding a bool value. It returns an error instead of a
// Field when key is invalid.
func Bool(key string, value bool) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Bool(value), nil
}

// Int returns a Field holding an int value. It returns an error instead of a
// Field when key is invalid.
func Int(key string, value int) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Int(value), nil
}

// Int64 returns a Field holding an int64 value. It returns an error instead of
// a Field when key is invalid.
func Int64(key string, value int64) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Int64(value), nil
}

// Uint64 returns a Field holding a uint64 value. It returns an error instead of
// a Field when key is invalid.
func Uint64(key string, value uint64) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Uint64(value), nil
}

// Float64 returns a Field holding a finite float64 value. NaN and positive or
// negative infinity, which JSON cannot represent, return ErrFieldFloatNotFinite
// instead of a Field.
func Float64(key string, value float64) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Float64(value)
}

// Any returns a Field holding a JSON-encodable value, including objects and
// arrays. The value is encoded to JSON when Any is called, so later changes to
// the original map, slice or pointer do not affect the Field. A value that
// cannot be encoded, nests deeper than MaxFieldNestingDepth, or cannot be
// restored from its snapshot returns ErrFieldValueNotJSON instead of a Field.
func Any(key string, value any) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Any(value)
}

// String returns a Field holding a string value.
func (k FieldKey) String(value string) Field {
	return k.field(value)
}

// Bool returns a Field holding a bool value.
func (k FieldKey) Bool(value bool) Field {
	return k.field(value)
}

// Int returns a Field holding an int value.
func (k FieldKey) Int(value int) Field {
	return k.field(value)
}

// Int64 returns a Field holding an int64 value.
func (k FieldKey) Int64(value int64) Field {
	return k.field(value)
}

// Uint64 returns a Field holding a uint64 value.
func (k FieldKey) Uint64(value uint64) Field {
	return k.field(value)
}

// Float64 returns a Field holding a finite float64 value. NaN and positive or
// negative infinity, which JSON cannot represent, return ErrFieldFloatNotFinite
// instead of a Field.
func (k FieldKey) Float64(value float64) (Field, error) {
	key := k.validatedValue()
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return Field{}, fmt.Errorf("%w: key %q has value %v", ErrFieldFloatNotFinite, key, value)
	}
	return Field{key: key, value: value}, nil
}

// Any snapshots a JSON-encodable value, including objects and arrays, and
// returns a Field. A value that cannot be encoded, nests deeper than
// MaxFieldNestingDepth, or cannot be restored from its snapshot returns
// ErrFieldValueNotJSON instead of a Field.
func (k FieldKey) Any(value any) (Field, error) {
	key := k.validatedValue()
	encoded, err := json.Marshal(value)
	if err == nil {
		err = validateNestingDepth(encoded)
	}
	if err == nil {
		// Valueが使うものと同じ復元を、fieldを受け付ける前に確かめます。成功した
		// constructorの値が、後でnilにならないようにするためです。
		_, err = decodeJSONValue(encoded)
	}
	if err != nil {
		// JSON化のerrorは記録しようとした値やMarshalJSONが返したerrorを保持し得るため、
		// errors.Asで取り出せないよう原因としては包みません。種類はErrFieldValueNotJSONで判定します。
		return Field{}, fmt.Errorf(
			"%w: key %q has Go type %T: %v",
			ErrFieldValueNotJSON,
			key,
			value,
			err,
		)
	}
	return Field{
		key:   key,
		value: jsonSnapshot(append([]byte(nil), encoded...)),
	}, nil
}

// Key returns the name of the attribute.
func (f Field) Key() string {
	return f.key
}

// Value returns the value of the attribute. For a Field created by Any, each
// call returns an independent JSON value, so callers cannot modify the snapshot.
func (f Field) Value() any {
	snapshot, ok := f.value.(jsonSnapshot)
	if !ok {
		return f.value
	}
	value, err := decodeJSONValue(snapshot)
	if err != nil {
		// jsonSnapshotはAnyで検証してから非公開型として保存するため、通常は到達しません。
		return nil
	}
	return value
}

// IsDefined reports whether the Field has a valid key. The constructors reject
// an empty key, so the zero value reports false and loggers ignore it among
// their variadic fields.
func (f Field) IsDefined() bool {
	return f.key != "" || f.emptyKeyDefined
}

func (k FieldKey) field(value any) Field {
	return Field{key: k.validatedValue(), value: value}
}

func (k FieldKey) validatedValue() string {
	if k.value == "" {
		panic(fmt.Errorf("%w: zero FieldKey cannot create a Field", ErrFieldKeyInvalid))
	}
	return k.value
}

func validateFieldKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: key must not be empty", ErrFieldKeyInvalid)
	}
	if !utf8.ValidString(key) {
		return fmt.Errorf("%w: key %q must be valid UTF-8", ErrFieldKeyInvalid, key)
	}
	for byteIndex, character := range key {
		if unicode.IsSpace(character) {
			return fmt.Errorf(
				"%w: key %q contains whitespace %U at byte index %d",
				ErrFieldKeyInvalid,
				key,
				character,
				byteIndex,
			)
		}
		if unicode.IsControl(character) {
			return fmt.Errorf(
				"%w: key %q contains control character %U at byte index %d",
				ErrFieldKeyInvalid,
				key,
				character,
				byteIndex,
			)
		}
	}
	return nil
}

// validateNestingDepthは、符号化済みJSONの配列・objectの入れ子がMaxFieldNestingDepth以下かを
// 確認します。encoding/jsonの実装ごとの上限に依存しないよう、文字列の外側の括弧を数えます。
// encodedはjson.Marshalが生成した妥当なJSONです。
func validateNestingDepth(encoded []byte) error {
	depth := 0
	inString, escaped := false, false
	for _, character := range encoded {
		if inString {
			switch {
			case escaped:
				escaped = false
			case character == '\\':
				escaped = true
			case character == '"':
				inString = false
			}
			continue
		}
		switch character {
		case '"':
			inString = true
		case '[', '{':
			depth++
			if depth > MaxFieldNestingDepth {
				return fmt.Errorf("value nests arrays and objects deeper than %d levels", MaxFieldNestingDepth)
			}
		case ']', '}':
			depth--
		}
	}
	return nil
}

func decodeJSONValue(encoded []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("decode encoded field value: %w", err)
	}

	return value, nil
}

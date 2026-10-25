package logging

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
	// ErrFieldKeyInvalidは、空、UTF-8として不正、または空白・制御文字を含むため、
	// 構造化log属性のkeyとして使用できない文字列を示します。
	ErrFieldKeyInvalid = errors.New("log field key is invalid")
	// ErrFieldValueNotJSONは、Anyへ渡された値をJSONとして表現できない、または
	// MaxFieldNestingDepthを超えるネストなどのためJSON snapshotとして保持・復元できない
	// ことを示します。
	ErrFieldValueNotJSON = errors.New("log field value is not JSON-encodable")
	// ErrFieldFloatNotFiniteは、JSONに数値として表現できないNaNまたはInfを示します。
	ErrFieldFloatNotFinite = errors.New("log field float value must be finite")
)

// MaxFieldNestingDepthは、AnyとslogのFieldが保持できる配列・objectの入れ子の最大段数です。
// 文字列や数値などの値だけなら0段、配列一つで包めば1段と数えます。
//
// spool record、stdout・fileのJSON、各backendの送信形式は、値の外側に数段の階層を加えます。
// JSONの符号化・復号が扱える10000段に対し、それらの階層のために1000段の余裕を残します。
// これを超える値は、保存後に復元できなくなる前に生成時点で拒否します。
const MaxFieldNestingDepth = 9000

// Fieldは、applicationからLoggerへ渡す一つの構造化log属性です。
//
// 構造体の各fieldは非公開のため、利用者はpackageが提供する型付きconstructor以外から
// 有効なFieldを組み立てられません。これにより、application入力の型保持、keyの検証、
// Anyのsnapshotを一つの生成経路で保証します。zero valueは未設定Fieldであり、Loggerは
// 無視します。
type Field struct {
	key   string
	value any // primitive値、またはAnyが生成したjsonSnapshot
	// slog permits an empty attribute key. Only its internal adapter can set
	// this flag; the public field constructors retain their strict key checks.
	emptyKeyDefined bool
}

// FieldKeyは、構造化log属性名として検証済みのkeyです。
// NewFieldKeyまたはMustFieldKeyで一度生成すれば、primitive値のFieldをerrorなしで
// 繰り返し作れます。zero valueは無効であり、値生成methodを呼ぶとpanicします。
type FieldKey struct {
	value string
}

// jsonSnapshotは、Anyへ渡された値を呼出時点で固定したJSONです。
// privateな識別型としてvalueへ格納し、Valueからbyte slice自体を外部へ公開しません。
type jsonSnapshot []byte

var requestIDFieldKey = MustFieldKey("request_id")

// NewFieldKeyはkeyを検証し、繰り返しField生成に使用できるFieldKeyを返します。
// runtime設定など、source code上の固定値ではないkeyにはこの関数を使用します。
func NewFieldKey(key string) (FieldKey, error) {
	if err := validateFieldKey(key); err != nil {
		return FieldKey{}, err
	}
	return FieldKey{value: key}, nil
}

// MustFieldKeyは検証済みのFieldKeyを返し、keyが不正ならpanicします。
// package変数など、開発時に値が確定している固定keyだけに使用します。
func MustFieldKey(key string) FieldKey {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		panic(err)
	}
	return fieldKey
}

// WithRequestIDは、このlog呼び出しでcontext上のrequest IDを上書きするFieldを返します。
func WithRequestID(id string) Field {
	return requestIDFieldKey.String(id)
}

// Stringはstring値を保持するFieldを返します。keyが不正な場合はFieldを生成しません。
func String(key, value string) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.String(value), nil
}

// Boolはbool値を保持するFieldを返します。keyが不正な場合はFieldを生成しません。
func Bool(key string, value bool) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Bool(value), nil
}

// Intはint値を保持するFieldを返します。keyが不正な場合はFieldを生成しません。
func Int(key string, value int) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Int(value), nil
}

// Int64はint64値を保持するFieldを返します。keyが不正な場合はFieldを生成しません。
func Int64(key string, value int64) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Int64(value), nil
}

// Uint64はuint64値を保持するFieldを返します。keyが不正な場合はFieldを生成しません。
func Uint64(key string, value uint64) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Uint64(value), nil
}

// Float64は有限なfloat64値を保持するFieldを返します。
// JSONが表現できないNaNと正負のInfはFieldを生成せず、ErrFieldFloatNotFiniteを返します。
func Float64(key string, value float64) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Float64(value)
}

// Anyはobjectやarrayを含むJSON化可能な値のFieldを返します。
// 値は呼出時にJSONへencodeして保持するため、生成後に元のmap、slice、pointerを変更しても
// Fieldへ反映されません。JSON化できない値、MaxFieldNestingDepthを超えて入れ子になった値、
// snapshotから復元できない値はFieldを生成せず、ErrFieldValueNotJSONを返します。
func Any(key string, value any) (Field, error) {
	fieldKey, err := NewFieldKey(key)
	if err != nil {
		return Field{}, err
	}
	return fieldKey.Any(value)
}

// Stringはstring値を保持するFieldを返します。
func (k FieldKey) String(value string) Field {
	return k.field(value)
}

// Boolはbool値を保持するFieldを返します。
func (k FieldKey) Bool(value bool) Field {
	return k.field(value)
}

// Intはint値を保持するFieldを返します。
func (k FieldKey) Int(value int) Field {
	return k.field(value)
}

// Int64はint64値を保持するFieldを返します。
func (k FieldKey) Int64(value int64) Field {
	return k.field(value)
}

// Uint64はuint64値を保持するFieldを返します。
func (k FieldKey) Uint64(value uint64) Field {
	return k.field(value)
}

// Float64は有限なfloat64値を保持するFieldを返します。
// JSONが表現できないNaNと正負のInfはFieldを生成せず、ErrFieldFloatNotFiniteを返します。
func (k FieldKey) Float64(value float64) (Field, error) {
	key := k.validatedValue()
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return Field{}, fmt.Errorf("%w: key %q has value %v", ErrFieldFloatNotFinite, key, value)
	}
	return Field{key: key, value: value}, nil
}

// Anyはobjectやarrayを含むJSON化可能な値を呼出時点でsnapshotし、Fieldを返します。
// JSON化できない値、MaxFieldNestingDepthを超えて入れ子になった値、snapshotから復元できない値は
// Fieldを生成せず、ErrFieldValueNotJSONを返します。
func (k FieldKey) Any(value any) (Field, error) {
	key := k.validatedValue()
	encoded, err := json.Marshal(value)
	if err == nil {
		err = validateNestingDepth(encoded)
	}
	if err == nil {
		// Validate the same restoration used by Value before accepting a field,
		// so a successful constructor cannot later turn its value into nil.
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

// KeyはFieldの属性名を返します。
func (f Field) Key() string {
	return f.key
}

// ValueはFieldの値を返します。Anyで生成したFieldは、内部snapshotを外部から変更できないよう
// 呼び出しごとに独立したJSON値を返します。
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

// IsDefinedは、有効なkeyを持つFieldかを返します。constructorは空keyを受理しないため、
// Fieldのzero valueはfalseとなり、Loggerへの可変長引数に含まれていても無視されます。
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

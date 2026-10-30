package otlpcommon

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ErrInvalidJSONは、読めないOTLP/JSONを表します。
var ErrInvalidJSON = errors.New("invalid OTLP JSON")

// FromJSONResourceは、OTLP/JSONのResourceとschemaUrlを、Resourceに戻します。
func FromJSONResource(resource JSONResource, schemaURL string) (*telemetrydata.Resource, error) {
	attrs, err := FromJSONKeyValues(resource.Attributes)
	if err != nil {
		return nil, err
	}
	return &telemetrydata.Resource{
		Attrs:        attrs,
		DroppedAttrs: int(resource.DroppedAttributesCount),
		SchemaURL:    schemaURL,
	}, nil
}

// FromJSONKeyValuesは、OTLP/JSONのKeyValueを属性に戻します。値は、文字列、bool、int64、
// float64、[]anyです。ToJSONKeyValuesが書かない値（bytesValue・kvlistValueなど）は、
// ErrInvalidJSONを包んだerrorにします。
func FromJSONKeyValues(kvs []JSONKeyValue) ([]pkgobserver.Attr, error) {
	if len(kvs) == 0 {
		return nil, nil
	}
	attrs := make([]pkgobserver.Attr, len(kvs))
	for i, kv := range kvs {
		value, err := fromJSONAnyValue(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", kv.Key, err)
		}
		attrs[i] = pkgobserver.Attr{Key: kv.Key, Value: value}
	}
	return attrs, nil
}

func fromJSONAnyValue(v JSONAnyValue) (any, error) {
	switch {
	case v.StringValue != nil:
		return *v.StringValue, nil
	case v.BoolValue != nil:
		return *v.BoolValue, nil
	case v.IntValue != nil:
		i, err := strconv.ParseInt(*v.IntValue, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: intValue %q is not an integer", ErrInvalidJSON, *v.IntValue)
		}
		return i, nil
	case v.DoubleValue != nil:
		return FromJSONDouble(v.DoubleValue)
	case v.ArrayValue != nil:
		values := make([]any, len(v.ArrayValue.Values))
		for i, element := range v.ArrayValue.Values {
			value, err := fromJSONAnyValue(element)
			if err != nil {
				return nil, err
			}
			values[i] = value
		}
		return values, nil
	default:
		return nil, fmt.Errorf("%w: the value has no supported field", ErrInvalidJSON)
	}
}

// FromJSONDoubleは、JSONDoubleが書いた浮動小数点数を読みます。
func FromJSONDouble(raw json.RawMessage) (float64, error) {
	switch string(raw) {
	case `"NaN"`:
		return math.NaN(), nil
	case `"Infinity"`:
		return math.Inf(1), nil
	case `"-Infinity"`:
		return math.Inf(-1), nil
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0, fmt.Errorf("%w: doubleValue %s is not a number", ErrInvalidJSON, raw)
	}
	return f, nil
}

// FromJSONTimeは、10進数の文字列の時刻を読みます。0は、時刻がないものにします。
func FromJSONTime(s, field string) (time.Time, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n > math.MaxInt64 {
		return time.Time{}, fmt.Errorf("%w: %s is not a time in nanoseconds", ErrInvalidJSON, field)
	}
	if n == 0 {
		return time.Time{}, nil
	}
	return time.Unix(0, int64(n)).UTC(), nil
}

// DecodeIDは、16進数のIDをdstへ読みます。optionalなら、空の値はIDがないものにします。
func DecodeID(dst []byte, s, field string, optional bool) error {
	if s == "" && optional {
		return nil
	}
	if len(s) != 2*len(dst) {
		return fmt.Errorf("%w: %s has %d hexadecimal digits, want %d", ErrInvalidJSON, field, len(s), 2*len(dst))
	}
	if _, err := hex.Decode(dst, []byte(s)); err != nil {
		return fmt.Errorf("%w: %s is not hexadecimal", ErrInvalidJSON, field)
	}
	return nil
}

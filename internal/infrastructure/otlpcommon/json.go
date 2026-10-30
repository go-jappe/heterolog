package otlpcommon

import (
	"encoding/json"
	"math"
	"strconv"
	"time"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
)

// JSONResourceは、OTLP/JSONのResourceです。
type JSONResource struct {
	Attributes             []JSONKeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32         `json:"droppedAttributesCount,omitempty"`
}

// JSONScopeは、OTLP/JSONのInstrumentationScopeです。
type JSONScope struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// JSONKeyValueは、OTLP/JSONのKeyValueです。
type JSONKeyValue struct {
	Key   string       `json:"key"`
	Value JSONAnyValue `json:"value"`
}

// JSONAnyValueは、AnyValueのoneofのうち、1つだけを書きます。
type JSONAnyValue struct {
	StringValue *string         `json:"stringValue,omitempty"`
	BoolValue   *bool           `json:"boolValue,omitempty"`
	IntValue    *string         `json:"intValue,omitempty"`
	DoubleValue json.RawMessage `json:"doubleValue,omitempty"`
	ArrayValue  *JSONArrayValue `json:"arrayValue,omitempty"`
}

// JSONArrayValueは、OTLP/JSONのArrayValueです。
type JSONArrayValue struct {
	Values []JSONAnyValue `json:"values"`
}

// ToJSONResourceは、ResourceをOTLP/JSONのResourceと、ResourceSpansなどのschemaUrlにします。
// nilなら、空のResourceと空のschemaUrlです。
func ToJSONResource(resource *telemetrydata.Resource) (JSONResource, string) {
	if resource == nil {
		return JSONResource{}, ""
	}
	return JSONResource{
		Attributes:             ToJSONKeyValues(ToKeyValues(resource.Attrs)),
		DroppedAttributesCount: JSONCount(resource.DroppedAttrs),
	}, resource.SchemaURL
}

// ToJSONKeyValuesは、属性をOTLP/JSONのKeyValueにします。
func ToJSONKeyValues(attrs []KeyValue) []JSONKeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]JSONKeyValue, len(attrs))
	for i, attr := range attrs {
		out[i] = JSONKeyValue{Key: attr.key, Value: toJSONAnyValue(attr.value)}
	}
	return out
}

func toJSONAnyValue(v value) JSONAnyValue {
	switch v.kind {
	case kindBool:
		return JSONAnyValue{BoolValue: &v.b}
	case kindInt:
		s := strconv.FormatInt(v.i, 10)
		return JSONAnyValue{IntValue: &s}
	case kindDouble:
		return JSONAnyValue{DoubleValue: JSONDouble(v.f)}
	case kindArray:
		values := make([]JSONAnyValue, len(v.array))
		for i, element := range v.array {
			values[i] = toJSONAnyValue(element)
		}
		return JSONAnyValue{ArrayValue: &JSONArrayValue{Values: values}}
	default:
		return JSONAnyValue{StringValue: &v.str}
	}
}

// JSONDoubleは、protobufのJSONの規則で浮動小数点数を書きます。JSONの数値で表せない
// NaNと無限大は、文字列にします。
func JSONDouble(f float64) json.RawMessage {
	switch {
	case math.IsNaN(f):
		return json.RawMessage(`"NaN"`)
	case math.IsInf(f, 1):
		return json.RawMessage(`"Infinity"`)
	case math.IsInf(f, -1):
		return json.RawMessage(`"-Infinity"`)
	}
	return strconv.AppendFloat(nil, f, 'g', -1, 64)
}

// JSONCountは、数をuint32の範囲に抑えます。
func JSONCount(n int) uint32 {
	return uint32(min(max(n, 0), math.MaxUint32))
}

// JSONTimeは、時刻をOTLP/JSONの、Unix epochからのnanosecondの10進数の文字列にします。
func JSONTime(t time.Time) string {
	return strconv.FormatUint(UnixNano(t), 10)
}

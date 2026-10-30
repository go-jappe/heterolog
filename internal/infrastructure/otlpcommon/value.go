// Package otlpcommonは、OTLP（OpenTelemetry Protocol）の、spanとmetricsに共通する部分の
// 変換です。属性（KeyValueとAnyValue）、Resource、InstrumentationScopeを、protobufとOTLP/JSON
// へ書き、OTLP/JSONから読みます。OTLP/JSONは、IDは16進数、enumは数値、64-bitの整数は文字列と
// いう規則に従います。生成したprotobufの型は使わず、google.golang.org/protobuf/encoding/
// protowireで直接書きます。protobufの型とreflectionを、heterologを使うすべてのprogramへ
// 入れないためです。
package otlpcommon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// valueKindは、OTLPのAnyValueのうち、どの値を持つかです。
type valueKind int

const (
	kindString valueKind = iota
	kindBool
	kindInt
	kindDouble
	kindArray
)

// valueは、属性の値をAnyValueに変換したものです。
type value struct {
	kind  valueKind
	str   string
	b     bool
	i     int64
	f     float64
	array []value
}

// toValueは、属性の値を、型を保ったままAnyValueに変換します。対応しない型は、
// internal/application/otelと同じくfmt.Sprintで文字列にします。
func toValue(v any) value {
	switch v := v.(type) {
	case string:
		return value{kind: kindString, str: v}
	case bool:
		return value{kind: kindBool, b: v}
	case int:
		return value{kind: kindInt, i: int64(v)}
	case int8:
		return value{kind: kindInt, i: int64(v)}
	case int16:
		return value{kind: kindInt, i: int64(v)}
	case int32:
		return value{kind: kindInt, i: int64(v)}
	case int64:
		return value{kind: kindInt, i: v}
	case uint:
		return uintValue(uint64(v))
	case uint8:
		return value{kind: kindInt, i: int64(v)}
	case uint16:
		return value{kind: kindInt, i: int64(v)}
	case uint32:
		return value{kind: kindInt, i: int64(v)}
	case uint64:
		return uintValue(v)
	case float32:
		return value{kind: kindDouble, f: float64(v)}
	case float64:
		return value{kind: kindDouble, f: v}
	case []string:
		return arrayValue(v)
	case []bool:
		return arrayValue(v)
	case []int:
		return arrayValue(v)
	case []int64:
		return arrayValue(v)
	case []float64:
		return arrayValue(v)
	case []any:
		return arrayValue(v)
	default:
		// fmt.Sprintは、errorとfmt.Stringerのメソッドを呼び、nil pointerでpanicした場合も
		// 文字列にします。
		return value{kind: kindString, str: fmt.Sprint(v)}
	}
}

// uintValueは、int64で表せない値を文字列にします。
func uintValue(v uint64) value {
	if v > math.MaxInt64 {
		return value{kind: kindString, str: fmt.Sprint(v)}
	}
	return value{kind: kindInt, i: int64(v)}
}

func arrayValue[T any](values []T) value {
	out := make([]value, len(values))
	for i, v := range values {
		out[i] = toValue(v)
	}
	return value{kind: kindArray, array: out}
}

// KeyValueは、変換した属性です。
type KeyValue struct {
	key   string
	value value
}

// ToKeyValuesは、属性を、型を保ったままAnyValueの属性に変換します。
func ToKeyValues(attrs []pkgobserver.Attr) []KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]KeyValue, len(attrs))
	for i, attr := range attrs {
		out[i] = KeyValue{key: attr.Key, value: toValue(attr.Value)}
	}
	return out
}

// PlainValueは、属性の値を、OTLPでない形式（OpenSearchの文書など）で使う値にします。
// 文字列・bool・整数（int64）・小数（float64）はそのまま返します。JSONの数値で表せない
// NaNと無限大は、OTLP/JSONと同じ文字列にします。配列は、OpenTelemetryの仕様がOTLPでない
// 送り先に勧めるとおり、JSONの配列の文字列にします。
func PlainValue(v any) any {
	return toValue(v).plain()
}

// StringValueは、属性の値を文字列にします（ClickHouseのMap(String, String)など）。文字列は
// そのまま、boolと数はJSONと同じ書き方、配列はJSONの配列の文字列にします。
func StringValue(v any) string {
	return toValue(v).text()
}

func (v value) plain() any {
	switch v.kind {
	case kindBool:
		return v.b
	case kindInt:
		return v.i
	case kindDouble:
		if math.IsNaN(v.f) || math.IsInf(v.f, 0) {
			return v.text()
		}
		return v.f
	case kindArray:
		return v.text()
	default:
		return v.str
	}
}

func (v value) text() string {
	switch v.kind {
	case kindBool:
		return strconv.FormatBool(v.b)
	case kindInt:
		return strconv.FormatInt(v.i, 10)
	case kindDouble:
		return strings.Trim(string(JSONDouble(v.f)), `"`)
	case kindArray:
		return string(v.appendJSON(nil))
	default:
		return v.str
	}
}

// appendJSONは、値をJSONで書きます。配列の要素の小数は、JSONDoubleで書きます。
func (v value) appendJSON(b []byte) []byte {
	switch v.kind {
	case kindBool:
		return strconv.AppendBool(b, v.b)
	case kindInt:
		return strconv.AppendInt(b, v.i, 10)
	case kindDouble:
		return append(b, JSONDouble(v.f)...)
	case kindArray:
		b = append(b, '[')
		for i, element := range v.array {
			if i > 0 {
				b = append(b, ',')
			}
			b = element.appendJSON(b)
		}
		return append(b, ']')
	default:
		// 読みやすいよう、<・>・&はescapeしません。
		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.SetEscapeHTML(false)
		_ = encoder.Encode(v.str)
		return append(b, bytes.TrimSuffix(buf.Bytes(), []byte("\n"))...)
	}
}

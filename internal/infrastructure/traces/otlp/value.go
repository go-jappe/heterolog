// Package otlpは、終わったspanをOTLP（OpenTelemetry Protocol）のprotobufとJSONに変換します。
// protobufは、OTLP/HTTPのExportTraceServiceRequestと同じ形のTracesDataです。JSONは、
// OTLP/JSONの規則（IDは16進数、enumは数値、64-bitの整数は文字列）に従います。
// 生成したprotobufの型は使わず、google.golang.org/protobuf/encoding/protowireで直接
// 書きます。protobufの型とreflectionを、heterologを使うすべてのprogramへ入れないためです。
package otlp

import (
	"fmt"
	"math"

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

// keyValueは、変換した属性です。
type keyValue struct {
	key   string
	value value
}

func toKeyValues(attrs []pkgobserver.Attr) []keyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]keyValue, len(attrs))
	for i, attr := range attrs {
		out[i] = keyValue{key: attr.Key, value: toValue(attr.Value)}
	}
	return out
}

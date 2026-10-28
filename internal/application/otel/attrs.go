package otel

import (
	"fmt"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"go.opentelemetry.io/otel/attribute"
)

// toOtelAttrsは、値の型を保ったままOpenTelemetryの属性に変換します。対応しない型は
// 文字列にします。
func toOtelAttrs(attrs []pkgobserver.Attr) []attribute.KeyValue {
	otelAttrs := make([]attribute.KeyValue, 0, len(attrs))

	for _, a := range attrs {
		switch v := a.Value.(type) {
		case string:
			otelAttrs = append(otelAttrs, attribute.String(a.Key, v))
		case bool:
			otelAttrs = append(otelAttrs, attribute.Bool(a.Key, v))
		case int:
			otelAttrs = append(otelAttrs, attribute.Int(a.Key, v))
		case int8:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case int16:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case int32:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case int64:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, v))
		case uint:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case uint8:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case uint16:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case uint32:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case uint64:
			if v <= uint64(^uint64(0)>>1) {
				otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
			} else {
				otelAttrs = append(otelAttrs, attribute.String(a.Key, fmt.Sprint(v)))
			}
		case float32:
			otelAttrs = append(otelAttrs, attribute.Float64(a.Key, float64(v)))
		case float64:
			otelAttrs = append(otelAttrs, attribute.Float64(a.Key, v))
		default:
			otelAttrs = append(otelAttrs, attribute.String(a.Key, fmt.Sprint(v)))
		}
	}

	return otelAttrs
}

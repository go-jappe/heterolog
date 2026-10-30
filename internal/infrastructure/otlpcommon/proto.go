package otlpcommon

import (
	"math"
	"time"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	"google.golang.org/protobuf/encoding/protowire"
)

// AppendResourceは、ResourceのmessageをResource（attributes = 1、dropped_attributes_count
// = 2）として書きます。nilなら、空のmessageにします。
func AppendResource(b []byte, num protowire.Number, resource *telemetrydata.Resource) []byte {
	return AppendMessage(b, num, func(b []byte) []byte {
		if resource == nil {
			return b
		}
		b = AppendKeyValues(b, 1, ToKeyValues(resource.Attrs))
		return AppendUint32(b, 2, resource.DroppedAttrs)
	})
}

// AppendScopeは、ScopeをInstrumentationScope（name = 1、version = 2）として書きます。
func AppendScope(b []byte, num protowire.Number, scope telemetrydata.Scope) []byte {
	return AppendMessage(b, num, func(b []byte) []byte {
		b = AppendString(b, 1, scope.Name)
		return AppendString(b, 2, scope.Version)
	})
}

// AppendKeyValuesは、属性を繰り返しのKeyValue（key = 1、value = 2）として書きます。
func AppendKeyValues(b []byte, num protowire.Number, attrs []KeyValue) []byte {
	for _, attr := range attrs {
		b = AppendMessage(b, num, func(b []byte) []byte {
			b = AppendString(b, 1, attr.key)
			return AppendMessage(b, 2, func(b []byte) []byte { return appendAnyValue(b, attr.value) })
		})
	}
	return b
}

// appendAnyValueは、AnyValueのoneofを書きます。既定値でも、どの値かを表すため書きます。
func appendAnyValue(b []byte, v value) []byte {
	switch v.kind {
	case kindBool:
		b = protowire.AppendTag(b, 2, protowire.VarintType)
		return protowire.AppendVarint(b, protowire.EncodeBool(v.b))
	case kindInt:
		b = protowire.AppendTag(b, 3, protowire.VarintType)
		return protowire.AppendVarint(b, uint64(v.i))
	case kindDouble:
		b = protowire.AppendTag(b, 4, protowire.Fixed64Type)
		return protowire.AppendFixed64(b, math.Float64bits(v.f))
	case kindArray:
		// ArrayValueのvalues = 1
		return AppendMessage(b, 5, func(b []byte) []byte {
			for _, element := range v.array {
				b = AppendMessage(b, 1, func(b []byte) []byte { return appendAnyValue(b, element) })
			}
			return b
		})
	default:
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		return protowire.AppendString(b, v.str)
	}
}

// AppendMessageは、writeが書いたmessageを、長さを付けたfieldとして書きます。
func AppendMessage(b []byte, num protowire.Number, write func([]byte) []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, write(nil))
}

// AppendStringは、空でない文字列を書きます。
func AppendString(b []byte, num protowire.Number, s string) []byte {
	if s == "" {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

// AppendBytesは、bytesを、空でも書きます。
func AppendBytes(b []byte, num protowire.Number, v []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

// AppendVarintは、0でない値を書きます。
func AppendVarint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

// AppendUint32は、正の値を、uint32の範囲に抑えて書きます。
func AppendUint32(b []byte, num protowire.Number, v int) []byte {
	if v <= 0 {
		return b
	}
	return AppendVarint(b, num, uint64(min(v, math.MaxUint32)))
}

// AppendFixed64は、0でない値を書きます。
func AppendFixed64(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.Fixed64Type)
	return protowire.AppendFixed64(b, v)
}

// AppendDoubleは、浮動小数点数を、0でも書きます。oneofの値として使うためです。
func AppendDouble(b []byte, num protowire.Number, v float64) []byte {
	b = protowire.AppendTag(b, num, protowire.Fixed64Type)
	return protowire.AppendFixed64(b, math.Float64bits(v))
}

// UnixNanoは、時刻をUnix epochからのnanosecondにします。zero値と、epochより前の時刻は
// 0（不明）にします。
func UnixNano(t time.Time) uint64 {
	if t.IsZero() || t.UnixNano() <= 0 {
		return 0
	}
	return uint64(t.UnixNano())
}

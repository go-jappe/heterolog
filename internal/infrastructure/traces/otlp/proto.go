package otlp

import (
	"math"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"google.golang.org/protobuf/encoding/protowire"
)

// MarshalProtoは、spansをOTLPのExportTraceServiceRequestのprotobufにします。
// field番号は、opentelemetry-protoのtrace/v1・common/v1・resource/v1と同じです。
// proto3の規則に従い、既定値（0や空文字列）のfieldは書きません。ただし、AnyValueの
// oneofは、どの値かを表すため既定値でも書きます。
func MarshalProto(spans []tracing.SpanData, defaultResource *tracing.Resource) []byte {
	var b []byte
	for _, group := range groupSpans(spans, defaultResource) {
		// ExportTraceServiceRequest.resource_spans = 1
		b = appendMessage(b, 1, func(b []byte) []byte { return appendResourceSpans(b, group) })
	}
	return b
}

func appendResourceSpans(b []byte, group resourceGroup) []byte {
	// ResourceSpans.resource = 1
	b = appendMessage(b, 1, func(b []byte) []byte {
		if group.resource == nil {
			return b
		}
		b = appendKeyValues(b, 1, toKeyValues(group.resource.Attrs))
		return appendUint32(b, 2, group.resource.DroppedAttrs)
	})
	for _, scope := range group.scopes {
		// ResourceSpans.scope_spans = 2
		b = appendMessage(b, 2, func(b []byte) []byte { return appendScopeSpans(b, scope) })
	}
	if group.resource != nil {
		// ResourceSpans.schema_url = 3
		b = appendString(b, 3, group.resource.SchemaURL)
	}
	return b
}

func appendScopeSpans(b []byte, scope scopeGroup) []byte {
	// ScopeSpans.scope = 1（InstrumentationScopeのname = 1、version = 2）
	b = appendMessage(b, 1, func(b []byte) []byte {
		b = appendString(b, 1, scope.scope.Name)
		return appendString(b, 2, scope.scope.Version)
	})
	for _, span := range scope.spans {
		// ScopeSpans.spans = 2
		b = appendMessage(b, 2, func(b []byte) []byte { return appendSpan(b, span) })
	}
	return b
}

func appendSpan(b []byte, span *tracing.SpanData) []byte {
	b = appendBytes(b, 1, span.TraceID[:])
	b = appendBytes(b, 2, span.SpanID[:])
	if !span.ParentSpanID.IsZero() {
		b = appendBytes(b, 4, span.ParentSpanID[:])
	}
	b = appendString(b, 5, span.Name)
	b = appendVarint(b, 6, uint64(otlpSpanKind(span.Kind)))
	b = appendFixed64(b, 7, unixNano(span.StartTime))
	b = appendFixed64(b, 8, unixNano(span.EndTime))
	b = appendKeyValues(b, 9, toKeyValues(span.Attrs))
	b = appendUint32(b, 10, span.DroppedAttrs)
	for _, event := range span.Events {
		b = appendMessage(b, 11, func(b []byte) []byte {
			b = appendFixed64(b, 1, unixNano(event.Time))
			b = appendString(b, 2, event.Name)
			b = appendKeyValues(b, 3, toKeyValues(event.Attrs))
			return appendUint32(b, 4, event.DroppedAttrs)
		})
	}
	b = appendUint32(b, 12, span.DroppedEvents)
	for _, link := range span.Links {
		b = appendMessage(b, 13, func(b []byte) []byte {
			b = appendBytes(b, 1, link.TraceID[:])
			b = appendBytes(b, 2, link.SpanID[:])
			b = appendKeyValues(b, 4, toKeyValues(link.Attrs))
			return appendUint32(b, 5, link.DroppedAttrs)
		})
	}
	b = appendUint32(b, 14, span.DroppedLinks)
	if span.Status != 0 || span.StatusMessage != "" {
		// Span.status = 15（Statusのmessage = 2、code = 3。1は廃止された番号です）
		b = appendMessage(b, 15, func(b []byte) []byte {
			b = appendString(b, 2, span.StatusMessage)
			return appendVarint(b, 3, uint64(otlpStatusCode(span.Status)))
		})
	}
	return b
}

// appendKeyValuesは、属性を繰り返しのKeyValue（key = 1、value = 2）として書きます。
func appendKeyValues(b []byte, num protowire.Number, attrs []keyValue) []byte {
	for _, attr := range attrs {
		b = appendMessage(b, num, func(b []byte) []byte {
			b = appendString(b, 1, attr.key)
			return appendMessage(b, 2, func(b []byte) []byte { return appendAnyValue(b, attr.value) })
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
		return appendMessage(b, 5, func(b []byte) []byte {
			for _, element := range v.array {
				b = appendMessage(b, 1, func(b []byte) []byte { return appendAnyValue(b, element) })
			}
			return b
		})
	default:
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		return protowire.AppendString(b, v.str)
	}
}

// appendMessageは、writeが書いたmessageを、長さを付けたfieldとして書きます。
func appendMessage(b []byte, num protowire.Number, write func([]byte) []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, write(nil))
}

func appendString(b []byte, num protowire.Number, s string) []byte {
	if s == "" {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func appendBytes(b []byte, num protowire.Number, v []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

func appendVarint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func appendUint32(b []byte, num protowire.Number, v int) []byte {
	if v <= 0 {
		return b
	}
	return appendVarint(b, num, uint64(min(v, math.MaxUint32)))
}

func appendFixed64(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.Fixed64Type)
	return protowire.AppendFixed64(b, v)
}

// unixNanoは、時刻をUnix epochからのnanosecondにします。zero値と、epochより前の時刻は
// 0（不明）にします。
func unixNano(t time.Time) uint64 {
	if t.IsZero() || t.UnixNano() <= 0 {
		return 0
	}
	return uint64(t.UnixNano())
}

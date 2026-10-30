// Package otlpは、終わったspanをOTLP（OpenTelemetry Protocol）のprotobufとJSONに変換します。
// protobufは、OTLP/HTTPのExportTraceServiceRequestと同じ形のTracesDataです。JSONは、
// OTLP/JSONの規則（IDは16進数、enumは数値、64-bitの整数は文字列）に従います。属性、
// Resource、Scopeの変換は、metricsと共通のinternal/infrastructure/otlpcommonが受け持ちます。
package otlp

import (
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	"github.com/go-jappe/heterolog/internal/application/tracing"
	oc "github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
)

// MarshalProtoは、spansをprotobufのTracesData（ExportTraceServiceRequestと同じ形）にします。
// Resourceがnilのspanは、defaultResourceで送ります。
func MarshalProto(spans []tracing.SpanData, defaultResource *tracing.Resource) []byte {
	var b []byte
	for _, group := range groupSpans(spans, defaultResource) {
		// ExportTraceServiceRequest.resource_spans = 1
		b = oc.AppendMessage(b, 1, func(b []byte) []byte { return appendResourceSpans(b, group) })
	}
	return b
}

// groupSpansは、spansをResourceとScopeごとにまとめます。
func groupSpans(spans []tracing.SpanData, defaultResource *tracing.Resource) []oc.ResourceGroup[tracing.SpanData] {
	return oc.Group(spans, defaultResource,
		func(span *tracing.SpanData) *telemetrydata.Resource { return span.Resource },
		func(span *tracing.SpanData) telemetrydata.Scope { return span.Scope },
	)
}

func appendResourceSpans(b []byte, group oc.ResourceGroup[tracing.SpanData]) []byte {
	// ResourceSpans.resource = 1
	b = oc.AppendResource(b, 1, group.Resource)
	for _, scope := range group.Scopes {
		// ResourceSpans.scope_spans = 2
		b = oc.AppendMessage(b, 2, func(b []byte) []byte {
			// ScopeSpans.scope = 1、spans = 2
			b = oc.AppendScope(b, 1, scope.Scope)
			for _, span := range scope.Items {
				b = oc.AppendMessage(b, 2, func(b []byte) []byte { return appendSpan(b, span) })
			}
			return b
		})
	}
	if group.Resource != nil {
		// ResourceSpans.schema_url = 3
		b = oc.AppendString(b, 3, group.Resource.SchemaURL)
	}
	return b
}

func appendSpan(b []byte, span *tracing.SpanData) []byte {
	b = oc.AppendBytes(b, 1, span.TraceID[:])
	b = oc.AppendBytes(b, 2, span.SpanID[:])
	if !span.ParentSpanID.IsZero() {
		b = oc.AppendBytes(b, 4, span.ParentSpanID[:])
	}
	b = oc.AppendString(b, 5, span.Name)
	b = oc.AppendVarint(b, 6, uint64(SpanKind(span.Kind)))
	b = oc.AppendFixed64(b, 7, oc.UnixNano(span.StartTime))
	b = oc.AppendFixed64(b, 8, oc.UnixNano(span.EndTime))
	b = oc.AppendKeyValues(b, 9, oc.ToKeyValues(span.Attrs))
	b = oc.AppendUint32(b, 10, span.DroppedAttrs)
	for _, event := range span.Events {
		b = oc.AppendMessage(b, 11, func(b []byte) []byte {
			b = oc.AppendFixed64(b, 1, oc.UnixNano(event.Time))
			b = oc.AppendString(b, 2, event.Name)
			b = oc.AppendKeyValues(b, 3, oc.ToKeyValues(event.Attrs))
			return oc.AppendUint32(b, 4, event.DroppedAttrs)
		})
	}
	b = oc.AppendUint32(b, 12, span.DroppedEvents)
	for _, link := range span.Links {
		b = oc.AppendMessage(b, 13, func(b []byte) []byte {
			b = oc.AppendBytes(b, 1, link.TraceID[:])
			b = oc.AppendBytes(b, 2, link.SpanID[:])
			b = oc.AppendKeyValues(b, 4, oc.ToKeyValues(link.Attrs))
			return oc.AppendUint32(b, 5, link.DroppedAttrs)
		})
	}
	b = oc.AppendUint32(b, 14, span.DroppedLinks)
	if span.Status != 0 || span.StatusMessage != "" {
		// Span.status = 15（Statusのmessage = 2、code = 3。1は廃止された番号です）
		b = oc.AppendMessage(b, 15, func(b []byte) []byte {
			b = oc.AppendString(b, 2, span.StatusMessage)
			return oc.AppendVarint(b, 3, uint64(StatusCode(span.Status)))
		})
	}
	return b
}

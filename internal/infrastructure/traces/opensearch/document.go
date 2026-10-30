// Package opensearchは、spanをOpenSearchへ文書として書く送り先です。文書は、OpenSearch
// Data Prepperがtrace analyticsのために書くspanの文書（otel-v1-apm-span）と同じ形にし、
// OpenSearch DashboardsのTrace Analyticsで見られるようにします。文書はBulk APIのcreateで
// 書き、_idはtraceのIDとspanのIDから決めます。送り直したspanが既に書かれていれば、
// OpenSearchは409を返し、それを成功として扱うため、spanは重複しません。
package opensearch

import (
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlp"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// spanKindNamesは、OTLPのSpan.SpanKindの値の名前です。Data Prepperは、この名前を
// kindに書きます。
var spanKindNames = [...]string{
	"SPAN_KIND_UNSPECIFIED",
	"SPAN_KIND_INTERNAL",
	"SPAN_KIND_SERVER",
	"SPAN_KIND_CLIENT",
	"SPAN_KIND_PRODUCER",
	"SPAN_KIND_CONSUMER",
}

// documentIDは、spanの文書の_idです。traceのIDとspanのIDの16進数をつなげます。
func documentID(span tracing.SpanData) string {
	return span.TraceID.String() + span.SpanID.String()
}

// documentは、spanを、Data Prepperと同じ形の文書にします。defaultResourceは、Resourceを
// 持たないspanに付けます。
//
// Data Prepperと同じく、spanとResourceの属性は、keyの.を@に替えて、
// span.attributes.<key>とresource.attributes.<key>のfieldにします（例:
// span.attributes.http@method）。属性のkeyに.があっても、OpenSearchがobjectとして
// 展開しないようにするためです。eventとlinkの属性は、attributesのobjectに同じkeyで
// 入れます。traceGroupとtraceGroupFieldsは、親のないspan（traceの根のspan）にだけ
// 入れます。Data Prepperは、ほかのspanにも根のspanの値を後から補いますが、spanを
// まとめて待つ必要があるため、heterologは補いません。
func document(span tracing.SpanData, defaultResource *tracing.Resource) map[string]any {
	resource := span.Resource
	if resource == nil {
		resource = defaultResource
	}
	endTime := isoTime(span.EndTime)
	duration := max(span.EndTime.Sub(span.StartTime).Nanoseconds(), 0)
	statusCode := otlp.StatusCode(span.Status)
	parentSpanID := ""
	if !span.ParentSpanID.IsZero() {
		parentSpanID = span.ParentSpanID.String()
	}
	doc := map[string]any{
		"traceId":                span.TraceID.String(),
		"spanId":                 span.SpanID.String(),
		"parentSpanId":           parentSpanID,
		"traceState":             "",
		"name":                   span.Name,
		"kind":                   spanKindNames[otlp.SpanKind(span.Kind)],
		"startTime":              isoTime(span.StartTime),
		"endTime":                endTime,
		"durationInNanos":        duration,
		"status.code":            statusCode,
		"droppedAttributesCount": span.DroppedAttrs,
		"events":                 events(span.Events),
		"droppedEventsCount":     span.DroppedEvents,
		"links":                  links(span.Links),
		"droppedLinksCount":      span.DroppedLinks,
	}
	if span.StatusMessage != "" {
		doc["status.message"] = span.StatusMessage
	}
	if span.ParentSpanID.IsZero() {
		doc["traceGroup"] = span.Name
		doc["traceGroupFields"] = map[string]any{
			"endTime":         endTime,
			"durationInNanos": duration,
			"statusCode":      statusCode,
		}
	}
	if span.Scope.Name != "" {
		doc["instrumentationScope.name"] = span.Scope.Name
	}
	if span.Scope.Version != "" {
		doc["instrumentationScope.version"] = span.Scope.Version
	}
	addAttributes(doc, "span.attributes.", span.Attrs)
	if resource != nil {
		addAttributes(doc, "resource.attributes.", resource.Attrs)
		for _, attr := range resource.Attrs {
			if name, ok := attr.Value.(string); ok && attr.Key == "service.name" && name != "" {
				doc["serviceName"] = name
			}
		}
	}
	return doc
}

func events(events []tracing.Event) []map[string]any {
	out := make([]map[string]any, len(events))
	for i, event := range events {
		out[i] = map[string]any{
			"name":                   event.Name,
			"time":                   isoTime(event.Time),
			"attributes":             attributes(event.Attrs),
			"droppedAttributesCount": event.DroppedAttrs,
		}
	}
	return out
}

func links(links []tracing.Link) []map[string]any {
	out := make([]map[string]any, len(links))
	for i, link := range links {
		out[i] = map[string]any{
			"traceId":                link.TraceID.String(),
			"spanId":                 link.SpanID.String(),
			"traceState":             "",
			"attributes":             attributes(link.Attrs),
			"droppedAttributesCount": link.DroppedAttrs,
		}
	}
	return out
}

// addAttributesは、属性を、prefixに、.を@に替えたkeyをつなげたfieldとしてdocに入れます。
func addAttributes(doc map[string]any, prefix string, attrs []pkgobserver.Attr) {
	for _, attr := range attrs {
		doc[prefix+attributeKey(attr.Key)] = otlpcommon.PlainValue(attr.Value)
	}
}

// attributesは、属性を、.を@に替えたkeyのobjectにします。
func attributes(attrs []pkgobserver.Attr) map[string]any {
	out := make(map[string]any, len(attrs))
	for _, attr := range attrs {
		out[attributeKey(attr.Key)] = otlpcommon.PlainValue(attr.Value)
	}
	return out
}

func attributeKey(key string) string {
	return strings.ReplaceAll(key, ".", "@")
}

// isoTimeは、時刻を、UTCのRFC 3339の文字列にします。OpenSearchのdate_nanosは、
// nanosecondまで読みます。
func isoTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

package otlp

import (
	"encoding/json"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	oc "github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
)

// MarshalJSONは、spansをOTLP/JSONのTracesDataにします。OTLP/JSONの規則に従い、
// trace IDとspan IDは16進数、enumは数値、64-bitの整数と時刻は10進数の文字列にします。
// 改行を含まないため、1行に1回分を書けます。
func MarshalJSON(spans []tracing.SpanData, defaultResource *tracing.Resource) ([]byte, error) {
	data := jsonTracesData{ResourceSpans: []jsonResourceSpans{}}
	for _, group := range groupSpans(spans, defaultResource) {
		rs := jsonResourceSpans{}
		rs.Resource, rs.SchemaURL = oc.ToJSONResource(group.Resource)
		for _, scope := range group.Scopes {
			ss := jsonScopeSpans{
				Scope: oc.JSONScope{Name: scope.Scope.Name, Version: scope.Scope.Version},
				Spans: make([]jsonSpan, 0, len(scope.Items)),
			}
			for _, span := range scope.Items {
				ss.Spans = append(ss.Spans, toJSONSpan(span))
			}
			rs.ScopeSpans = append(rs.ScopeSpans, ss)
		}
		data.ResourceSpans = append(data.ResourceSpans, rs)
	}
	return json.Marshal(data)
}

type jsonTracesData struct {
	ResourceSpans []jsonResourceSpans `json:"resourceSpans"`
}

type jsonResourceSpans struct {
	Resource   oc.JSONResource  `json:"resource"`
	ScopeSpans []jsonScopeSpans `json:"scopeSpans"`
	SchemaURL  string           `json:"schemaUrl,omitempty"`
}

type jsonScopeSpans struct {
	Scope oc.JSONScope `json:"scope"`
	Spans []jsonSpan   `json:"spans"`
}

type jsonSpan struct {
	TraceID                string            `json:"traceId"`
	SpanID                 string            `json:"spanId"`
	ParentSpanID           string            `json:"parentSpanId,omitempty"`
	Name                   string            `json:"name"`
	Kind                   int               `json:"kind"`
	StartTimeUnixNano      string            `json:"startTimeUnixNano"`
	EndTimeUnixNano        string            `json:"endTimeUnixNano"`
	Attributes             []oc.JSONKeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32            `json:"droppedAttributesCount,omitempty"`
	Events                 []jsonEvent       `json:"events,omitempty"`
	DroppedEventsCount     uint32            `json:"droppedEventsCount,omitempty"`
	Links                  []jsonLink        `json:"links,omitempty"`
	DroppedLinksCount      uint32            `json:"droppedLinksCount,omitempty"`
	Status                 *jsonStatus       `json:"status,omitempty"`
}

type jsonEvent struct {
	TimeUnixNano           string            `json:"timeUnixNano"`
	Name                   string            `json:"name"`
	Attributes             []oc.JSONKeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32            `json:"droppedAttributesCount,omitempty"`
}

type jsonLink struct {
	TraceID                string            `json:"traceId"`
	SpanID                 string            `json:"spanId"`
	Attributes             []oc.JSONKeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32            `json:"droppedAttributesCount,omitempty"`
}

type jsonStatus struct {
	Message string `json:"message,omitempty"`
	Code    int    `json:"code,omitempty"`
}

func toJSONSpan(span *tracing.SpanData) jsonSpan {
	out := jsonSpan{
		TraceID:                span.TraceID.String(),
		SpanID:                 span.SpanID.String(),
		Name:                   span.Name,
		Kind:                   SpanKind(span.Kind),
		StartTimeUnixNano:      oc.JSONTime(span.StartTime),
		EndTimeUnixNano:        oc.JSONTime(span.EndTime),
		Attributes:             oc.ToJSONKeyValues(oc.ToKeyValues(span.Attrs)),
		DroppedAttributesCount: oc.JSONCount(span.DroppedAttrs),
		DroppedEventsCount:     oc.JSONCount(span.DroppedEvents),
		DroppedLinksCount:      oc.JSONCount(span.DroppedLinks),
	}
	if !span.ParentSpanID.IsZero() {
		out.ParentSpanID = span.ParentSpanID.String()
	}
	for _, event := range span.Events {
		out.Events = append(out.Events, jsonEvent{
			TimeUnixNano:           oc.JSONTime(event.Time),
			Name:                   event.Name,
			Attributes:             oc.ToJSONKeyValues(oc.ToKeyValues(event.Attrs)),
			DroppedAttributesCount: oc.JSONCount(event.DroppedAttrs),
		})
	}
	for _, link := range span.Links {
		out.Links = append(out.Links, jsonLink{
			TraceID:                link.TraceID.String(),
			SpanID:                 link.SpanID.String(),
			Attributes:             oc.ToJSONKeyValues(oc.ToKeyValues(link.Attrs)),
			DroppedAttributesCount: oc.JSONCount(link.DroppedAttrs),
		})
	}
	if span.Status != 0 || span.StatusMessage != "" {
		out.Status = &jsonStatus{Message: span.StatusMessage, Code: StatusCode(span.Status)}
	}
	return out
}

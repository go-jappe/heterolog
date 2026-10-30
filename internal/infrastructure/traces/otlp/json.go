package otlp

import (
	"encoding/json"
	"math"
	"strconv"

	"github.com/go-jappe/heterolog/internal/application/tracing"
)

// MarshalJSONは、spansをOTLP/JSONのTracesDataにします。OTLP/JSONの規則に従い、
// trace IDとspan IDは16進数、enumは数値、64-bitの整数と時刻は10進数の文字列にします。
// 改行を含まないため、1行に1回分を書けます。
func MarshalJSON(spans []tracing.SpanData, defaultResource *tracing.Resource) ([]byte, error) {
	data := jsonTracesData{ResourceSpans: []jsonResourceSpans{}}
	for _, group := range groupSpans(spans, defaultResource) {
		rs := jsonResourceSpans{}
		if group.resource != nil {
			rs.Resource = jsonResource{
				Attributes:             toJSONKeyValues(toKeyValues(group.resource.Attrs)),
				DroppedAttributesCount: jsonCount(group.resource.DroppedAttrs),
			}
			rs.SchemaURL = group.resource.SchemaURL
		}
		for _, scope := range group.scopes {
			ss := jsonScopeSpans{
				Scope: jsonScope{Name: scope.scope.Name, Version: scope.scope.Version},
				Spans: make([]jsonSpan, 0, len(scope.spans)),
			}
			for _, span := range scope.spans {
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
	Resource   jsonResource     `json:"resource"`
	ScopeSpans []jsonScopeSpans `json:"scopeSpans"`
	SchemaURL  string           `json:"schemaUrl,omitempty"`
}

type jsonResource struct {
	Attributes             []jsonKeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32         `json:"droppedAttributesCount,omitempty"`
}

type jsonScopeSpans struct {
	Scope jsonScope  `json:"scope"`
	Spans []jsonSpan `json:"spans"`
}

type jsonScope struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

type jsonSpan struct {
	TraceID                string         `json:"traceId"`
	SpanID                 string         `json:"spanId"`
	ParentSpanID           string         `json:"parentSpanId,omitempty"`
	Name                   string         `json:"name"`
	Kind                   int            `json:"kind"`
	StartTimeUnixNano      string         `json:"startTimeUnixNano"`
	EndTimeUnixNano        string         `json:"endTimeUnixNano"`
	Attributes             []jsonKeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32         `json:"droppedAttributesCount,omitempty"`
	Events                 []jsonEvent    `json:"events,omitempty"`
	DroppedEventsCount     uint32         `json:"droppedEventsCount,omitempty"`
	Links                  []jsonLink     `json:"links,omitempty"`
	DroppedLinksCount      uint32         `json:"droppedLinksCount,omitempty"`
	Status                 *jsonStatus    `json:"status,omitempty"`
}

type jsonEvent struct {
	TimeUnixNano           string         `json:"timeUnixNano"`
	Name                   string         `json:"name"`
	Attributes             []jsonKeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32         `json:"droppedAttributesCount,omitempty"`
}

type jsonLink struct {
	TraceID                string         `json:"traceId"`
	SpanID                 string         `json:"spanId"`
	Attributes             []jsonKeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32         `json:"droppedAttributesCount,omitempty"`
}

type jsonStatus struct {
	Message string `json:"message,omitempty"`
	Code    int    `json:"code,omitempty"`
}

type jsonKeyValue struct {
	Key   string       `json:"key"`
	Value jsonAnyValue `json:"value"`
}

// jsonAnyValueは、AnyValueのoneofのうち、1つだけを書きます。
type jsonAnyValue struct {
	StringValue *string         `json:"stringValue,omitempty"`
	BoolValue   *bool           `json:"boolValue,omitempty"`
	IntValue    *string         `json:"intValue,omitempty"`
	DoubleValue json.RawMessage `json:"doubleValue,omitempty"`
	ArrayValue  *jsonArrayValue `json:"arrayValue,omitempty"`
}

type jsonArrayValue struct {
	Values []jsonAnyValue `json:"values"`
}

func toJSONSpan(span *tracing.SpanData) jsonSpan {
	out := jsonSpan{
		TraceID:                span.TraceID.String(),
		SpanID:                 span.SpanID.String(),
		Name:                   span.Name,
		Kind:                   otlpSpanKind(span.Kind),
		StartTimeUnixNano:      strconv.FormatUint(unixNano(span.StartTime), 10),
		EndTimeUnixNano:        strconv.FormatUint(unixNano(span.EndTime), 10),
		Attributes:             toJSONKeyValues(toKeyValues(span.Attrs)),
		DroppedAttributesCount: jsonCount(span.DroppedAttrs),
		DroppedEventsCount:     jsonCount(span.DroppedEvents),
		DroppedLinksCount:      jsonCount(span.DroppedLinks),
	}
	if !span.ParentSpanID.IsZero() {
		out.ParentSpanID = span.ParentSpanID.String()
	}
	for _, event := range span.Events {
		out.Events = append(out.Events, jsonEvent{
			TimeUnixNano:           strconv.FormatUint(unixNano(event.Time), 10),
			Name:                   event.Name,
			Attributes:             toJSONKeyValues(toKeyValues(event.Attrs)),
			DroppedAttributesCount: jsonCount(event.DroppedAttrs),
		})
	}
	for _, link := range span.Links {
		out.Links = append(out.Links, jsonLink{
			TraceID:                link.TraceID.String(),
			SpanID:                 link.SpanID.String(),
			Attributes:             toJSONKeyValues(toKeyValues(link.Attrs)),
			DroppedAttributesCount: jsonCount(link.DroppedAttrs),
		})
	}
	if span.Status != 0 || span.StatusMessage != "" {
		out.Status = &jsonStatus{Message: span.StatusMessage, Code: otlpStatusCode(span.Status)}
	}
	return out
}

func toJSONKeyValues(attrs []keyValue) []jsonKeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]jsonKeyValue, len(attrs))
	for i, attr := range attrs {
		out[i] = jsonKeyValue{Key: attr.key, Value: toJSONAnyValue(attr.value)}
	}
	return out
}

func toJSONAnyValue(v value) jsonAnyValue {
	switch v.kind {
	case kindBool:
		return jsonAnyValue{BoolValue: &v.b}
	case kindInt:
		s := strconv.FormatInt(v.i, 10)
		return jsonAnyValue{IntValue: &s}
	case kindDouble:
		return jsonAnyValue{DoubleValue: jsonDouble(v.f)}
	case kindArray:
		values := make([]jsonAnyValue, len(v.array))
		for i, element := range v.array {
			values[i] = toJSONAnyValue(element)
		}
		return jsonAnyValue{ArrayValue: &jsonArrayValue{Values: values}}
	default:
		return jsonAnyValue{StringValue: &v.str}
	}
}

// jsonDoubleは、protobufのJSONの規則で浮動小数点数を書きます。JSONの数値で表せない
// NaNと無限大は、文字列にします。
func jsonDouble(f float64) json.RawMessage {
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

func jsonCount(n int) uint32 {
	return uint32(min(max(n, 0), math.MaxUint32))
}

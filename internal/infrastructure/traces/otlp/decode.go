package otlp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	oc "github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ErrInvalidJSONは、UnmarshalJSONが読めないOTLP/JSONを表します。
var ErrInvalidJSON = oc.ErrInvalidJSON

// UnmarshalJSONは、MarshalJSONが書いたOTLP/JSONのTracesDataを、spanに戻します。同じ
// ResourceSpansのspanは、同じResourceのpointerを持ちます。属性の値は、文字列、bool、
// int64、float64、[]anyに戻します。MarshalJSONが書かない値（bytesValue・kvlistValue
// など）と、IDの長さや時刻が不正な値は、ErrInvalidJSONを包んだerrorにします。
func UnmarshalJSON(data []byte) ([]tracing.SpanData, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var traces jsonTracesData
	if err := decoder.Decode(&traces); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}
	var spans []tracing.SpanData
	for _, rs := range traces.ResourceSpans {
		resource, err := oc.FromJSONResource(rs.Resource, rs.SchemaURL)
		if err != nil {
			return nil, err
		}
		for _, ss := range rs.ScopeSpans {
			scope := tracing.Scope{Name: ss.Scope.Name, Version: ss.Scope.Version}
			for _, js := range ss.Spans {
				span, err := fromJSONSpan(js)
				if err != nil {
					return nil, err
				}
				span.Scope = scope
				span.Resource = resource
				spans = append(spans, span)
			}
		}
	}
	return spans, nil
}

func fromJSONSpan(js jsonSpan) (tracing.SpanData, error) {
	span := tracing.SpanData{
		Name:          js.Name,
		Kind:          observerSpanKind(js.Kind),
		DroppedAttrs:  int(js.DroppedAttributesCount),
		DroppedEvents: int(js.DroppedEventsCount),
		DroppedLinks:  int(js.DroppedLinksCount),
	}
	var errs []error
	errs = append(errs, oc.DecodeID(span.TraceID[:], js.TraceID, "traceId", false))
	errs = append(errs, oc.DecodeID(span.SpanID[:], js.SpanID, "spanId", false))
	errs = append(errs, oc.DecodeID(span.ParentSpanID[:], js.ParentSpanID, "parentSpanId", true))
	var err error
	span.StartTime, err = oc.FromJSONTime(js.StartTimeUnixNano, "startTimeUnixNano")
	errs = append(errs, err)
	span.EndTime, err = oc.FromJSONTime(js.EndTimeUnixNano, "endTimeUnixNano")
	errs = append(errs, err)
	span.Attrs, err = oc.FromJSONKeyValues(js.Attributes)
	errs = append(errs, err)
	for _, je := range js.Events {
		event := tracing.Event{Name: je.Name, DroppedAttrs: int(je.DroppedAttributesCount)}
		event.Time, err = oc.FromJSONTime(je.TimeUnixNano, "timeUnixNano")
		errs = append(errs, err)
		event.Attrs, err = oc.FromJSONKeyValues(je.Attributes)
		errs = append(errs, err)
		span.Events = append(span.Events, event)
	}
	for _, jl := range js.Links {
		link := tracing.Link{DroppedAttrs: int(jl.DroppedAttributesCount)}
		errs = append(errs, oc.DecodeID(link.TraceID[:], jl.TraceID, "link traceId", false))
		errs = append(errs, oc.DecodeID(link.SpanID[:], jl.SpanID, "link spanId", false))
		link.Attrs, err = oc.FromJSONKeyValues(jl.Attributes)
		errs = append(errs, err)
		span.Links = append(span.Links, link)
	}
	if js.Status != nil {
		span.Status = observerStatusCode(js.Status.Code)
		span.StatusMessage = js.Status.Message
	}
	return span, errors.Join(errs...)
}

// observerSpanKindは、SpanKindの逆です。
func observerSpanKind(kind int) pkgobserver.SpanKind {
	switch kind {
	case 2:
		return pkgobserver.SpanKindServer
	case 3:
		return pkgobserver.SpanKindClient
	case 4:
		return pkgobserver.SpanKindProducer
	case 5:
		return pkgobserver.SpanKindConsumer
	default:
		return pkgobserver.SpanKindInternal
	}
}

// observerStatusCodeは、StatusCodeの逆です。
func observerStatusCode(code int) pkgobserver.StatusCode {
	switch code {
	case 1:
		return pkgobserver.StatusOK
	case 2:
		return pkgobserver.StatusError
	default:
		return pkgobserver.StatusUnset
	}
}

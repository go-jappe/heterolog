package otlp

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ErrInvalidJSONは、UnmarshalJSONが読めないOTLP/JSONを表します。
var ErrInvalidJSON = errors.New("invalid OTLP JSON")

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
		resourceAttrs, err := fromJSONKeyValues(rs.Resource.Attributes)
		if err != nil {
			return nil, err
		}
		resource := &tracing.Resource{
			Attrs:        resourceAttrs,
			DroppedAttrs: int(rs.Resource.DroppedAttributesCount),
			SchemaURL:    rs.SchemaURL,
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
	errs = append(errs, decodeID(span.TraceID[:], js.TraceID, "traceId", false))
	errs = append(errs, decodeID(span.SpanID[:], js.SpanID, "spanId", false))
	errs = append(errs, decodeID(span.ParentSpanID[:], js.ParentSpanID, "parentSpanId", true))
	var err error
	span.StartTime, err = fromUnixNano(js.StartTimeUnixNano, "startTimeUnixNano")
	errs = append(errs, err)
	span.EndTime, err = fromUnixNano(js.EndTimeUnixNano, "endTimeUnixNano")
	errs = append(errs, err)
	span.Attrs, err = fromJSONKeyValues(js.Attributes)
	errs = append(errs, err)
	for _, je := range js.Events {
		event := tracing.Event{Name: je.Name, DroppedAttrs: int(je.DroppedAttributesCount)}
		event.Time, err = fromUnixNano(je.TimeUnixNano, "timeUnixNano")
		errs = append(errs, err)
		event.Attrs, err = fromJSONKeyValues(je.Attributes)
		errs = append(errs, err)
		span.Events = append(span.Events, event)
	}
	for _, jl := range js.Links {
		link := tracing.Link{DroppedAttrs: int(jl.DroppedAttributesCount)}
		errs = append(errs, decodeID(link.TraceID[:], jl.TraceID, "link traceId", false))
		errs = append(errs, decodeID(link.SpanID[:], jl.SpanID, "link spanId", false))
		link.Attrs, err = fromJSONKeyValues(jl.Attributes)
		errs = append(errs, err)
		span.Links = append(span.Links, link)
	}
	if js.Status != nil {
		span.Status = observerStatusCode(js.Status.Code)
		span.StatusMessage = js.Status.Message
	}
	return span, errors.Join(errs...)
}

// decodeIDは、16進数のIDをdstへ読みます。optionalなら、空の値はIDがないものにします。
func decodeID(dst []byte, s, field string, optional bool) error {
	if s == "" && optional {
		return nil
	}
	if len(s) != 2*len(dst) {
		return fmt.Errorf("%w: %s has %d hexadecimal digits, want %d", ErrInvalidJSON, field, len(s), 2*len(dst))
	}
	if _, err := hex.Decode(dst, []byte(s)); err != nil {
		return fmt.Errorf("%w: %s is not hexadecimal", ErrInvalidJSON, field)
	}
	return nil
}

// fromUnixNanoは、10進数の文字列の時刻を読みます。0は、時刻がないものにします。
func fromUnixNano(s, field string) (time.Time, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n > math.MaxInt64 {
		return time.Time{}, fmt.Errorf("%w: %s is not a time in nanoseconds", ErrInvalidJSON, field)
	}
	if n == 0 {
		return time.Time{}, nil
	}
	return time.Unix(0, int64(n)).UTC(), nil
}

func fromJSONKeyValues(kvs []jsonKeyValue) ([]pkgobserver.Attr, error) {
	if len(kvs) == 0 {
		return nil, nil
	}
	attrs := make([]pkgobserver.Attr, len(kvs))
	for i, kv := range kvs {
		value, err := fromJSONAnyValue(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", kv.Key, err)
		}
		attrs[i] = pkgobserver.Attr{Key: kv.Key, Value: value}
	}
	return attrs, nil
}

func fromJSONAnyValue(v jsonAnyValue) (any, error) {
	switch {
	case v.StringValue != nil:
		return *v.StringValue, nil
	case v.BoolValue != nil:
		return *v.BoolValue, nil
	case v.IntValue != nil:
		i, err := strconv.ParseInt(*v.IntValue, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: intValue %q is not an integer", ErrInvalidJSON, *v.IntValue)
		}
		return i, nil
	case v.DoubleValue != nil:
		return fromJSONDouble(v.DoubleValue)
	case v.ArrayValue != nil:
		values := make([]any, len(v.ArrayValue.Values))
		for i, element := range v.ArrayValue.Values {
			value, err := fromJSONAnyValue(element)
			if err != nil {
				return nil, err
			}
			values[i] = value
		}
		return values, nil
	default:
		return nil, fmt.Errorf("%w: the value has no supported field", ErrInvalidJSON)
	}
}

// fromJSONDoubleは、jsonDoubleが書いた浮動小数点数を読みます。
func fromJSONDouble(raw json.RawMessage) (float64, error) {
	switch string(raw) {
	case `"NaN"`:
		return math.NaN(), nil
	case `"Infinity"`:
		return math.Inf(1), nil
	case `"-Infinity"`:
		return math.Inf(-1), nil
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0, fmt.Errorf("%w: doubleValue %s is not a number", ErrInvalidJSON, raw)
	}
	return f, nil
}

// observerSpanKindは、otlpSpanKindの逆です。
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

// observerStatusCodeは、otlpStatusCodeの逆です。
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

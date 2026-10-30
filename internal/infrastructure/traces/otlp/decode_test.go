package otlp

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestUnmarshalJSONRestoresWhatMarshalJSONWrote(t *testing.T) {
	spans, defaultResource := testSpans()
	spans[0].Attrs = append(spans[0].Attrs,
		pkgobserver.Attr{Key: "nan", Value: math.NaN()},
		pkgobserver.Attr{Key: "inf", Value: math.Inf(-1)},
		pkgobserver.Attr{Key: "mixed", Value: []any{"a", int64(1), 2.5, true}},
	)
	data, err := MarshalJSON(spans, defaultResource)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalJSON(data)
	if err != nil {
		t.Fatalf("UnmarshalJSON = %v", err)
	}
	if len(decoded) != len(spans) {
		t.Fatalf("decoded %d spans, want %d", len(decoded), len(spans))
	}
	// 戻したspanは、元のspanと同じprotobufとJSONになります。Resourceは、spanが持つため
	// 配送の仕組みのResourceを使いません。
	if got, want := MarshalProto(decoded, nil), MarshalProto(spans, defaultResource); !bytes.Equal(got, want) {
		t.Error("the decoded spans do not encode to the same protobuf")
	}
	again, err := MarshalJSON(decoded, nil)
	if err != nil || !bytes.Equal(again, data) {
		t.Errorf("the decoded spans encode to\n%s\nwant\n%s", again, data)
	}
	// 同じResourceSpansのspanは、同じResourceを共有します。
	if decoded[0].Resource != decoded[1].Resource || decoded[0].Resource == decoded[2].Resource {
		t.Error("the spans do not share the Resource of their ResourceSpans")
	}
	root := decoded[0]
	if root.TraceID != spans[0].TraceID || root.StartTime != spans[0].StartTime || root.Kind != pkgobserver.SpanKindServer ||
		root.Status != pkgobserver.StatusError || root.Scope != spans[0].Scope {
		t.Errorf("root = %+v", root)
	}
}

func TestUnmarshalJSONRejectsWhatItCannotRestore(t *testing.T) {
	span := func(fields string) string {
		return `{"resourceSpans":[{"resource":{},"scopeSpans":[{"scope":{},"spans":[{` + fields + `}]}]}]}`
	}
	const ids = `"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"0101010101010101",`
	const times = `"startTimeUnixNano":"1","endTimeUnixNano":"2",`
	for name, data := range map[string]string{
		"not JSON":          `{`,
		"unknown field":     `{"resourceSpans":[],"extra":1}`,
		"short trace ID":    span(`"traceId":"01","spanId":"0101010101010101","name":"a","kind":1,` + times[:len(times)-1]),
		"non-hex span ID":   span(`"traceId":"0102030405060708090a0b0c0d0e0f10","spanId":"zzzzzzzzzzzzzzzz","name":"a","kind":1,` + times[:len(times)-1]),
		"bad time":          span(ids + `"name":"a","kind":1,"startTimeUnixNano":"x","endTimeUnixNano":"2"`),
		"bytes value":       span(ids + times + `"name":"a","kind":1,"attributes":[{"key":"k","value":{"bytesValue":"AA=="}}]`),
		"empty value":       span(ids + times + `"name":"a","kind":1,"attributes":[{"key":"k","value":{}}]`),
		"bad int value":     span(ids + times + `"name":"a","kind":1,"attributes":[{"key":"k","value":{"intValue":"1.5"}}]`),
		"bad double value":  span(ids + times + `"name":"a","kind":1,"attributes":[{"key":"k","value":{"doubleValue":"x"}}]`),
		"bad link trace ID": span(ids + times + `"name":"a","kind":1,"links":[{"traceId":"01","spanId":"0101010101010101"}]`),
	} {
		if _, err := UnmarshalJSON([]byte(data)); !errors.Is(err, ErrInvalidJSON) {
			t.Errorf("%s: UnmarshalJSON = %v, want ErrInvalidJSON", name, err)
		}
	}
	// 親のないspanのparentSpanIdは省けます。
	decoded, err := UnmarshalJSON([]byte(span(ids + times + `"name":"a","kind":1`)))
	if err != nil || len(decoded) != 1 || !decoded[0].ParentSpanID.IsZero() || decoded[0].SpanID != (tracing.SpanID{1, 1, 1, 1, 1, 1, 1, 1}) {
		t.Errorf("UnmarshalJSON = %+v, %v", decoded, err)
	}
}

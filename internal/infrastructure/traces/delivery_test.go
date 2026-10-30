package traces

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlp"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestCodecKeepsEachSpanAsOTLPJSONWithItsResource(t *testing.T) {
	resource := &tracing.Resource{Attrs: []pkgobserver.Attr{{Key: "service.name", Value: "checkout"}}}
	codec := NewCodec(resource)
	span := tracing.SpanData{
		TraceID: tracing.TraceID{1}, SpanID: tracing.SpanID{1}, Name: "op",
		StartTime: time.Unix(1, 0), EndTime: time.Unix(2, 0),
		Attrs: []pkgobserver.Attr{{Key: "count", Value: int64(1) << 60}, {Key: "ratio", Value: 0.5}},
	}
	payload, err := codec.Encode(span)
	if err != nil {
		t.Fatal(err)
	}
	// failure storeは、mapをJSONにしてrecordへ書きます。
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(data)
	if err != nil {
		t.Fatalf("Decode = %v", err)
	}
	if decoded.Name != "op" || decoded.SpanID != span.SpanID || decoded.Resource == nil ||
		decoded.Resource.Attrs[0].Value != "checkout" || decoded.Attrs[0].Value != int64(1)<<60 {
		t.Errorf("decoded span = %+v, want the stored span with its Resource", decoded)
	}
	// 64-bitの整数は、OTLP/JSONの規則どおり文字列で、精度を失いません。
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	stored := object["resourceSpans"].([]any)[0].(map[string]any)["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)[0].(map[string]any)
	if got := stored["attributes"].([]any)[0].(map[string]any)["value"].(map[string]any)["intValue"]; got != "1152921504606846976" {
		t.Errorf("intValue = %v, want the exact value as a string", got)
	}
	// 1件のspanのrecordでなければ、戻しません。
	for _, payload := range []string{`{"resourceSpans":[]}`, `{`} {
		if _, err := codec.Decode([]byte(payload)); !errors.Is(err, otlp.ErrInvalidJSON) {
			t.Errorf("Decode(%s) = %v, want ErrInvalidJSON", payload, err)
		}
	}
}

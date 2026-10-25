package field

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"go.opentelemetry.io/otel/trace"
)

func TestExtractIDsAndExtraFieldsAcceptsApplicationFields(t *testing.T) {
	t.Parallel()

	ids, extra := ExtractIDsAndExtraFields([]applogging.Field{
		applogging.WithRequestID("request-1"),
		applogging.MustFieldKey("trace_id").String("trace-1"),
		applogging.MustFieldKey("span_id").String("span-1"),
		applogging.MustFieldKey("success").Bool(true),
		applogging.MustFieldKey("count").Int(2),
		{},
	})

	wantIDs := IDs{RequestID: "request-1", TraceID: "trace-1", SpanID: "span-1"}
	if ids != wantIDs {
		t.Fatalf("IDs = %#v, want %#v", ids, wantIDs)
	}
	wantExtra := map[string]any{"success": true, "count": 2}
	if !reflect.DeepEqual(extra, wantExtra) {
		t.Fatalf("extra = %#v, want %#v", extra, wantExtra)
	}
}

func TestReservedIDFieldsPreserveNonStringValuesAndContextIDs(t *testing.T) {
	t.Parallel()

	traceID := trace.TraceID{1, 2, 3}
	spanID := trace.SpanID{4, 5, 6}
	ctx := trace.ContextWithSpanContext(requestctx.WithRequestID(context.Background(), "context-request"),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}))
	wantIDs := IDs{RequestID: "context-request", TraceID: traceID.String(), SpanID: spanID.String()}
	for _, key := range []string{"request_id", "trace_id", "span_id"} {
		t.Run(key, func(t *testing.T) {
			k := applogging.MustFieldKey(key)
			floatField, err := k.Float64(1.25)
			if err != nil {
				t.Fatal(err)
			}
			fields := []applogging.Field{k.Int(42), k.Int64(-9007199254740993), k.Uint64(18446744073709551615), k.Bool(true), floatField}
			for _, value := range []any{nil, map[string]any{"count": json.Number("9007199254740993")}, []any{true, json.Number("2")}} {
				field, err := k.Any(value)
				if err != nil {
					t.Fatal(err)
				}
				fields = append(fields, field)
			}
			for _, field := range fields {
				ids, extra := ResolveIDsAndExtraFields(ctx, []applogging.Field{field})
				if ids != wantIDs {
					t.Errorf("%T field changed context IDs: %#v, want %#v", field.Value(), ids, wantIDs)
				}
				want := map[string]any{key: field.Value()}
				if !reflect.DeepEqual(extra, want) {
					t.Errorf("%T field = %#v, want %#v with original type", field.Value(), extra, want)
				}
			}
		})
	}
}

// 同じkeyの重複は後の値だけを残してから分類します。前の値がmetadataと構造化fieldの
// どちらであっても、残った値の型だけで結果が決まり、両方へ同時に出力しません。
func TestReservedIDDuplicatesKeepOnlyTheLastValue(t *testing.T) {
	t.Parallel()

	traceID := trace.TraceID{1, 2, 3}
	spanID := trace.SpanID{4, 5, 6}
	ctx := trace.ContextWithSpanContext(requestctx.WithRequestID(context.Background(), "context-request"),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}))
	contextIDs := IDs{RequestID: "context-request", TraceID: traceID.String(), SpanID: spanID.String()}
	keys := []string{"request_id", "trace_id", "span_id"}
	tests := []struct {
		name      string
		values    func(applogging.FieldKey) []applogging.Field
		wantIDs   IDs
		wantExtra map[string]any
	}{
		{
			name: "number last",
			values: func(k applogging.FieldKey) []applogging.Field {
				return []applogging.Field{k.String("earlier-id"), k.String("final-id"), k.Int(1), k.Int(2)}
			},
			wantIDs:   contextIDs,
			wantExtra: map[string]any{"request_id": 2, "trace_id": 2, "span_id": 2},
		},
		{
			name: "string last",
			values: func(k applogging.FieldKey) []applogging.Field {
				return []applogging.Field{k.Int(1), k.Int(2), k.String("earlier-id"), k.String("final-id")}
			},
			wantIDs: IDs{RequestID: "final-id", TraceID: "final-id", SpanID: "final-id"},
		},
		{
			name: "empty string last",
			values: func(k applogging.FieldKey) []applogging.Field {
				return []applogging.Field{k.String("earlier-id"), k.Int(1), k.String("")}
			},
			wantIDs: contextIDs,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fields []applogging.Field
			for _, key := range keys {
				fields = append(fields, tt.values(applogging.MustFieldKey(key))...)
			}
			ids, extra := ResolveIDsAndExtraFields(ctx, fields)
			if ids != tt.wantIDs {
				t.Errorf("metadata = %#v, want %#v", ids, tt.wantIDs)
			}
			if !reflect.DeepEqual(extra, tt.wantExtra) {
				t.Errorf("attributes = %#v, want %#v", extra, tt.wantExtra)
			}
		})
	}
}

func TestStringIDFieldsKeepContextPrecedence(t *testing.T) {
	t.Parallel()

	traceID := trace.TraceID{1, 2, 3}
	spanID := trace.SpanID{4, 5, 6}
	ctx := trace.ContextWithSpanContext(requestctx.WithRequestID(context.Background(), "context-request"),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}))
	for _, explicit := range []string{"", "explicit-id"} {
		fields := []applogging.Field{
			applogging.WithRequestID(explicit),
			applogging.MustFieldKey("trace_id").String(explicit),
			applogging.MustFieldKey("span_id").String(explicit),
		}
		ids, extra := ResolveIDsAndExtraFields(ctx, fields)
		wantIDs := IDs{RequestID: explicit, TraceID: explicit, SpanID: explicit}
		if explicit == "" {
			wantIDs = IDs{RequestID: "context-request", TraceID: traceID.String(), SpanID: spanID.String()}
		}
		if ids != wantIDs || extra != nil {
			t.Errorf("explicit=%q: IDs=%#v extra=%#v, want IDs=%#v without ordinary attributes", explicit, ids, extra, wantIDs)
		}
	}
}

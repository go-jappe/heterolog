package bootstrap_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/loggerruntime"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	"github.com/go-jappe/heterolog/internal/presentation/sloghandler"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"go.opentelemetry.io/otel/trace"
)

func TestRuntimesPreserveReservedIDAttributes(t *testing.T) {
	traceID := trace.TraceID{1, 2, 3}
	spanID := trace.SpanID{4, 5, 6}
	ctx := trace.ContextWithSpanContext(requestctx.WithRequestID(context.Background(), "context-request"),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}))
	contextIDs := map[string]string{"request_id": "context-request", "trace_id": traceID.String(), "span_id": spanID.String()}
	keys := []string{"request_id", "trace_id", "span_id"}
	for _, mode := range []string{"sync", "async", "delivery", "slog"} {
		for _, scenario := range []string{"numeric", "numeric_with_context", "other_types", "string_overrides", "mixed_duplicates", "string_last_duplicates"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				logCtx := ctx
				wantIDs := contextIDs
				wantFields := make(map[string]any)
				var fields []pkglogger.Field
				for _, key := range keys {
					k := pkglogger.MustFieldKey(key)
					switch scenario {
					case "numeric", "numeric_with_context":
						fields = append(fields, k.Int(424242))
						wantFields[key] = json.Number("424242")
					case "other_types":
						values := map[string]any{"request_id": false, "trace_id": map[string]any{"id": json.Number("9007199254740993")}, "span_id": nil}
						field, err := k.Any(values[key])
						if err != nil {
							t.Fatal(err)
						}
						fields = append(fields, field)
						wantFields[key] = values[key]
					case "string_overrides":
						fields = append(fields, k.String("explicit-id"))
					case "mixed_duplicates":
						fields = append(fields, k.String("explicit-id"), k.Int(1), k.Int(2))
						wantFields[key] = json.Number("2")
					case "string_last_duplicates":
						fields = append(fields, k.Int(1), k.String("earlier-id"), k.String("explicit-id"))
					}
				}
				// どのAPIも、重複したkeyをまず最後の値にまとめてから、その一つの値を分類するため、
				// すべてのmodeで期待する結果は同じです。最後の値が文字列でない場合は、contextのIDを
				// metadataとして残します。
				if scenario == "numeric" {
					logCtx = context.Background()
					wantIDs = nil
				} else if scenario == "string_overrides" || scenario == "string_last_duplicates" {
					wantIDs = map[string]string{"request_id": "explicit-id", "trace_id": "explicit-id", "span_id": "explicit-id"}
				}
				output := testutil.CaptureStdout(t, func() {
					logReservedFieldsThroughRuntime(t, mode, logCtx, fields)
				})
				prefix, encoded, hasFields := strings.Cut(output, " fields:")
				for _, key := range keys {
					if want := wantIDs[key]; want != "" {
						if !strings.Contains(prefix, key+": "+want) {
							t.Errorf("metadata %s=%q missing from %q", key, want, prefix)
						}
					} else if strings.Contains(prefix, key+": ") {
						t.Errorf("non-string %s was promoted to metadata: %q", key, prefix)
					}
				}
				var gotFields map[string]any
				if hasFields {
					decoder := json.NewDecoder(strings.NewReader(encoded))
					decoder.UseNumber()
					if err := decoder.Decode(&gotFields); err != nil {
						t.Fatalf("decode output fields: %v; output=%q", err, output)
					}
				}
				for _, key := range keys {
					got, found := gotFields[key]
					want, expected := wantFields[key]
					if found != expected || !reflect.DeepEqual(got, want) {
						t.Errorf("field %s = %#v (present=%t), want %#v (present=%t); output=%q", key, got, found, want, expected, output)
					}
				}
			})
		}
	}
}

func logReservedFieldsThroughRuntime(t *testing.T, mode string, ctx context.Context, fields []pkglogger.Field) {
	t.Helper()
	const message = "reserved ID attribute values"
	switch mode {
	case "sync", "slog":
		logger, err := bootstrap.New(context.Background(), loggerruntime.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer logger.Close()
		if mode == "sync" {
			err = logger.Info(ctx, message, fields...)
		} else {
			record := slog.NewRecord(time.Now(), slog.LevelInfo, message, 0)
			for _, field := range fields {
				record.AddAttrs(slog.Any(field.Key(), field.Value()))
			}
			err = sloghandler.New(logger, nil).Handle(ctx, record)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}
	case "async":
		logger, err := bootstrap.NewAsync(context.Background(), loggerruntime.DefaultAsyncConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer logger.Close()
		logger.Info(ctx, message, fields...)
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}
	case "delivery":
		logger, err := bootstrap.NewDelivery(context.Background(), loggerruntime.DefaultDeliveryConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer logger.Close()
		report, err := logger.Info(ctx, message, fields...)
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Targets) != 1 || report.Targets[0].Status != pkglogger.DeliverySucceeded {
			t.Fatalf("delivery report = %+v, want one successful stdout target", report)
		}
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unrecognized runtime mode %q", mode)
	}
}

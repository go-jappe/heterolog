package sloghandler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/slogtest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/bootstrap"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/multi"
	"github.com/go-jappe/heterolog/internal/presentation/sloghandler"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"go.opentelemetry.io/otel/trace"
)

type slogCapturedRecord struct {
	ctx     context.Context
	level   slog.Level
	message string
	fields  map[string]any
}

type slogCaptureLogger struct {
	mu      sync.Mutex
	records []slogCapturedRecord
	err     error
}

func (l *slogCaptureLogger) log(ctx context.Context, level slog.Level, message string, fields []pkglogger.Field) error {
	values := make(map[string]any, len(fields))
	for _, field := range fields {
		if field.IsDefined() {
			values[field.Key()] = field.Value()
		}
	}
	l.mu.Lock()
	l.records = append(l.records, slogCapturedRecord{ctx: ctx, level: level, message: message, fields: values})
	l.mu.Unlock()
	return l.err
}

func (l *slogCaptureLogger) Debug(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.log(ctx, slog.LevelDebug, message, fields)
}

func (l *slogCaptureLogger) Info(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.log(ctx, slog.LevelInfo, message, fields)
}

func (l *slogCaptureLogger) Warn(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.log(ctx, slog.LevelWarn, message, fields)
}

func (l *slogCaptureLogger) Error(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.log(ctx, slog.LevelError, message, fields)
}

func (l *slogCaptureLogger) ErrorWithStack(ctx context.Context, message string, _ error, fields ...pkglogger.Field) error {
	return l.log(ctx, slog.LevelError, message, fields)
}

func (l *slogCaptureLogger) snapshot() []slogCapturedRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]slogCapturedRecord(nil), l.records...)
}

func TestHandlerConformance(t *testing.T) {
	sink := &slogCaptureLogger{}
	handler := sloghandler.New(sink, &slog.HandlerOptions{AddSource: true})
	if err := slogtest.TestHandler(handler, func() []map[string]any {
		var results []map[string]any
		for _, record := range sink.snapshot() {
			results = append(results, record.fields)
		}
		return results
	}); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerMatchesStandardJSONHandler(t *testing.T) {
	tests := []struct {
		name   string
		derive func(slog.Handler) slog.Handler
		attrs  []slog.Attr
	}{
		{
			name: "all value types and unrestricted keys",
			attrs: []slog.Attr{
				slog.String("", "empty key"), slog.String("two words", "spaces"), slog.String("a\nb", "newline"),
				slog.Int64("int", math.MinInt64), slog.Uint64("uint", math.MaxUint64), slog.Float64("float", 1.5),
				slog.Bool("bool", true), slog.Duration("duration", time.Second), slog.Time("date", time.Unix(123, 456).UTC()),
				slog.Any("error", errors.New("example")), slog.Any("unsupported", make(chan int)), slog.Float64("nan", math.NaN()),
				slog.Any("slice", []int{1, 2}), slog.Group("", slog.String("inlined", "value")), slog.Group("empty"),
			},
		},
		{
			name: "nested groups and bound attributes",
			derive: func(h slog.Handler) slog.Handler {
				return h.WithAttrs([]slog.Attr{slog.String("service", "test")}).WithGroup("request").
					WithAttrs([]slog.Attr{slog.Int("id", 7)}).WithGroup("details")
			},
			attrs: []slog.Attr{slog.String("secret", "redact me"), slog.String("path", "/example")},
		},
		{
			name: "empty group omitted",
			derive: func(h slog.Handler) slog.Handler {
				return h.WithAttrs([]slog.Attr{slog.Int("a", 1)}).WithGroup("empty")
			},
			attrs: []slog.Attr{slog.Group("nested")},
		},
		{
			name: "duplicate values retain final value",
			derive: func(h slog.Handler) slog.Handler {
				return h.WithAttrs([]slog.Attr{slog.String("key", "bound")})
			},
			attrs: []slog.Attr{slog.Int("key", 1), slog.Int("key", 2)},
		},
		{
			name: "new group replaces same-named bound group attribute",
			derive: func(h slog.Handler) slog.Handler {
				return h.WithAttrs([]slog.Attr{slog.Group("g", slog.Int("old", 1))}).WithGroup("g")
			},
			attrs: []slog.Attr{slog.Int("new", 2)},
		},
		{
			name: "continuing group retains bound fields",
			derive: func(h slog.Handler) slog.Handler {
				return h.WithGroup("g").WithAttrs([]slog.Attr{slog.Int("old", 1)})
			},
			attrs: []slog.Attr{slog.Int("new", 2)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := &slog.HandlerOptions{ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
				if attr.Key == "secret" {
					return slog.Attr{}
				}
				if attr.Key == "path" {
					return slog.String("path", strings.Join(groups, ".")+":"+attr.Value.String())
				}
				if len(groups) == 0 && attr.Key == slog.MessageKey {
					attr.Key = "message"
				}
				return attr
			}}
			sink := &slogCaptureLogger{}
			var gotHandler slog.Handler = sloghandler.New(sink, opts)
			var buffer bytes.Buffer
			var wantHandler slog.Handler = slog.NewJSONHandler(&buffer, opts)
			if test.derive != nil {
				gotHandler = test.derive(gotHandler)
				wantHandler = test.derive(wantHandler)
			}
			record := slog.NewRecord(time.Date(2026, 10, 21, 0, 0, 0, 123, time.UTC), slog.LevelWarn+1, "message", 0)
			record.AddAttrs(test.attrs...)
			if err := gotHandler.Handle(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			if err := wantHandler.Handle(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			var want map[string]any
			decoder := json.NewDecoder(&buffer)
			decoder.UseNumber()
			if err := decoder.Decode(&want); err != nil {
				t.Fatal(err)
			}
			got := sink.snapshot()[0].fields
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fields = %#v\nwant standard JSON = %#v", got, want)
			}
		})
	}
}

func TestHandlerLevelFilteringAndDeliveryError(t *testing.T) {
	ctx := context.Background()
	sink := &slogCaptureLogger{}
	defaultHandler := sloghandler.New(sink, nil)
	if defaultHandler.Enabled(ctx, slog.LevelDebug) || !defaultHandler.Enabled(ctx, slog.LevelInfo) {
		t.Fatal("default handler must enable Info and above")
	}
	var minimum slog.LevelVar
	handler := sloghandler.New(sink, &slog.HandlerOptions{Level: &minimum})
	logger := slog.New(handler)
	logger.Debug("filtered")
	minimum.Set(slog.LevelDebug)
	logger.Debug("accepted")
	if got := len(sink.snapshot()); got != 1 {
		t.Fatalf("record count = %d, want 1", got)
	}
	for _, test := range []struct{ level, want slog.Level }{
		{slog.LevelDebug - 1, slog.LevelDebug}, {slog.LevelInfo - 1, slog.LevelDebug},
		{slog.LevelWarn - 1, slog.LevelInfo}, {slog.LevelError - 1, slog.LevelWarn},
		{slog.LevelError + 10, slog.LevelError},
	} {
		err := handler.Handle(ctx, slog.NewRecord(time.Now(), test.level, "custom", 0))
		if err != nil {
			t.Fatal(err)
		}
		records := sink.snapshot()
		if got := records[len(records)-1].level; got != test.want {
			t.Fatalf("level %s routed to %s, want %s", test.level, got, test.want)
		}
	}
	sentinel := errors.New("delivery failed")
	sink.err = sentinel
	if err := handler.Handle(ctx, slog.NewRecord(time.Now(), slog.LevelInfo, "failure", 0)); !errors.Is(err, sentinel) {
		t.Fatalf("Handle error = %v, want delivery error", err)
	}
}

type countingSlogValuer struct{ calls *atomic.Int32 }

func (v countingSlogValuer) LogValue() slog.Value {
	v.calls.Add(1)
	return slog.StringValue("resolved")
}

func TestHandlerSnapshotsAndConcurrentDerivatives(t *testing.T) {
	sink := &slogCaptureLogger{}
	base := sloghandler.New(sink, nil)
	var calls atomic.Int32
	payload := map[string]any{"value": "original"}
	bound := base.WithAttrs([]slog.Attr{slog.Any("payload", payload), slog.Any("valuer", countingSlogValuer{&calls})})
	payload["value"] = "mutated"
	left := bound.WithGroup("left").WithAttrs([]slog.Attr{slog.String("branch", "left")})
	right := bound.WithGroup("right").WithAttrs([]slog.Attr{slog.String("branch", "right")})
	var wg sync.WaitGroup
	for index := range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler := left
			if index%2 != 0 {
				handler = right
			}
			child := handler.WithAttrs([]slog.Attr{slog.Int("worker", index)})
			if err := child.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "parallel", 0)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("bound LogValuer resolved %d times, want 1", got)
	}
	for _, record := range sink.snapshot() {
		if got := record.fields["payload"].(map[string]any)["value"]; got != "original" {
			t.Fatalf("bound snapshot changed: %v", got)
		}
		_, hasLeft := record.fields["left"]
		_, hasRight := record.fields["right"]
		if hasLeft == hasRight {
			t.Fatalf("derived groups leaked: %#v", record.fields)
		}
	}
	if err := base.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "base", 0)); err != nil {
		t.Fatal(err)
	}
	records := sink.snapshot()
	if _, ok := records[len(records)-1].fields["payload"]; ok {
		t.Fatal("WithAttrs changed its parent handler")
	}
}

type slogBackendConfig struct{ limit int }

func (slogBackendConfig) Timeout() time.Duration  { return time.Second }
func (c slogBackendConfig) MaxLogEntryBytes() int { return c.limit }

type slogBackendCapture struct{ entry *infralogging.LogEntry }

func (s *slogBackendCapture) HandleLogEntry(_ context.Context, entry *infralogging.LogEntry) error {
	s.entry = entry
	return nil
}

func TestHandlerPreservesBackendTimeContextAndSource(t *testing.T) {
	backend := &slogBackendCapture{}
	logger, err := multi.NewMultiLogger(slogBackendConfig{pkglogger.DefaultMaxLogEntryBytes}, backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logger.Close() })
	handler := sloghandler.New(logger, &slog.HandlerOptions{AddSource: true})
	traceID := trace.TraceID{1, 2, 3}
	spanID := trace.SpanID{4, 5, 6}
	ctx := trace.ContextWithSpanContext(requestctx.WithRequestID(context.Background(), "request-123"),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}))
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])
	wantTime := time.Date(2020, 2, 3, 4, 5, 6, 789, time.FixedZone("example", 9*60*60))
	record := slog.NewRecord(wantTime, slog.LevelWarn, "original message", pcs[0])
	record.AddAttrs(slog.String("", "empty key"), slog.String("with space", "accepted"))
	if err := handler.Handle(ctx, record); err != nil {
		t.Fatal(err)
	}
	entry := backend.entry
	if entry == nil || !entry.OccurredAt.Equal(wantTime) || entry.OccurredAt.Location() != time.UTC {
		t.Fatalf("backend occurrence time not preserved: %#v", entry)
	}
	if entry.TraceID != traceID.String() || entry.SpanID != spanID.String() || entry.RequestID != "request-123" {
		t.Fatalf("context IDs not preserved: %#v", entry)
	}
	if entry.Fields[""] != "empty key" || entry.Fields["with space"] != "accepted" {
		t.Fatalf("slog attribute keys rejected: %#v", entry.Fields)
	}
	source, ok := entry.Fields[slog.SourceKey].(map[string]any)
	if !ok || !strings.HasSuffix(source["file"].(string), "handler_test.go") {
		t.Fatalf("caller source not preserved: %#v", entry.Fields[slog.SourceKey])
	}
	if _, err := pkglogger.String("", "still invalid"); !errors.Is(err, pkglogger.ErrFieldKeyInvalid) {
		t.Fatalf("public Field constructor became permissive: %v", err)
	}
}

func TestHandlerRetainsEntrySizeLimit(t *testing.T) {
	backend := &slogBackendCapture{}
	logger, err := multi.NewMultiLogger(slogBackendConfig{128}, backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logger.Close() })
	handler := sloghandler.New(logger, nil)
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "too large", 0)
	record.AddAttrs(slog.String("payload", strings.Repeat("a", 1024)))
	var tooLarge *pkglogger.LogEntryTooLargeError
	if err := handler.Handle(context.Background(), record); !errors.As(err, &tooLarge) {
		t.Fatalf("Handle error = %v, want LogEntryTooLargeError", err)
	}
	if backend.entry != nil {
		t.Fatal("oversized record reached backend")
	}
}

func TestHandlerWritesStructuredFieldsThroughARuntime(t *testing.T) {
	wantTime := time.Date(2020, 2, 3, 4, 5, 6, 789, time.UTC)
	output := testutil.CaptureStdout(t, func() {
		logger, err := bootstrap.New(context.Background(), bootstrap.DefaultConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = logger.Close() })
		handler := sloghandler.New(logger, nil).
			WithAttrs([]slog.Attr{slog.String("", "empty key"), slog.Bool("enabled", true)}).
			WithGroup("request").WithAttrs([]slog.Attr{slog.Int("attempt", 2)})
		record := slog.NewRecord(wantTime, slog.LevelInfo, "slog backend output", 0)
		record.AddAttrs(slog.Uint64("large", math.MaxUint64), slog.Float64("ratio", 0.5), slog.String("status", "accepted"))
		if err := handler.Handle(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		if err := logger.Close(); err != nil {
			t.Fatal(err)
		}
	})
	wantPrefix := "[INFO] " + wantTime.Format(time.RFC3339Nano) + " - slog backend output fields:"
	if !strings.HasPrefix(output, wantPrefix) {
		t.Fatalf("output timestamp/message = %q, want prefix %q", output, wantPrefix)
	}
	decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(output, wantPrefix)))
	decoder.UseNumber()
	var fields map[string]any
	if err := decoder.Decode(&fields); err != nil {
		t.Fatal(err)
	}
	if fields[""] != "empty key" || fields["enabled"] != true {
		t.Fatalf("root attributes lost in backend output: %#v", fields)
	}
	wantGroup := map[string]any{
		"attempt": json.Number("2"), "large": json.Number("18446744073709551615"),
		"ratio": json.Number("0.5"), "status": "accepted",
	}
	if !reflect.DeepEqual(fields["request"], wantGroup) {
		t.Fatalf("backend group = %#v, want %#v", fields["request"], wantGroup)
	}
}

type redactedSlogMessage struct {
	Secret string `json:"-"`
	Label  string `json:"label"`
}

func (m redactedSlogMessage) String() string { return m.Secret }

func TestHandlerMessageReplacementReachesBackendOutput(t *testing.T) {
	const original = "SYNTHETIC_PRIVATE_MESSAGE"
	for _, test := range []struct {
		name        string
		replacement slog.Attr
		wantMessage string
	}{
		{"redacted", slog.String(slog.MessageKey, "redacted"), "redacted"},
		{"removed", slog.Attr{}, ""},
		{"renamed", slog.String("summary", "redacted"), "redacted"},
		{"non-string", slog.Int("summary", 42), "42"},
		{"JSON object", slog.Any("summary", redactedSlogMessage{Secret: original, Label: "redacted"}), `{"label":"redacted"}`},
		{"inline group", slog.Group("", slog.String("summary", "redacted")), ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := testutil.CaptureStdout(t, func() {
				logger, err := bootstrap.New(context.Background(), bootstrap.DefaultConfig())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = logger.Close() })
				handler := sloghandler.New(logger, &slog.HandlerOptions{
					ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
						if len(groups) == 0 && attr.Key == slog.MessageKey {
							return test.replacement
						}
						return attr
					},
				})
				if err := handler.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, original, 0)); err != nil {
					t.Fatal(err)
				}
				if err := logger.Close(); err != nil {
					t.Fatal(err)
				}
			})
			if strings.Contains(output, original) {
				t.Fatalf("original message leaked after ReplaceAttr: %s", output)
			}
			if !strings.Contains(output, " - "+test.wantMessage+" fields:") {
				t.Fatalf("native message = %q, want %q", output, test.wantMessage)
			}
		})
	}
}

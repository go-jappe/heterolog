package otelexport

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type recordingSink struct {
	mu    sync.Mutex
	spans []tracing.SpanData
}

func (s *recordingSink) Enqueue(span tracing.SpanData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans = append(s.spans, span)
}

func TestSpanExporterConvertsSDKSpans(t *testing.T) {
	sink := &recordingSink{}
	res := resource.NewSchemaless(attribute.String("service.name", "sdk-service"))
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(NewSpanExporter(sink)),
		sdktrace.WithResource(res),
	)
	tracer := provider.Tracer("sdk-lib", trace.WithInstrumentationVersion("1.2.3"))

	ctx, parent := tracer.Start(context.Background(), "parent", trace.WithSpanKind(trace.SpanKindServer))
	_, child := tracer.Start(ctx, "child",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int("count", 3), attribute.StringSlice("tags", []string{"a"})),
		trace.WithLinks(trace.Link{SpanContext: parent.SpanContext(), Attributes: []attribute.KeyValue{attribute.Bool("linked", true)}}),
	)
	child.AddEvent("retry", trace.WithAttributes(attribute.Float64("delay", 0.5)))
	child.SetStatus(codes.Error, "failed")
	child.End()
	parent.End()
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(sink.spans) != 2 {
		t.Fatalf("spans = %d, want 2", len(sink.spans))
	}
	got, parentData := sink.spans[0], sink.spans[1]
	if got.Name != "child" || got.Kind != pkgobserver.SpanKindClient || parentData.Kind != pkgobserver.SpanKindServer {
		t.Errorf("names and kinds = %s %v / %s %v", got.Name, got.Kind, parentData.Name, parentData.Kind)
	}
	if got.TraceID != tracing.TraceID(child.SpanContext().TraceID()) || got.ParentSpanID != parentData.SpanID || !parentData.ParentSpanID.IsZero() {
		t.Errorf("IDs: trace %s parent %s, parent span %s", got.TraceID, got.ParentSpanID, parentData.SpanID)
	}
	if len(got.Attrs) != 2 || got.Attrs[0] != (pkgobserver.Attr{Key: "count", Value: int64(3)}) {
		t.Errorf("attrs = %+v, want typed values", got.Attrs)
	}
	if tags, ok := got.Attrs[1].Value.([]string); !ok || len(tags) != 1 {
		t.Errorf("slice attribute = %#v, want []string", got.Attrs[1].Value)
	}
	if len(got.Events) != 1 || got.Events[0].Name != "retry" || got.Events[0].Attrs[0].Value != 0.5 {
		t.Errorf("events = %+v", got.Events)
	}
	if len(got.Links) != 1 || got.Links[0].SpanID != parentData.SpanID || got.Links[0].Attrs[0].Value != true {
		t.Errorf("links = %+v", got.Links)
	}
	if got.Status != pkgobserver.StatusError || got.StatusMessage != "failed" {
		t.Errorf("status = %v %q", got.Status, got.StatusMessage)
	}
	if got.Scope != (tracing.Scope{Name: "sdk-lib", Version: "1.2.3"}) {
		t.Errorf("scope = %+v", got.Scope)
	}
	// 同じResourceは、変換した同じpointerを使います。
	if got.Resource == nil || got.Resource != parentData.Resource || got.Resource.Attrs[0].Value != "sdk-service" {
		t.Errorf("resources = %p %p %+v, want one shared converted resource", got.Resource, parentData.Resource, got.Resource)
	}
}

func TestResourceCacheKeepsTheTelemetryResourceForEmptyResources(t *testing.T) {
	var cache resourceCache
	if got := cache.convert(resource.Empty()); got != nil {
		t.Errorf("convert(empty) = %+v, want nil", got)
	}
	if got := cache.convert(nil); got != nil {
		t.Errorf("convert(nil) = %+v, want nil", got)
	}
}

func TestSpanExporterRejectsSpansAfterShutdown(t *testing.T) {
	exporter := NewSpanExporter(&recordingSink{})
	if err := exporter.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := exporter.ExportSpans(context.Background(), nil); !errors.Is(err, ErrExporterShutdown) {
		t.Errorf("ExportSpans after Shutdown = %v, want ErrExporterShutdown", err)
	}
}

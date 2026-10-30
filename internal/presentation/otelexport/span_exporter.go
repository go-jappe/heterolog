// Package otelexportは、OpenTelemetry SDKから届くspanの入口です。SDKのspanをSpanDataに
// 変え、NewTelemetryのTracerと同じ配送の仕組みへ渡します。SDKをlinkしないprogramに
// SDKを持ち込まないよう、root packageの隣の公開package otelexportだけがimportします。
package otelexport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// ErrExporterShutdownは、Shutdownの後にExportSpansが呼ばれたことを表します。
var ErrExporterShutdown = errors.New("heterolog span exporter is shut down")

// maxCachedResourcesは、変換したResourceを覚えておく数の上限です。TracerProviderは
// 1つのResourceを使い続けるため、通常はTracerProviderの数だけ覚えます。
const maxCachedResources = 64

// SpanExporterは、sdktrace.SpanExporterの実装です。
type SpanExporter struct {
	sink     tracing.SpanSink
	shutdown atomic.Bool

	// muは、resourcesを守ります。resourcesは、SDKのResourceを変換した結果です。
	// 同じResourceのspanに同じpointerを付け、送るときに1つのResourceSpansへまとめます。
	mu        sync.Mutex
	resources map[*resource.Resource]*tracing.Resource
}

// NewSpanExporterは、spanをsinkへ渡すSpanExporterを返します。
func NewSpanExporter(sink tracing.SpanSink) *SpanExporter {
	return &SpanExporter{sink: sink, resources: make(map[*resource.Resource]*tracing.Resource)}
}

// ExportSpansは、spansを配送の仕組みの待ち行列へ入れます。送り終わるのを待たずに返り、
// 待ち行列が一杯で捨てたspanは、配送の仕組みのmetricsで数えます。
func (e *SpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	if e.shutdown.Load() {
		return ErrExporterShutdown
	}
	for _, span := range spans {
		data := toSpanData(span)
		data.Resource = e.toResource(span.Resource())
		e.sink.Enqueue(data)
	}
	return nil
}

// Shutdownは、以後のExportSpansを断ります。受け取ったspanは、NewTelemetryのTelemetryの
// Shutdownまで送り続けます。
func (e *SpanExporter) Shutdown(context.Context) error {
	e.shutdown.Store(true)
	return nil
}

// toSpanDataは、Resource以外を変換します。
func toSpanData(span sdktrace.ReadOnlySpan) tracing.SpanData {
	sc := span.SpanContext()
	data := tracing.SpanData{
		TraceID:       tracing.TraceID(sc.TraceID()),
		SpanID:        tracing.SpanID(sc.SpanID()),
		Name:          span.Name(),
		Kind:          toSpanKind(span.SpanKind()),
		StartTime:     span.StartTime(),
		EndTime:       span.EndTime(),
		Attrs:         toAttrs(span.Attributes()),
		DroppedAttrs:  span.DroppedAttributes(),
		DroppedEvents: span.DroppedEvents(),
		DroppedLinks:  span.DroppedLinks(),
		Status:        toStatusCode(span.Status().Code),
		StatusMessage: span.Status().Description,
		Scope: tracing.Scope{
			Name:    span.InstrumentationScope().Name,
			Version: span.InstrumentationScope().Version,
		},
	}
	if parent := span.Parent(); parent.IsValid() {
		data.ParentSpanID = tracing.SpanID(parent.SpanID())
	}
	for _, event := range span.Events() {
		data.Events = append(data.Events, tracing.Event{
			Name:         event.Name,
			Time:         event.Time,
			Attrs:        toAttrs(event.Attributes),
			DroppedAttrs: event.DroppedAttributeCount,
		})
	}
	for _, link := range span.Links() {
		data.Links = append(data.Links, tracing.Link{
			TraceID:      tracing.TraceID(link.SpanContext.TraceID()),
			SpanID:       tracing.SpanID(link.SpanContext.SpanID()),
			Attrs:        toAttrs(link.Attributes),
			DroppedAttrs: link.DroppedAttributeCount,
		})
	}
	return data
}

// toResourceは、SDKのResourceを変換します。nilと空のResourceは、NewTelemetryに設定した
// Resourceで送るため、nilにします。
func (e *SpanExporter) toResource(r *resource.Resource) *tracing.Resource {
	if r == nil || r.Len() == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if converted, ok := e.resources[r]; ok {
		return converted
	}
	converted := &tracing.Resource{Attrs: toAttrs(r.Attributes()), SchemaURL: r.SchemaURL()}
	if len(e.resources) < maxCachedResources {
		e.resources[r] = converted
	}
	return converted
}

// toAttrsは、属性を、型を保ったGoの値（bool・int64・float64・stringと、それらのslice）に
// します。
func toAttrs(attrs []attribute.KeyValue) []pkgobserver.Attr {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]pkgobserver.Attr, len(attrs))
	for i, attr := range attrs {
		out[i] = pkgobserver.Attr{Key: string(attr.Key), Value: attr.Value.AsInterface()}
	}
	return out
}

func toSpanKind(kind trace.SpanKind) pkgobserver.SpanKind {
	switch kind {
	case trace.SpanKindServer:
		return pkgobserver.SpanKindServer
	case trace.SpanKindClient:
		return pkgobserver.SpanKindClient
	case trace.SpanKindProducer:
		return pkgobserver.SpanKindProducer
	case trace.SpanKindConsumer:
		return pkgobserver.SpanKindConsumer
	default:
		return pkgobserver.SpanKindInternal
	}
}

func toStatusCode(code codes.Code) pkgobserver.StatusCode {
	switch code {
	case codes.Ok:
		return pkgobserver.StatusOK
	case codes.Error:
		return pkgobserver.StatusError
	default:
		return pkgobserver.StatusUnset
	}
}

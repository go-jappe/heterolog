package tracing

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// InstrumentationScopeは、Tracerが作るspanのinstrumentation scopeの名前です。
const InstrumentationScope = "github.com/go-jappe/heterolog"

// spanの属性・event・linkの数の上限です。OpenTelemetry SDKの既定値と同じにし、上限を
// 超えた分は捨てて、捨てた数を送ります。終わっていないspanが際限なくmemoryを使わない
// ようにするためのもので、設定では変えません。
const (
	maxAttrsPerSpan  = 128
	maxEventsPerSpan = 128
	maxLinksPerSpan  = 128
	maxAttrsPerItem  = 128
)

// Tracerは、pkg/observerのTracerとOptionsTracerの実装です。contextのSpanContextを親に
// して、spanを始めます。親がなければ新しいtraceを始め、samplingの割合で記録するかを
// 決めます。親があれば、親の判定（Sampled）に従います。記録するspanは、Endで
// SpanSinkへ渡します。
type Tracer struct {
	sink  SpanSink
	ratio float64
	now   func() time.Time
}

// NewTracerは、終わったspanをsinkへ渡すTracerを返します。samplingRatioは、新しく始める
// traceを記録する割合（0から1）です。範囲の検証は、設定を受け取る側で済ませます。
func NewTracer(sink SpanSink, samplingRatio float64) *Tracer {
	return &Tracer{sink: sink, ratio: samplingRatio, now: time.Now}
}

// StartSpanは、StartOptionsを指定せずにspanを始めます。
func (t *Tracer) StartSpan(ctx context.Context, name string, attrs ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	return t.StartSpanWithOptions(ctx, name, pkgobserver.StartOptions{}, attrs...)
}

// StartSpanWithOptionsは、spanを始めます。返すcontextには、新しいspanのSpanContextが
// 入ります。記録しないspanも、子のspanと送り先のprocessが同じ判定に従えるよう、
// SpanContextを持ちます。
func (t *Tracer) StartSpanWithOptions(
	ctx context.Context,
	name string,
	opts pkgobserver.StartOptions,
	attrs ...pkgobserver.Attr,
) (context.Context, pkgobserver.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	parent := pkgobserver.SpanContextFromContext(ctx)
	var traceID TraceID
	var parentSpanID SpanID
	var sampled bool
	if parent.IsValid() {
		traceID = ParseTraceID(parent.TraceID)
		parentSpanID = ParseSpanID(parent.SpanID)
		sampled = parent.Sampled
	} else {
		traceID = newTraceID()
		sampled = sampleTrace(traceID, t.ratio)
	}
	spanID := newSpanID()
	sc := pkgobserver.SpanContext{
		TraceID: pkgobserver.TraceID(traceID.String()),
		SpanID:  pkgobserver.SpanID(spanID.String()),
		Sampled: sampled,
	}
	ctx = pkgobserver.ContextWithSpanContext(ctx, sc)
	// loggerが送信の失敗の記録にtrace IDを残し、overloadなどのeventを呼び出し元のspanへ
	// 足せるよう、loggerが包んだtracerと同じくspanとtrace IDもcontextへ入れます。
	ctx = observer.WithTraceID(ctx, sc.TraceID)
	if !sampled {
		span := nonRecordingSpan{traceID: sc.TraceID}
		return observer.WithSpan(ctx, span), span
	}

	span := &recordingSpan{
		sink: t.sink,
		now:  t.now,
		data: SpanData{
			TraceID:      traceID,
			SpanID:       spanID,
			ParentSpanID: parentSpanID,
			Name:         name,
			Kind:         opts.Kind,
			StartTime:    t.now(),
			Scope:        Scope{Name: InstrumentationScope},
		},
	}
	span.setAttributes(attrs)
	for _, link := range opts.Links {
		if !link.SpanContext.IsValid() {
			continue
		}
		if len(span.data.Links) >= maxLinksPerSpan {
			span.data.DroppedLinks++
			continue
		}
		linkAttrs, dropped := limitAttrs(link.Attrs)
		span.data.Links = append(span.data.Links, Link{
			TraceID:      ParseTraceID(link.SpanContext.TraceID),
			SpanID:       ParseSpanID(link.SpanContext.SpanID),
			Attrs:        linkAttrs,
			DroppedAttrs: dropped,
		})
	}
	return observer.WithSpan(ctx, span), span
}

// sampleTraceは、traceIDのtraceを記録するかを、割合ratioで決めます。OpenTelemetry SDKの
// TraceIDRatioBasedと同じく、IDの後半8 byteで決めるため、同じtraceはどのprocessでも
// 同じ判定になります。
func sampleTrace(traceID TraceID, ratio float64) bool {
	if ratio >= 1 {
		return true
	}
	if !(ratio > 0) {
		return false
	}
	bound := uint64(ratio * math.MaxInt64)
	return binary.BigEndian.Uint64(traceID[8:])>>1 < bound
}

// newTraceIDとnewSpanIDは、すべて0ではない乱数のIDを返します。math/rand/v2の関数は、
// processごとに乱数で初期化され、並行に呼べます。
func newTraceID() TraceID {
	var id TraceID
	for id.IsZero() {
		binary.BigEndian.PutUint64(id[:8], rand.Uint64())
		binary.BigEndian.PutUint64(id[8:], rand.Uint64())
	}
	return id
}

func newSpanID() SpanID {
	var id SpanID
	for id.IsZero() {
		binary.BigEndian.PutUint64(id[:], rand.Uint64())
	}
	return id
}

// limitAttrsは、上限までの属性と、捨てた属性の数を返します。同じkeyは後の値で置き換えます。
func limitAttrs(attrs []pkgobserver.Attr) ([]pkgobserver.Attr, int) {
	if len(attrs) == 0 {
		return nil, 0
	}
	out := make([]pkgobserver.Attr, 0, min(len(attrs), maxAttrsPerItem))
	dropped := 0
	for _, attr := range attrs {
		out, dropped = putAttr(out, attr, maxAttrsPerItem, dropped)
	}
	return out, dropped
}

// putAttrは、attrsにattrを足します。同じkeyがあれば値を置き換え、なければ上限limitまで
// 足します。上限を超えた属性は捨て、droppedに数えます。
func putAttr(attrs []pkgobserver.Attr, attr pkgobserver.Attr, limit, dropped int) ([]pkgobserver.Attr, int) {
	for i := range attrs {
		if attrs[i].Key == attr.Key {
			attrs[i].Value = attr.Value
			return attrs, dropped
		}
	}
	if len(attrs) >= limit {
		return attrs, dropped + 1
	}
	return append(attrs, attr), dropped
}

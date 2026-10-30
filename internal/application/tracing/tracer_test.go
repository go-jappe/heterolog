package tracing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// recordingSinkは、受け取ったspanを順に保持します。
type recordingSink struct {
	mu    sync.Mutex
	spans []SpanData
}

func (s *recordingSink) Enqueue(span SpanData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans = append(s.spans, span)
}

func (s *recordingSink) ended() []SpanData {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SpanData(nil), s.spans...)
}

func newTestTracer(ratio float64) (*Tracer, *recordingSink) {
	sink := &recordingSink{}
	tracer := NewTracer(sink, ratio)
	tick := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	tracer.now = func() time.Time {
		tick = tick.Add(time.Millisecond)
		return tick
	}
	return tracer, sink
}

func TestChildSpanContinuesTheTraceOfItsParent(t *testing.T) {
	tracer, sink := newTestTracer(1)

	ctx, parent := tracer.StartSpan(context.Background(), "parent")
	parentSC := pkgobserver.SpanContextFromContext(ctx)
	_, child := tracer.StartSpan(ctx, "child", pkgobserver.Attr{Key: "k", Value: "v"})
	child.End()
	parent.End()

	ended := sink.ended()
	if len(ended) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(ended))
	}
	childData, parentData := ended[0], ended[1]
	if childData.TraceID != parentData.TraceID || childData.TraceID.String() != string(parentSC.TraceID) {
		t.Errorf("child trace %s, parent trace %s, context %s; want the same trace", childData.TraceID, parentData.TraceID, parentSC.TraceID)
	}
	if childData.ParentSpanID != parentData.SpanID || !parentData.ParentSpanID.IsZero() {
		t.Errorf("child parent = %s, parent span = %s, parent's parent = %s; want child -> parent -> none",
			childData.ParentSpanID, parentData.SpanID, parentData.ParentSpanID)
	}
	if !parentSC.Sampled || parentData.SpanID.String() != string(parentSC.SpanID) {
		t.Errorf("context span context = %+v, want the sampled parent span %s", parentSC, parentData.SpanID)
	}
	if len(childData.Attrs) != 1 || childData.Attrs[0] != (pkgobserver.Attr{Key: "k", Value: "v"}) {
		t.Errorf("child attrs = %+v, want the start attribute", childData.Attrs)
	}
	if !childData.EndTime.After(childData.StartTime) || childData.Scope.Name != InstrumentationScope {
		t.Errorf("child times %v..%v scope %+v; want an end after the start and the heterolog scope",
			childData.StartTime, childData.EndTime, childData.Scope)
	}
	// loggerが送信の失敗にtrace IDを残せるよう、内部のcontextにもspanとtrace IDを入れます。
	if observer.SpanFromContext(ctx) != parent || observer.TraceIDFromContext(ctx) != parentSC.TraceID {
		t.Error("the started span and its trace ID are not in the context for the loggers")
	}
}

func TestRemoteParentSamplingDecisionIsFollowed(t *testing.T) {
	// samplingの割合が0でも、親が記録すると決めたtraceは記録します。
	tracer, sink := newTestTracer(0)
	remote, err := pkgobserver.ParseTraceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatal(err)
	}
	_, span := tracer.StartSpan(pkgobserver.ContextWithSpanContext(context.Background(), remote), "server")
	span.End()
	ended := sink.ended()
	if len(ended) != 1 || ended[0].TraceID.String() != string(remote.TraceID) || ended[0].ParentSpanID.String() != string(remote.SpanID) {
		t.Fatalf("ended = %+v, want one child of the remote span", ended)
	}

	// 割合が1でも、親が記録しないと決めたtraceは記録せず、その判定を子へ伝えます。
	tracer, sink = newTestTracer(1)
	remote.Sampled = false
	ctx, span := tracer.StartSpan(pkgobserver.ContextWithSpanContext(context.Background(), remote), "server")
	span.End()
	sc := pkgobserver.SpanContextFromContext(ctx)
	if len(sink.ended()) != 0 || sc.Sampled || sc.TraceID != remote.TraceID || span.TraceID() != remote.TraceID {
		t.Errorf("ended %d spans, context %+v, span trace %q; want no span and an unsampled child context of the remote trace",
			len(sink.ended()), sc, span.TraceID())
	}
}

func TestSamplingRatioDecidesNewTraces(t *testing.T) {
	tracer, sink := newTestTracer(0)
	ctx, span := tracer.StartSpan(context.Background(), "dropped")
	span.End()
	if len(sink.ended()) != 0 || !pkgobserver.SpanContextFromContext(ctx).IsValid() {
		t.Error("ratio 0 recorded a span or left no span context to propagate")
	}

	tracer, sink = newTestTracer(0.5)
	const n = 2000
	for range n {
		_, span := tracer.StartSpan(context.Background(), "maybe")
		span.End()
	}
	if got := len(sink.ended()); got < n/4 || got > 3*n/4 {
		t.Errorf("ratio 0.5 recorded %d of %d traces", got, n)
	}
}

func TestSampleTraceUsesTheLowerHalfOfTheTraceID(t *testing.T) {
	var low, high TraceID
	high[8] = 0xff
	if !sampleTrace(low, 0.5) || sampleTrace(high, 0.5) {
		t.Error("sampleTrace does not compare the lower half of the ID with the ratio")
	}
}

func TestSpanOptionsSetKindAndLinks(t *testing.T) {
	tracer, sink := newTestTracer(1)
	linked := pkgobserver.SpanContext{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7"}
	_, span := pkgobserver.StartSpanWithOptions(context.Background(), tracer, "consume", pkgobserver.StartOptions{
		Kind: pkgobserver.SpanKindConsumer,
		Links: []pkgobserver.Link{
			{SpanContext: linked, Attrs: []pkgobserver.Attr{{Key: "message", Value: 1}}},
			{SpanContext: pkgobserver.SpanContext{TraceID: "invalid"}},
		},
	})
	span.End()
	data := sink.ended()[0]
	if data.Kind != pkgobserver.SpanKindConsumer {
		t.Errorf("kind = %v, want consumer", data.Kind)
	}
	if len(data.Links) != 1 || data.Links[0].TraceID.String() != string(linked.TraceID) ||
		data.Links[0].SpanID.String() != string(linked.SpanID) || len(data.Links[0].Attrs) != 1 {
		t.Errorf("links = %+v, want only the valid link with its attribute", data.Links)
	}
}

func TestStatusFollowsTheObserverRules(t *testing.T) {
	tracer, sink := newTestTracer(1)

	_, span := tracer.StartSpan(context.Background(), "failed")
	span.RecordError(errors.New("boom"), pkgobserver.Attr{Key: "attempt", Value: 2})
	span.SetStatus(pkgobserver.StatusUnset, "")
	span.End()

	_, span = tracer.StartSpan(context.Background(), "ok")
	span.SetStatus(pkgobserver.StatusOK, "ignored")
	span.SetStatus(pkgobserver.StatusError, "too late")
	span.RecordError(errors.New("after ok"))
	span.End()

	ended := sink.ended()
	failed, ok := ended[0], ended[1]
	if failed.Status != pkgobserver.StatusError || failed.StatusMessage != "boom" {
		t.Errorf("failed status = %v %q, want error boom", failed.Status, failed.StatusMessage)
	}
	if len(failed.Events) != 1 || failed.Events[0].Name != "exception" || len(failed.Events[0].Attrs) != 3 ||
		failed.Events[0].Attrs[1] != (pkgobserver.Attr{Key: "exception.message", Value: "boom"}) {
		t.Errorf("failed events = %+v, want one exception event with the message and the attribute", failed.Events)
	}
	if ok.Status != pkgobserver.StatusOK || ok.StatusMessage != "" || len(ok.Events) != 1 {
		t.Errorf("ok status = %v %q with %d events, want a final OK without description and the exception event",
			ok.Status, ok.StatusMessage, len(ok.Events))
	}
}

func TestEndedSpanIgnoresLaterCalls(t *testing.T) {
	tracer, sink := newTestTracer(1)
	_, span := tracer.StartSpan(context.Background(), "once")
	span.End()
	span.End()
	span.SetAttributes(pkgobserver.Attr{Key: "late", Value: true})
	span.AddEvent("late")
	span.SetStatus(pkgobserver.StatusError, "late")

	ended := sink.ended()
	if len(ended) != 1 || len(ended[0].Attrs) != 0 || len(ended[0].Events) != 0 || ended[0].Status != pkgobserver.StatusUnset {
		t.Errorf("ended = %+v, want one span without the later changes", ended)
	}
}

func TestSpanLimitsCountDroppedItems(t *testing.T) {
	tracer, sink := newTestTracer(1)
	_, span := tracer.StartSpan(context.Background(), "limited")
	for i := range maxAttrsPerSpan + 2 {
		span.SetAttributes(pkgobserver.Attr{Key: string(rune('a'+i%26)) + string(rune('a'+i/26)), Value: i})
	}
	// 同じkeyは、上限を超えていても値を置き換えます。
	span.SetAttributes(pkgobserver.Attr{Key: "aa", Value: "replaced"})
	for range maxEventsPerSpan + 3 {
		span.AddEvent("e")
	}
	span.End()

	data := sink.ended()[0]
	if len(data.Attrs) != maxAttrsPerSpan || data.DroppedAttrs != 2 || data.Attrs[0].Value != "replaced" {
		t.Errorf("attrs = %d (first %v), dropped %d; want %d, the replaced value and 2 dropped",
			len(data.Attrs), data.Attrs[0], data.DroppedAttrs, maxAttrsPerSpan)
	}
	if len(data.Events) != maxEventsPerSpan || data.DroppedEvents != 3 {
		t.Errorf("events = %d, dropped %d; want %d and 3 dropped", len(data.Events), data.DroppedEvents, maxEventsPerSpan)
	}
}

func TestSpanMethodsAreSafeForConcurrentUse(t *testing.T) {
	tracer, sink := newTestTracer(1)
	_, span := tracer.StartSpan(context.Background(), "concurrent")
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			span.SetAttributes(pkgobserver.Attr{Key: "k", Value: i})
			span.AddEvent("e")
			span.RecordError(errors.New("e"))
			span.End()
		})
	}
	wg.Wait()
	if len(sink.ended()) != 1 {
		t.Errorf("ended spans = %d, want 1", len(sink.ended()))
	}
}

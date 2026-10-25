package tracer_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/trace/span"
	"github.com/go-jappe/heterolog/internal/application/trace/tracer"
)

// testExporter observes the public export boundary without racing an async writer.
type testExporter struct {
	spans chan span.SpanInput
}

func (e *testExporter) Export(_ context.Context, spans []span.SpanInput) error {
	for _, record := range spans {
		e.spans <- record
	}
	return nil
}

func (e *testExporter) Shutdown(context.Context) error { return nil }

func recordingTracer() (tracer.Tracer, *testExporter) {
	exporter := &testExporter{spans: make(chan span.SpanInput, 8)}
	return tracer.NewTracer(tracer.WithExporter(exporter)), exporter
}

func awaitExportedSpan(t *testing.T, exporter *testExporter) span.SpanInput {
	t.Helper()
	select {
	case record := <-exporter.spans:
		return record
	case <-time.After(5 * time.Second):
		t.Fatal("ended span was not exported")
		return span.SpanInput{}
	}
}

func TestTracer_StartSpan(t *testing.T) {
	tests := []struct {
		name     string
		spanName string
		opts     []tracer.SpanOption
		wantKind span.SpanKind
	}{
		{
			name:     "デフォルトのSpan作成",
			spanName: "test-span",
			wantKind: span.KindInternal,
		},
		{
			name:     "ServerタイプのSpan作成",
			spanName: "server-span",
			opts: []tracer.SpanOption{
				tracer.WithSpanKind(span.KindServer),
			},
			wantKind: span.KindServer,
		},
		{
			name:     "属性付きSpan作成",
			spanName: "span-with-attrs",
			opts: []tracer.SpanOption{
				tracer.WithAttributes(
					span.Attribute{Key: "http.method", Value: "GET"},
					span.Attribute{Key: "http.url", Value: "/api/users"},
				),
			},
			wantKind: span.KindInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, exporter := recordingTracer()
			ctx := context.Background()

			ctx, sp := tr.StartSpan(ctx, tt.spanName, tt.opts...)
			if sp == nil {
				t.Fatal("Spanがnilです")
			}

			// Spanが記録中であることを確認
			if !sp.IsRecording() {
				t.Error("Spanが記録中ではありません")
			}

			// コンテキストからSpanを取得できることを確認
			extractedSpan := tr.Extract(ctx)
			if extractedSpan == nil {
				t.Error("コンテキストからSpanを取得できません")
			}

			sp.End()
			exported := awaitExportedSpan(t, exporter)
			if exported.Kind != tt.wantKind || exported.Name != tt.spanName {
				t.Errorf("exported span = %q kind %v, want %q kind %v", exported.Name, exported.Kind, tt.spanName, tt.wantKind)
			}

			// 終了後は記録中でないことを確認
			if sp.IsRecording() {
				t.Error("終了後もSpanが記録中です")
			}
		})
	}
}

func TestTracer_NestedSpans(t *testing.T) {
	tr := tracer.NewTracer()
	ctx := context.Background()

	// 親Spanを作成
	ctx, parentSpan := tr.StartSpan(ctx, "parent-span")
	parentCtx := parentSpan.SpanContext()

	// 子Spanを作成
	ctx, childSpan := tr.StartSpan(ctx, "child-span")
	childCtx := childSpan.SpanContext()

	// TraceIDが同じであることを確認
	if parentCtx.TraceID != childCtx.TraceID {
		t.Errorf("親子SpanのTraceIDが異なります: parent=%s, child=%s",
			parentCtx.TraceID, childCtx.TraceID)
	}

	// SpanIDが異なることを確認
	if parentCtx.SpanID == childCtx.SpanID {
		t.Error("親子SpanのSpanIDが同じです")
	}

	childSpan.End()
	parentSpan.End()
}

func TestSpan_SetAttributes(t *testing.T) {
	tr, exporter := recordingTracer()
	_, sp := tr.StartSpan(context.Background(), "test-span")
	want := []span.Attribute{{Key: "user.id", Value: "12345"}, {Key: "user.name", Value: "test-user"}}
	sp.SetAttributes(want...)
	sp.End()
	got := awaitExportedSpan(t, exporter)
	if !reflect.DeepEqual(got.Attributes, want) {
		t.Fatalf("exported attributes = %#v, want %#v", got.Attributes, want)
	}
}

func TestSpan_AddEvent(t *testing.T) {
	tr, exporter := recordingTracer()
	_, sp := tr.StartSpan(context.Background(), "test-span")
	queryAttrs := []span.Attribute{{Key: "db.statement", Value: "SELECT * FROM users"}, {Key: "db.type", Value: "sql"}}
	cacheAttrs := []span.Attribute{{Key: "cache.key", Value: "user:12345"}}
	sp.AddEvent("database.query", queryAttrs...)
	sp.AddEvent("cache.hit", cacheAttrs...)
	sp.End()
	got := awaitExportedSpan(t, exporter)
	if len(got.Events) != 2 {
		t.Fatalf("exported events = %#v, want two ordered events", got.Events)
	}
	for i, want := range []struct {
		name  string
		attrs []span.Attribute
	}{
		{"database.query", queryAttrs}, {"cache.hit", cacheAttrs},
	} {
		event := got.Events[i]
		if event.Name != want.name || !reflect.DeepEqual(event.Attrs, want.attrs) {
			t.Errorf("event %d = %#v, want %s with %#v", i, event, want.name, want.attrs)
		}
		if event.Time.Before(got.StartTime) || event.Time.After(got.EndTime) {
			t.Errorf("event %d occurred outside the span lifetime: %v", i, event.Time)
		}
	}
}

type recordedError struct{}

func (*recordedError) Error() string { return "something went wrong" }

func TestSpan_RecordError(t *testing.T) {
	tr, exporter := recordingTracer()
	_, sp := tr.StartSpan(context.Background(), "test-span")
	sp.RecordError(nil)
	sp.RecordError(&recordedError{})
	sp.End()
	got := awaitExportedSpan(t, exporter)
	wantStatus := span.SpanStatus{Code: span.StatusError, Message: "something went wrong"}
	if got.Status != wantStatus {
		t.Errorf("exported status = %+v, want %+v", got.Status, wantStatus)
	}
	if len(got.Events) != 1 || got.Events[0].Name != "error" {
		t.Fatalf("exported error events = %#v, want exactly one", got.Events)
	}
	wantAttrs := []span.Attribute{{Key: "error.type", Value: "*tracer_test.recordedError"}, {Key: "error.message", Value: "something went wrong"}}
	if !reflect.DeepEqual(got.Events[0].Attrs, wantAttrs) {
		t.Errorf("error attributes = %#v, want %#v", got.Events[0].Attrs, wantAttrs)
	}
}

func TestSpan_SetStatus(t *testing.T) {
	tr, exporter := recordingTracer()
	_, sp := tr.StartSpan(context.Background(), "test-span")
	sp.SetStatus(span.StatusError, "operation failed")
	sp.End()
	got := awaitExportedSpan(t, exporter)
	want := span.SpanStatus{Code: span.StatusError, Message: "operation failed"}
	if got.Status != want {
		t.Fatalf("exported status = %+v, want %+v", got.Status, want)
	}
}

func TestSampler_AlwaysSample(t *testing.T) {
	sampler := tracer.AlwaysSample()
	tr := tracer.NewTracer(tracer.WithSampler(sampler))
	ctx := context.Background()

	ctx, sp := tr.StartSpan(ctx, "test-span")
	spanCtx := sp.SpanContext()

	// サンプリングされていることを確認
	if spanCtx.TraceFlags&1 != 1 {
		t.Error("Spanがサンプリングされていません")
	}

	sp.End()
}

func TestSampler_NeverSample(t *testing.T) {
	sampler := tracer.NeverSample()
	tr := tracer.NewTracer(tracer.WithSampler(sampler))
	ctx := context.Background()

	ctx, sp := tr.StartSpan(ctx, "test-span")
	spanCtx := sp.SpanContext()

	// サンプリングされていないことを確認
	if spanCtx.TraceFlags&1 != 0 {
		t.Error("Spanがサンプリングされています")
	}

	sp.End()
}

func TestSampler_ProbabilitySample(t *testing.T) {
	tests := []struct {
		name        string
		probability float64
		wantSampled bool
	}{
		{
			name:        "確率0%",
			probability: 0.0,
			wantSampled: false,
		},
		{
			name:        "確率100%",
			probability: 1.0,
			wantSampled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sampler := tracer.ProbabilitySample(tt.probability)
			tr := tracer.NewTracer(tracer.WithSampler(sampler))
			ctx := context.Background()

			ctx, sp := tr.StartSpan(ctx, "test-span")
			spanCtx := sp.SpanContext()

			sampled := spanCtx.TraceFlags&1 == 1
			if sampled != tt.wantSampled {
				t.Errorf("サンプリング結果が期待と異なります: got=%v, want=%v",
					sampled, tt.wantSampled)
			}

			sp.End()
		})
	}
}

func TestExporter_Integration(t *testing.T) {
	tr, exporter := recordingTracer()
	_, sp := tr.StartSpan(context.Background(), "test-span", tracer.WithAttributes(span.Attribute{Key: "test", Value: true}))
	sp.AddEvent("test-event")
	sp.SetStatus(span.StatusOK, "success")
	sp.End()
	got := awaitExportedSpan(t, exporter)
	if got.Name != "test-span" || got.Kind != span.KindInternal || !got.Sampled {
		t.Fatalf("exported span identity = %+v", got)
	}
	if !reflect.DeepEqual(got.Attributes, []span.Attribute{{Key: "test", Value: true}}) {
		t.Errorf("exported attributes = %#v", got.Attributes)
	}
	if len(got.Events) != 1 || got.Events[0].Name != "test-event" {
		t.Errorf("exported events = %#v", got.Events)
	}
	if got.Status != (span.SpanStatus{Code: span.StatusOK, Message: "success"}) {
		t.Errorf("exported status = %+v", got.Status)
	}
}

func TestParentBasedSampler(t *testing.T) {
	// 親がサンプリングされている場合
	t.Run("親がサンプリングされている", func(t *testing.T) {
		rootSampler := tracer.NeverSample() // ルートは通常サンプリングしない
		sampler := tracer.ParentBasedSample(rootSampler)
		tr := tracer.NewTracer(tracer.WithSampler(sampler))

		// 親Spanを強制的にサンプリング
		parentTr := tracer.NewTracer(tracer.WithSampler(tracer.AlwaysSample()))
		ctx := context.Background()
		ctx, parentSpan := parentTr.StartSpan(ctx, "parent")

		// 子Spanを作成
		ctx, childSpan := tr.StartSpan(ctx, "child")
		childCtx := childSpan.SpanContext()

		// 子もサンプリングされていることを確認
		if childCtx.TraceFlags&1 != 1 {
			t.Error("親がサンプリングされているのに子がサンプリングされていません")
		}

		childSpan.End()
		parentSpan.End()
	})

	// 親がサンプリングされていない場合
	t.Run("親がサンプリングされていない", func(t *testing.T) {
		rootSampler := tracer.AlwaysSample() // ルートは通常サンプリングする
		sampler := tracer.ParentBasedSample(rootSampler)
		tr := tracer.NewTracer(tracer.WithSampler(sampler))

		// 親Spanを強制的にサンプリングしない
		parentTr := tracer.NewTracer(tracer.WithSampler(tracer.NeverSample()))
		ctx := context.Background()
		ctx, parentSpan := parentTr.StartSpan(ctx, "parent")

		// 子Spanを作成
		ctx, childSpan := tr.StartSpan(ctx, "child")
		childCtx := childSpan.SpanContext()

		// 子もサンプリングされていないことを確認
		if childCtx.TraceFlags&1 != 0 {
			t.Error("親がサンプリングされていないのに子がサンプリングされています")
		}

		childSpan.End()
		parentSpan.End()
	})
}

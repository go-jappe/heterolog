package traces

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

// fakeExporterは、送ったbatchを記録します。blockを閉じるまでExportを止められます。
type fakeExporter struct {
	name  string
	err   error
	panic bool
	block chan struct{}

	mu      sync.Mutex
	batches [][]string
	closed  int
}

func (e *fakeExporter) Name() string { return e.name }

func (e *fakeExporter) Export(_ context.Context, spans []tracing.SpanData) error {
	if e.block != nil {
		<-e.block
	}
	if e.panic {
		panic("exporter bug")
	}
	names := make([]string, len(spans))
	for i, span := range spans {
		names[i] = span.Name
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.batches = append(e.batches, names)
	return e.err
}

func (e *fakeExporter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed++
	return nil
}

func (e *fakeExporter) snapshot() ([][]string, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([][]string(nil), e.batches...), e.closed
}

func spanNamed(name string) tracing.SpanData { return tracing.SpanData{Name: name} }

func spanCount(meter *observertest.Meter, exporter, outcome string) int64 {
	return meter.Count(observertest.Key(telemetry.TraceSpans,
		telemetry.AttrExporter, exporter, telemetry.AttrOutcome, outcome))
}

func TestPipelineSendsFullBatchesAndTheRestAtShutdown(t *testing.T) {
	meter := observertest.NewMeter()
	exporter := &fakeExporter{name: "a"}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 2, ExportInterval: time.Hour}, []Exporter{exporter}, meter)
	for _, name := range []string{"1", "2", "3"} {
		p.Enqueue(spanNamed(name))
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	batches, closed := exporter.snapshot()
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 1 || batches[1][0] != "3" || closed != 1 {
		t.Errorf("batches = %v, closed %d; want [1 2] [3] and one Close", batches, closed)
	}
	if got := spanCount(meter, "a", telemetry.OutcomeSuccess); got != 3 {
		t.Errorf("success spans = %d, want 3", got)
	}

	p.Enqueue(spanNamed("late"))
	if got := spanCount(meter, "a", telemetry.OutcomeDropped); got != 1 {
		t.Errorf("dropped spans after shutdown = %d, want 1", got)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("second Shutdown = %v", err)
	}
}

func TestPipelineSendsAfterTheInterval(t *testing.T) {
	exporter := &fakeExporter{name: "a"}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 10, ExportInterval: 10 * time.Millisecond}, []Exporter{exporter}, nil)
	defer p.Shutdown(context.Background())
	p.Enqueue(spanNamed("1"))
	deadline := time.Now().Add(5 * time.Second)
	for {
		if batches, _ := exporter.snapshot(); len(batches) == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the span was not sent after the interval")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPipelineDropsWhenTheQueueIsFullWithoutBlockingOtherExporters(t *testing.T) {
	meter := observertest.NewMeter()
	slow := &fakeExporter{name: "slow", block: make(chan struct{})}
	fast := &fakeExporter{name: "fast"}
	p := NewPipeline(Config{QueueSize: 2, BatchSize: 1, ExportInterval: time.Hour}, []Exporter{slow, fast}, meter)

	// slowは1件目を送る途中で止まるため、待ち行列に入るのは次の2件までです。fastの待ち行列も
	// 2件のため、fastが受け取るのを待ってから次を入れます。
	p.Enqueue(spanNamed("first"))
	waitFor(t, func() bool { return len(slow.queue(p)) == 0 })
	for i, name := range []string{"2", "3", "4", "5"} {
		p.Enqueue(spanNamed(name))
		waitFor(t, func() bool { batches, _ := fast.snapshot(); return len(batches) == i+2 })
	}
	if got := spanCount(meter, "slow", telemetry.OutcomeDropped); got != 2 {
		t.Errorf("slow dropped = %d, want 2", got)
	}
	if got := spanCount(meter, "fast", telemetry.OutcomeDropped); got != 0 {
		t.Errorf("fast dropped = %d, want 0", got)
	}
	close(slow.block)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if batches, _ := slow.snapshot(); len(batches) != 3 {
		t.Errorf("slow batches = %v, want the first span and the two queued spans", batches)
	}
}

// queueは、exporterの待ち行列を返します。
func (e *fakeExporter) queue(p *Pipeline) chan tracing.SpanData {
	for _, w := range p.workers {
		if w.exporter == e {
			return w.queue
		}
	}
	return nil
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPipelineCountsFailuresAndSurvivesPanics(t *testing.T) {
	meter := observertest.NewMeter()
	failing := &fakeExporter{name: "failing", err: errors.New("down")}
	partial := &fakeExporter{name: "partial", err: &PartialFailureError{Failed: 1, Err: errors.New("rejected")}}
	panicking := &fakeExporter{name: "panicking", panic: true}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 2, ExportInterval: time.Hour}, []Exporter{failing, partial, panicking}, meter)
	for _, name := range []string{"1", "2", "3"} {
		p.Enqueue(spanNamed(name))
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		exporter, outcome string
		want              int64
	}{
		{"failing", telemetry.OutcomeFailure, 3},
		{"failing", telemetry.OutcomeSuccess, 0},
		// partialは、2件と1件のbatchそれぞれで1件ずつ受け付けられませんでした。
		{"partial", telemetry.OutcomeFailure, 2},
		{"partial", telemetry.OutcomeSuccess, 1},
		{"panicking", telemetry.OutcomeFailure, 3},
	} {
		if got := spanCount(meter, tc.exporter, tc.outcome); got != tc.want {
			t.Errorf("%s %s spans = %d, want %d", tc.exporter, tc.outcome, got, tc.want)
		}
	}
	if got := meter.HistogramValues(observertest.Key(telemetry.TraceExportDuration,
		telemetry.AttrExporter, "failing", telemetry.AttrOutcome, telemetry.OutcomeFailure,
	)); len(got) != 2 {
		t.Errorf("failure durations = %v, want one per batch", got)
	}
}

func TestShutdownStopsWaitingWhenTheContextEnds(t *testing.T) {
	blocked := &fakeExporter{name: "blocked", block: make(chan struct{})}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 1, ExportInterval: time.Hour}, []Exporter{blocked}, nil)
	p.Enqueue(spanNamed("1"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want the context error", err)
	}
	// 待つのをやめても送信は続き、後のShutdownは同じ終了を待ちます。
	close(blocked.block)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if batches, closed := blocked.snapshot(); len(batches) != 1 || closed != 1 {
		t.Errorf("batches %v, closed %d; want the span sent and the exporter closed", batches, closed)
	}
}

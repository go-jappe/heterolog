package traces

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"slices"
	"strings"
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

// destinationsは、failure storeのない送り先にします。
func destinations(exporters ...Exporter) []Destination {
	out := make([]Destination, len(exporters))
	for i, exporter := range exporters {
		out[i] = Destination{Exporter: exporter}
	}
	return out
}

// errorRecorderは、Pipelineが知らせたerrorを記録します。
type errorRecorder struct {
	mu   sync.Mutex
	errs []string
}

func (r *errorRecorder) report(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err.Error())
}

func (r *errorRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.errs)
}

func spanCount(meter *observertest.Meter, exporter, outcome string) int64 {
	return meter.Count(observertest.Key(telemetry.TraceSpans,
		telemetry.AttrExporter, exporter, telemetry.AttrOutcome, outcome))
}

func TestPipelineSendsFullBatchesAndTheRestAtShutdown(t *testing.T) {
	meter := observertest.NewMeter()
	exporter := &fakeExporter{name: "a"}
	errs := &errorRecorder{}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 2, ExportInterval: time.Hour}, destinations(exporter), meter, errs.report)
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
	p.Enqueue(spanNamed("later"))
	if got := spanCount(meter, "a", telemetry.OutcomeDropped); got != 2 {
		t.Errorf("dropped spans after shutdown = %d, want 2", got)
	}
	// Shutdownの後に捨てたspanは、最初の1回だけ知らせます。
	if got := errs.snapshot(); len(got) != 1 || !strings.Contains(got[0], "after the Shutdown") {
		t.Errorf("reported errors = %q, want one about the spans after Shutdown", got)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("second Shutdown = %v", err)
	}
}

func TestPipelineSendsAfterTheInterval(t *testing.T) {
	exporter := &fakeExporter{name: "a"}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 10, ExportInterval: 10 * time.Millisecond}, destinations(exporter), nil, nil)
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
	errs := &errorRecorder{}
	p := NewPipeline(Config{QueueSize: 2, BatchSize: 1, ExportInterval: time.Hour}, destinations(slow, fast), meter, errs.report)

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
	// 捨てたspanは、1つの知らせにまとめます。
	if got := errs.snapshot(); len(got) != 1 || !strings.Contains(got[0], "dropped 2 spans for the slow trace exporter") {
		t.Errorf("reported errors = %q, want one report of the 2 dropped spans", got)
	}
}

func TestPipelineReportsDroppedSpansAtMostOncePerInterval(t *testing.T) {
	blocked := &fakeExporter{name: "blocked", block: make(chan struct{})}
	errs := &errorRecorder{}
	p := NewPipeline(Config{QueueSize: 1, BatchSize: 1, ExportInterval: time.Hour}, destinations(blocked), nil, errs.report)
	w := p.workers[0]
	// 前に知らせたばかりなら、intervalが過ぎるまで知らせず、終わるときにまとめて知らせます。
	w.lastDropReport = time.Now()
	p.Enqueue(spanNamed("sent"))
	waitFor(t, func() bool { return len(w.queue) == 0 })
	for _, name := range []string{"queued", "dropped 1", "dropped 2"} {
		p.Enqueue(spanNamed(name))
	}
	close(blocked.block)
	waitFor(t, func() bool { batches, _ := blocked.snapshot(); return len(batches) == 2 })
	if got := errs.snapshot(); len(got) != 0 {
		t.Errorf("reported errors before the interval = %q, want none", got)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := errs.snapshot(); len(got) != 1 || !strings.Contains(got[0], "dropped 2 spans") {
		t.Errorf("reported errors at shutdown = %q, want one report of the 2 dropped spans", got)
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
	partial := &fakeExporter{name: "partial", err: &ExportError{Failures: []SpanFailure{
		{Start: 0, End: 1, Disposition: DispositionQuarantine, Rejected: 1, Err: errors.New("rejected")},
	}}}
	panicking := &fakeExporter{name: "panicking", panic: true}
	errs := &errorRecorder{}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 2, ExportInterval: time.Hour}, destinations(failing, partial, panicking), meter, errs.report)
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
		// partialは、2件と1件のbatchそれぞれで1件ずつ届きませんでした。
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
	// 失敗したbatchごとに知らせます。
	got := errs.snapshot()
	slices.Sort(got)
	want := []string{
		"the failing trace exporter could not send 1 spans: down",
		"the failing trace exporter could not send 2 spans: down",
		"the panicking trace exporter could not send 1 spans: trace exporter panicking panicked: exporter bug",
		"the panicking trace exporter could not send 2 spans: trace exporter panicking panicked: exporter bug",
		"the partial trace exporter could not send 1 of 2 spans: rejected",
		"the partial trace exporter could not send 1 spans: rejected",
	}
	if !slices.Equal(got, want) {
		t.Errorf("reported errors =\n%q\nwant\n%q", got, want)
	}
}

func TestPipelineSurvivesPanicsOfTheErrorHandlerAndLogsWithoutOne(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	failing := &fakeExporter{name: "failing", err: errors.New("down")}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 1, ExportInterval: time.Hour}, destinations(failing), nil,
		func(error) { panic("handler bug") })
	p.Enqueue(spanNamed("1"))
	p.Enqueue(spanNamed("2"))
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if batches, _ := failing.snapshot(); len(batches) != 2 {
		t.Errorf("batches = %v, want both spans sent after the panic", batches)
	}
	if got := strings.Count(buf.String(), "the error handler panicked: handler bug"); got != 2 {
		t.Errorf("log = %q, want two lines about the panics", buf.String())
	}

	buf.Reset()
	p = NewPipeline(Config{QueueSize: 10, BatchSize: 1, ExportInterval: time.Hour}, destinations(failing), nil, nil)
	p.Enqueue(spanNamed("3"))
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); !strings.Contains(got, "[INTERNAL][ERROR] telemetry: the failing trace exporter could not send 1 spans: down") {
		t.Errorf("log = %q, want the error in the standard logger", got)
	}
}

func TestShutdownStopsWaitingWhenTheContextEnds(t *testing.T) {
	blocked := &fakeExporter{name: "blocked", block: make(chan struct{})}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 1, ExportInterval: time.Hour}, destinations(blocked), nil, nil)
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

// fakeStoreは、保存したspanの名前と扱いを記録します。errを返すこともできます。
type fakeStore struct {
	err    error
	events *[]string

	mu     sync.Mutex
	stored []string
}

func (s *fakeStore) Store(_ context.Context, batch []tracing.SpanData, failures []SpanFailure) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, failure := range failures {
		for _, span := range batch[failure.Start:failure.End] {
			s.stored = append(s.stored, span.Name+":"+failure.Disposition.Outcome())
		}
	}
	return s.err
}

func (s *fakeStore) Shutdown(context.Context) error {
	*s.events = append(*s.events, "store shutdown")
	return nil
}

// closingExporterは、Closeを呼ばれた順をeventsへ記録します。
type closingExporter struct {
	*fakeExporter
	events *[]string
}

func (e closingExporter) Close() error {
	*e.events = append(*e.events, "exporter close")
	return e.fakeExporter.Close()
}

func TestPipelineStoresFailedSpansAndReportsOnlyThoseNotResent(t *testing.T) {
	var events []string
	exporter := closingExporter{fakeExporter: &fakeExporter{name: "otlp", err: &ExportError{Failures: []SpanFailure{
		{Start: 0, End: 1, Disposition: DispositionRetry, Err: errors.New("unavailable")},
		{Start: 1, End: 2, Disposition: DispositionDead, StatusCode: 400, Err: errors.New("bad request")},
	}}}, events: &events}
	store := &fakeStore{events: &events}
	meter := observertest.NewMeter()
	errs := &errorRecorder{}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 3, ExportInterval: time.Hour},
		[]Destination{{Exporter: exporter, Store: store}}, meter, errs.report)
	for _, name := range []string{"retry", "dead", "sent"} {
		p.Enqueue(spanNamed(name))
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"retry:retryable", "dead:dead"}; !slices.Equal(store.stored, want) {
		t.Errorf("stored = %v, want %v", store.stored, want)
	}
	for _, tc := range []struct {
		outcome string
		want    int64
	}{
		{telemetry.OutcomeSuccess, 1},
		{telemetry.OutcomeRetryable, 1},
		{telemetry.OutcomeDead, 1},
		{telemetry.OutcomeFailure, 0},
	} {
		if got := spanCount(meter, "otlp", tc.outcome); got != tc.want {
			t.Errorf("%s spans = %d, want %d", tc.outcome, got, tc.want)
		}
	}
	// 送り直すspanは届く見込みがあるため知らせず、送り直さないspanだけを知らせます。
	want := []string{"the otlp trace exporter could not send 1 of 3 spans: bad request; they were stored as dead and are not resent"}
	if got := errs.snapshot(); !slices.Equal(got, want) {
		t.Errorf("reported errors = %q, want %q", got, want)
	}
	// failure storeの送り直しは送り先の接続を使うため、storeを止めてから送り先を閉じます。
	if want := []string{"store shutdown", "exporter close"}; !slices.Equal(events, want) {
		t.Errorf("shutdown order = %v, want %v", events, want)
	}
}

func TestPipelineReportsSpansThatCouldNotBeStored(t *testing.T) {
	var events []string
	exporter := &fakeExporter{name: "otlp", err: errors.New("unavailable")}
	store := &fakeStore{err: errors.New("disk full"), events: &events}
	meter := observertest.NewMeter()
	errs := &errorRecorder{}
	p := NewPipeline(Config{QueueSize: 10, BatchSize: 2, ExportInterval: time.Hour},
		[]Destination{{Exporter: exporter, Store: store}}, meter, errs.report)
	p.Enqueue(spanNamed("1"))
	p.Enqueue(spanNamed("2"))
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	// ExportErrorでないerrorは、すべてのspanを送り直す扱いで保存しようとします。
	if want := []string{"1:retryable", "2:retryable"}; !slices.Equal(store.stored, want) {
		t.Errorf("stored = %v, want %v", store.stored, want)
	}
	if got := spanCount(meter, "otlp", telemetry.OutcomeFailure); got != 2 {
		t.Errorf("failure spans = %d, want 2", got)
	}
	want := []string{"the otlp trace exporter could not send 2 spans: unavailable, and they could not be stored: disk full"}
	if got := errs.snapshot(); !slices.Equal(got, want) {
		t.Errorf("reported errors = %q, want %q", got, want)
	}
}

package metrics

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/testutil/observertest"
)

// fakeExporterは、送ったbatchのmetricの名前を記録します。blockを閉じるまでExportを
// 止められます。
type fakeExporter struct {
	name  string
	err   error
	block chan struct{}

	mu      sync.Mutex
	batches [][]string
	closed  int
}

func (e *fakeExporter) Name() string { return e.name }

func (e *fakeExporter) Export(_ context.Context, items []metrics.Metric) error {
	if e.block != nil {
		<-e.block
	}
	names := make([]string, len(items))
	for i, m := range items {
		names[i] = m.Name
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
	return slices.Clone(e.batches), e.closed
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

// metricOfは、points個のdata pointを持つmetricです。
func metricOf(name string, points int) metrics.Metric {
	return metrics.Metric{Name: name, Kind: metrics.KindGauge, Points: make([]metrics.Point, points)}
}

func pointCountOf(meter *observertest.Meter, exporter, outcome string) int64 {
	return meter.Count(observertest.Key(telemetry.MetricPoints, telemetry.AttrExporter, exporter, telemetry.AttrOutcome, outcome))
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

func TestPipelineCollectsEveryIntervalAndOnceMoreAtShutdown(t *testing.T) {
	meter := metrics.NewMeter(metrics.Config{CardinalityLimit: 10}, nil)
	exporter := &fakeExporter{name: "otlp"}
	selfMeter := observertest.NewMeter()
	p := NewPipeline(Config{ExportInterval: 10 * time.Millisecond, QueueSize: 4}, meter,
		[]Destination{{Exporter: exporter}}, selfMeter, nil)
	meter.Counter("requests").Add(context.Background(), 1)
	waitFor(t, func() bool { batches, _ := exporter.snapshot(); return len(batches) > 0 })
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	batches, closed := exporter.snapshot()
	if closed != 1 || len(batches) < 2 || batches[len(batches)-1][0] != "requests" {
		t.Errorf("batches %v, closed %d; want collections on the interval and one at shutdown", batches, closed)
	}
	if got := pointCountOf(selfMeter, "otlp", telemetry.OutcomeSuccess); got != int64(len(batches)) {
		t.Errorf("success points = %d, want one per batch", got)
	}
}

func TestPipelineSendsPushedMetricsAndDropsThemAfterShutdown(t *testing.T) {
	exporter := &fakeExporter{name: "otlp"}
	selfMeter := observertest.NewMeter()
	errs := &errorRecorder{}
	p := NewPipeline(Config{ExportInterval: time.Hour, QueueSize: 4}, nil,
		[]Destination{{Exporter: exporter}}, selfMeter, errs.report)
	p.Push([]metrics.Metric{metricOf("sdk", 2)})
	p.Push(nil)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if batches, _ := exporter.snapshot(); len(batches) != 1 || batches[0][0] != "sdk" {
		t.Errorf("batches = %v, want the pushed batch", batches)
	}
	p.Push([]metrics.Metric{metricOf("late", 3)})
	p.Push([]metrics.Metric{metricOf("later", 1)})
	if got := pointCountOf(selfMeter, "otlp", telemetry.OutcomeDropped); got != 4 {
		t.Errorf("dropped points = %d, want 4", got)
	}
	if got := errs.snapshot(); len(got) != 1 || !strings.Contains(got[0], "after the Shutdown") {
		t.Errorf("reported errors = %q, want one about the metrics after Shutdown", got)
	}
}

func TestPipelineDropsBatchesWhenTheQueueIsFull(t *testing.T) {
	slow := &fakeExporter{name: "slow", block: make(chan struct{})}
	selfMeter := observertest.NewMeter()
	errs := &errorRecorder{}
	p := NewPipeline(Config{ExportInterval: time.Hour, QueueSize: 1}, nil,
		[]Destination{{Exporter: slow}}, selfMeter, errs.report)
	p.Push([]metrics.Metric{metricOf("sending", 1)})
	waitFor(t, func() bool { return len(p.workers[0].queue) == 0 })
	p.Push([]metrics.Metric{metricOf("queued", 1)})
	p.Push([]metrics.Metric{metricOf("dropped", 2)})
	close(slow.block)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := pointCountOf(selfMeter, "slow", telemetry.OutcomeDropped); got != 2 {
		t.Errorf("dropped points = %d, want 2", got)
	}
	if got := errs.snapshot(); len(got) != 1 || !strings.Contains(got[0], "dropped 2 data points for the slow metric exporter") {
		t.Errorf("reported errors = %q, want one report of the dropped points", got)
	}
}

// fakeStoreは、保存したmetricの名前と扱いを記録します。
type fakeStore struct {
	err    error
	stored []string
}

func (s *fakeStore) Store(_ context.Context, batch []metrics.Metric, failures []delivery.Failure) error {
	for _, failure := range failures {
		for _, m := range batch[failure.Start:failure.End] {
			s.stored = append(s.stored, m.Name+":"+failure.Disposition.Outcome())
		}
	}
	return s.err
}

func (s *fakeStore) Shutdown(context.Context) error { return nil }

func TestPipelineStoresFailedMetricsAndCountsTheirPoints(t *testing.T) {
	exporter := &fakeExporter{name: "otlp", err: &delivery.ExportError{Failures: []delivery.Failure{
		{Start: 0, End: 1, Disposition: delivery.DispositionRetry, Err: errors.New("unavailable")},
		{Start: 1, End: 2, Disposition: delivery.DispositionQuarantine, Rejected: 1, Err: errors.New("partial")},
	}}}
	store := &fakeStore{}
	selfMeter := observertest.NewMeter()
	errs := &errorRecorder{}
	p := NewPipeline(Config{ExportInterval: time.Hour, QueueSize: 4}, nil,
		[]Destination{{Exporter: exporter, Store: store}}, selfMeter, errs.report)
	p.Push([]metrics.Metric{metricOf("retry", 2), metricOf("partial", 3), metricOf("sent", 4)})
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"retry:retryable", "partial:quarantine"}; !slices.Equal(store.stored, want) {
		t.Errorf("stored = %v, want %v", store.stored, want)
	}
	// 部分的な成功では、受信側が数えたdata pointだけを届かなかったものとして数えます。
	for outcome, want := range map[string]int64{
		telemetry.OutcomeSuccess: 6, telemetry.OutcomeRetryable: 2, telemetry.OutcomeQuarantine: 1, telemetry.OutcomeFailure: 0,
	} {
		if got := pointCountOf(selfMeter, "otlp", outcome); got != want {
			t.Errorf("%s points = %d, want %d", outcome, got, want)
		}
	}
	want := []string{"the otlp metric exporter could not send 1 of 9 data points: partial; they were stored as quarantine and are not resent"}
	if got := errs.snapshot(); !slices.Equal(got, want) {
		t.Errorf("reported errors = %q, want %q", got, want)
	}
}

func TestPipelineReportsMetricsThatCouldNotBeSent(t *testing.T) {
	exporter := &fakeExporter{name: "file", err: errors.New("disk full")}
	selfMeter := observertest.NewMeter()
	errs := &errorRecorder{}
	p := NewPipeline(Config{ExportInterval: time.Hour, QueueSize: 4}, nil,
		[]Destination{{Exporter: exporter}}, selfMeter, errs.report)
	p.Push([]metrics.Metric{metricOf("a", 2)})
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := pointCountOf(selfMeter, "file", telemetry.OutcomeFailure); got != 2 {
		t.Errorf("failure points = %d, want 2", got)
	}
	want := []string{"the file metric exporter could not send 2 data points: disk full"}
	if got := errs.snapshot(); !slices.Equal(got, want) {
		t.Errorf("reported errors = %q, want %q", got, want)
	}
}

// countingCollectorは、集めるたびに回数を名前に入れたmetricを返します。
type countingCollector struct {
	mu sync.Mutex
	n  int
}

func (c *countingCollector) Collect() []metrics.Metric {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return []metrics.Metric{metricOf("collection-"+strconv.Itoa(c.n), 1)}
}

func TestPipelineWaitsForRoomForTheLastCollection(t *testing.T) {
	slow := &fakeExporter{name: "slow", block: make(chan struct{})}
	collector := &countingCollector{}
	p := NewPipeline(Config{ExportInterval: time.Hour, QueueSize: 1}, collector,
		[]Destination{{Exporter: slow}}, nil, func(error) {})
	// 送っている途中のbatchと、待ち行列のbatchで、待ち行列を一杯にします。
	p.Push([]metrics.Metric{metricOf("sending", 1)})
	waitFor(t, func() bool { return len(p.workers[0].queue) == 0 })
	p.Push([]metrics.Metric{metricOf("queued", 1)})
	done := make(chan error, 1)
	go func() { done <- p.Shutdown(context.Background()) }()
	// 最後に集めた回は、捨てずに待ち行列が空くのを待ちます。
	waitFor(t, func() bool { collector.mu.Lock(); defer collector.mu.Unlock(); return collector.n == 1 })
	close(slow.block)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	batches, _ := slow.snapshot()
	want := [][]string{{"sending"}, {"queued"}, {"collection-1"}}
	if len(batches) != len(want) || batches[2][0] != "collection-1" {
		t.Errorf("batches = %v, want %v", batches, want)
	}
}

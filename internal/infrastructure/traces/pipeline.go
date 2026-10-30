// Package tracesは、NewTelemetryのtraceの配送の仕組みです。終わったspanを送り先
// （exporter）ごとの待ち行列で受け取り、batchにまとめて送ります。送り先ごとに待ち行列と
// goroutineを分け、遅い送り先がほかの送り先を待たせないようにします。
//
// 配送の仕組みは、自分の送信（batchの送信、HTTPの要求、再試行）のspanを作りません。
// 自分のspanを送るためにまたspanを作る繰り返しを避けるためです。様子はmetrics
// （heterolog_trace_*）で知らせます。
package traces

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// Exporterは、spanのbatchを1つの送り先へ送ります。Exportは、同じExporterに対して
// 並行には呼ばれません。
type Exporter interface {
	// Nameは、metricsのexporter属性の値（otlp・file・stdoutなど）です。
	Name() string
	// Exportは、spansを送ります。spansは、Exportが返った後に再利用されるため、
	// 保持しません。一部のspanだけを送れなかった場合は、*PartialFailureErrorを返します。
	Export(ctx context.Context, spans []tracing.SpanData) error
	// Closeは、送り先の資源（fileなど）を手放します。最後のExportの後に1回だけ呼ばれます。
	Close() error
}

// PartialFailureErrorは、batchのうちFailed件のspanを送れなかったことを表します。
// 残りのspanは送れています。
type PartialFailureError struct {
	Failed int
	Err    error
}

func (e *PartialFailureError) Error() string {
	return fmt.Sprintf("%d spans were not accepted: %v", e.Failed, e.Err)
}

func (e *PartialFailureError) Unwrap() error { return e.Err }

// Configは、待ち行列とbatchの設定です。値の範囲は、pkg/telemetryの検証で確かめます。
type Config struct {
	QueueSize      int
	BatchSize      int
	ExportInterval time.Duration
}

// Pipelineは、tracing.SpanSinkの実装です。
type Pipeline struct {
	meter   pkgobserver.Meter
	workers []*worker

	// muは、closedと、待ち行列のchannelを閉じることを守ります。Enqueueは読み取りの
	// lockを持ったまま送るため、閉じたchannelへ送ることはありません。
	mu     sync.RWMutex
	closed bool

	shutdownOnce sync.Once
	done         chan struct{}
	closeErr     error
}

// NewPipelineは、exportersへspanを送るPipelineを作り、送り先ごとのgoroutineを始めます。
// 止めるにはShutdownを呼びます。
func NewPipeline(cfg Config, exporters []Exporter, meter pkgobserver.Meter) *Pipeline {
	meter = observer.EnsureMeter(meter)
	p := &Pipeline{meter: meter, done: make(chan struct{})}
	for _, exporter := range exporters {
		w := &worker{
			exporter: exporter,
			meter:    meter,
			queue:    make(chan tracing.SpanData, cfg.QueueSize),
			batch:    cfg.BatchSize,
			interval: cfg.ExportInterval,
			done:     make(chan struct{}),
		}
		p.workers = append(p.workers, w)
		go w.run()
	}
	return p
}

// Enqueueは、spanを送り先ごとの待ち行列へ入れます。待たずに返り、待ち行列が一杯の
// 送り先と、Shutdownの後のspanは捨てて数えます。
func (p *Pipeline) Enqueue(span tracing.SpanData) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, w := range p.workers {
		if p.closed {
			recordSpans(context.Background(), p.meter, w.exporter.Name(), telemetry.OutcomeDropped, 1)
			continue
		}
		w.enqueue(span)
	}
}

// Shutdownは、新しいspanの受け付けを止め、待ち行列のspanを送り終えるのを待ちます。
// ctxが終わった場合は待つのをやめてctx.Err()を返しますが、送信は続けます。2回目からの
// 呼び出しも、同じ終了を待ちます。送り先を閉じたときのerrorを返します。
func (p *Pipeline) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		for _, w := range p.workers {
			close(w.queue)
		}
		p.mu.Unlock()
		go func() {
			var errs []error
			for _, w := range p.workers {
				<-w.done
				if err := w.exporter.Close(); err != nil {
					errs = append(errs, fmt.Errorf("close %s trace exporter: %w", w.exporter.Name(), err))
				}
			}
			p.closeErr = errors.Join(errs...)
			close(p.done)
		}()
	})
	select {
	case <-p.done:
		return p.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// workerは、1つの送り先の待ち行列とbatchを受け持ちます。
type worker struct {
	exporter Exporter
	meter    pkgobserver.Meter
	queue    chan tracing.SpanData
	batch    int
	interval time.Duration
	done     chan struct{}
}

func (w *worker) enqueue(span tracing.SpanData) {
	select {
	case w.queue <- span:
	default:
		recordSpans(context.Background(), w.meter, w.exporter.Name(), telemetry.OutcomeDropped, 1)
	}
}

// runは、batchが一杯になるか、前に送ってからintervalが過ぎたら、batchを送ります。
// 待ち行列が閉じたら、残りを送って終わります。
func (w *worker) run() {
	defer close(w.done)
	batch := make([]tracing.SpanData, 0, w.batch)
	timer := time.NewTimer(w.interval)
	defer timer.Stop()
	flush := func() {
		if len(batch) > 0 {
			w.export(batch)
			clear(batch)
			batch = batch[:0]
		}
		timer.Reset(w.interval)
	}
	for {
		select {
		case span, ok := <-w.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, span)
			if len(batch) >= w.batch {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

// exportは、batchを送り、結果を数えます。送り先のpanicは失敗として数え、goroutineを
// 止めません。
func (w *worker) export(batch []tracing.SpanData) {
	ctx := context.Background()
	name := w.exporter.Name()
	start := time.Now()
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("trace exporter %s panicked: %v", name, r)
			}
		}()
		return w.exporter.Export(ctx, batch)
	}()
	outcome := telemetry.OutcomeSuccess
	failed := 0
	var partial *PartialFailureError
	switch {
	case err == nil:
	case errors.As(err, &partial):
		failed = min(max(partial.Failed, 0), len(batch))
	default:
		outcome = telemetry.OutcomeFailure
		failed = len(batch)
	}
	w.meter.Histogram(telemetry.TraceExportDuration).Record(ctx, time.Since(start).Seconds(),
		pkgobserver.Attr{Key: telemetry.AttrExporter, Value: name},
		pkgobserver.Attr{Key: telemetry.AttrOutcome, Value: outcome},
	)
	recordSpans(ctx, w.meter, name, telemetry.OutcomeSuccess, len(batch)-failed)
	recordSpans(ctx, w.meter, name, telemetry.OutcomeFailure, failed)
}

func recordSpans(ctx context.Context, meter pkgobserver.Meter, exporter, outcome string, count int) {
	if count <= 0 {
		return
	}
	meter.Counter(telemetry.TraceSpans).Add(ctx, int64(count),
		pkgobserver.Attr{Key: telemetry.AttrExporter, Value: exporter},
		pkgobserver.Attr{Key: telemetry.AttrOutcome, Value: outcome},
	)
}

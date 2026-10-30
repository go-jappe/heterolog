// Package tracesは、NewTelemetryのtraceの配送の仕組みです。終わったspanを送り先
// （exporter）ごとの待ち行列で受け取り、batchにまとめて送ります。送り先ごとに待ち行列と
// goroutineを分け、遅い送り先がほかの送り先を待たせないようにします。送れなかったspanは、
// 送り先にfailure storeがあればそこへ保存し、なければ捨てます。
//
// 配送の仕組みは、自分の送信（batchの送信、HTTPの要求、再試行）のspanを作りません。
// 自分のspanを送るためにまたspanを作る繰り返しを避けるためです。様子はmetrics
// （heterolog_trace_*）で知らせ、送れなかったspanと捨てたspanは、error handlerにも
// 知らせます。
package traces

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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
	// 保持しません。送れなかったspanの範囲と扱いが分かる場合は、*ExportErrorを返します。
	// ほかのerrorは、すべてのspanを送れなかったことを表します。
	Export(ctx context.Context, spans []tracing.SpanData) error
	// Closeは、送り先の資源（fileなど）を手放します。最後のExportの後に1回だけ呼ばれます。
	Close() error
}

// Destinationは、spanの送り先と、そこへ送れなかったspanを保存するfailure storeの組です。
// Storeがnilなら、送れなかったspanは捨てます。
type Destination struct {
	Exporter Exporter
	Store    FailureStore
}

// Configは、待ち行列とbatchの設定です。値の範囲は、pkg/telemetryの検証で確かめます。
type Config struct {
	QueueSize      int
	BatchSize      int
	ExportInterval time.Duration
}

// ErrorHandlerは、配送のerrorを受け取る関数です。複数のgoroutineから並行に呼ばれます。
type ErrorHandler func(error)

// Pipelineは、tracing.SpanSinkの実装です。
type Pipeline struct {
	meter   pkgobserver.Meter
	report  ErrorHandler
	workers []*worker

	// droppedAfterShutdownは、Shutdownの後に終わったspanを捨てたことを、1回だけ知らせます。
	droppedAfterShutdown sync.Once

	// muは、closedと、待ち行列のchannelを閉じることを守ります。Enqueueは読み取りの
	// lockを持ったまま送るため、閉じたchannelへ送ることはありません。
	mu     sync.RWMutex
	closed bool

	shutdownOnce sync.Once
	done         chan struct{}
	closeErr     error
}

// NewPipelineは、destinationsへspanを送るPipelineを作り、送り先ごとのgoroutineを始めます。
// 止めるにはShutdownを呼びます。reportがnilなら、errorを標準のloggerへ1行ずつ書きます。
func NewPipeline(cfg Config, destinations []Destination, meter pkgobserver.Meter, report ErrorHandler) *Pipeline {
	meter = observer.EnsureMeter(meter)
	report = NewErrorHandler(report)
	p := &Pipeline{meter: meter, report: report, done: make(chan struct{})}
	for _, destination := range destinations {
		w := &worker{
			exporter: destination.Exporter,
			store:    destination.Store,
			meter:    meter,
			report:   report,
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
// 送り先と、Shutdownの後のspanは捨てて数えます。待ち行列が一杯で捨てたspanは、送り先の
// goroutineがまとめて知らせます。Shutdownの後に捨てたspanは、最初の1回だけ知らせます。
func (p *Pipeline) Enqueue(span tracing.SpanData) {
	p.mu.RLock()
	closed := p.closed
	for _, w := range p.workers {
		if closed {
			recordSpans(context.Background(), p.meter, w.exporter.Name(), telemetry.OutcomeDropped, 1)
			continue
		}
		w.enqueue(span)
	}
	p.mu.RUnlock()
	if closed && len(p.workers) > 0 {
		p.droppedAfterShutdown.Do(func() {
			p.report(errors.New("dropped spans that ended after the Shutdown of the telemetry started"))
		})
	}
}

// Shutdownは、新しいspanの受け付けを止め、待ち行列のspanを送り終えるのを待ちます。
// 送り終えた送り先は、failure storeを止めてから閉じます。failure storeの送り直しが、
// 送り先の接続を使うためです。ctxが終わった場合は待つのをやめてctx.Err()を返しますが、
// 送信は続けます。2回目からの呼び出しも、同じ終了を待ちます。failure storeを止めたときと
// 送り先を閉じたときのerrorを返します。
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
				if w.store != nil {
					if err := w.store.Shutdown(context.Background()); err != nil {
						errs = append(errs, fmt.Errorf("shut down the failure store of the %s trace exporter: %w", w.exporter.Name(), err))
					}
				}
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
	store    FailureStore
	meter    pkgobserver.Meter
	report   ErrorHandler
	queue    chan tracing.SpanData
	batch    int
	interval time.Duration
	done     chan struct{}

	// droppedは、待ち行列が一杯で捨て、まだ知らせていないspanの数です。
	dropped atomic.Int64
	// lastDropReportは、捨てたspanを最後に知らせた時刻です。runのgoroutineだけが使います。
	lastDropReport time.Time
}

func (w *worker) enqueue(span tracing.SpanData) {
	select {
	case w.queue <- span:
	default:
		w.dropped.Add(1)
		recordSpans(context.Background(), w.meter, w.exporter.Name(), telemetry.OutcomeDropped, 1)
	}
}

// reportDroppedは、待ち行列が一杯で捨てたspanの数を知らせます。知らせが多くなりすぎない
// よう、forceでなければ、前に知らせてからintervalが過ぎるまで知らせません。
func (w *worker) reportDropped(force bool) {
	if w.dropped.Load() == 0 || (!force && time.Since(w.lastDropReport) < w.interval) {
		return
	}
	w.lastDropReport = time.Now()
	n := w.dropped.Swap(0)
	w.report(fmt.Errorf("dropped %d spans for the %s trace exporter because its queue was full", n, w.exporter.Name()))
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
		w.reportDropped(false)
		timer.Reset(w.interval)
	}
	for {
		select {
		case span, ok := <-w.queue:
			if !ok {
				flush()
				w.reportDropped(true)
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

// exportは、batchを送り、結果を数えます。送れなかったspanは、failure storeがあれば
// 保存します。後で送り直さないspanは届かないため、error handlerへ知らせます。送り先の
// panicは失敗として扱い、goroutineを止めません。
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
	var failures []SpanFailure
	failed := 0
	if err != nil {
		failures = ExportFailures(err, len(batch))
		for _, failure := range failures {
			failed += failure.Failed()
		}
	}
	outcome := telemetry.OutcomeSuccess
	if err != nil && failed >= len(batch) {
		outcome = telemetry.OutcomeFailure
	}
	w.meter.Histogram(telemetry.TraceExportDuration).Record(ctx, time.Since(start).Seconds(),
		pkgobserver.Attr{Key: telemetry.AttrExporter, Value: name},
		pkgobserver.Attr{Key: telemetry.AttrOutcome, Value: outcome},
	)
	recordSpans(ctx, w.meter, name, telemetry.OutcomeSuccess, len(batch)-failed)
	if err == nil {
		return
	}
	if w.store == nil {
		recordSpans(ctx, w.meter, name, telemetry.OutcomeFailure, failed)
		w.report(w.sendError(failed, len(batch), err))
		return
	}
	if storeErr := w.store.Store(ctx, batch, failures); storeErr != nil {
		recordSpans(ctx, w.meter, name, telemetry.OutcomeFailure, failed)
		w.report(fmt.Errorf("%w, and they could not be stored: %w", w.sendError(failed, len(batch), err), storeErr))
		return
	}
	for _, failure := range failures {
		recordSpans(ctx, w.meter, name, failure.Disposition.Outcome(), failure.Failed())
		if failure.Disposition != DispositionRetry {
			w.report(fmt.Errorf("%w; they were stored as %s and are not resent",
				w.sendError(failure.Failed(), len(batch), failure.Err), failure.Disposition.Outcome()))
		}
	}
}

// sendErrorは、batchのtotal件のうちfailed件を送れなかったことを表すerrorを返します。
func (w *worker) sendError(failed, total int, err error) error {
	if failed < total {
		return fmt.Errorf("the %s trace exporter could not send %d of %d spans: %w", w.exporter.Name(), failed, total, err)
	}
	return fmt.Errorf("the %s trace exporter could not send %d spans: %w", w.exporter.Name(), failed, err)
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

// NewErrorHandlerは、reportのpanicで配送のgoroutineが止まらないようにします。reportが
// nilなら、errorを標準のloggerへ1行ずつ書きます。panicした場合も、errorを標準のloggerへ
// 書きます。
func NewErrorHandler(report ErrorHandler) ErrorHandler {
	if report == nil {
		return logError
	}
	return func(err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[INTERNAL][ERROR] telemetry: the error handler panicked: %v; error: %v", r, err)
			}
		}()
		report(err)
	}
}

func logError(err error) {
	log.Printf("[INTERNAL][ERROR] telemetry: %v", err)
}

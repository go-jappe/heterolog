// Package metricsは、NewTelemetryのmetricsの配送の仕組みです。heterologのMeterの累積値を
// ExportIntervalごとに集め、OpenTelemetry SDKから届いたmetricsとともに、送り先（exporter）
// ごとの待ち行列へ入れて送ります。送り先ごとに待ち行列とgoroutineを分け、遅い送り先が
// ほかの送り先を待たせないようにします。送れなかったmetricは、送り先にfailure storeが
// あればそこへ保存し、なければ捨てます。
//
// 配送の仕組みは、自分の送信のspanを作りません。様子はmetrics（heterolog_metric_*）で
// 知らせ、送れなかったmetricと捨てたmetricは、error handlerにも知らせます。
package metrics

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// Exporterは、metricのbatchを1つの送り先へ送ります。
type Exporter = delivery.Exporter[metrics.Metric]

// Destinationは、metricの送り先と、そこへ送れなかったmetricを保存するfailure storeの組です。
// Storeがnilなら、送れなかったmetricは捨てます。
type Destination struct {
	Exporter Exporter
	Store    delivery.FailureStore[metrics.Metric]
}

// Collectorは、送るmetricsを集めます。heterologのMeterが実装します。
type Collector interface {
	Collect() []metrics.Metric
}

// Configは、集める間隔と待ち行列の設定です。値の範囲は、pkg/telemetryの検証で確かめます。
type Config struct {
	// ExportIntervalは、Collectorから集める間隔です。
	ExportInterval time.Duration
	// QueueSizeは、送り先ごとに、送るのを待つbatchの数の上限です。
	QueueSize int
}

// Pipelineは、metricsを集めて送り先へ送ります。
type Pipeline struct {
	meter     pkgobserver.Meter
	report    delivery.ErrorHandler
	collector Collector
	interval  time.Duration
	workers   []*worker

	// muは、closedと、待ち行列のchannelを閉じることを守ります。Pushは読み取りのlockを
	// 持ったまま送るため、閉じたchannelへ送ることはありません。
	mu     sync.RWMutex
	closed bool
	// droppedAfterShutdownは、Shutdownの後に届いたmetricsを捨てたことを、1回だけ知らせます。
	droppedAfterShutdown sync.Once

	stopCollecting chan struct{}
	collectorDone  chan struct{}
	shutdownOnce   sync.Once
	done           chan struct{}
	closeErr       error
}

// NewPipelineは、destinationsへmetricsを送るPipelineを作り、collectorから集めるgoroutineと、
// 送り先ごとのgoroutineを始めます。collectorがnilなら、Pushで届いたmetricsだけを送ります。
// 止めるにはShutdownを呼びます。reportがnilなら、errorを標準のloggerへ1行ずつ書きます。
func NewPipeline(cfg Config, collector Collector, destinations []Destination, meter pkgobserver.Meter, report delivery.ErrorHandler) *Pipeline {
	meter = observer.EnsureMeter(meter)
	report = delivery.NewErrorHandler(report)
	p := &Pipeline{
		meter:          meter,
		report:         report,
		collector:      collector,
		interval:       cfg.ExportInterval,
		stopCollecting: make(chan struct{}),
		collectorDone:  make(chan struct{}),
		done:           make(chan struct{}),
	}
	for _, destination := range destinations {
		w := &worker{
			exporter: destination.Exporter,
			store:    destination.Store,
			meter:    meter,
			report:   report,
			interval: cfg.ExportInterval,
			queue:    make(chan []metrics.Metric, cfg.QueueSize),
			done:     make(chan struct{}),
		}
		p.workers = append(p.workers, w)
		go w.run()
	}
	go p.collect()
	return p
}

// collectは、intervalごとにcollectorから集めて送り先へ渡します。止めるよう求められたら、
// 最後にもう一度集めて渡してから終わります。最後の回は、値が累積の最新の合計のため、
// 待ち行列が一杯でも捨てず、空くのを待ちます。送り先のgoroutineは待ち行列を送り続けて
// いるため、待ち行列は空きます。
func (p *Pipeline) collect() {
	defer close(p.collectorDone)
	if p.collector == nil {
		<-p.stopCollecting
		return
	}
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.Push(p.collector.Collect())
		case <-p.stopCollecting:
			p.push(p.collector.Collect(), true)
			return
		}
	}
}

// Pushは、batchを送り先ごとの待ち行列へ入れます。待たずに返り、待ち行列が一杯の送り先と、
// Shutdownの後のbatchは捨てて数えます。待ち行列が一杯で捨てたbatchは、送り先の
// goroutineがまとめて知らせます。Shutdownの後に捨てたbatchは、最初の1回だけ知らせます。
// batchは、Pushが返った後も変更してはいけません。
func (p *Pipeline) Push(batch []metrics.Metric) {
	p.push(batch, false)
}

// pushは、batchを送り先ごとの待ち行列へ入れます。waitなら、待ち行列が空くのを待ちます。
func (p *Pipeline) push(batch []metrics.Metric, wait bool) {
	if len(batch) == 0 {
		return
	}
	points := pointCount(batch)
	p.mu.RLock()
	closed := p.closed
	for _, w := range p.workers {
		if closed {
			recordPoints(context.Background(), p.meter, w.exporter.Name(), telemetry.OutcomeDropped, points)
			continue
		}
		if wait {
			w.queue <- batch
			continue
		}
		w.enqueue(batch, points)
	}
	p.mu.RUnlock()
	if closed && len(p.workers) > 0 {
		p.droppedAfterShutdown.Do(func() {
			p.report(errors.New("dropped metrics that arrived after the Shutdown of the telemetry started"))
		})
	}
}

// Shutdownは、最後にもう一度collectorから集め（この回は待ち行列が一杯でも捨てません）、
// 新しいmetricsの受け付けを止め、待ち行列のmetricsを送り終えるのを待ちます。送り終えた送り先は、failure storeを止めてから閉じます。
// ctxが終わった場合は待つのをやめてctx.Err()を返しますが、送信は続けます。2回目からの
// 呼び出しも、同じ終了を待ちます。failure storeを止めたときと送り先を閉じたときのerrorを
// 返します。
func (p *Pipeline) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.shutdownOnce.Do(func() {
		go func() {
			close(p.stopCollecting)
			<-p.collectorDone
			p.mu.Lock()
			p.closed = true
			for _, w := range p.workers {
				close(w.queue)
			}
			p.mu.Unlock()
			var errs []error
			for _, w := range p.workers {
				<-w.done
				if w.store != nil {
					if err := w.store.Shutdown(context.Background()); err != nil {
						errs = append(errs, fmt.Errorf("shut down the failure store of the %s metric exporter: %w", w.exporter.Name(), err))
					}
				}
				if err := w.exporter.Close(); err != nil {
					errs = append(errs, fmt.Errorf("close %s metric exporter: %w", w.exporter.Name(), err))
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

// workerは、1つの送り先の待ち行列を受け持ちます。
type worker struct {
	exporter Exporter
	store    delivery.FailureStore[metrics.Metric]
	meter    pkgobserver.Meter
	report   delivery.ErrorHandler
	interval time.Duration
	queue    chan []metrics.Metric
	done     chan struct{}

	// droppedは、待ち行列が一杯で捨て、まだ知らせていないdata pointの数です。
	dropped atomic.Int64
	// lastDropReportは、捨てたmetricsを最後に知らせた時刻です。runのgoroutineだけが使います。
	lastDropReport time.Time
}

func (w *worker) enqueue(batch []metrics.Metric, points int) {
	select {
	case w.queue <- batch:
	default:
		w.dropped.Add(int64(points))
		recordPoints(context.Background(), w.meter, w.exporter.Name(), telemetry.OutcomeDropped, points)
	}
}

// runは、待ち行列のbatchを順に送ります。待ち行列が閉じたら、残りを送って終わります。
func (w *worker) run() {
	defer close(w.done)
	for batch := range w.queue {
		w.export(batch)
		w.reportDropped(false)
	}
	w.reportDropped(true)
}

// reportDroppedは、待ち行列が一杯で捨てたdata pointの数を知らせます。知らせが多くなり
// すぎないよう、forceでなければ、前に知らせてからintervalが過ぎるまで知らせません。
func (w *worker) reportDropped(force bool) {
	if w.dropped.Load() == 0 || (!force && time.Since(w.lastDropReport) < w.interval) {
		return
	}
	w.lastDropReport = time.Now()
	n := w.dropped.Swap(0)
	w.report(fmt.Errorf("dropped %d data points for the %s metric exporter because its queue was full", n, w.exporter.Name()))
}

// exportは、batchを送り、結果を数えます。送れなかったmetricは、failure storeがあれば
// 保存します。後で送り直さないmetricは届かないため、error handlerへ知らせます。送り先の
// panicは失敗として扱い、goroutineを止めません。
func (w *worker) export(batch []metrics.Metric) {
	ctx := context.Background()
	name := w.exporter.Name()
	start := time.Now()
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("metric exporter %s panicked: %v", name, r)
			}
		}()
		return w.exporter.Export(ctx, batch)
	}()
	total := pointCount(batch)
	var failures []delivery.Failure
	failed := 0
	if err != nil {
		failures = delivery.ExportFailures(err, len(batch))
		for _, failure := range failures {
			failed += failedPoints(batch, failure)
		}
	}
	outcome := telemetry.OutcomeSuccess
	if err != nil && failed >= total {
		outcome = telemetry.OutcomeFailure
	}
	w.meter.Histogram(telemetry.MetricExportDuration).Record(ctx, time.Since(start).Seconds(),
		pkgobserver.Attr{Key: telemetry.AttrExporter, Value: name},
		pkgobserver.Attr{Key: telemetry.AttrOutcome, Value: outcome},
	)
	recordPoints(ctx, w.meter, name, telemetry.OutcomeSuccess, total-failed)
	if err == nil {
		return
	}
	if w.store == nil {
		recordPoints(ctx, w.meter, name, telemetry.OutcomeFailure, failed)
		w.report(w.sendError(failed, total, err))
		return
	}
	if storeErr := w.store.Store(ctx, batch, failures); storeErr != nil {
		recordPoints(ctx, w.meter, name, telemetry.OutcomeFailure, failed)
		w.report(fmt.Errorf("%w, and they could not be stored: %w", w.sendError(failed, total, err), storeErr))
		return
	}
	for _, failure := range failures {
		points := failedPoints(batch, failure)
		recordPoints(ctx, w.meter, name, failure.Disposition.Outcome(), points)
		if failure.Disposition != delivery.DispositionRetry {
			w.report(fmt.Errorf("%w; they were stored as %s and are not resent",
				w.sendError(points, total, failure.Err), failure.Disposition.Outcome()))
		}
	}
}

// sendErrorは、batchのtotal個のdata pointのうちfailed個を送れなかったことを表すerrorを
// 返します。
func (w *worker) sendError(failed, total int, err error) error {
	if failed < total {
		return fmt.Errorf("the %s metric exporter could not send %d of %d data points: %w", w.exporter.Name(), failed, total, err)
	}
	return fmt.Errorf("the %s metric exporter could not send %d data points: %w", w.exporter.Name(), failed, err)
}

// failedPointsは、失敗の範囲のmetricのうち、届かなかったdata pointの数です。部分的な
// 成功では、受信側が数えたdata pointの数を、範囲のdata pointの数までに抑えます。
func failedPoints(batch []metrics.Metric, failure delivery.Failure) int {
	points := pointCount(batch[failure.Start:failure.End])
	if failure.Rejected > 0 {
		return min(failure.Rejected, points)
	}
	return points
}

func pointCount(batch []metrics.Metric) int {
	n := 0
	for _, metric := range batch {
		n += len(metric.Points)
	}
	return n
}

func recordPoints(ctx context.Context, meter pkgobserver.Meter, exporter, outcome string, count int) {
	if count <= 0 {
		return
	}
	meter.Counter(telemetry.MetricPoints).Add(ctx, int64(count),
		pkgobserver.Attr{Key: telemetry.AttrExporter, Value: exporter},
		pkgobserver.Attr{Key: telemetry.AttrOutcome, Value: outcome},
	)
}

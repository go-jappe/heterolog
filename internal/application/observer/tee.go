package observer

import (
	"context"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// TeeMeterは、記録を二つのMeterの両方へ渡すMeterを返します。loggerとtelemetryが、
// 利用者のMeterとは別に、statusの報告に使う内部のMeterへも同じmetricsを記録するために
// 使います。secondがnilか何もしないMeterなら、firstをそのまま返します。
func TeeMeter(first, second pkgobserver.Meter) pkgobserver.Meter {
	if IsNilInterface(second) {
		return EnsureMeter(first)
	}
	if _, nop := second.(NopMeter); nop {
		return EnsureMeter(first)
	}
	if IsNilInterface(first) {
		return second
	}
	if _, nop := first.(NopMeter); nop {
		return second
	}
	return teeMeter{first: first, second: second}
}

type teeMeter struct {
	first, second pkgobserver.Meter
}

func (m teeMeter) Counter(name string) pkgobserver.Counter {
	return teeCounter{m.first.Counter(name), m.second.Counter(name)}
}

func (m teeMeter) Gauge(name string) pkgobserver.Gauge {
	return teeRecorder{m.first.Gauge(name), m.second.Gauge(name)}
}

func (m teeMeter) Histogram(name string) pkgobserver.Histogram {
	return teeRecorder{m.first.Histogram(name), m.second.Histogram(name)}
}

type teeCounter struct {
	first, second pkgobserver.Counter
}

func (c teeCounter) Add(ctx context.Context, value int64, attrs ...pkgobserver.Attr) {
	c.first.Add(ctx, value, attrs...)
	c.second.Add(ctx, value, attrs...)
}

// recorderは、GaugeとHistogramに共通のRecordです。
type recorder interface {
	Record(ctx context.Context, value float64, attrs ...pkgobserver.Attr)
}

type teeRecorder struct {
	first, second recorder
}

func (r teeRecorder) Record(ctx context.Context, value float64, attrs ...pkgobserver.Attr) {
	r.first.Record(ctx, value, attrs...)
	r.second.Record(ctx, value, attrs...)
}

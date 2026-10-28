// Package observertestは、loggerが記録したmetricsとspanを試験で確かめるための、
// 記録するだけのTracerとMeterを提供します。どちらも複数のgoroutineから使えます。
package observertest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// Meterは、counterの合計と、gaugeとhistogramへ記録した値を、名前と属性の組ごとに保持します。
type Meter struct {
	mu         sync.Mutex
	counters   map[string]int64
	gauges     map[string][]float64
	histograms map[string][]float64
}

// NewMeterは、空のMeterを返します。
func NewMeter() *Meter {
	return &Meter{
		counters:   make(map[string]int64),
		gauges:     make(map[string][]float64),
		histograms: make(map[string][]float64),
	}
}

// Keyは、metricの名前と属性から、記録を引くためのkeyを作ります。属性は名前の順に並べるため、
// 渡す順序は問いません。例: Key("x_total", "backend", "loki")。
func Key(name string, keyValues ...string) string {
	if len(keyValues)%2 != 0 {
		panic("observertest.Key needs key and value pairs")
	}
	attrs := make([]pkgobserver.Attr, 0, len(keyValues)/2)
	for i := 0; i < len(keyValues); i += 2 {
		attrs = append(attrs, pkgobserver.Attr{Key: keyValues[i], Value: keyValues[i+1]})
	}
	return key(name, attrs)
}

func key(name string, attrs []pkgobserver.Attr) string {
	parts := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		parts = append(parts, fmt.Sprintf("%s=%v", attr.Key, attr.Value))
	}
	sort.Strings(parts)
	return name + "{" + strings.Join(parts, ",") + "}"
}

func (m *Meter) Counter(name string) pkgobserver.Counter { return counter{meter: m, name: name} }
func (m *Meter) Gauge(name string) pkgobserver.Gauge {
	return recorder{meter: m, name: name, gauge: true}
}
func (m *Meter) Histogram(name string) pkgobserver.Histogram {
	return recorder{meter: m, name: name}
}

// Countは、Keyで作ったkeyのcounterの合計を返します。
func (m *Meter) Count(key string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[key]
}

// Countersは、記録したすべてのcounterの合計をkeyごとに返します。
func (m *Meter) Counters() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.counters))
	for k, v := range m.counters {
		out[k] = v
	}
	return out
}

// HistogramValuesは、Keyで作ったkeyのhistogramへ記録した値を、記録した順に返します。
func (m *Meter) HistogramValues(key string) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]float64(nil), m.histograms[key]...)
}

// GaugeValuesは、Keyで作ったkeyのgaugeへ記録した値を、記録した順に返します。
func (m *Meter) GaugeValues(key string) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]float64(nil), m.gauges[key]...)
}

type counter struct {
	meter *Meter
	name  string
}

func (c counter) Add(_ context.Context, value int64, attrs ...pkgobserver.Attr) {
	c.meter.mu.Lock()
	defer c.meter.mu.Unlock()
	c.meter.counters[key(c.name, attrs)] += value
}

type recorder struct {
	meter *Meter
	name  string
	gauge bool
}

func (r recorder) Record(_ context.Context, value float64, attrs ...pkgobserver.Attr) {
	r.meter.mu.Lock()
	defer r.meter.mu.Unlock()
	k := key(r.name, attrs)
	if r.gauge {
		r.meter.gauges[k] = append(r.meter.gauges[k], value)
		return
	}
	r.meter.histograms[k] = append(r.meter.histograms[k], value)
}

// Tracerは、始めたspanを始めた順に保持します。親のspanは、contextに入れたspanから決めます。
// constructorがcallerのtracerを包むときと同じく、始めたspanはobserver.SpanFromContextでも
// 取り出せます。
type Tracer struct {
	mu    sync.Mutex
	spans []*Span
}

type spanKey struct{}

func (t *Tracer) StartSpan(ctx context.Context, name string, attrs ...pkgobserver.Attr) (context.Context, pkgobserver.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	parent, _ := ctx.Value(spanKey{}).(*Span)
	sp := &Span{Name: name, Parent: parent, attrs: make(map[string]any)}
	for _, attr := range attrs {
		sp.attrs[attr.Key] = attr.Value
	}
	t.mu.Lock()
	t.spans = append(t.spans, sp)
	t.mu.Unlock()
	return observer.WithSpan(context.WithValue(ctx, spanKey{}, sp), sp), sp
}

// Spansは、始めたspanのうち、名前がnameのものを始めた順に返します。
func (t *Tracer) Spans(name string) []*Span {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []*Span
	for _, sp := range t.spans {
		if sp.Name == name {
			out = append(out, sp)
		}
	}
	return out
}

// Namesは、始めたすべてのspanの名前を始めた順に返します。
func (t *Tracer) Names() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	names := make([]string, 0, len(t.spans))
	for _, sp := range t.spans {
		names = append(names, sp.Name)
	}
	return names
}

// Spanは、記録されたspanです。
type Span struct {
	Name   string
	Parent *Span

	mu     sync.Mutex
	attrs  map[string]any
	events []string
	errors []error
	ends   int
}

func (*Span) TraceID() pkgobserver.TraceID { return "" }

func (sp *Span) End() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.ends++
}

func (sp *Span) AddEvent(name string, attrs ...pkgobserver.Attr) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.events = append(sp.events, name)
}

func (sp *Span) SetAttributes(attrs ...pkgobserver.Attr) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for _, attr := range attrs {
		sp.attrs[attr.Key] = attr.Value
	}
}

func (sp *Span) RecordError(err error, _ ...pkgobserver.Attr) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.errors = append(sp.errors, err)
}

// Attrは、spanの属性keyの値を返します。
func (sp *Span) Attr(key string) (any, bool) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	v, ok := sp.attrs[key]
	return v, ok
}

// Eventsは、spanに足したeventの名前を足した順に返します。
func (sp *Span) Events() []string {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return append([]string(nil), sp.events...)
}

// Errorsは、spanに記録したerrorを記録した順に返します。
func (sp *Span) Errors() []error {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return append([]error(nil), sp.errors...)
}

// Endsは、Endを呼んだ回数を返します。
func (sp *Span) Ends() int {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.ends
}

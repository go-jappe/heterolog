package otel

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-jappe/heterolog/internal/application/observer"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type OtelMeter struct {
	m metric.Meter

	mu       sync.RWMutex
	counters map[string]metric.Int64Counter
	gauges   map[string]metric.Float64Gauge
}

func NewOtelMeter(m metric.Meter) *OtelMeter {
	return &OtelMeter{
		m:        m,
		counters: make(map[string]metric.Int64Counter),
		gauges:   make(map[string]metric.Float64Gauge),
	}
}

type otelCounter struct {
	c metric.Int64Counter
}

func (c *otelCounter) Add(ctx context.Context, value int64, attrs ...pkgobserver.Attr) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || observer.IsNilInterface(c.c) {
		return
	}

	c.c.Add(ctx, value, metric.WithAttributes(toOtelMetricAttrs(attrs)...))
}

func (o *OtelMeter) Counter(name string) pkgobserver.Counter {
	if o == nil || observer.IsNilInterface(o.m) {
		return observer.NopCounter{}
	}

	c, err := o.getOrCreateCounter(name)
	if err != nil {
		return observer.NopCounter{}
	}

	return &otelCounter{c: c}
}

func (o *OtelMeter) getOrCreateCounter(name string) (metric.Int64Counter, error) {
	o.mu.RLock()
	c, ok := o.counters[name]
	o.mu.RUnlock()
	if ok {
		return c, nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.counters == nil {
		o.counters = make(map[string]metric.Int64Counter)
	}

	if c, ok := o.counters[name]; ok {
		return c, nil
	}

	c, err := o.m.Int64Counter(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create int64 counter %q: %w", name, err)
	}

	o.counters[name] = c
	return c, nil
}

type otelGauge struct {
	g metric.Float64Gauge
}

func (g *otelGauge) Record(ctx context.Context, value float64, attrs ...pkgobserver.Attr) {
	if ctx == nil {
		ctx = context.Background()
	}
	if g == nil || observer.IsNilInterface(g.g) {
		return
	}

	g.g.Record(ctx, value, metric.WithAttributes(toOtelMetricAttrs(attrs)...))
}

func (o *OtelMeter) Gauge(name string) pkgobserver.Gauge {
	if o == nil || observer.IsNilInterface(o.m) {
		return observer.NopGauge{}
	}

	g, err := o.getOrCreateGauge(name)
	if err != nil {
		return observer.NopGauge{}
	}

	return &otelGauge{g: g}
}

func (o *OtelMeter) getOrCreateGauge(name string) (metric.Float64Gauge, error) {
	o.mu.RLock()
	g, ok := o.gauges[name]
	o.mu.RUnlock()
	if ok {
		return g, nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	if o.gauges == nil {
		o.gauges = make(map[string]metric.Float64Gauge)
	}

	if g, ok := o.gauges[name]; ok {
		return g, nil
	}

	g, err := o.m.Float64Gauge(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create float64 gauge %q: %w", name, err)
	}

	o.gauges[name] = g
	return g, nil
}

func toOtelMetricAttrs(attrs []pkgobserver.Attr) []attribute.KeyValue {
	otelAttrs := make([]attribute.KeyValue, 0, len(attrs))

	for _, a := range attrs {
		switch v := a.Value.(type) {
		case string:
			otelAttrs = append(otelAttrs, attribute.String(a.Key, v))
		case bool:
			otelAttrs = append(otelAttrs, attribute.Bool(a.Key, v))
		case int:
			otelAttrs = append(otelAttrs, attribute.Int(a.Key, v))
		case int8:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case int16:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case int32:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case int64:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, v))
		case uint:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case uint8:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case uint16:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case uint32:
			otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
		case uint64:
			if v <= uint64(^uint64(0)>>1) {
				otelAttrs = append(otelAttrs, attribute.Int64(a.Key, int64(v)))
			} else {
				otelAttrs = append(otelAttrs, attribute.String(a.Key, fmt.Sprint(v)))
			}
		case float32:
			otelAttrs = append(otelAttrs, attribute.Float64(a.Key, float64(v)))
		case float64:
			otelAttrs = append(otelAttrs, attribute.Float64(a.Key, v))
		default:
			otelAttrs = append(otelAttrs, attribute.String(a.Key, fmt.Sprint(v)))
		}
	}

	return otelAttrs
}

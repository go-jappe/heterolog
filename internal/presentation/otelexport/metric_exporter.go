package otelexport

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	"github.com/go-jappe/heterolog/internal/application/telemetryruntime"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ErrMetricExporterShutdownは、Shutdownの後にExportが呼ばれたことを表します。
var ErrMetricExporterShutdown = errors.New("heterolog metric exporter is shut down")

// ErrUnsupportedMetricは、NewTelemetryが送れない種類（exponential histogramとsummary）の
// metricをExportが受け取ったことを表します。ほかのmetricは送ります。
var ErrUnsupportedMetric = errors.New("heterolog telemetry cannot send the metric")

// MetricExporterは、sdkmetric.Exporterの実装です。
type MetricExporter struct {
	sink      telemetryruntime.MetricSink
	shutdown  atomic.Bool
	resources resourceCache
}

// NewMetricExporterは、metricsをsinkへ渡すMetricExporterを返します。
func NewMetricExporter(sink telemetryruntime.MetricSink) *MetricExporter {
	return &MetricExporter{sink: sink}
}

// Temporalityは、すべての種類で累積値（cumulative）を選びます。送り直したmetricが2回
// 届いても、受信側の合計が変わらないようにするためです。
func (e *MetricExporter) Temporality(sdkmetric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

// Aggregationは、SDKの既定の集計を選びます。
func (e *MetricExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(kind)
}

// Exportは、rmを変換して配送の仕組みの待ち行列へ入れます。送り終わるのを待たずに返ります。
// SDKはExportが返った後にrmを使い回すため、値はすべて写します。送れない種類のmetricは
// 入れずに、ErrUnsupportedMetricを包んだerrorで知らせます。
func (e *MetricExporter) Export(_ context.Context, rm *metricdata.ResourceMetrics) error {
	if e.shutdown.Load() {
		return ErrMetricExporterShutdown
	}
	if rm == nil {
		return nil
	}
	resource := e.resources.convert(rm.Resource)
	var batch []metrics.Metric
	var unsupported []string
	for _, sm := range rm.ScopeMetrics {
		scope := telemetrydata.Scope{Name: sm.Scope.Name, Version: sm.Scope.Version}
		for _, m := range sm.Metrics {
			converted, ok := toMetric(m)
			if !ok {
				unsupported = append(unsupported, m.Name)
				continue
			}
			converted.Scope, converted.Resource = scope, resource
			batch = append(batch, converted)
		}
	}
	e.sink.Push(batch)
	if len(unsupported) > 0 {
		return fmt.Errorf("%w (exponential histograms and summaries are not supported): %s",
			ErrUnsupportedMetric, strings.Join(unsupported, ", "))
	}
	return nil
}

// ForceFlushは、何もしません。Exportは待ち行列へ入れるだけで、送るのはNewTelemetryの
// Telemetryです。
func (e *MetricExporter) ForceFlush(context.Context) error { return nil }

// Shutdownは、以後のExportを断ります。受け取ったmetricsは、NewTelemetryのTelemetryの
// Shutdownまで送り続けます。
func (e *MetricExporter) Shutdown(context.Context) error {
	e.shutdown.Store(true)
	return nil
}

// toMetricは、ResourceとScope以外を変換します。送れない種類なら、falseを返します。
func toMetric(m metricdata.Metrics) (metrics.Metric, bool) {
	out := metrics.Metric{Name: m.Name, Description: m.Description, Unit: m.Unit}
	switch data := m.Data.(type) {
	case metricdata.Gauge[int64]:
		out.Kind, out.Points = metrics.KindGauge, numberPoints(data.DataPoints)
	case metricdata.Gauge[float64]:
		out.Kind, out.Points = metrics.KindGauge, numberPoints(data.DataPoints)
	case metricdata.Sum[int64]:
		out.Kind, out.Monotonic, out.Temporality = metrics.KindSum, data.IsMonotonic, toTemporality(data.Temporality)
		out.Points = numberPoints(data.DataPoints)
	case metricdata.Sum[float64]:
		out.Kind, out.Monotonic, out.Temporality = metrics.KindSum, data.IsMonotonic, toTemporality(data.Temporality)
		out.Points = numberPoints(data.DataPoints)
	case metricdata.Histogram[int64]:
		out.Kind, out.Temporality = metrics.KindHistogram, toTemporality(data.Temporality)
		out.Points = histogramPoints(data.DataPoints)
	case metricdata.Histogram[float64]:
		out.Kind, out.Temporality = metrics.KindHistogram, toTemporality(data.Temporality)
		out.Points = histogramPoints(data.DataPoints)
	default:
		return out, false
	}
	return out, true
}

func toTemporality(t metricdata.Temporality) metrics.Temporality {
	if t == metricdata.DeltaTemporality {
		return metrics.TemporalityDelta
	}
	return metrics.TemporalityCumulative
}

func numberPoints[N int64 | float64](dps []metricdata.DataPoint[N]) []metrics.Point {
	points := make([]metrics.Point, len(dps))
	for i, dp := range dps {
		points[i] = metrics.Point{Attrs: toAttrs(dp.Attributes.ToSlice()), StartTime: dp.StartTime, Time: dp.Time}
		switch v := any(dp.Value).(type) {
		case int64:
			points[i].IsInt, points[i].Int = true, v
		case float64:
			points[i].Float = v
		}
	}
	return points
}

func histogramPoints[N int64 | float64](dps []metricdata.HistogramDataPoint[N]) []metrics.Point {
	points := make([]metrics.Point, len(dps))
	for i, dp := range dps {
		p := metrics.Point{
			Attrs: toAttrs(dp.Attributes.ToSlice()), StartTime: dp.StartTime, Time: dp.Time,
			Count: dp.Count, Sum: float64(dp.Sum),
			Bounds: slices.Clone(dp.Bounds), BucketCounts: slices.Clone(dp.BucketCounts),
		}
		minimum, hasMin := dp.Min.Value()
		maximum, hasMax := dp.Max.Value()
		if hasMin && hasMax {
			p.HasMinMax, p.Min, p.Max = true, float64(minimum), float64(maximum)
		}
		points[i] = p
	}
	return points
}

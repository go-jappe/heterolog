package otelexport

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// recordingMetricSinkは、受け取ったbatchを記録します。
type recordingMetricSink struct {
	batches [][]metrics.Metric
}

func (s *recordingMetricSink) Push(batch []metrics.Metric) { s.batches = append(s.batches, batch) }

func TestMetricExporterConvertsTheSDKData(t *testing.T) {
	start := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	now := start.Add(time.Minute)
	attrs := attribute.NewSet(attribute.String("region", "eu"))
	bounds := []float64{1, 10}
	counts := []uint64{1, 2, 0}
	res := resource.NewSchemaless(attribute.String("service.name", "sdk-service"))
	rm := &metricdata.ResourceMetrics{
		Resource: res,
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope: instrumentation.Scope{Name: "sdk-lib", Version: "1.0"},
			Metrics: []metricdata.Metrics{
				{Name: "orders", Unit: "1", Data: metricdata.Sum[int64]{
					Temporality: metricdata.CumulativeTemporality, IsMonotonic: true,
					DataPoints: []metricdata.DataPoint[int64]{{Attributes: attrs, StartTime: start, Time: now, Value: 3}},
				}},
				{Name: "temperature", Data: metricdata.Gauge[float64]{
					DataPoints: []metricdata.DataPoint[float64]{{Time: now, Value: 21.5}},
				}},
				{Name: "latency", Data: metricdata.Histogram[float64]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[float64]{{
						StartTime: start, Time: now, Count: 3, Sum: 12, Bounds: bounds, BucketCounts: counts,
						Min: metricdata.NewExtrema(0.5), Max: metricdata.NewExtrema(9.0),
					}},
				}},
				{Name: "sizes", Data: metricdata.ExponentialHistogram[float64]{}},
			},
		}},
	}
	sink := &recordingMetricSink{}
	exporter := NewMetricExporter(sink)
	err := exporter.Export(context.Background(), rm)
	// 送れない種類は入れずに、errorで知らせます。
	if !errors.Is(err, ErrUnsupportedMetric) {
		t.Errorf("Export = %v, want ErrUnsupportedMetric for the exponential histogram", err)
	}
	// SDKはExportの後にrmを使い回すため、写した値が変わらないことを確かめます。
	bounds[0], counts[0] = 99, 99
	if len(sink.batches) != 1 || len(sink.batches[0]) != 3 {
		t.Fatalf("batches = %+v, want one batch of three metrics", sink.batches)
	}
	orders, temperature, latency := sink.batches[0][0], sink.batches[0][1], sink.batches[0][2]
	if orders.Kind != metrics.KindSum || !orders.Monotonic || orders.Temporality != metrics.TemporalityCumulative ||
		orders.Unit != "1" || !orders.Points[0].IsInt || orders.Points[0].Int != 3 || !orders.Points[0].StartTime.Equal(start) ||
		orders.Points[0].Attrs[0].Value != "eu" || orders.Scope.Name != "sdk-lib" || orders.Resource.Attrs[0].Value != "sdk-service" {
		t.Errorf("orders = %+v", orders)
	}
	if temperature.Kind != metrics.KindGauge || temperature.Points[0].IsInt || temperature.Points[0].Float != 21.5 {
		t.Errorf("temperature = %+v", temperature)
	}
	if p := latency.Points[0]; latency.Kind != metrics.KindHistogram || p.Count != 3 || p.Sum != 12 || !p.HasMinMax || p.Min != 0.5 ||
		p.Max != 9 || !slices.Equal(p.Bounds, []float64{1, 10}) || !slices.Equal(p.BucketCounts, []uint64{1, 2, 0}) {
		t.Errorf("latency = %+v", latency)
	}
	// 同じResourceのmetricは、同じpointerを持ちます。
	if orders.Resource != latency.Resource {
		t.Error("the metrics do not share the converted Resource")
	}
}

func TestMetricExporterAsksForCumulativeTemporalityAndStopsAfterShutdown(t *testing.T) {
	exporter := NewMetricExporter(&recordingMetricSink{})
	for _, kind := range []sdkmetric.InstrumentKind{
		sdkmetric.InstrumentKindCounter, sdkmetric.InstrumentKindUpDownCounter, sdkmetric.InstrumentKindHistogram,
		sdkmetric.InstrumentKindObservableCounter, sdkmetric.InstrumentKindGauge,
	} {
		if got := exporter.Temporality(kind); got != metricdata.CumulativeTemporality {
			t.Errorf("Temporality(%v) = %v, want cumulative", kind, got)
		}
	}
	if err := exporter.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := exporter.Export(context.Background(), &metricdata.ResourceMetrics{}); !errors.Is(err, ErrMetricExporterShutdown) {
		t.Errorf("Export after Shutdown = %v, want ErrMetricExporterShutdown", err)
	}
}

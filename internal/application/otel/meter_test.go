package otel

import (
	"context"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// histogramConfigMeterは、作成したhistogramの設定を名前ごとに記録します。
type histogramConfigMeter struct {
	noop.Meter
	configs map[string]metric.Float64HistogramConfig
}

func (m *histogramConfigMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	m.configs[name] = metric.NewFloat64HistogramConfig(opts...)
	return m.Meter.Float64Histogram(name, opts...)
}

func TestHistogramUsesSecondBucketsForSecondsNames(t *testing.T) {
	otelMeter := &histogramConfigMeter{configs: make(map[string]metric.Float64HistogramConfig)}
	meter := NewOtelMeter(otelMeter)

	meter.Histogram("heterolog_test_duration_seconds").Record(context.Background(), 0.2)
	meter.Histogram("heterolog_test_records").Record(context.Background(), 3)

	seconds := otelMeter.configs["heterolog_test_duration_seconds"]
	if seconds.Unit() != "s" {
		t.Errorf("seconds histogram unit = %q, want s", seconds.Unit())
	}
	if !slices.Equal(seconds.ExplicitBucketBoundaries(), secondsBucketBoundaries) {
		t.Errorf("seconds histogram boundaries = %v, want %v", seconds.ExplicitBucketBoundaries(), secondsBucketBoundaries)
	}
	records := otelMeter.configs["heterolog_test_records"]
	if records.Unit() != "" || records.ExplicitBucketBoundaries() != nil {
		t.Errorf("records histogram unit = %q, boundaries = %v, want the SDK defaults", records.Unit(), records.ExplicitBucketBoundaries())
	}
}

func TestHistogramCreatesEachNameOnce(t *testing.T) {
	otelMeter := &countingHistogramMeter{}
	meter := NewOtelMeter(otelMeter)

	for range 3 {
		meter.Histogram("heterolog_test_records").Record(context.Background(), 1)
	}

	if otelMeter.created != 1 {
		t.Errorf("Float64Histogram called %d times, want once", otelMeter.created)
	}
}

// countingHistogramMeterは、histogramを作成した回数を数えます。
type countingHistogramMeter struct {
	noop.Meter
	created int
}

func (m *countingHistogramMeter) Float64Histogram(name string, opts ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	m.created++
	return m.Meter.Float64Histogram(name, opts...)
}

func TestNilMeterReturnsNopHistogram(t *testing.T) {
	var meter *OtelMeter
	meter.Histogram("heterolog_test_records").Record(context.Background(), 1)
	NewOtelMeter(nil).Histogram("heterolog_test_records").Record(context.Background(), 1)
}

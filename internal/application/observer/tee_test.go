package observer

import (
	"context"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestTeeMeterRecordsToBothMeters(t *testing.T) {
	first, second := metrics.NewMeter(metrics.Config{}, nil), metrics.NewMeter(metrics.Config{}, nil)
	meter := TeeMeter(first, second)
	ctx := context.Background()
	attr := pkgobserver.Attr{Key: "backend", Value: "stdout"}
	meter.Counter("records").Add(ctx, 3, attr)
	meter.Gauge("ratio").Record(ctx, 0.5)
	meter.Histogram("seconds").Record(ctx, 1.5, attr)
	for name, m := range map[string]*metrics.Meter{"first": first, "second": second} {
		collected := m.Collect()
		if len(collected) != 3 {
			t.Fatalf("%s meter collected %d metrics, want 3", name, len(collected))
		}
		byName := map[string]metrics.Metric{}
		for _, metric := range collected {
			byName[metric.Name] = metric
		}
		if p := byName["records"].Points; len(p) != 1 || p[0].Int != 3 || len(p[0].Attrs) != 1 {
			t.Errorf("%s records = %+v, want 3 with the backend attribute", name, p)
		}
		if p := byName["seconds"].Points; len(p) != 1 || p[0].Count != 1 || p[0].Sum != 1.5 {
			t.Errorf("%s seconds = %+v, want one value of 1.5", name, p)
		}
	}
}

// 片方がない、または何もしないMeterなら、もう片方をそのまま使います。
func TestTeeMeterSkipsMissingMeters(t *testing.T) {
	meter := metrics.NewMeter(metrics.Config{}, nil)
	if got := TeeMeter(meter, nil); got != pkgobserver.Meter(meter) {
		t.Errorf("TeeMeter(meter, nil) = %T, want the meter itself", got)
	}
	if got := TeeMeter(NopMeter{}, meter); got != pkgobserver.Meter(meter) {
		t.Errorf("TeeMeter(nop, meter) = %T, want the meter itself", got)
	}
	if got := TeeMeter(nil, NopMeter{}); got != pkgobserver.Meter(NopMeter{}) {
		t.Errorf("TeeMeter(nil, nop) = %T, want NopMeter", got)
	}
}

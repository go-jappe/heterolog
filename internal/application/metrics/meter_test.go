package metrics

import (
	"context"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func testMeter(cfg Config) (*Meter, *time.Time) {
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	return NewMeter(cfg, func() time.Time { return now }), &now
}

func TestMeterCollectsCumulativeSumsPerAttributeSet(t *testing.T) {
	m, now := testMeter(Config{})
	start := *now
	ctx := context.Background()
	c := m.Counter("requests")
	c.Add(ctx, 2, pkgobserver.Attr{Key: "code", Value: 200}, pkgobserver.Attr{Key: "method", Value: "GET"})
	// 属性の順と、同じ鍵の重複（後の値を残します）によらず、同じ組です。
	m.Counter("requests").Add(ctx, 3, pkgobserver.Attr{Key: "method", Value: "POST"}, pkgobserver.Attr{Key: "code", Value: 200},
		pkgobserver.Attr{Key: "method", Value: "GET"})
	// 型の違う値は、別の組です。
	c.Add(ctx, 1, pkgobserver.Attr{Key: "code", Value: "200"}, pkgobserver.Attr{Key: "method", Value: "GET"})
	c.Add(ctx, -5, pkgobserver.Attr{Key: "code", Value: 200}, pkgobserver.Attr{Key: "method", Value: "GET"})
	*now = now.Add(time.Minute)

	metrics := m.Collect()
	if len(metrics) != 1 {
		t.Fatalf("metrics = %+v, want one", metrics)
	}
	got := metrics[0]
	if got.Name != "requests" || got.Kind != KindSum || !got.Monotonic || got.Temporality != TemporalityCumulative ||
		got.Scope.Name != InstrumentationScope || len(got.Points) != 2 {
		t.Fatalf("metric = %+v", got)
	}
	first := got.Points[0]
	if !first.IsInt || first.Int != 5 || !first.StartTime.Equal(start) || !first.Time.Equal(*now) ||
		len(first.Attrs) != 2 || first.Attrs[0].Key != "code" || first.Attrs[1].Value != "GET" {
		t.Errorf("first point = %+v, want 5 with the attributes in key order", first)
	}
	if got.Points[1].Int != 1 {
		t.Errorf("second point = %+v, want the string code counted separately", got.Points[1])
	}
	// 値は累積のため、次のCollectでも同じ値に加えます。
	c.Add(ctx, 1, pkgobserver.Attr{Key: "method", Value: "GET"}, pkgobserver.Attr{Key: "code", Value: 200})
	if again := m.Collect(); again[0].Points[0].Int != 6 {
		t.Errorf("collected again = %+v, want the cumulative 6", again[0].Points[0])
	}
}

func TestMeterCollectsGaugesAndHistograms(t *testing.T) {
	m, now := testMeter(Config{HistogramBounds: []float64{0.1, 1}})
	ctx := context.Background()
	m.Gauge("memory").Record(ctx, 0.5)
	*now = now.Add(time.Second)
	recordedAt := *now
	m.Gauge("memory").Record(ctx, 0.75)
	h := m.Histogram("duration")
	for _, v := range []float64{0.05, 0.1, 0.5, 2, math.NaN()} {
		h.Record(ctx, v)
	}
	*now = now.Add(time.Second)

	metrics := m.Collect()
	if len(metrics) != 2 || metrics[0].Kind != KindGauge || metrics[1].Kind != KindHistogram {
		t.Fatalf("metrics = %+v, want the gauge, then the histogram", metrics)
	}
	if p := metrics[0].Points[0]; p.IsInt || p.Float != 0.75 || !p.Time.Equal(recordedAt) || !p.StartTime.IsZero() {
		t.Errorf("gauge point = %+v, want the last value at the time it was recorded", p)
	}
	// 上限の値は、そのbucketに入ります。NaNは数えません。
	p := metrics[1].Points[0]
	if p.Count != 4 || p.Sum != 2.65 || !p.HasMinMax || p.Min != 0.05 || p.Max != 2 ||
		!slices.Equal(p.Bounds, []float64{0.1, 1}) || !slices.Equal(p.BucketCounts, []uint64{2, 1, 1}) {
		t.Errorf("histogram point = %+v", p)
	}
}

func TestMeterMergesAttributeSetsBeyondTheCardinalityLimit(t *testing.T) {
	m, _ := testMeter(Config{CardinalityLimit: 3})
	c := m.Counter("requests")
	for i := range 5 {
		c.Add(context.Background(), 1, pkgobserver.Attr{Key: "id", Value: i})
	}
	points := m.Collect()[0].Points
	// 上限の3組のうち1組は、まとめる組です。
	if len(points) != 3 || points[2].Int != 3 || len(points[2].Attrs) != 1 || points[2].Attrs[0] != OverflowAttr {
		t.Errorf("points = %+v, want two sets and the overflow set with the other 3", points)
	}
}

func TestMeterRecordsConcurrently(t *testing.T) {
	m, _ := testMeter(Config{HistogramBounds: []float64{1}})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				m.Counter("c").Add(context.Background(), 1)
				m.Histogram("h").Record(context.Background(), 0.5)
				_ = m.Collect()
			}
		}()
	}
	wg.Wait()
	metrics := m.Collect()
	if metrics[0].Points[0].Int != 8000 || metrics[1].Points[0].Count != 8000 {
		t.Errorf("metrics = %+v, want 8000 each", metrics)
	}
}

package metrics

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// OverflowAttrは、属性の組の数がConfig.CardinalityLimitに達した後の記録をまとめる、
// OpenTelemetry SDKと同じ属性です。
var OverflowAttr = pkgobserver.Attr{Key: "otel.metric.overflow", Value: true}

// Configは、Meterの集計の設定です。値の範囲は、pkg/telemetryの検証で確かめます。
type Config struct {
	// HistogramBoundsは、Histogramのbucketの上限（その値を含みます）の昇順の並びです。
	HistogramBounds []float64
	// CardinalityLimitは、1つのmetricが持つ属性の組の数の上限です。上限に達した後に
	// 現れた組の記録は、OverflowAttrだけを持つ組にまとめます。まとめる組も上限に数えます。
	CardinalityLimit int
}

// Meterは、pkg/observerのMeterの実装です。記録を名前と属性の組ごとに集計し、Collectで
// 累積値のsnapshotを返します。メソッドは並行に呼べます。
type Meter struct {
	cfg   Config
	now   func() time.Time
	start time.Time

	mu         sync.Mutex
	counters   map[string]*instrument
	gauges     map[string]*instrument
	histograms map[string]*instrument
}

// NewMeterは、Meterを返します。nowは、集計を始めた時刻と、値を集めた時刻に使います。
// nilなら、time.Nowを使います。
func NewMeter(cfg Config, now func() time.Time) *Meter {
	if now == nil {
		now = time.Now
	}
	return &Meter{
		cfg:        cfg,
		now:        now,
		start:      now(),
		counters:   make(map[string]*instrument),
		gauges:     make(map[string]*instrument),
		histograms: make(map[string]*instrument),
	}
}

func (m *Meter) Counter(name string) pkgobserver.Counter {
	return counter{m.instrument(m.counters, name)}
}

func (m *Meter) Gauge(name string) pkgobserver.Gauge {
	return gauge{m.instrument(m.gauges, name)}
}

func (m *Meter) Histogram(name string) pkgobserver.Histogram {
	return histogram{m.instrument(m.histograms, name)}
}

func (m *Meter) instrument(instruments map[string]*instrument, name string) *instrument {
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := instruments[name]
	if !ok {
		in = &instrument{limit: m.cfg.CardinalityLimit, bounds: m.cfg.HistogramBounds, now: m.now, index: make(map[string]*series)}
		instruments[name] = in
	}
	return in
}

// Collectは、すべてのmetricの今の累積値を、名前の順に返します。Counterは整数のSum、
// Gaugeは最後に記録した値、HistogramはMeterの設定のbucketの分布です。一度も記録して
// いないmetricは返しません。
func (m *Meter) Collect() []Metric {
	now := m.now()
	m.mu.Lock()
	counters, gauges, histograms := sortedInstruments(m.counters), sortedInstruments(m.gauges), sortedInstruments(m.histograms)
	m.mu.Unlock()

	var out []Metric
	scope := telemetrydata.Scope{Name: InstrumentationScope}
	for _, named := range counters {
		points := named.in.points(func(s *series) Point {
			return Point{StartTime: m.start, Time: now, IsInt: true, Int: s.sum}
		})
		if len(points) > 0 {
			out = append(out, Metric{Name: named.name, Kind: KindSum, Monotonic: true, Temporality: TemporalityCumulative, Points: points, Scope: scope})
		}
	}
	for _, named := range gauges {
		points := named.in.points(func(s *series) Point {
			return Point{Time: s.recordedAt, Float: s.last}
		})
		if len(points) > 0 {
			out = append(out, Metric{Name: named.name, Kind: KindGauge, Points: points, Scope: scope})
		}
	}
	for _, named := range histograms {
		points := named.in.points(func(s *series) Point {
			return Point{
				StartTime: m.start, Time: now,
				Count: s.count, Sum: s.histogramSum, HasMinMax: s.count > 0, Min: s.min, Max: s.max,
				Bounds: m.cfg.HistogramBounds, BucketCounts: slices.Clone(s.buckets),
			}
		})
		if len(points) > 0 {
			out = append(out, Metric{Name: named.name, Kind: KindHistogram, Temporality: TemporalityCumulative, Points: points, Scope: scope})
		}
	}
	return out
}

type namedInstrument struct {
	name string
	in   *instrument
}

func sortedInstruments(instruments map[string]*instrument) []namedInstrument {
	out := make([]namedInstrument, 0, len(instruments))
	for name, in := range instruments {
		out = append(out, namedInstrument{name: name, in: in})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// instrumentは、1つのmetricの、属性の組ごとの集計です。
type instrument struct {
	limit int
	// boundsは、Histogramのbucketの上限です。
	bounds []float64
	// nowは、Gaugeに記録した時刻です。
	now func() time.Time

	mu    sync.Mutex
	order []*series
	index map[string]*series
}

// seriesは、1つの属性の組の集計です。使うfieldは、metricの種類によります。
type series struct {
	attrs []pkgobserver.Attr

	sum int64

	last       float64
	recordedAt time.Time

	count        uint64
	histogramSum float64
	min, max     float64
	buckets      []uint64
}

// recordは、attrsの組のseriesを、mu.Lockを持ったままupdateへ渡します。
func (in *instrument) record(attrs []pkgobserver.Attr, update func(*series)) {
	attrs, key := normalize(attrs)
	in.mu.Lock()
	defer in.mu.Unlock()
	s, ok := in.index[key]
	if !ok {
		if in.limit > 0 && len(in.order) >= in.limit-1 {
			attrs = []pkgobserver.Attr{OverflowAttr}
			key = overflowKey
			s, ok = in.index[key]
		}
		if !ok {
			s = &series{attrs: attrs}
			in.index[key] = s
			in.order = append(in.order, s)
		}
	}
	update(s)
}

// pointsは、各seriesを、記録した順にPointにします。
func (in *instrument) points(toPoint func(*series) Point) []Point {
	in.mu.Lock()
	defer in.mu.Unlock()
	points := make([]Point, 0, len(in.order))
	for _, s := range in.order {
		p := toPoint(s)
		p.Attrs = s.attrs
		points = append(points, p)
	}
	return points
}

type counter struct{ in *instrument }

// Addは、valueを足します。CounterのSumは減らないため、負の値は無視します。
func (c counter) Add(_ context.Context, value int64, attrs ...pkgobserver.Attr) {
	if value < 0 {
		return
	}
	c.in.record(attrs, func(s *series) { s.sum += value })
}

type gauge struct{ in *instrument }

func (g gauge) Record(_ context.Context, value float64, attrs ...pkgobserver.Attr) {
	now := g.in.now()
	g.in.record(attrs, func(s *series) { s.last, s.recordedAt = value, now })
}

type histogram struct{ in *instrument }

// Recordは、valueを分布に加えます。NaNは、どのbucketにも入らないため無視します。
func (h histogram) Record(_ context.Context, value float64, attrs ...pkgobserver.Attr) {
	if math.IsNaN(value) {
		return
	}
	h.in.record(attrs, func(s *series) {
		if s.buckets == nil {
			s.buckets = make([]uint64, len(h.in.bounds)+1)
		}
		if s.count == 0 || value < s.min {
			s.min = value
		}
		if s.count == 0 || value > s.max {
			s.max = value
		}
		s.count++
		s.histogramSum += value
		s.buckets[sort.SearchFloat64s(h.in.bounds, value)]++
	})
}

// overflowKeyは、OverflowAttrの組の鍵です。normalizeが作る鍵とぶつからないよう、
// 区切りの文字だけにします。
const overflowKey = "\x1e"

// normalizeは、属性の組を鍵の順に並べ、同じ鍵は後の値を残し、組を表す鍵を返します。
// 値は、型と値の両方で区別します。
func normalize(attrs []pkgobserver.Attr) ([]pkgobserver.Attr, string) {
	if len(attrs) == 0 {
		return nil, ""
	}
	sorted := slices.Clone(attrs)
	slices.SortStableFunc(sorted, func(a, b pkgobserver.Attr) int { return strings.Compare(a.Key, b.Key) })
	out := sorted[:0]
	for i, attr := range sorted {
		if i+1 < len(sorted) && sorted[i+1].Key == attr.Key {
			continue
		}
		out = append(out, attr)
	}
	var key strings.Builder
	for _, attr := range out {
		fmt.Fprintf(&key, "%s\x1f%T\x1f%v\x1d", attr.Key, attr.Value, attr.Value)
	}
	return out, key.String()
}

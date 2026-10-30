// Package metricsは、heterologのMeterを実装します。Counter・Gauge・Histogramへの記録を、
// 名前と属性の組ごとにmemoryで集計し、Collectで、送り先へ渡す累積値（cumulative）の
// snapshotにします。集計したmetricsを周期的に集めて送る処理、送り先、再試行、既定値は
// infrastructure層が受け持ちます。OpenTelemetry SDKから届いたmetricsも、presentation層が
// Metricに変えて同じ配送の仕組みへ渡します。
package metrics

import (
	"time"

	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// InstrumentationScopeは、heterologのMeterが記録したmetricsのinstrumentation scopeです。
const InstrumentationScope = "github.com/go-jappe/heterolog"

// Kindは、metricの種類です。
type Kind int

const (
	// KindSumは、合計です。Counterの値です。
	KindSum Kind = iota
	// KindGaugeは、最後に記録した値です。
	KindGauge
	// KindHistogramは、値の分布です。
	KindHistogram
)

// Temporalityは、SumとHistogramの値が、いつからの集計かです。
type Temporality int

const (
	// TemporalityCumulativeは、StartTimeから集計を続けた値です。同じ値を2回送っても、
	// 受信側の合計は変わりません。
	TemporalityCumulative Temporality = iota
	// TemporalityDeltaは、前に送ってからの増分です。OpenTelemetry SDKがこの方式で集計した
	// 場合だけ使います。
	TemporalityDelta
)

// Metricは、1つのmetricと、その属性の組ごとの値（Points）です。
type Metric struct {
	Name        string
	Description string
	Unit        string
	Kind        Kind
	// Monotonicは、Sumの値が減らないことを表します。
	Monotonic bool
	// Temporalityは、SumとHistogramの値の集計の方式です。
	Temporality Temporality
	Points      []Point
	Scope       telemetrydata.Scope
	// Resourceがnilのmetricは、配送の仕組みに設定したResourceで送ります。
	Resource *telemetrydata.Resource
}

// Pointは、1つの属性の組の値です。SumとGaugeはIsInt・Int・Float、Histogramは
// Count以下を使います。
type Point struct {
	Attrs []pkgobserver.Attr
	// StartTimeは、SumとHistogramの集計を始めた時刻です。Gaugeでは使いません。
	StartTime time.Time
	// Timeは、値を集めた時刻です。
	Time time.Time

	// IsIntは、値が整数（Int）であることを表します。falseなら、Floatです。
	IsInt bool
	Int   int64
	Float float64

	Count uint64
	Sum   float64
	// HasMinMaxは、MinとMaxがあることを表します。
	HasMinMax bool
	Min, Max  float64
	// Boundsは、bucketの上限（その値を含みます）の昇順の並びです。BucketCountsは、
	// len(Bounds)+1個で、最後は最大の上限より大きい値の数です。
	Bounds       []float64
	BucketCounts []uint64
}

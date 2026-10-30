package otlp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	oc "github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
)

// MarshalJSONは、metricsをOTLP/JSONのMetricsDataにします。OTLP/JSONの規則に従い、enumは
// 数値、64-bitの整数と時刻は10進数の文字列にします。改行を含まないため、1行に1回分を
// 書けます。
func MarshalJSON(items []metrics.Metric, defaultResource *telemetrydata.Resource) ([]byte, error) {
	data := jsonMetricsData{ResourceMetrics: []jsonResourceMetrics{}}
	for _, group := range groupMetrics(items, defaultResource) {
		rm := jsonResourceMetrics{}
		rm.Resource, rm.SchemaURL = oc.ToJSONResource(group.Resource)
		for _, scope := range group.Scopes {
			sm := jsonScopeMetrics{
				Scope:   oc.JSONScope{Name: scope.Scope.Name, Version: scope.Scope.Version},
				Metrics: make([]jsonMetric, 0, len(scope.Items)),
			}
			for _, metric := range scope.Items {
				sm.Metrics = append(sm.Metrics, toJSONMetric(metric))
			}
			rm.ScopeMetrics = append(rm.ScopeMetrics, sm)
		}
		data.ResourceMetrics = append(data.ResourceMetrics, rm)
	}
	return json.Marshal(data)
}

type jsonMetricsData struct {
	ResourceMetrics []jsonResourceMetrics `json:"resourceMetrics"`
}

type jsonResourceMetrics struct {
	Resource     oc.JSONResource    `json:"resource"`
	ScopeMetrics []jsonScopeMetrics `json:"scopeMetrics"`
	SchemaURL    string             `json:"schemaUrl,omitempty"`
}

type jsonScopeMetrics struct {
	Scope   oc.JSONScope `json:"scope"`
	Metrics []jsonMetric `json:"metrics"`
}

type jsonMetric struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Unit        string         `json:"unit,omitempty"`
	Gauge       *jsonGauge     `json:"gauge,omitempty"`
	Sum         *jsonSum       `json:"sum,omitempty"`
	Histogram   *jsonHistogram `json:"histogram,omitempty"`
}

type jsonGauge struct {
	DataPoints []jsonNumberPoint `json:"dataPoints"`
}

type jsonSum struct {
	DataPoints             []jsonNumberPoint `json:"dataPoints"`
	AggregationTemporality int               `json:"aggregationTemporality"`
	IsMonotonic            bool              `json:"isMonotonic,omitempty"`
}

type jsonHistogram struct {
	DataPoints             []jsonHistogramPoint `json:"dataPoints"`
	AggregationTemporality int                  `json:"aggregationTemporality"`
}

type jsonNumberPoint struct {
	Attributes        []oc.JSONKeyValue `json:"attributes,omitempty"`
	StartTimeUnixNano string            `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      string            `json:"timeUnixNano"`
	AsDouble          json.RawMessage   `json:"asDouble,omitempty"`
	AsInt             *string           `json:"asInt,omitempty"`
}

type jsonHistogramPoint struct {
	Attributes        []oc.JSONKeyValue `json:"attributes,omitempty"`
	StartTimeUnixNano string            `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      string            `json:"timeUnixNano"`
	Count             string            `json:"count"`
	Sum               json.RawMessage   `json:"sum"`
	BucketCounts      []string          `json:"bucketCounts,omitempty"`
	ExplicitBounds    []json.RawMessage `json:"explicitBounds,omitempty"`
	Min               json.RawMessage   `json:"min,omitempty"`
	Max               json.RawMessage   `json:"max,omitempty"`
}

func toJSONMetric(m *metrics.Metric) jsonMetric {
	out := jsonMetric{Name: m.Name, Description: m.Description, Unit: m.Unit}
	switch m.Kind {
	case metrics.KindGauge:
		out.Gauge = &jsonGauge{DataPoints: toJSONNumberPoints(m.Points)}
	case metrics.KindHistogram:
		points := make([]jsonHistogramPoint, len(m.Points))
		for i := range m.Points {
			points[i] = toJSONHistogramPoint(&m.Points[i])
		}
		out.Histogram = &jsonHistogram{DataPoints: points, AggregationTemporality: int(otlpTemporality(m.Temporality))}
	default:
		out.Sum = &jsonSum{
			DataPoints:             toJSONNumberPoints(m.Points),
			AggregationTemporality: int(otlpTemporality(m.Temporality)),
			IsMonotonic:            m.Monotonic,
		}
	}
	return out
}

func toJSONNumberPoints(points []metrics.Point) []jsonNumberPoint {
	out := make([]jsonNumberPoint, len(points))
	for i, p := range points {
		out[i] = jsonNumberPoint{
			Attributes:        oc.ToJSONKeyValues(oc.ToKeyValues(p.Attrs)),
			StartTimeUnixNano: optionalJSONTime(p),
			TimeUnixNano:      oc.JSONTime(p.Time),
		}
		if p.IsInt {
			s := strconv.FormatInt(p.Int, 10)
			out[i].AsInt = &s
		} else {
			out[i].AsDouble = oc.JSONDouble(p.Float)
		}
	}
	return out
}

func toJSONHistogramPoint(p *metrics.Point) jsonHistogramPoint {
	out := jsonHistogramPoint{
		Attributes:        oc.ToJSONKeyValues(oc.ToKeyValues(p.Attrs)),
		StartTimeUnixNano: optionalJSONTime(*p),
		TimeUnixNano:      oc.JSONTime(p.Time),
		Count:             strconv.FormatUint(p.Count, 10),
		Sum:               oc.JSONDouble(p.Sum),
	}
	for _, count := range p.BucketCounts {
		out.BucketCounts = append(out.BucketCounts, strconv.FormatUint(count, 10))
	}
	for _, bound := range p.Bounds {
		out.ExplicitBounds = append(out.ExplicitBounds, oc.JSONDouble(bound))
	}
	if p.HasMinMax {
		out.Min, out.Max = oc.JSONDouble(p.Min), oc.JSONDouble(p.Max)
	}
	return out
}

// optionalJSONTimeは、StartTimeがなければ空にし、JSONから省きます。
func optionalJSONTime(p metrics.Point) string {
	if oc.UnixNano(p.StartTime) == 0 {
		return ""
	}
	return oc.JSONTime(p.StartTime)
}

// ErrInvalidJSONは、UnmarshalJSONが読めないOTLP/JSONを表します。
var ErrInvalidJSON = oc.ErrInvalidJSON

// UnmarshalJSONは、MarshalJSONが書いたOTLP/JSONのMetricsDataを、metricsに戻します。同じ
// ResourceMetricsのmetricは、同じResourceのpointerを持ちます。MarshalJSONが書かない種類
// （exponentialHistogramなど）と、不正な値は、ErrInvalidJSONを包んだerrorにします。
func UnmarshalJSON(data []byte) ([]metrics.Metric, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var parsed jsonMetricsData
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}
	var out []metrics.Metric
	for _, rm := range parsed.ResourceMetrics {
		resource, err := oc.FromJSONResource(rm.Resource, rm.SchemaURL)
		if err != nil {
			return nil, err
		}
		for _, sm := range rm.ScopeMetrics {
			scope := telemetrydata.Scope{Name: sm.Scope.Name, Version: sm.Scope.Version}
			for _, jm := range sm.Metrics {
				m, err := fromJSONMetric(jm)
				if err != nil {
					return nil, fmt.Errorf("metric %q: %w", jm.Name, err)
				}
				m.Scope, m.Resource = scope, resource
				out = append(out, m)
			}
		}
	}
	return out, nil
}

func fromJSONMetric(jm jsonMetric) (metrics.Metric, error) {
	m := metrics.Metric{Name: jm.Name, Description: jm.Description, Unit: jm.Unit}
	var err error
	switch {
	case jm.Sum != nil && jm.Gauge == nil && jm.Histogram == nil:
		m.Kind, m.Monotonic = metrics.KindSum, jm.Sum.IsMonotonic
		m.Temporality, err = fromTemporality(jm.Sum.AggregationTemporality)
		if err == nil {
			m.Points, err = fromJSONNumberPoints(jm.Sum.DataPoints)
		}
	case jm.Gauge != nil && jm.Sum == nil && jm.Histogram == nil:
		m.Kind = metrics.KindGauge
		m.Points, err = fromJSONNumberPoints(jm.Gauge.DataPoints)
	case jm.Histogram != nil && jm.Sum == nil && jm.Gauge == nil:
		m.Kind = metrics.KindHistogram
		m.Temporality, err = fromTemporality(jm.Histogram.AggregationTemporality)
		for _, jp := range jm.Histogram.DataPoints {
			if err != nil {
				break
			}
			var p metrics.Point
			p, err = fromJSONHistogramPoint(jp)
			m.Points = append(m.Points, p)
		}
	default:
		err = fmt.Errorf("%w: the metric must have exactly one of sum, gauge and histogram", ErrInvalidJSON)
	}
	return m, err
}

func fromTemporality(v int) (metrics.Temporality, error) {
	switch v {
	case temporalityCumulative:
		return metrics.TemporalityCumulative, nil
	case temporalityDelta:
		return metrics.TemporalityDelta, nil
	default:
		return 0, fmt.Errorf("%w: aggregationTemporality %d is not delta or cumulative", ErrInvalidJSON, v)
	}
}

func fromJSONNumberPoints(jps []jsonNumberPoint) ([]metrics.Point, error) {
	points := make([]metrics.Point, len(jps))
	for i, jp := range jps {
		p, err := fromJSONPointBase(jp.Attributes, jp.StartTimeUnixNano, jp.TimeUnixNano)
		if err != nil {
			return nil, err
		}
		switch {
		case jp.AsInt != nil && jp.AsDouble == nil:
			p.IsInt = true
			if p.Int, err = strconv.ParseInt(*jp.AsInt, 10, 64); err != nil {
				return nil, fmt.Errorf("%w: asInt %q is not an integer", ErrInvalidJSON, *jp.AsInt)
			}
		case jp.AsDouble != nil && jp.AsInt == nil:
			if p.Float, err = oc.FromJSONDouble(jp.AsDouble); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w: the data point must have exactly one of asInt and asDouble", ErrInvalidJSON)
		}
		points[i] = p
	}
	return points, nil
}

func fromJSONHistogramPoint(jp jsonHistogramPoint) (metrics.Point, error) {
	p, err := fromJSONPointBase(jp.Attributes, jp.StartTimeUnixNano, jp.TimeUnixNano)
	if err != nil {
		return p, err
	}
	var errs []error
	p.Count, err = strconv.ParseUint(jp.Count, 10, 64)
	if err != nil {
		errs = append(errs, fmt.Errorf("%w: count %q is not an unsigned integer", ErrInvalidJSON, jp.Count))
	}
	p.Sum, err = oc.FromJSONDouble(jp.Sum)
	errs = append(errs, err)
	for _, s := range jp.BucketCounts {
		count, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("%w: bucket count %q is not an unsigned integer", ErrInvalidJSON, s))
		}
		p.BucketCounts = append(p.BucketCounts, count)
	}
	for _, raw := range jp.ExplicitBounds {
		bound, err := oc.FromJSONDouble(raw)
		errs = append(errs, err)
		p.Bounds = append(p.Bounds, bound)
	}
	if len(p.BucketCounts) != len(p.Bounds)+1 {
		errs = append(errs, fmt.Errorf("%w: %d bucket counts for %d bounds", ErrInvalidJSON, len(p.BucketCounts), len(p.Bounds)))
	}
	if jp.Min != nil && jp.Max != nil {
		p.HasMinMax = true
		p.Min, err = oc.FromJSONDouble(jp.Min)
		errs = append(errs, err)
		p.Max, err = oc.FromJSONDouble(jp.Max)
		errs = append(errs, err)
	}
	return p, errors.Join(errs...)
}

func fromJSONPointBase(attributes []oc.JSONKeyValue, start, end string) (metrics.Point, error) {
	var p metrics.Point
	var err error
	if p.Attrs, err = oc.FromJSONKeyValues(attributes); err != nil {
		return p, err
	}
	if start != "" {
		if p.StartTime, err = oc.FromJSONTime(start, "startTimeUnixNano"); err != nil {
			return p, err
		}
	}
	p.Time, err = oc.FromJSONTime(end, "timeUnixNano")
	return p, err
}

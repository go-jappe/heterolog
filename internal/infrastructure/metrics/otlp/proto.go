// Package otlpは、metricsをOTLP（OpenTelemetry Protocol）のprotobufとJSONに変換します。
// protobufは、OTLP/HTTPのExportMetricsServiceRequestと同じ形のMetricsDataです。JSONは、
// OTLP/JSONの規則（enumは数値、64-bitの整数は文字列）に従います。属性、Resource、Scopeの
// 変換は、spanと共通のinternal/infrastructure/otlpcommonが受け持ちます。
package otlp

import (
	"math"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	oc "github.com/go-jappe/heterolog/internal/infrastructure/otlpcommon"
	"google.golang.org/protobuf/encoding/protowire"
)

// OTLPのAggregationTemporalityの値です。
const (
	temporalityDelta      = 1
	temporalityCumulative = 2
)

// MarshalProtoは、metricsをprotobufのMetricsData（ExportMetricsServiceRequestと同じ形）に
// します。Resourceがnilのmetricは、defaultResourceで送ります。
func MarshalProto(items []metrics.Metric, defaultResource *telemetrydata.Resource) []byte {
	var b []byte
	for _, group := range groupMetrics(items, defaultResource) {
		// ExportMetricsServiceRequest.resource_metrics = 1
		b = oc.AppendMessage(b, 1, func(b []byte) []byte {
			// ResourceMetrics.resource = 1、scope_metrics = 2、schema_url = 3
			b = oc.AppendResource(b, 1, group.Resource)
			for _, scope := range group.Scopes {
				b = oc.AppendMessage(b, 2, func(b []byte) []byte {
					// ScopeMetrics.scope = 1、metrics = 2
					b = oc.AppendScope(b, 1, scope.Scope)
					for _, metric := range scope.Items {
						b = oc.AppendMessage(b, 2, func(b []byte) []byte { return appendMetric(b, metric) })
					}
					return b
				})
			}
			if group.Resource != nil {
				b = oc.AppendString(b, 3, group.Resource.SchemaURL)
			}
			return b
		})
	}
	return b
}

// groupMetricsは、metricsをResourceとScopeごとにまとめます。
func groupMetrics(items []metrics.Metric, defaultResource *telemetrydata.Resource) []oc.ResourceGroup[metrics.Metric] {
	return oc.Group(items, defaultResource,
		func(m *metrics.Metric) *telemetrydata.Resource { return m.Resource },
		func(m *metrics.Metric) telemetrydata.Scope { return m.Scope },
	)
}

func appendMetric(b []byte, m *metrics.Metric) []byte {
	// Metric.name = 1、description = 2、unit = 3、gauge = 5、sum = 7、histogram = 9
	b = oc.AppendString(b, 1, m.Name)
	b = oc.AppendString(b, 2, m.Description)
	b = oc.AppendString(b, 3, m.Unit)
	switch m.Kind {
	case metrics.KindGauge:
		return oc.AppendMessage(b, 5, func(b []byte) []byte {
			// Gauge.data_points = 1
			return appendNumberPoints(b, m.Points)
		})
	case metrics.KindHistogram:
		return oc.AppendMessage(b, 9, func(b []byte) []byte {
			// Histogram.data_points = 1、aggregation_temporality = 2
			for i := range m.Points {
				b = oc.AppendMessage(b, 1, func(b []byte) []byte { return appendHistogramPoint(b, &m.Points[i]) })
			}
			return oc.AppendVarint(b, 2, otlpTemporality(m.Temporality))
		})
	default:
		return oc.AppendMessage(b, 7, func(b []byte) []byte {
			// Sum.data_points = 1、aggregation_temporality = 2、is_monotonic = 3
			b = appendNumberPoints(b, m.Points)
			b = oc.AppendVarint(b, 2, otlpTemporality(m.Temporality))
			return oc.AppendVarint(b, 3, protowire.EncodeBool(m.Monotonic))
		})
	}
}

func appendNumberPoints(b []byte, points []metrics.Point) []byte {
	for i := range points {
		p := &points[i]
		b = oc.AppendMessage(b, 1, func(b []byte) []byte {
			// NumberDataPoint.attributes = 7、start_time_unix_nano = 2、time_unix_nano = 3、
			// as_double = 4、as_int = 6（sfixed64）
			b = oc.AppendKeyValues(b, 7, oc.ToKeyValues(p.Attrs))
			b = oc.AppendFixed64(b, 2, oc.UnixNano(p.StartTime))
			b = oc.AppendFixed64(b, 3, oc.UnixNano(p.Time))
			if p.IsInt {
				b = protowire.AppendTag(b, 6, protowire.Fixed64Type)
				return protowire.AppendFixed64(b, uint64(p.Int))
			}
			return oc.AppendDouble(b, 4, p.Float)
		})
	}
	return b
}

func appendHistogramPoint(b []byte, p *metrics.Point) []byte {
	// HistogramDataPoint.attributes = 9、start_time_unix_nano = 2、time_unix_nano = 3、
	// count = 4（fixed64）、sum = 5、bucket_counts = 6（packed fixed64）、
	// explicit_bounds = 7（packed double）、min = 11、max = 12
	b = oc.AppendKeyValues(b, 9, oc.ToKeyValues(p.Attrs))
	b = oc.AppendFixed64(b, 2, oc.UnixNano(p.StartTime))
	b = oc.AppendFixed64(b, 3, oc.UnixNano(p.Time))
	b = protowire.AppendTag(b, 4, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, p.Count)
	b = oc.AppendDouble(b, 5, p.Sum)
	if len(p.BucketCounts) > 0 {
		b = oc.AppendMessage(b, 6, func(b []byte) []byte {
			for _, count := range p.BucketCounts {
				b = protowire.AppendFixed64(b, count)
			}
			return b
		})
	}
	if len(p.Bounds) > 0 {
		b = oc.AppendMessage(b, 7, func(b []byte) []byte {
			for _, bound := range p.Bounds {
				b = protowire.AppendFixed64(b, math.Float64bits(bound))
			}
			return b
		})
	}
	if p.HasMinMax {
		b = oc.AppendDouble(b, 11, p.Min)
		b = oc.AppendDouble(b, 12, p.Max)
	}
	return b
}

func otlpTemporality(t metrics.Temporality) uint64 {
	if t == metrics.TemporalityDelta {
		return temporalityDelta
	}
	return temporalityCumulative
}

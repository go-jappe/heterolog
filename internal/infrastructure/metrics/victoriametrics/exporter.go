// Package victoriametricsは、metricsをPrometheusのtext形式（exposition format）で
// VictoriaMetricsの/api/v1/import/prometheusへ送る送り先です。送信、送り直し、分割、
// circuit breakerは、OTLP/HTTPの送り先と同じdelivery/otlphttpが受け持ちます。
package victoriametrics

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/otlphttp"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// ExporterNameは、metricsのexporter属性の値です。
const ExporterName = "victoriametrics"

// BackendNameは、circuit breakerとfailure storeのmetricsのbackend属性と、failure storeの
// recordのbackendの値です。
const BackendName = "victoriametrics"

// NewExporterは、metricsをVictoriaMetricsへ送る送り先を返します。resourceは、Resourceを
// 持たないmetricに付けます。
func NewExporter(cfg otlphttp.Config, resource *telemetrydata.Resource, meter pkgobserver.Meter) *otlphttp.Exporter[metrics.Metric] {
	return otlphttp.New(cfg, otlphttp.Signal[metrics.Metric]{
		Encode:        func(items []metrics.Metric) []byte { return MarshalText(items, resource) },
		RetriesMetric: telemetry.MetricExportRetries,
		BackendName:   BackendName,
		Unit:          "data points",
		Name:          ExporterName,
		ContentType:   "text/plain; version=0.0.4",
	}, meter)
}

// MarshalTextは、metricsをPrometheusのtext形式にします。各data pointは、値を集めた時刻
// （ミリ秒）を持つ1行です。
//
//   - SumとGaugeは、metricの名前の1行です。
//   - Histogramは、上限ごとの累積の数（_bucket、le）、+Inf、合計（_sum）、数（_count）です。
//
// labelは、Resourceの属性とdata pointの属性で、同じ名前ならdata pointの属性を使います。
// Histogramでは、bucketの上限のleと同じ名前の属性は使いません。
// metricとlabelの名前で使えない文字は_にします。Resourceがnilのmetricは、defaultResourceの
// 属性を付けます。
func MarshalText(items []metrics.Metric, defaultResource *telemetrydata.Resource) []byte {
	var b strings.Builder
	for _, m := range items {
		resource := m.Resource
		if resource == nil {
			resource = defaultResource
		}
		name := sanitizeName(m.Name, true)
		for _, p := range m.Points {
			labels := mergeLabels(resource, p.Attrs)
			timestamp := strconv.FormatInt(p.Time.UnixMilli(), 10)
			switch m.Kind {
			case metrics.KindHistogram:
				// leはbucketの上限のlabelのため、同じ名前の属性は使いません。
				labels = slices.DeleteFunc(labels, func(l label) bool { return l.name == "le" })
				var cumulative uint64
				for i, bound := range p.Bounds {
					if i < len(p.BucketCounts) {
						cumulative += p.BucketCounts[i]
					}
					writeLine(&b, name+"_bucket", labels, "le", formatFloat(bound), strconv.FormatUint(cumulative, 10), timestamp)
				}
				writeLine(&b, name+"_bucket", labels, "le", "+Inf", strconv.FormatUint(p.Count, 10), timestamp)
				writeLine(&b, name+"_sum", labels, "", "", formatFloat(p.Sum), timestamp)
				writeLine(&b, name+"_count", labels, "", "", strconv.FormatUint(p.Count, 10), timestamp)
			default:
				value := formatFloat(p.Float)
				if p.IsInt {
					value = strconv.FormatInt(p.Int, 10)
				}
				writeLine(&b, name, labels, "", "", value, timestamp)
			}
		}
	}
	return []byte(b.String())
}

type label struct{ name, value string }

// mergeLabelsは、Resourceの属性とdata pointの属性を、名前の順のlabelにします。
func mergeLabels(resource *telemetrydata.Resource, attrs []pkgobserver.Attr) []label {
	values := make(map[string]string)
	if resource != nil {
		for _, attr := range resource.Attrs {
			values[sanitizeName(attr.Key, false)] = labelValue(attr.Value)
		}
	}
	for _, attr := range attrs {
		values[sanitizeName(attr.Key, false)] = labelValue(attr.Value)
	}
	labels := make([]label, 0, len(values))
	for name, value := range values {
		labels = append(labels, label{name, value})
	}
	slices.SortFunc(labels, func(a, b label) int { return strings.Compare(a.name, b.name) })
	return labels
}

// writeLineは、1行を書きます。extraNameが空でなければ、そのlabel（le）を最後に足します。
func writeLine(b *strings.Builder, name string, labels []label, extraName, extraValue, value, timestamp string) {
	b.WriteString(name)
	if len(labels) > 0 || extraName != "" {
		b.WriteByte('{')
		first := true
		write := func(n, v string) {
			if !first {
				b.WriteByte(',')
			}
			first = false
			b.WriteString(n)
			b.WriteString(`="`)
			b.WriteString(escapeLabelValue(v))
			b.WriteByte('"')
		}
		for _, l := range labels {
			write(l.name, l.value)
		}
		if extraName != "" {
			write(extraName, extraValue)
		}
		b.WriteByte('}')
	}
	b.WriteByte(' ')
	b.WriteString(value)
	b.WriteByte(' ')
	b.WriteString(timestamp)
	b.WriteByte('\n')
}

// sanitizeNameは、Prometheusのmetric（metricならtrue）とlabelの名前で使えない文字を_に
// します。metricの名前は:も使えます。数字で始まる名前は、前に_を付けます。
func sanitizeName(name string, metric bool) string {
	if name == "" {
		return "_"
	}
	var b strings.Builder
	for i, r := range name {
		valid := r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (metric && r == ':') ||
			(i > 0 && r >= '0' && r <= '9')
		switch {
		case valid:
			b.WriteRune(r)
		case i == 0 && r >= '0' && r <= '9':
			b.WriteByte('_')
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// labelValueは、属性の値をlabelの値にします。
func labelValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// escapeLabelValueは、labelの値の\と"と改行を、text形式の規則でescapeします。
func escapeLabelValue(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

// formatFloatは、浮動小数点数をtext形式で書きます。NaNと無限大は、NaN・+Inf・-Infです。
func formatFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

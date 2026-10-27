package victoriametrics

import (
	"fmt"
	"sort"
	"strings"
)

// MetricFormatter is the interface for converting metrics to the Prometheus
// format.
type MetricFormatter interface {
	Format(metrics []Metric) (string, error)
}

// Metric is a struct that represents a Prometheus metric.
type Metric struct {
	Name   string            // metric name
	Labels map[string]string // labels (key-value)
	Value  float64           // value
}

// Validate checks whether the metric is valid. Use errors.Is with
// ErrEmptyMetricName, ErrInvalidMetricName or ErrInvalidLabelName to identify
// the kind of problem.
func (m *Metric) Validate() error {
	if m.Name == "" {
		return ErrEmptyMetricName
	}

	// Prometheusのメトリクス名の規則をチェック
	if !isValidMetricName(m.Name) {
		return newValidationError(ErrInvalidMetricName, "invalid metric name: %s", m.Name)
	}

	// ラベル名の妥当性チェック
	for key := range m.Labels {
		if !isValidLabelName(key) {
			return newValidationError(ErrInvalidLabelName, "invalid label name: %s", key)
		}
	}

	return nil
}

// PrometheusTextFormatter formats metrics in the Prometheus text format.
type PrometheusTextFormatter struct {
	// 将来の拡張のためのフィールドを追加可能
}

// NewPrometheusTextFormatter creates a new PrometheusTextFormatter.
func NewPrometheusTextFormatter() *PrometheusTextFormatter {
	return &PrometheusTextFormatter{}
}

// Format converts metrics to the Prometheus text format.
func (f *PrometheusTextFormatter) Format(metrics []Metric) (string, error) {
	if len(metrics) == 0 {
		return "", nil
	}

	// 事前に容量を見積もる
	estimatedSize := len(metrics) * 100
	var sb strings.Builder
	sb.Grow(estimatedSize)

	for index, metric := range metrics {
		if err := metric.Validate(); err != nil {
			return "", fmt.Errorf("invalid metric at index %d: %w", index, err)
		}

		line, err := f.formatMetricLine(metric)
		if err != nil {
			return "", err
		}

		sb.WriteString(line)
		sb.WriteByte('\n')
	}

	return sb.String(), nil
}

// formatMetricLine は単一のメトリクスを1行のテキストにフォーマットする
func (f *PrometheusTextFormatter) formatMetricLine(m Metric) (string, error) {
	labelStr := f.formatLabels(m.Labels)

	// timestamp なしバージョン
	return fmt.Sprintf("%s%s %g", m.Name, labelStr, m.Value), nil
}

// formatLabels はラベルをPrometheus形式の文字列に変換する
func (f *PrometheusTextFormatter) formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}

	// ラベルキーをソート
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// ラベルを構築
	labelParts := make([]string, 0, len(keys))
	for _, k := range keys {
		v := escapeLabelValue(labels[k])
		labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, k, v))
	}

	return "{" + strings.Join(labelParts, ",") + "}"
}

// escapeLabelValue はラベル値をエスケープする
func escapeLabelValue(value string) string {
	// エスケープ処理の順序は重要
	value = strings.ReplaceAll(value, "\\", `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, "\"", `\"`)
	return value
}

// isValidMetricName はメトリクス名がPrometheusの命名規則に従っているかチェックする
func isValidMetricName(name string) bool {
	if name == "" {
		return false
	}

	// Prometheusのメトリクス名は[a-zA-Z_:][a-zA-Z0-9_:]*の形式
	for i, r := range name {
		if i == 0 {
			if !isLetter(r) && r != '_' && r != ':' {
				return false
			}
		} else {
			if !isAlphaNumeric(r) && r != '_' && r != ':' {
				return false
			}
		}
	}

	return true
}

// isValidLabelName はラベル名がPrometheusの命名規則に従っているかチェックする
func isValidLabelName(name string) bool {
	if name == "" {
		return false
	}

	// Prometheusのラベル名は[a-zA-Z_][a-zA-Z0-9_]*の形式
	// ただし、__で始まるものは予約されている
	if strings.HasPrefix(name, "__") {
		return false
	}

	for i, r := range name {
		if i == 0 {
			if !isLetter(r) && r != '_' {
				return false
			}
		} else {
			if !isAlphaNumeric(r) && r != '_' {
				return false
			}
		}
	}

	return true
}

// isLetter は文字がアルファベットかチェックする
func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// isAlphaNumeric は文字がアルファベットまたは数字かチェックする
func isAlphaNumeric(r rune) bool {
	return isLetter(r) || (r >= '0' && r <= '9')
}

// FormatPrometheusText is kept for backward compatibility.
//
// Deprecated: Use NewPrometheusTextFormatter().Format() instead.
func FormatPrometheusText(metrics []Metric) string {
	formatter := NewPrometheusTextFormatter()
	result, err := formatter.Format(metrics)
	if err != nil {
		// 後方互換性のため、エラーの場合は空文字列を返す
		return ""
	}
	return result
}

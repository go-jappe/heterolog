package victoriametrics

import (
	"errors"
	"testing"
)

func TestMetric_Validate(t *testing.T) {
	tests := []struct {
		name   string
		metric Metric
		// wantErrは報告される不正の種類です。nilなら成功を求めます。
		wantErr error
	}{
		{
			name: "valid metric",
			metric: Metric{
				Name:   "http_requests_total",
				Labels: map[string]string{"method": "GET", "status": "200"},
				Value:  42.0,
			},
		},
		{
			name: "empty metric name",
			metric: Metric{
				Name:   "",
				Labels: map[string]string{"method": "GET"},
				Value:  42.0,
			},
			wantErr: ErrEmptyMetricName,
		},
		{
			name: "invalid metric name starting with number",
			metric: Metric{
				Name:   "123_invalid",
				Labels: map[string]string{},
				Value:  42.0,
			},
			wantErr: ErrInvalidMetricName,
		},
		{
			name: "valid metric name with colon",
			metric: Metric{
				Name:   "prometheus:metric_name",
				Labels: map[string]string{},
				Value:  42.0,
			},
		},
		{
			name: "invalid label name",
			metric: Metric{
				Name:   "valid_metric",
				Labels: map[string]string{"123invalid": "value"},
				Value:  42.0,
			},
			wantErr: ErrInvalidLabelName,
		},
		{
			name: "reserved label name",
			metric: Metric{
				Name:   "valid_metric",
				Labels: map[string]string{"__reserved": "value"},
				Value:  42.0,
			},
			wantErr: ErrInvalidLabelName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.metric.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("Metric.Validate() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Metric.Validate() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestPrometheusTextFormatter_Format(t *testing.T) {
	formatter := NewPrometheusTextFormatter()

	tests := []struct {
		name    string
		metrics []Metric
		want    string
		wantErr bool
	}{
		{
			name:    "empty metrics",
			metrics: []Metric{},
			want:    "",
			wantErr: false,
		},
		{
			name: "single metric without labels",
			metrics: []Metric{
				{
					Name:  "temperature_celsius",
					Value: 23.5,
				},
			},
			want:    "temperature_celsius 23.5\n",
			wantErr: false,
		},
		{
			name: "single metric with labels",
			metrics: []Metric{
				{
					Name:   "http_requests_total",
					Labels: map[string]string{"method": "GET", "status": "200"},
					Value:  1234,
				},
			},
			want:    `http_requests_total{method="GET",status="200"} 1234` + "\n",
			wantErr: false,
		},
		{
			name: "multiple metrics",
			metrics: []Metric{
				{
					Name:  "cpu_usage",
					Value: 0.85,
				},
				{
					Name:   "memory_usage_bytes",
					Labels: map[string]string{"host": "server1"},
					Value:  1073741824,
				},
			},
			want: "cpu_usage 0.85\n" +
				`memory_usage_bytes{host="server1"} 1.073741824e+09` + "\n",
			wantErr: false,
		},
		{
			name: "metric with escaped label values",
			metrics: []Metric{
				{
					Name: "test_metric",
					Labels: map[string]string{
						"path":    "/api/v1/query",
						"message": `Error: "failed"\nRetry`,
					},
					Value: 1,
				},
			},
			want:    `test_metric{message="Error: \"failed\"\\nRetry",path="/api/v1/query"} 1` + "\n",
			wantErr: false,
		},
		{
			name: "invalid metric",
			metrics: []Metric{
				{
					Name:  "",
					Value: 42,
				},
			},
			wantErr: true,
		},
		{
			name: "labels are sorted",
			metrics: []Metric{
				{
					Name: "sorted_labels",
					Labels: map[string]string{
						"zebra": "z",
						"alpha": "a",
						"beta":  "b",
					},
					Value: 1,
				},
			},
			want:    `sorted_labels{alpha="a",beta="b",zebra="z"} 1` + "\n",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := formatter.Format(tt.metrics)
			if (err != nil) != tt.wantErr {
				t.Errorf("PrometheusTextFormatter.Format() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PrometheusTextFormatter.Format() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatPrometheusText_BackwardCompatibility(t *testing.T) {
	tests := []struct {
		name    string
		metrics []Metric
		want    string
	}{
		{
			name: "valid metrics",
			metrics: []Metric{
				{
					Name:   "test_metric",
					Labels: map[string]string{"env": "prod"},
					Value:  42,
				},
			},
			want: `test_metric{env="prod"} 42` + "\n",
		},
		{
			name: "invalid metrics return empty string",
			metrics: []Metric{
				{
					Name:  "",
					Value: 42,
				},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatPrometheusText(tt.metrics)
			if got != tt.want {
				t.Errorf("FormatPrometheusText() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEscapeLabelValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no escape needed",
			input: "simple_value",
			want:  "simple_value",
		},
		{
			name:  "escape backslash",
			input: `path\to\file`,
			want:  `path\\to\\file`,
		},
		{
			name:  "escape newline",
			input: "line1\nline2",
			want:  `line1\nline2`,
		},
		{
			name:  "escape quotes",
			input: `value with "quotes"`,
			want:  `value with \"quotes\"`,
		},
		{
			name:  "escape all special characters",
			input: `complex\path\with\n"quotes"`,
			want:  `complex\\path\\with\\n\"quotes\"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeLabelValue(tt.input)
			if got != tt.want {
				t.Errorf("escapeLabelValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsValidMetricName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", false},
		{"valid simple", "metric_name", true},
		{"valid with colon", "prometheus:metric", true},
		{"valid starting with underscore", "_metric", true},
		{"valid with numbers", "metric_123", true},
		{"invalid starting with number", "123_metric", false},
		{"invalid with dash", "metric-name", false},
		{"invalid with space", "metric name", false},
		{"valid complex", "prometheus:http_requests_total", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidMetricName(tt.input)
			if got != tt.want {
				t.Errorf("isValidMetricName(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsValidLabelName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", false},
		{"valid simple", "label_name", true},
		{"valid starting with letter", "method", true},
		{"valid with numbers", "label_123", true},
		{"invalid reserved", "__reserved", false},
		{"invalid starting with number", "123_label", false},
		{"invalid with dash", "label-name", false},
		{"valid with underscore", "label_name_123", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidLabelName(tt.input)
			if got != tt.want {
				t.Errorf("isValidLabelName(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func BenchmarkPrometheusTextFormatter_Format(b *testing.B) {
	formatter := NewPrometheusTextFormatter()
	metrics := []Metric{
		{
			Name:   "http_requests_total",
			Labels: map[string]string{"method": "GET", "status": "200", "endpoint": "/api/v1/query"},
			Value:  1234567,
		},
		{
			Name:   "http_request_duration_seconds",
			Labels: map[string]string{"method": "GET", "status": "200", "endpoint": "/api/v1/query"},
			Value:  0.123,
		},
		{
			Name:   "go_memstats_alloc_bytes",
			Labels: map[string]string{"job": "prometheus", "instance": "localhost:9090"},
			Value:  12345678,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = formatter.Format(metrics)
	}
}

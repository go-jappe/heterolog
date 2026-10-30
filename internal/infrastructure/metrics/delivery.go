package metrics

import (
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/metrics"
	"github.com/go-jappe/heterolog/internal/application/telemetrydata"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/failurestore"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/jsonlines"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/otlphttp"
	"github.com/go-jappe/heterolog/internal/infrastructure/metrics/otlp"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// OTLPBackendNameは、otlpの送り先のcircuit breakerとfailure storeのmetricsのbackend属性と、
// failure storeのrecordのbackendの値です。ログの出力先とtraceの送り先と区別できるよう、
// metricsの送り先であることを名前に入れます。
const OTLPBackendName = "otlp_metrics"

// FailureOperationは、failure storeに保存するmetricのrecordのoperationです。
const FailureOperation = "export_metrics"

// NewOTLPExporterは、metricsをOTLP/HTTPで送る送り先を返します。resourceは、Resourceを
// 持たないmetricに付けます。
func NewOTLPExporter(cfg otlphttp.Config, resource *telemetrydata.Resource, meter pkgobserver.Meter) *otlphttp.Exporter[metrics.Metric] {
	return otlphttp.New(cfg, otlphttp.Signal[metrics.Metric]{
		Encode:        func(items []metrics.Metric) []byte { return otlp.MarshalProto(items, resource) },
		RetriesMetric: telemetry.MetricExportRetries,
		BackendName:   OTLPBackendName,
		Unit:          "data points",
	}, meter)
}

// MarshalJSONは、Resourceを持たないmetricにdefaultResourceを付けて、metricのbatchを
// OTLP/JSONの1行にする関数を返します。fileとstdoutの送り先に使います。
func MarshalJSON(defaultResource *telemetrydata.Resource) jsonlines.Marshal[metrics.Metric] {
	return func(items []metrics.Metric) ([]byte, error) { return otlp.MarshalJSON(items, defaultResource) }
}

// NewCodecは、metricを1件ずつ（そのdata pointをすべて含めて）、ResourceとScopeを含む
// OTLP/JSONのMetricsDataとしてfailure storeのrecordにするCodecを返します。defaultResource
// は、Resourceを持たないmetricに付けます。
func NewCodec(defaultResource *telemetrydata.Resource) failurestore.Codec[metrics.Metric] {
	return failurestore.Codec[metrics.Metric]{
		Encode: func(metric metrics.Metric) (map[string]any, error) {
			data, err := otlp.MarshalJSON([]metrics.Metric{metric}, defaultResource)
			if err != nil {
				return nil, fmt.Errorf("encode the metric for the failure store: %w", err)
			}
			return failurestore.JSONObject(data)
		},
		Decode: func(payload []byte) (metrics.Metric, error) {
			items, err := otlp.UnmarshalJSON(payload)
			if err != nil {
				return metrics.Metric{}, err
			}
			if len(items) != 1 {
				return metrics.Metric{}, fmt.Errorf("%w: the record holds %d metrics, want 1", otlp.ErrInvalidJSON, len(items))
			}
			return items[0], nil
		},
	}
}

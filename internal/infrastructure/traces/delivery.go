package traces

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/failurestore"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/jsonlines"
	"github.com/go-jappe/heterolog/internal/infrastructure/delivery/otlphttp"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlp"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// OTLPBackendNameは、otlpの送り先のcircuit breakerとfailure storeのmetricsのbackend属性と、
// failure storeのrecordのbackendの値です。ログの出力先と区別できるよう、traceの送り先で
// あることを名前に入れます。
const OTLPBackendName = "otlp_traces"

// FailureOperationは、failure storeに保存するspanのrecordのoperationです。
const FailureOperation = "export_traces"

// NewOTLPExporterは、spanをOTLP/HTTPで送る送り先を返します。resourceは、Resourceを持たない
// spanに付けます。
func NewOTLPExporter(cfg otlphttp.Config, resource *tracing.Resource, meter pkgobserver.Meter) *otlphttp.Exporter[tracing.SpanData] {
	return otlphttp.New(cfg, otlphttp.Signal[tracing.SpanData]{
		Encode:        func(spans []tracing.SpanData) []byte { return otlp.MarshalProto(spans, resource) },
		RetriesMetric: telemetry.TraceExportRetries,
		BackendName:   OTLPBackendName,
		Unit:          "spans",
	}, meter)
}

// MarshalJSONは、Resourceを持たないspanにdefaultResourceを付けて、spanのbatchを
// OTLP/JSONの1行にする関数を返します。fileとstdoutの送り先に使います。
func MarshalJSON(defaultResource *tracing.Resource) jsonlines.Marshal[tracing.SpanData] {
	return func(spans []tracing.SpanData) ([]byte, error) { return otlp.MarshalJSON(spans, defaultResource) }
}

// NewCodecは、spanを1件ずつ、そのResourceとinstrumentation scopeを含むOTLP/JSONの
// TracesDataとしてfailure storeのrecordにするCodecを返します。defaultResourceは、Resourceを
// 持たないspanに付けます。
func NewCodec(defaultResource *tracing.Resource) failurestore.Codec[tracing.SpanData] {
	return failurestore.Codec[tracing.SpanData]{
		Encode: func(span tracing.SpanData) (map[string]any, error) {
			data, err := otlp.MarshalJSON([]tracing.SpanData{span}, defaultResource)
			if err != nil {
				return nil, fmt.Errorf("encode the span for the failure store: %w", err)
			}
			return decodeObject(data)
		},
		Decode: func(payload []byte) (tracing.SpanData, error) {
			spans, err := otlp.UnmarshalJSON(payload)
			if err != nil {
				return tracing.SpanData{}, err
			}
			if len(spans) != 1 {
				return tracing.SpanData{}, fmt.Errorf("%w: the record holds %d spans, want 1", otlp.ErrInvalidJSON, len(spans))
			}
			return spans[0], nil
		},
	}
}

// decodeObjectは、JSONのobjectをmapにします。failure storeのrecordはJSONのobjectを
// 受け取るためです。数値は、json.Numberのまま書き戻せるよう、そのまま残します。
func decodeObject(data []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, fmt.Errorf("encode the record for the failure store: %w", err)
	}
	return object, nil
}

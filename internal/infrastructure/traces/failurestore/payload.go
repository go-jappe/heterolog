package failurestore

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/tracing"
	"github.com/go-jappe/heterolog/internal/infrastructure/traces/otlp"
)

// encodePayloadは、1件のspanを、そのResourceとinstrumentation scopeを含むOTLP/JSONの
// TracesDataにします。failure storeのrecordはJSONのobjectを受け取るため、mapにします。
// 数値は、json.Numberのまま書き戻せるよう、そのまま残します。
func encodePayload(span tracing.SpanData, defaultResource *tracing.Resource) (map[string]any, error) {
	data, err := otlp.MarshalJSON([]tracing.SpanData{span}, defaultResource)
	if err != nil {
		return nil, fmt.Errorf("encode the span for the failure store: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("encode the span for the failure store: %w", err)
	}
	return payload, nil
}

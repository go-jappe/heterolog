package opensearch

import (
	"encoding/json"
	"testing"
)

func TestErrorDetailDecodesStrictDynamicMappingErrorType(t *testing.T) {
	t.Parallel()

	var detail ErrorDetail
	if err := json.Unmarshal(
		[]byte(`{"type":"strict_dynamic_mapping_exception","reason":"new field is not allowed"}`),
		&detail,
	); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if detail.Type != ErrorTypeStrictDynamicMapping {
		t.Fatalf("ErrorDetail.Type = %q, want %q", detail.Type, ErrorTypeStrictDynamicMapping)
	}
}

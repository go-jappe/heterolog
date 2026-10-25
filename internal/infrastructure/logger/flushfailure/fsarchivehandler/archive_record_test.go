package fsarchivehandler

import (
	"bytes"
	"encoding/json"
	"testing"

	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
)

func TestArchiveFailureRecordSerializesFailureCategory(t *testing.T) {
	for _, test := range []struct {
		failureCategory string
		want            string
	}{
		{archiveRecordFailureCategoryRetryable, "retryable"},
		{archiveRecordFailureCategoryDead, "dead"},
		{archiveRecordFailureCategoryQuarantine, "quarantine"},
	} {
		t.Run(test.want, func(t *testing.T) {
			record := buildArchiveFailureRecord(test.failureCategory, loggerfailure.SendResult{}, loggerfailure.FailedRecord{
				Payload: map[string]any{"id": "failed-record"},
			})
			var output bytes.Buffer
			if _, err := writeArchiveJSONL(&output, record); err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
				t.Fatal(err)
			}
			if _, exists := fields["kind"]; exists {
				t.Fatal("serialized archive record contains obsolete kind field")
			}
			if _, exists := fields["type"]; exists {
				t.Fatal("serialized archive record contains redundant type field")
			}
			var failureCategory string
			if err := json.Unmarshal(fields["failure_category"], &failureCategory); err != nil {
				t.Fatalf("decode failure_category: %v", err)
			}
			if failureCategory != test.want {
				t.Fatalf("failure_category = %q, want %q", failureCategory, test.want)
			}
			var payload map[string]string
			if err := json.Unmarshal(fields["payload"], &payload); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			if len(payload) != 1 || payload["id"] != "failed-record" {
				t.Fatalf("payload = %v, want map[id:failed-record]", payload)
			}
		})
	}
}

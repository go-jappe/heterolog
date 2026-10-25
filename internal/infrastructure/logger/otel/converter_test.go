package otel

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

// ConvertOtelLogEntryToMapはJSON tagの名前で入れ子のattributes・resourceを展開し、
// omitemptyの空値だけを省きます。保存先へ渡す全fieldを比較し、欠落や余分なkeyを検出します。
func TestOtelLogEntryToMap(t *testing.T) {
	tests := []struct {
		name  string
		entry OtelLogEntry
		want  map[string]interface{}
	}{
		{
			name: "populated entry",
			entry: OtelLogEntry{
				Timestamp:      "2025-01-16T15:30:45Z",
				SeverityText:   "ERROR",
				SeverityNumber: 17,
				Body:           "An error occurred",
				Attributes: LogAttributes{
					RequestId:           "req-123",
					ExceptionMessage:    "null pointer exception",
					ExceptionStacktrace: "at main.go:42\nat handler.go:15",
					InternalError:       true,
					Source:              "api-handler",
					Fields: map[string]any{
						"user_id": "user-456",
						"action":  "create_order",
					},
				},
				Resource: &LogResource{
					ServiceName:    "example-service",
					ServiceVersion: "1.0.0",
					HostName:       "server-01",
					OSType:         "linux",
				},
			},
			want: map[string]interface{}{
				"timestamp":       "2025-01-16T15:30:45Z",
				"severity_text":   "ERROR",
				"severity_number": 17,
				"body":            "An error occurred",
				"attributes": map[string]interface{}{
					"request_id":           "req-123",
					"exception.message":    "null pointer exception",
					"exception.stacktrace": "at main.go:42\nat handler.go:15",
					"internal_error":       true,
					"source":               "api-handler",
					"fields": map[string]any{
						"user_id": "user-456",
						"action":  "create_order",
					},
				},
				"resource": map[string]interface{}{
					"service.name":    "example-service",
					"service.version": "1.0.0",
					"host.name":       "server-01",
					"os.type":         "linux",
				},
			},
		},
		{
			name: "empty optional values",
			entry: OtelLogEntry{
				Timestamp:      "2025-01-16T15:30:45Z",
				SeverityText:   "INFO",
				SeverityNumber: 9,
				Body:           "Simple log message",
				Attributes:     LogAttributes{},
				Resource:       nil,
			},
			want: map[string]interface{}{
				"timestamp":       "2025-01-16T15:30:45Z",
				"severity_text":   "INFO",
				"severity_number": 9,
				"body":            "Simple log message",
				"attributes":      map[string]interface{}{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ConvertOtelLogEntryToMap(tt.entry); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ConvertOtelLogEntryToMap() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// ExampleOtelLogEntry は OtelLogEntry の使用例です。
func ExampleOtelLogEntry() {
	entry := OtelLogEntry{
		Timestamp:      "2025-01-16T15:30:45Z",
		SeverityText:   "WARN",
		SeverityNumber: 13,
		Body:           "High memory usage detected",
		Attributes: LogAttributes{
			Source: "memory-monitor",
			Fields: map[string]any{
				"memory_percent": 85.5,
				"threshold":      80,
			},
		},
	}

	result := ConvertOtelLogEntryToMap(entry)
	jsonData, _ := json.Marshal(result)
	fmt.Printf("JSON出力: %s\n", string(jsonData))

	// Output:
	// JSON出力: {"attributes":{"fields":{"memory_percent":85.5,"threshold":80},"source":"memory-monitor"},"body":"High memory usage detected","severity_number":13,"severity_text":"WARN","timestamp":"2025-01-16T15:30:45Z"}
}

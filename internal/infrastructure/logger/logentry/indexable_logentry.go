package logentry

// BaseとなるLogEntryはlogging/logging.goにある
// opensearch/elasticsearchを想定LogEntryをこれに整形して永続化
// Otelに準拠していない
// otelにOpenTelemetry準拠の構造体がある
type IndexableLogEntry struct {
	Timestamp     string         `json:"timestamp"`
	Level         string         `json:"level"`
	Message       string         `json:"message"`
	RequestId     string         `json:"request_id,omitempty"`
	File          []string       `json:"file,omitempty"`
	Line          []int          `json:"line,omitempty"`
	FuncName      []string       `json:"func_name,omitempty"`
	Err           string         `json:"error,omitempty"`
	InternalError bool           `json:"internal_error,omitempty"`
	Source        string         `json:"source,omitempty"`
	Fields        map[string]any `json:"fields,omitempty"`
}

package formatter

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/otel"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

var _ Formatter = GoogleCloudLoggingFormatter{}

// GoogleCloudLoggingFormatterは、LogEntryをGoogle Cloud Loggingの構造化ログ（1行のJSON）へ
// 変換します。Cloud Loggingはseverityとtimeをlog entryの項目として読み取り、messageを一覧の
// 行に表示して、ほかの項目はjsonPayloadに残します。
type GoogleCloudLoggingFormatter struct{}

// googleCloudLogLineは、1行のJSONの項目です。fieldsをtop-levelへ展開すると、applicationの
// keyがseverityなどの特別な項目を上書きするため、fieldsの下に置きます。
//
// trace_idは、特別な項目logging.googleapis.com/traceへ入れません。その値にはprojectのIDが
// 必要で（projects/PROJECT_ID/traces/TRACE_ID）、このformatterはIDを持たないためです。
type googleCloudLogLine struct {
	Severity      string         `json:"severity"`
	Message       string         `json:"message"`
	Time          string         `json:"time"`
	TraceID       string         `json:"trace_id,omitempty"`
	SpanID        string         `json:"span_id,omitempty"`
	RequestID     string         `json:"request_id,omitempty"`
	Error         string         `json:"error,omitempty"`
	Stacktrace    string         `json:"stacktrace,omitempty"`
	InternalError bool           `json:"internal_error,omitempty"`
	Source        string         `json:"source,omitempty"`
	Sampled       bool           `json:"sampled,omitempty"`
	Fields        map[string]any `json:"fields,omitempty"`
	FieldsError   string         `json:"fields_error,omitempty"`
}

// Formatはentryを1行のJSONへ変換します。fieldsをJSONにできない場合も、重大度と本文を失わない
// よう、fieldsだけを外してfields_errorに理由を書きます。
func (GoogleCloudLoggingFormatter) Format(entry *logging.LogEntry) string {
	line := googleCloudLogLine{
		Severity:      googleCloudSeverity(entry.Level),
		Message:       entry.Message,
		Time:          logging.ResolveEntryOccurredAt(entry).Format(time.RFC3339Nano),
		TraceID:       entry.TraceID,
		SpanID:        entry.SpanID,
		RequestID:     entry.RequestID,
		Stacktrace:    otel.FormatStackTrace(logging.BuildStackTraceArray(*entry)),
		InternalError: entry.Internal,
		Source:        entry.Source,
		Sampled:       entry.Sampled,
		Fields:        entry.Fields,
	}
	if entry.Err != nil {
		line.Error = entry.Err.Error()
	}
	encoded, err := encodeJSONLine(line)
	if err != nil {
		line.Fields = nil
		line.FieldsError = err.Error()
		// fieldsを外した残りは文字列と真偽値だけなので、失敗しません。
		encoded, _ = encodeJSONLine(line)
	}
	return encoded
}

// googleCloudSeverityは、levelをCloud LoggingのLogSeverityの名前へ変換します。名前が異なるのは、
// WARN（WARNING）とFATAL（CRITICAL）です。
func googleCloudSeverity(level pkglogger.LogLevel) string {
	switch level {
	case pkglogger.LogLevelDebug:
		return "DEBUG"
	case pkglogger.LogLevelInfo:
		return "INFO"
	case pkglogger.LogLevelWarn:
		return "WARNING"
	case pkglogger.LogLevelError:
		return "ERROR"
	case pkglogger.LogLevelFatal:
		return "CRITICAL"
	default:
		return "DEFAULT"
	}
}

// encodeJSONLineは、valueを改行のない1行のJSONへ変換します。文字列の中の改行もエスケープされる
// ため、複数行の本文でも1行のlog entryになります。
func encodeJSONLine(value any) (string, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buf.String(), "\n"), nil
}

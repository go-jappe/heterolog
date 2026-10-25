package otel

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/pkg/otel"
)

// LogEntryをOtelLogEntryへ変換します。rがnilの場合、省略可能なresourceを出力しません。
func ToOtelLogEntry(e *logging.LogEntry, r resource.OtelLogResourceProvider) OtelLogEntry {
	var errMsg string
	if e.Err != nil {
		errMsg = e.Err.Error()
	}

	var logResource *LogResource
	if r != nil {
		logResource = &LogResource{
			ServiceName:    r.ServiceName(),
			ServiceVersion: r.ServiceVersion(),
			CloudProvider:  r.CloudProvider(),
			HostName:       r.HostName(),
			OSType:         r.OSType(),
			Environment:    r.Environment(),
		}
	}

	return OtelLogEntry{
		// OTelのtimestampはmachine-readableな発生時刻です。人間向けログ本文の
		// timezone・表示形式とは分離し、同じ瞬間をUTC RFC3339Nanoで表します。
		Timestamp:      logging.ResolveEntryOccurredAt(e).Format(time.RFC3339Nano),
		SeverityText:   e.Level.String(),
		SeverityNumber: otel.LogLevelMaps[e.Level.String()],
		Body:           e.Message,
		Attributes: LogAttributes{
			TraceID:             e.TraceID,
			SpanID:              e.SpanID,
			RequestId:           e.RequestID,
			ExceptionMessage:    errMsg,
			ExceptionStacktrace: FormatStackTrace(logging.BuildStackTraceArray(*e)),
			InternalError:       e.Internal,
			Source:              e.Source,
			Fields:              e.Fields,
		},
		Resource: logResource,
	}
}

func FormatStackTrace(traces []map[string]interface{}) string {
	var sb strings.Builder
	for _, t := range traces {
		sb.WriteString("at ")
		if fn, ok := t["function"].(string); ok {
			sb.WriteString(fn)
		}
		if file, ok := t["file"].(string); ok {
			sb.WriteString(" (")
			sb.WriteString(file)
			if line, ok := t["line"].(int); ok {
				sb.WriteString(fmt.Sprintf(":%d", line))
			}
			sb.WriteString(")")
		}
		sb.WriteString("\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

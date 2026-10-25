package opensearch

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	infraotel "github.com/go-jappe/heterolog/internal/infrastructure/logger/otel"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
)

// ログエントリを所定の形式に変換するインターフェースの実装を渡す
type LogEntryToDocumentConverter interface {
	convertLogEntriesToDocuments([]*logging.LogEntry) []map[string]any
	convertLogEntryToDocument(*logging.LogEntry) map[string]any
}

func NewPlainConverter(timestampFormatter timestamp.Formatter) *PlainConverter {
	return &PlainConverter{timestampFormatter: timestampFormatter}
}

type PlainConverter struct {
	timestampFormatter timestamp.Formatter
}

func NewOtelConverter(resourceProvider resource.OtelLogResourceProvider) *OtelConverter {
	return &OtelConverter{
		resource: resourceProvider,
	}
}

type OtelConverter struct {
	resource resource.OtelLogResourceProvider
}

func (p *PlainConverter) convertLogEntriesToDocuments(entries []*logging.LogEntry) []map[string]any {
	docs := make([]map[string]any, len(entries))
	for i, entry := range entries {
		docs[i] = p.convertLogEntryToDocument(entry)
	}
	return docs
}

func (p *PlainConverter) convertLogEntryToDocument(entry *logging.LogEntry) map[string]any {
	formattedEntry := entry
	if entry != nil {
		entryCopy := *entry
		entryCopy.OccurredAt = logging.ResolveEntryOccurredAt(&entryCopy)
		formattedEntry = &entryCopy
	}
	// スタックトレース情報の構築
	stacktraceStr := formatter.BuildStacktraceString(formattedEntry)

	// フィールド情報の構築
	fieldsStr := formatter.BuildFieldsString(formattedEntry)

	// ベースドキュメントの作成
	doc := map[string]any{
		"trace_id":   formattedEntry.TraceID,
		"span_id":    formattedEntry.SpanID,
		"request_id": formattedEntry.RequestID,
		"timestamp":  p.timestampFormatter.Format(logging.ResolveEntryOccurredAt(formattedEntry)),
		"level":      formattedEntry.Level.String(),
		"message":    formattedEntry.Message,
		"error":      formattedEntry.Err,
		"stacktrace": stacktraceStr,
		"source":     formattedEntry.Source,
		"internal":   formattedEntry.Internal,
		"sampled":    formattedEntry.Sampled,
		"fields":     fieldsStr,
	}

	// エラー情報がある場合は追加
	if formattedEntry.Err != nil {
		doc["error"] = formattedEntry.Err.Error()
	}

	// スタックトレース情報がある場合は追加
	if len(formattedEntry.FuncName) > 0 {
		doc["stack_trace"] = logging.BuildStackTraceArray(*formattedEntry)
	}

	return doc
}

func (o *OtelConverter) convertLogEntriesToDocuments(entries []*logging.LogEntry) []map[string]any {
	docs := make([]map[string]any, len(entries))
	for i, entry := range entries {
		docs[i] = o.convertLogEntryToDocument(entry)
	}
	return docs
}

func (o *OtelConverter) convertLogEntryToDocument(entry *logging.LogEntry) map[string]any {
	doc := infraotel.ToOtelLogEntry(entry, o.Resource())
	return infraotel.ConvertOtelLogEntryToMap(doc)
}

func (o *OtelConverter) Resource() resource.OtelLogResourceProvider {
	return o.resource
}

func NewLogEntryConverter(
	useOtel bool,
	resource resource.OtelLogResourceProvider,
	timestampFormatter timestamp.Formatter,
) LogEntryToDocumentConverter {
	if useOtel {
		return NewOtelConverter(resource)
	}
	return NewPlainConverter(timestampFormatter)
}

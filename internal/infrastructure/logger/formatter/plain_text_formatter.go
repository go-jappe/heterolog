package formatter

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"strings"
)

// PlainTextFormatterは、構築時に指定された時刻表示規則でLogEntryをplain textへ変換します。
// 出力時にtimezoneやlayoutを推測せず構築時の規則だけを使うため、保存先ごとに一つ構築して
// 使用します。規則を持たないzero値はtimestamp.Formatterのzero値と同じUTCのRFC3339です。
type PlainTextFormatter struct {
	timestampFormatter timestamp.Formatter
}

var _ Formatter = PlainTextFormatter{}

// NewPlainTextFormatterはtimestampFormatterを使うPlainTextFormatterを作ります。
// zeroのtimestamp.Formatterは、その型の契約どおりUTCのRFC3339として動作します。
func NewPlainTextFormatter(timestampFormatter timestamp.Formatter) PlainTextFormatter {
	return PlainTextFormatter{timestampFormatter: timestampFormatter}
}

// IsZeroは時刻表示規則が明示的に設定されていないzero値かを返します。
// logger constructorが外部から渡された設定を尊重し、未設定時だけ補完するために使用します。
// zero値のFormatも、level、本文、error、fieldsを省略せずUTCのRFC3339で出力します。
func (f PlainTextFormatter) IsZero() bool {
	return f.timestampFormatter.IsZero()
}

// Formatはentryの発生時刻を構築時の表示規則で文字列化し、plain text logを返します。
// 表示規則が未設定のzero値ではtimestamp.Formatterのzero値と同じUTCのRFC3339を使います。
func (f PlainTextFormatter) Format(entry *logging.LogEntry) string {
	return formatPlainTextLog(entry, f.timestampFormatter)
}

func formatPlainTextLog(
	entry *logging.LogEntry,
	timestampFormatter timestamp.Formatter,
) string {
	var b strings.Builder
	estimated := len(entry.Message) + len(entry.FuncName)*64 + len(entry.File)*64
	if estimated < 256 {
		estimated = 256
	}
	b.Grow(estimated)

	// [INTERNAL]...
	if entry.Internal {
		b.WriteString("[INTERNAL]")
	}

	// [LEVEL] TIMESTAMP - MESSAGE
	b.WriteByte('[')
	b.WriteString(entry.Level.String())
	b.WriteString("] ")
	b.WriteString(timestampFormatter.Format(logging.ResolveEntryOccurredAt(entry)))
	//}
	b.WriteString(" - ")
	b.WriteString(entry.Message)

	// trace_id
	if entry.TraceID != "" {
		b.WriteString(" - trace_id: ")
		b.WriteString(entry.TraceID)
	}

	// span_id
	if entry.SpanID != "" {
		b.WriteString(" - span_id: ")
		b.WriteString(entry.SpanID)
	}

	// request_id
	if entry.RequestID != "" {
		b.WriteString(" - request_id: ")
		b.WriteString(entry.RequestID)
	}

	// Sampled
	if entry.Sampled {
		b.WriteString(" - sampled")
	}

	// err
	if entry.Err != nil {
		b.WriteString(" - err: ")
		b.WriteString(entry.Err.Error())
	}

	// Fields → JSON
	if entry.Fields != nil {
		b.WriteString(" fields:")
		b.WriteString(BuildFieldsString(entry))
	}

	// stacktrace
	if len(entry.File) > 0 {
		b.WriteString(BuildStacktraceString(entry))
	}

	return b.String()
}

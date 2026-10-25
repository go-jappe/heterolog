package clickhouse

import (
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkgclickhouse "github.com/go-jappe/heterolog/pkg/clickhouse"
	"github.com/go-jappe/heterolog/pkg/clock"
)

// convertClickHouseLogEntryToRecordはlogger bufferのentryを、実際のJSONEachRow送信と
// durable payloadの双方で使うClickHouse recordへ変換します。
func convertClickHouseLogEntryToRecord(
	base *LoggerBase,
	resourceProvider resource.ResourceProvider,
	entry *logging.LogEntry,
) pkgclickhouse.LogRecord {
	formattedEntry := entry
	if entry == nil {
		formattedEntry = &logging.LogEntry{
			OccurredAt: logging.ResolveOccurredAt(clickHouseOccurrenceClock(base)),
		}
	} else {
		entryCopy := *entry
		logging.EnsureEntryOccurredAt(&entryCopy, clickHouseOccurrenceClock(base))
		formattedEntry = &entryCopy
	}
	var logFormatter formatter.PlainTextFormatter
	if base != nil {
		logFormatter = base.LogFormatter
	}
	record := pkgclickhouse.LogRecord{
		Timestamp:  entryTimestamp(formattedEntry),
		Level:      formattedEntry.Level.String(),
		Message:    formattedEntry.Message,
		Line:       logFormatter.Format(formattedEntry),
		RequestID:  formattedEntry.RequestID,
		TraceID:    formattedEntry.TraceID,
		SpanID:     formattedEntry.SpanID,
		Fields:     formattedEntry.Fields,
		StackTrace: stackTrace(formattedEntry),
		Source:     common.SourceNameClickHouseLogger,
	}
	if base != nil && base.config != nil && base.config.Runtime() != nil {
		record.AppName = base.config.Runtime().AppName()
	}
	if resourceProvider != nil {
		if serviceName := resourceProvider.ServiceName(); serviceName != "" {
			record.AppName = serviceName
		}
		record.HostName = resourceProvider.HostName()
		record.Environment = resourceProvider.Environment()
		record.RuntimeType = resourceProvider.RuntimeType()
		record.ExecutionUnitID = resourceProvider.RuntimeInstanceID()
		record.Version = resourceProvider.ServiceVersion()
	}
	if formattedEntry.Err != nil {
		record.Error = formattedEntry.Err.Error()
	}
	return record
}

// clickHouseOccurrenceClockは、発生時刻が未設定のinfra内部entryへ時刻を補完する
// sourceを返します。表示形式とtimezoneはLoggerBase.LogFormatterが決定します。
func clickHouseOccurrenceClock(base *LoggerBase) clock.Clock {
	if base != nil && base.Clock != nil {
		return base.Clock
	}
	return clock.NewUTCClock()
}

// measureClickHouseLogEntryJSONEachRowBytesは実送信と同じLogRecordとencoderを使用し、
// row末尾newlineを含む未圧縮JSONEachRow byte数を返します。
func measureClickHouseLogEntryJSONEachRowBytes(
	base *LoggerBase,
	resourceProvider resource.ResourceProvider,
	entry *logging.LogEntry,
) (int, error) {
	record := convertClickHouseLogEntryToRecord(base, resourceProvider, entry)
	encoded, err := pkgclickhouse.MarshalJSONEachRow([]pkgclickhouse.LogRecord{record})
	if err != nil {
		return 0, fmt.Errorf("encode ClickHouse log entry as JSONEachRow: %w", err)
	}
	return len(encoded), nil
}

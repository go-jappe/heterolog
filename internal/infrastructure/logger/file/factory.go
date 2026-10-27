package file

import (
	"context"
	"github.com/go-jappe/heterolog/internal/application/requestctx"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func (l *FileLogger) InternalDebugEntry(msg string, fields ...pkglogger.Field) *logging.LogEntry {
	ids, extra := field.ResolveIDsAndExtraFields(nil, fields)

	entry := &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelDebug,
		Message:    msg,
		Internal:   true,
		Source:     fileLoggerInternalSource(l.logger),
		Fields:     extra,
	}
	if ids.RequestID != "" {
		entry.RequestID = ids.RequestID
	}
	if ids.SpanID != "" {
		entry.SpanID = ids.SpanID
	}
	if ids.TraceID != "" {
		entry.TraceID = ids.TraceID
	}

	return entry
}

func (l *FileLogger) InternalDebugEntryWithContext(ctx context.Context, msg string, fields ...pkglogger.Field) *logging.LogEntry {
	ids, extra := field.ResolveIDsAndExtraFields(ctx, fields)

	entry := &logging.LogEntry{
		TraceID:    ids.TraceID,
		SpanID:     ids.SpanID,
		RequestID:  ids.RequestID,
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelDebug,
		Message:    msg,
		Internal:   true,
		Source:     fileLoggerInternalSource(l.logger),
		Fields:     extra,
	}

	return entry
}

func (l *FileLogger) InternalInfoEntry(msg string, fields ...pkglogger.Field) *logging.LogEntry {
	ids, extra := field.ResolveIDsAndExtraFields(nil, fields)

	entry := &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelInfo,
		Message:    msg,
		Internal:   true,
		Source:     fileLoggerInternalSource(l.logger),
		Fields:     extra,
	}
	if ids.RequestID != "" {
		entry.RequestID = ids.RequestID
	}
	if ids.SpanID != "" {
		entry.SpanID = ids.SpanID
	}
	if ids.TraceID != "" {
		entry.TraceID = ids.TraceID
	}

	return entry
}

func (l *FileLogger) InternalInfoEntryWithContext(ctx context.Context, msg string, fields ...pkglogger.Field) *logging.LogEntry {
	ids, extra := field.ResolveIDsAndExtraFields(ctx, fields)

	entry := &logging.LogEntry{
		TraceID:    ids.TraceID,
		SpanID:     ids.SpanID,
		RequestID:  ids.RequestID,
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelInfo,
		Message:    msg,
		Internal:   true,
		Source:     fileLoggerInternalSource(l.logger),
		Fields:     extra,
	}

	return entry
}

func (l *FileLogger) InternalWarnEntry(msg string, fields ...pkglogger.Field) *logging.LogEntry {
	ids, extra := field.ResolveIDsAndExtraFields(nil, fields)

	entry := &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelWarn,
		Message:    msg,
		Internal:   true,
		Source:     fileLoggerInternalSource(l.logger),
		Fields:     extra,
	}
	if ids.RequestID != "" {
		entry.RequestID = ids.RequestID
	}
	if ids.SpanID != "" {
		entry.SpanID = ids.SpanID
	}
	if ids.TraceID != "" {
		entry.TraceID = ids.TraceID
	}

	return entry
}

func (l *FileLogger) InternalWarnEntryWithContext(ctx context.Context, msg string, fields ...pkglogger.Field) *logging.LogEntry {
	ids, extra := field.ResolveIDsAndExtraFields(ctx, fields)

	entry := &logging.LogEntry{
		TraceID:    ids.TraceID,
		SpanID:     ids.SpanID,
		RequestID:  ids.RequestID,
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelWarn,
		Message:    msg,
		Internal:   true,
		Source:     fileLoggerInternalSource(l.logger),
		Fields:     extra,
	}

	return entry
}

func (l *FileLogger) InternalErrorEntry(msg string, err error, parentEntry logging.LogEntry) *logging.LogEntry {

	entry := &logging.LogEntry{
		RequestID:  parentEntry.RequestID,
		SpanID:     parentEntry.SpanID,
		TraceID:    parentEntry.TraceID,
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    msg,
		Internal:   true,
		Err:        err,
		Source:     fileLoggerInternalSource(l.logger),
		Fields:     parentEntry.Fields,
	}

	return entry
}

func (l *FileLogger) InternalErrorEntryWithContext(ctx context.Context, msg string, err error) *logging.LogEntry {

	// コンテキストからtrace_id, span_id, request_idを取得
	ids := requestctx.IDsFromContext(ctx)

	entry := &logging.LogEntry{
		TraceID:    ids.TraceID,
		SpanID:     ids.SpanID,
		RequestID:  ids.RequestID,
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    msg,
		Internal:   true,
		Err:        err,
		Source:     fileLoggerInternalSource(l.logger),
	}

	return entry
}

// スタックトレース付きログ
func (l *FileLogger) InternalCallerErrorEntry(msg string, err error, fields ...pkglogger.Field) *logging.LogEntry {
	return l.newCallerLogEntry(pkglogger.LogLevelError, msg, err, true, fields...)
}

// スタックトレース付きログ
func (l *FileLogger) InternalCallerDebugEntry(msg string, fields ...pkglogger.Field) *logging.LogEntry {
	return l.newCallerLogEntry(pkglogger.LogLevelDebug, msg, nil, true, fields...)
}

func (l *FileLogger) newCallerLogEntry(level pkglogger.LogLevel, msg string, err error, internal bool, fields ...pkglogger.Field) *logging.LogEntry {
	ids, extra := field.ResolveIDsAndExtraFields(nil, fields)
	funcName, sourceFiles, lines := logging.CollectCallerDetails(
		logging.GetCallerDepth(NewStackTraceConfigAdapter(l.config.Runtime().IsDebug(), l.config.LogBody())),
	)
	entry := &logging.LogEntry{
		OccurredAt: logging.ResolveOccurredAt(l.Clock),
		Level:      level,
		Message:    msg,
		File:       sourceFiles,
		Line:       lines,
		FuncName:   funcName,
		Internal:   internal,
		Err:        err,
		Fields:     extra,
	}
	if internal {
		entry.Source = fileLoggerInternalSource(l.logger)
	}

	if ids.RequestID != "" {
		entry.RequestID = ids.RequestID
	}
	if ids.SpanID != "" {
		entry.SpanID = ids.SpanID
	}
	if ids.TraceID != "" {
		entry.TraceID = ids.TraceID
	}

	return entry
}

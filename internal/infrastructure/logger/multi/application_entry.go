package multi

import (
	"context"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	loggerfield "github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	infralogging "github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// buildApplicationLogEntryは、MultiLoggerがapplicationから受け取った一件の入力を
// fan-out前の共通LogEntryへ変換します。時刻とcontext上のIDはここで一度だけ確定し、
// 各backendにはこのentryのcloneを渡します。
func buildApplicationLogEntry(
	ctx context.Context,
	maxLogEntryBytes int,
	now func() time.Time,
	level pkglogger.LogLevel,
	message string,
	err error,
	stackTraceRequested bool,
	fields []applogging.Field,
) (*infralogging.LogEntry, error) {
	if validationErr := applogging.ValidateLogEntrySize(
		maxLogEntryBytes,
		message,
		err,
		fields...,
	); validationErr != nil {
		return nil, validationErr
	}

	ids, extra := loggerfield.ResolveIDsAndExtraFields(ctx, fields)
	if now == nil {
		now = time.Now
	}
	occurredAt, hasSlogTime := applogging.SlogRecordTime(ctx)
	if !hasSlogTime || occurredAt.IsZero() {
		occurredAt = now()
	}
	entry := &infralogging.LogEntry{
		OccurredAt:          occurredAt.UTC(),
		Level:               level,
		TraceID:             ids.TraceID,
		SpanID:              ids.SpanID,
		RequestID:           ids.RequestID,
		Message:             message,
		Err:                 err,
		StackTraceRequested: stackTraceRequested,
		Fields:              extra,
	}
	if stackTraceRequested {
		// CaptureStack -> buildApplicationLogEntry -> log -> ErrorWithStack を除き、
		// applicationの呼び出し位置から保存します。goroutineを開始する前に取得します。
		entry.CaptureStack(3)
	}
	return entry, nil
}

// canonicalLogEntryForFanOutは、各childへ複製する前の共通entryを作ります。
// OccurredAtが設定済みなら同じ瞬間をUTC表現へ正規化し、未設定ならMultiLoggerが
// 受け取った時刻を一度だけ確定します。呼び出し元が所有するentryは変更しません。
func canonicalLogEntryForFanOut(
	entry *infralogging.LogEntry,
	now func() time.Time,
) *infralogging.LogEntry {
	if entry == nil {
		return nil
	}

	canonical := infralogging.CloneLogEntry(entry)
	if canonical.OccurredAt.IsZero() {
		if now == nil {
			now = time.Now
		}
		canonical.OccurredAt = now().UTC()
	} else {
		canonical.OccurredAt = canonical.OccurredAt.UTC()
	}
	return canonical
}

package file

import (
	"context"

	loggerfield "github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// 以下のtest helperはphysical file handlerへapplication Logger契約を戻さず、
// application入力相当のLogEntryを明示的に作ってHandleLogEntryの挙動を検証します。
func newTestApplicationLogEntry(
	ctx context.Context,
	level pkglogger.LogLevel,
	message string,
	err error,
	stackTraceRequested bool,
	fields ...pkglogger.Field,
) *logging.LogEntry {
	ids, extra := loggerfield.ResolveIDsAndExtraFields(ctx, fields)
	return &logging.LogEntry{
		Level:               level,
		RequestID:           ids.RequestID,
		TraceID:             ids.TraceID,
		SpanID:              ids.SpanID,
		Message:             message,
		Err:                 err,
		StackTraceRequested: stackTraceRequested,
		Fields:              extra,
	}
}

func (l *FileLogger) testDebug(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelDebug, message, nil, false, fields...,
	))
}

func (l *FileLogger) testInfo(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelInfo, message, nil, false, fields...,
	))
}

func (l *FileLogger) testWarn(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelWarn, message, nil, false, fields...,
	))
}

func (l *FileLogger) testError(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, nil, false, fields...,
	))
}

func (l *FileLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...pkglogger.Field,
) error {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, err, true, fields...,
	))
}

func (l *DeliveryFileLogger) testDebug(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelDebug, message, nil, false, fields...,
	))
}

func (l *DeliveryFileLogger) testInfo(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelInfo, message, nil, false, fields...,
	))
}

func (l *DeliveryFileLogger) testWarn(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelWarn, message, nil, false, fields...,
	))
}

func (l *DeliveryFileLogger) testError(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, nil, false, fields...,
	))
}

func (l *DeliveryFileLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return l.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, err, true, fields...,
	))
}

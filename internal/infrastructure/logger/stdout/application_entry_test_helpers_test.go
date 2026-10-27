package stdout

import (
	"context"

	loggerfield "github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// test* helperはphysical stdout handlerへapplication Logger契約を戻さず、
// application入力相当のLogEntryを明示的に作ってHandleLogEntryを検証します。
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

func (s *StdoutLogger) testDebug(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelDebug, message, nil, false, fields...,
	))
}

func (s *StdoutLogger) testInfo(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelInfo, message, nil, false, fields...,
	))
}

func (s *StdoutLogger) testWarn(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelWarn, message, nil, false, fields...,
	))
}

func (s *StdoutLogger) testError(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, nil, false, fields...,
	))
}

func (s *StdoutLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...pkglogger.Field,
) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, err, true, fields...,
	))
}

func (s *DeliveryStdoutLogger) testDebug(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelDebug, message, nil, false, fields...,
	))
}

func (s *DeliveryStdoutLogger) testInfo(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelInfo, message, nil, false, fields...,
	))
}

func (s *DeliveryStdoutLogger) testWarn(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelWarn, message, nil, false, fields...,
	))
}

func (s *DeliveryStdoutLogger) testError(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, nil, false, fields...,
	))
}

func (s *DeliveryStdoutLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, err, true, fields...,
	))
}

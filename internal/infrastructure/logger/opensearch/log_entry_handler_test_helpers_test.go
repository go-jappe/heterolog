package opensearch

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func newTestApplicationLogEntry(
	ctx context.Context,
	occurredAt time.Time,
	level pkglogger.LogLevel,
	message string,
	err error,
	stackTraceRequested bool,
	fields ...pkglogger.Field,
) *logging.LogEntry {
	ids, extra := field.ResolveIDsAndExtraFields(ctx, fields)
	if !occurredAt.IsZero() {
		occurredAt = occurredAt.UTC()
	}
	return &logging.LogEntry{
		OccurredAt:          occurredAt,
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

func (l *OpenSearchLogger) testLog(
	ctx context.Context,
	occurredAt time.Time,
	level pkglogger.LogLevel,
	message string,
	fields ...pkglogger.Field,
) error {
	return l.HandleLogEntry(
		ctx,
		newTestApplicationLogEntry(ctx, occurredAt, level, message, nil, false, fields...),
	)
}

func (l *OpenSearchLogger) testDebug(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.testLog(ctx, time.Time{}, pkglogger.LogLevelDebug, message, fields...)
}

func (l *OpenSearchLogger) testInfo(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.testLog(ctx, time.Time{}, pkglogger.LogLevelInfo, message, fields...)
}

func (l *OpenSearchLogger) testWarn(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.testLog(ctx, time.Time{}, pkglogger.LogLevelWarn, message, fields...)
}

func (l *OpenSearchLogger) testError(ctx context.Context, message string, fields ...pkglogger.Field) error {
	return l.testLog(ctx, time.Time{}, pkglogger.LogLevelError, message, fields...)
}

func (l *OpenSearchLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...pkglogger.Field,
) error {
	return l.HandleLogEntry(
		ctx,
		newTestApplicationLogEntry(
			ctx,
			time.Time{},
			pkglogger.LogLevelError,
			message,
			err,
			true,
			fields...,
		),
	)
}

func (l *DeliveryOpenSearchLogger) testInfo(
	ctx context.Context,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return l.HandleLogEntry(
		ctx,
		newTestApplicationLogEntry(
			ctx,
			time.Time{},
			pkglogger.LogLevelInfo,
			message,
			nil,
			false,
			fields...,
		),
	)
}

func (l *DeliveryOpenSearchLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return l.HandleLogEntry(
		ctx,
		newTestApplicationLogEntry(
			ctx,
			time.Time{},
			pkglogger.LogLevelError,
			message,
			err,
			true,
			fields...,
		),
	)
}

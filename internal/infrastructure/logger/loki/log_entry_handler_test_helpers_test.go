package loki

import (
	"context"
	"time"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
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
	fields ...applogging.Field,
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

func (l *LokiLogger) testLog(
	ctx context.Context,
	occurredAt time.Time,
	level pkglogger.LogLevel,
	message string,
	fields ...applogging.Field,
) error {
	return l.HandleLogEntry(
		ctx,
		newTestApplicationLogEntry(ctx, occurredAt, level, message, nil, false, fields...),
	)
}

func (l *LokiLogger) testDebug(ctx context.Context, message string, fields ...applogging.Field) error {
	return l.testLog(ctx, time.Time{}, pkglogger.LogLevelDebug, message, fields...)
}

func (l *LokiLogger) testInfo(ctx context.Context, message string, fields ...applogging.Field) error {
	return l.testLog(ctx, time.Time{}, pkglogger.LogLevelInfo, message, fields...)
}

func (l *LokiLogger) testWarn(ctx context.Context, message string, fields ...applogging.Field) error {
	return l.testLog(ctx, time.Time{}, pkglogger.LogLevelWarn, message, fields...)
}

func (l *LokiLogger) testError(ctx context.Context, message string, fields ...applogging.Field) error {
	return l.testLog(ctx, time.Time{}, pkglogger.LogLevelError, message, fields...)
}

func (l *LokiLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...applogging.Field,
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

func (l *DeliveryLokiLogger) testDebug(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return l.testDeliveryLog(ctx, pkglogger.LogLevelDebug, message, fields...)
}

func (l *DeliveryLokiLogger) testInfo(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return l.testDeliveryLog(ctx, pkglogger.LogLevelInfo, message, fields...)
}

func (l *DeliveryLokiLogger) testWarn(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return l.testDeliveryLog(ctx, pkglogger.LogLevelWarn, message, fields...)
}

func (l *DeliveryLokiLogger) testError(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return l.testDeliveryLog(ctx, pkglogger.LogLevelError, message, fields...)
}

func (l *DeliveryLokiLogger) testDeliveryLog(
	ctx context.Context,
	level pkglogger.LogLevel,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return l.HandleLogEntry(
		ctx,
		newTestApplicationLogEntry(ctx, time.Time{}, level, message, nil, false, fields...),
	)
}

func (l *DeliveryLokiLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...applogging.Field,
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

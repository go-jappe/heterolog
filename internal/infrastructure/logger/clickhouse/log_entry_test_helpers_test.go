package clickhouse

import (
	"context"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func newTestLogEntry(
	level pkglogger.LogLevel,
	message string,
	err error,
	fields ...pkglogger.Field,
) *logging.LogEntry {
	ids, extra := field.ExtractIDsAndExtraFields(fields)
	return &logging.LogEntry{
		RequestID: ids.RequestID,
		TraceID:   ids.TraceID,
		SpanID:    ids.SpanID,
		Level:     level,
		Message:   message,
		Err:       err,
		Fields:    extra,
	}
}

func handleTestLogAt(
	ctx context.Context,
	logger *ClickHouseLogger,
	occurredAt time.Time,
	level pkglogger.LogLevel,
	message string,
	fields ...pkglogger.Field,
) error {
	entry := newTestLogEntry(level, message, nil, fields...)
	entry.OccurredAt = occurredAt.UTC()
	return logger.HandleLogEntry(ctx, entry)
}

func handleTestLog(
	ctx context.Context,
	logger *ClickHouseLogger,
	level pkglogger.LogLevel,
	message string,
	fields ...pkglogger.Field,
) error {
	return logger.HandleLogEntry(ctx, newTestLogEntry(level, message, nil, fields...))
}

func handleTestDebug(ctx context.Context, logger *ClickHouseLogger, message string, fields ...pkglogger.Field) error {
	return handleTestLog(ctx, logger, pkglogger.LogLevelDebug, message, fields...)
}

func handleTestInfo(ctx context.Context, logger *ClickHouseLogger, message string, fields ...pkglogger.Field) error {
	return handleTestLog(ctx, logger, pkglogger.LogLevelInfo, message, fields...)
}

func handleTestWarn(ctx context.Context, logger *ClickHouseLogger, message string, fields ...pkglogger.Field) error {
	return handleTestLog(ctx, logger, pkglogger.LogLevelWarn, message, fields...)
}

func handleTestError(ctx context.Context, logger *ClickHouseLogger, message string, fields ...pkglogger.Field) error {
	return handleTestLog(ctx, logger, pkglogger.LogLevelError, message, fields...)
}

func handleTestErrorWithStack(
	ctx context.Context,
	logger *ClickHouseLogger,
	message string,
	err error,
	fields ...pkglogger.Field,
) error {
	entry := newTestLogEntry(pkglogger.LogLevelError, message, err, fields...)
	entry.StackTraceRequested = true
	return logger.HandleLogEntry(ctx, entry)
}

func handleTestDeliveryLogAt(
	ctx context.Context,
	logger *DeliveryClickHouseLogger,
	occurredAt time.Time,
	level pkglogger.LogLevel,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	entry := newTestLogEntry(level, message, nil, fields...)
	entry.OccurredAt = occurredAt.UTC()
	return logger.HandleLogEntry(ctx, entry)
}

func handleTestDeliveryLog(
	ctx context.Context,
	logger *DeliveryClickHouseLogger,
	level pkglogger.LogLevel,
	message string,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	return logger.HandleLogEntry(ctx, newTestLogEntry(level, message, nil, fields...))
}

func handleTestDeliveryDebug(ctx context.Context, logger *DeliveryClickHouseLogger, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return handleTestDeliveryLog(ctx, logger, pkglogger.LogLevelDebug, message, fields...)
}

func handleTestDeliveryInfo(ctx context.Context, logger *DeliveryClickHouseLogger, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return handleTestDeliveryLog(ctx, logger, pkglogger.LogLevelInfo, message, fields...)
}

func handleTestDeliveryWarn(ctx context.Context, logger *DeliveryClickHouseLogger, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return handleTestDeliveryLog(ctx, logger, pkglogger.LogLevelWarn, message, fields...)
}

func handleTestDeliveryError(ctx context.Context, logger *DeliveryClickHouseLogger, message string, fields ...pkglogger.Field) (pkglogger.DeliveryReport, error) {
	return handleTestDeliveryLog(ctx, logger, pkglogger.LogLevelError, message, fields...)
}

func handleTestDeliveryErrorWithStack(
	ctx context.Context,
	logger *DeliveryClickHouseLogger,
	message string,
	err error,
	fields ...pkglogger.Field,
) (pkglogger.DeliveryReport, error) {
	entry := newTestLogEntry(pkglogger.LogLevelError, message, err, fields...)
	entry.StackTraceRequested = true
	return logger.HandleLogEntry(ctx, entry)
}

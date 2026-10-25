package slack

import (
	"context"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	loggerfield "github.com/go-jappe/heterolog/internal/infrastructure/logger/field"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// test* helperはphysical Slack handlerへapplication Logger契約を戻さず、
// application入力相当のLogEntryを明示的に作ってHandleLogEntryを検証します。
func newTestApplicationLogEntry(
	ctx context.Context,
	level pkglogger.LogLevel,
	message string,
	err error,
	stackTraceRequested bool,
	fields ...applogging.Field,
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

func (s *SlackLogger) testDebug(ctx context.Context, message string, fields ...applogging.Field) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelDebug, message, nil, false, fields...,
	))
}

func (s *SlackLogger) testInfo(ctx context.Context, message string, fields ...applogging.Field) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelInfo, message, nil, false, fields...,
	))
}

func (s *SlackLogger) testWarn(ctx context.Context, message string, fields ...applogging.Field) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelWarn, message, nil, false, fields...,
	))
}

func (s *SlackLogger) testError(ctx context.Context, message string, fields ...applogging.Field) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, nil, false, fields...,
	))
}

func (s *SlackLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...applogging.Field,
) error {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, err, true, fields...,
	))
}

func (s *DeliverySlackLogger) testDebug(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelDebug, message, nil, false, fields...,
	))
}

func (s *DeliverySlackLogger) testInfo(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelInfo, message, nil, false, fields...,
	))
}

func (s *DeliverySlackLogger) testWarn(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelWarn, message, nil, false, fields...,
	))
}

func (s *DeliverySlackLogger) testError(
	ctx context.Context,
	message string,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, nil, false, fields...,
	))
}

func (s *DeliverySlackLogger) testErrorWithStack(
	ctx context.Context,
	message string,
	err error,
	fields ...applogging.Field,
) (pkglogger.DeliveryReport, error) {
	return s.HandleLogEntry(ctx, newTestApplicationLogEntry(
		ctx, pkglogger.LogLevelError, message, err, true, fields...,
	))
}

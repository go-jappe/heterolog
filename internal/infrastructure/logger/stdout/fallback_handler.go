package stdout

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	resource "github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/sampling"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/logger"
)

// NotificationTargetは、内部ログの通知先（OnErrorConfig・OnWarnConfig・OnInfoConfig・
// OnDebugConfigのstdout）として使う書き込み設定です。NewNotificationTargetがloggerの構築時に
// 設定を検証してformatterを作るため、通知の時点で設定の不備により書き込み先を作れず、
// 通知を黙って捨てることはありません。
type NotificationTarget struct {
	config    StdoutLoggerConfigProvider
	formatter formatter.Formatter
}

// NewNotificationTargetは、通知先として使うstdoutの設定（sampling、時刻の表示、出力形式）を
// 検証し、書き込みに使うformatterを作ります。不正な設定はerrorとして返し、呼び出し元のloggerの
// 構築を設定errorで終えます。
func NewNotificationTarget(
	config StdoutLoggerConfigProvider,
	otelResource resource.OtelLogResourceProvider,
) (*NotificationTarget, error) {
	if config == nil {
		return nil, fmt.Errorf("stdout notification target config is nil")
	}
	if err := sampling.ValidateSamplingConfigProviderFor(logger.LogOutputStdout, config.Sampling()); err != nil {
		return nil, fmt.Errorf("stdout notification target sampling config: %w", err)
	}
	timestampFormatter, err := NewConfiguredTimestampFormatter(
		config.UseOpenTelemetryFormat(),
		config.Time().EnableLocalTime(),
		config.Time().TimeZone(),
		config.LogBody().LogLineDateFormat(),
	)
	if err != nil {
		return nil, err
	}
	logFormatter, err := NewConfiguredFormatter(
		config.UseOpenTelemetryFormat(),
		otelResource,
		timestampFormatter,
	)
	if err != nil {
		return nil, err
	}
	return &NotificationTarget{config: config, formatter: logFormatter}, nil
}

// NamedErrorHandlerは、OnErrorの通知を書き込む新しいhandlerを返します。
func (t *NotificationTarget) NamedErrorHandler() logging.NamedLoggerErrorHandler {
	write := t.newWriter()
	return logging.NamedLoggerErrorHandler{
		Name: common.HandlerNameStdoutLogger,
		// 呼び出し元のerrorの詳細はentryに含まれているため、書き込みには使いません。
		Handler: func(
			ctx context.Context,
			_ error,
			entry *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			write(ctx, entry)
		},
	}
}

// NamedWarnHandlerは、OnWarnの通知を書き込む新しいhandlerを返します。
func (t *NotificationTarget) NamedWarnHandler() logging.NamedLoggerHandler {
	return t.namedHandler(logger.LogLevelWarn)
}

// NamedInfoHandlerは、OnInfoの通知を書き込む新しいhandlerを返します。
func (t *NotificationTarget) NamedInfoHandler() logging.NamedLoggerHandler {
	return t.namedHandler(logger.LogLevelInfo)
}

// NamedDebugHandlerは、OnDebugの通知を書き込む新しいhandlerを返します。
func (t *NotificationTarget) NamedDebugHandler() logging.NamedLoggerHandler {
	return t.namedHandler(logger.LogLevelDebug)
}

func (t *NotificationTarget) namedHandler(level logger.LogLevel) logging.NamedLoggerHandler {
	write := t.newWriter()
	return logging.NamedLoggerHandler{
		Name: common.HandlerNameStdoutLogger,
		Handler: func(
			ctx context.Context,
			entry *logging.LogEntry,
			_ observer.Tracer,
			_ observer.Meter,
		) {
			write(ctx, entry)
		},
		Level: level,
	}
}

// newWriterは、handlerごとに独立した通知専用のinstanceを作り、entryを書き込む関数を返します。
func (t *NotificationTarget) newWriter() func(context.Context, *logging.LogEntry) {
	writer := t.newNotificationLogger()
	return func(ctx context.Context, entry *logging.LogEntry) {
		logging.EnsureEntryOccurredAt(entry, writer.Clock)
		// 書き込みの失敗はreportWriteErrorが失敗ごとに標準loggerへ残すため、ここでは扱いません。
		_ = writer.logToAutoSink(ctx, entry)
	}
}

// newNotificationLoggerは、通常出力とは独立した通知専用のStdoutLoggerを作ります。通知専用の
// instanceは通知先を持たず、書き込みの失敗を通知せずに失敗ごとに標準loggerへ一行残します。
// level filterとsamplingは適用しません（logToAutoSinkで書き込みます）。
func (t *NotificationTarget) newNotificationLogger() *StdoutLogger {
	return &StdoutLogger{
		Clock:            clock.NewUTCClock(),
		config:           t.config,
		formatter:        t.formatter,
		Tracer:           observer.NopTracer{},
		Meter:            observer.NopMeter{},
		notificationOnly: true,
	}
}

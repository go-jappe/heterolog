package clickhouse

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

type LoggerBase struct {
	Clock clock.Clock
	// LogFormatterはClickHouse recordのLineに記録する時刻表示規則を保持します。
	LogFormatter formatter.PlainTextFormatter
	config       ClickHouseConfigProvider

	OnError logging.LoggerErrorHandler
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc

	Tracer observer.Tracer
	Meter  observer.Meter

	// internalLogReentryは、On*へ渡すcontextへこのloggerの印を付け、通知を実行するgoroutineを記録します。
	// LoggerBaseはlogger instanceとそのFlushManagerだけが共有します。On*の中から同じloggerへの
	// 書き込みで発生した内部ログは、On*を再び呼ばずdirectInternalLogへ渡します（notifyError等）。
	internalLogReentry handler.ReentryGuard
	// directInternalLogは、再入した書き込みの内部ログを通知せず、このloggerのtableへ直接
	// 書き込みます（ClickHouseFlushManager.insertInternalLogDirectly）。constructorが
	// FlushManagerの構築後、loggerを公開する前に設定します。nilの場合は記録しません。
	directInternalLog handler.DirectInternalLogFunc
}

// notifyErrorは内部errorをOnErrorへ通知します。OnErrorの中から同じloggerへの書き込みで発生した
// 場合（再入）は、OnErrorを再び呼ばずdirectInternalLogへ渡します。notifyWarnも同じ規則です。
func (b *LoggerBase) notifyError(
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	b.internalLogReentry.CallErrorHandler(b.OnError, ctx, err, entry, tracer, meter, b.directInternalLog)
}

func (b *LoggerBase) notifyWarn(
	ctx context.Context,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	b.internalLogReentry.CallWarnHandler(b.OnWarn, ctx, entry, tracer, meter, b.directInternalLog)
}

// newDirectInternalLogWriterは、再入した書き込みの内部ログを設定したlog level以上の場合だけ、
// 内部ログを通知しないwriteで同じtableへ書き込む関数を返します。
func newDirectInternalLogWriter(
	cfg ClickHouseConfigProvider,
	write func(context.Context, *logging.LogEntry),
) handler.DirectInternalLogFunc {
	return func(ctx context.Context, entry *logging.LogEntry) {
		if cfg == nil || entry.Level < cfg.Runtime().LogLevel() {
			return
		}
		write(ctx, entry)
	}
}

func (b *LoggerBase) InternalDebugEntry(msg, requestID string) *logging.LogEntry {
	return b.internalEntry(pkglogger.LogLevelDebug, msg, requestID, nil)
}

func (b *LoggerBase) InternalInfoEntry(msg, requestID string) *logging.LogEntry {
	return b.internalEntry(pkglogger.LogLevelInfo, msg, requestID, nil)
}

func (b *LoggerBase) InternalWarnEntry(msg, requestID string) *logging.LogEntry {
	return b.internalEntry(pkglogger.LogLevelWarn, msg, requestID, nil)
}

func (b *LoggerBase) InternalErrorEntry(msg, requestID string, err error) *logging.LogEntry {
	return b.internalEntry(pkglogger.LogLevelError, msg, requestID, err)
}

func (b *LoggerBase) internalEntry(level pkglogger.LogLevel, msg, requestID string, err error) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestID,
		OccurredAt: logging.ResolveOccurredAt(b.Clock),
		Level:      level,
		Message:    msg,
		Err:        err,
		Internal:   true,
		Source:     common.SourceNameClickHouseLogger,
	}
}

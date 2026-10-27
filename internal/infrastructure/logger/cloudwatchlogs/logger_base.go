package cloudwatchlogs

import (
	"context"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

// LoggerBase は CloudWatch Logs logger 系で共有する設定、observer、handler を保持します。
// 通常 logger、delivery logger、fallback logger が同じ内部エントリ生成処理を使います。
type LoggerBase struct {
	Clock clock.Clock
	// LogFormatterはevent本文とbuffer byte計測に共通の時刻表示規則を保持します。
	LogFormatter formatter.PlainTextFormatter
	config       CloudWatchLogsConfigProvider

	OnError logging.LoggerErrorHandler
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc

	Tracer pkgobserver.Tracer
	Meter  pkgobserver.Meter

	// internalLogReentryは、On*へ渡すcontextへこのloggerの印を付け、通知を実行するgoroutineを記録します。
	// LoggerBaseはlogger instanceとそのFlushManagerだけが共有します。On*の中から同じloggerへの
	// 送信で発生した内部ログは、On*を再び呼ばずdirectInternalLogへ渡します（notifyError等）。
	internalLogReentry handler.ReentryGuard
	// directInternalLogは、再入した送信の内部ログを通知せず、このloggerのlog streamへ
	// FlushWithNoFallbackで直接書き込みます。constructorがFlushManagerの構築後、loggerを
	// 公開する前に設定します。nilの場合は記録しません。
	directInternalLog handler.DirectInternalLogFunc
}

// notifyErrorは内部errorをOnErrorへ通知します。OnErrorの中から同じloggerへの送信で発生した
// 場合（再入）は、OnErrorを再び呼ばずdirectInternalLogへ渡します。notifyWarnも同じ規則です。
func (b *LoggerBase) notifyError(
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) {
	b.internalLogReentry.CallErrorHandler(b.OnError, ctx, err, entry, tracer, meter, b.directInternalLog)
}

func (b *LoggerBase) notifyWarn(
	ctx context.Context,
	entry *logging.LogEntry,
	tracer pkgobserver.Tracer,
	meter pkgobserver.Meter,
) {
	b.internalLogReentry.CallWarnHandler(b.OnWarn, ctx, entry, tracer, meter, b.directInternalLog)
}

// newDirectInternalLogWriterは、再入した送信の内部ログを設定したlog level以上の場合だけ、
// 内部ログを通知しないflushで同じlog streamへ書き込む関数を返します。
func newDirectInternalLogWriter(
	cfg CloudWatchLogsConfigProvider,
	flushWithNoFallback func(context.Context, []*logging.LogEntry),
) handler.DirectInternalLogFunc {
	return func(ctx context.Context, entry *logging.LogEntry) {
		if cfg == nil || entry.Level < cfg.Runtime().LogLevel() {
			return
		}
		flushWithNoFallback(ctx, []*logging.LogEntry{entry})
	}
}

// InternalDebugEntry は logger 内部の debug 通知用 LogEntry を作ります。
func (b *LoggerBase) InternalDebugEntry(msg, requestID string) *logging.LogEntry {
	return b.internalEntry(pkglogger.LogLevelDebug, msg, requestID, nil)
}

// InternalInfoEntry は logger 内部の info 通知用 LogEntry を作ります。
func (b *LoggerBase) InternalInfoEntry(msg, requestID string) *logging.LogEntry {
	return b.internalEntry(pkglogger.LogLevelInfo, msg, requestID, nil)
}

// InternalWarnEntry は logger 内部の warn 通知用 LogEntry を作ります。
func (b *LoggerBase) InternalWarnEntry(msg, requestID string) *logging.LogEntry {
	return b.internalEntry(pkglogger.LogLevelWarn, msg, requestID, nil)
}

// InternalErrorEntry は logger 内部の error 通知用 LogEntry を作ります。
func (b *LoggerBase) InternalErrorEntry(msg, requestID string, err error) *logging.LogEntry {
	return b.internalEntry(pkglogger.LogLevelError, msg, requestID, err)
}

// internalEntry は外部から渡された user log と区別できるよう Internal=true を付けます。
func (b *LoggerBase) internalEntry(level pkglogger.LogLevel, msg, requestID string, err error) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestID,
		OccurredAt: logging.ResolveOccurredAt(b.Clock),
		Level:      level,
		Message:    msg,
		Err:        err,
		Internal:   true,
		Source:     common.SourceNameCloudWatchLogsLogger,
	}
}

package opensearch

import (
	"context"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/common"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// LoggerBase は共通のロギング機能を提供する基本構造体
type LoggerBase struct {
	Clock              clock.Clock
	TimestampFormatter timestamp.Formatter
	config             OpenSearchLoggerConfigProvider

	OnError logging.LoggerErrorHandler // 通知用の関数　内部エラー発生時にハンドル
	OnWarn  logging.LoggerHandlerFunc
	OnInfo  logging.LoggerHandlerFunc
	OnDebug logging.LoggerHandlerFunc

	Tracer observer.Tracer
	Meter  observer.Meter

	// internalLogReentryは、On*へ渡すcontextへこのloggerの印を付け、通知を実行するgoroutineを記録します。
	// LoggerBaseはlogger instanceとそのFlushManagerだけが共有します。On*の中から同じloggerへの
	// 送信で発生した内部ログは、On*を再び呼ばずdirectInternalLogへ渡します（notifyError等）。
	internalLogReentry handler.ReentryGuard
	// directInternalLogは、再入した送信の内部ログを通知せず、このloggerのindexへ直接
	// 書き込みます（OpenSearchFlushManager.indexInternalLogDirectly）。constructorが
	// FlushManagerの構築後、loggerを公開する前に設定します。nilの場合は記録しません。
	directInternalLog handler.DirectInternalLogFunc
}

// notifyErrorは内部errorをOnErrorへ通知します。OnErrorの中から同じloggerへの送信で発生した
// 場合（再入）は、OnErrorを再び呼ばずdirectInternalLogへ渡します。notifyWarn・notifyInfo・notifyDebugも同じ規則です。
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

func (b *LoggerBase) notifyInfo(
	ctx context.Context,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	b.internalLogReentry.CallInfoHandler(b.OnInfo, ctx, entry, tracer, meter, b.directInternalLog)
}

func (b *LoggerBase) notifyDebug(
	ctx context.Context,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
	isDebugMode bool,
) {
	b.internalLogReentry.CallDebugHandler(b.OnDebug, ctx, entry, tracer, meter, isDebugMode, b.directInternalLog)
}

// newDirectInternalLogWriterは、再入した送信の内部ログを設定したlog level以上の場合だけ、
// 内部ログを通知しないwriteで同じindexへ書き込む関数を返します。
func newDirectInternalLogWriter(
	cfg OpenSearchLoggerConfigProvider,
	write func(context.Context, *logging.LogEntry),
) handler.DirectInternalLogFunc {
	return func(ctx context.Context, entry *logging.LogEntry) {
		if cfg == nil || entry.Level < cfg.Runtime().LogLevel() {
			return
		}
		write(ctx, entry)
	}
}

// InternalDebugEntry はデバッグレベルのログエントリを生成します
func (b *LoggerBase) InternalDebugEntry(msg, requestId string) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(b.Clock),
		Level:      pkglogger.LogLevelDebug,
		Message:    msg,
		Internal:   true,
		Source:     common.SourceNameOpenSearchLogger,
	}
}

// InternalInfoEntry は情報レベルのログエントリを生成します
func (b *LoggerBase) InternalInfoEntry(msg, requestId string) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(b.Clock),
		Level:      pkglogger.LogLevelInfo,
		Message:    msg,
		Internal:   true,
		Source:     common.SourceNameOpenSearchLogger,
	}
}

// InternalWarnEntry は警告レベルのログエントリを生成します
func (b *LoggerBase) InternalWarnEntry(msg, requestId string) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(b.Clock),
		Level:      pkglogger.LogLevelWarn,
		Message:    msg,
		Internal:   true,
		Source:     common.SourceNameOpenSearchLogger,
	}
}

// InternalErrorEntry はエラーレベルのログエントリを生成します
func (b *LoggerBase) InternalErrorEntry(msg, requestId string, err error) *logging.LogEntry {
	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(b.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    msg,
		Err:        err,
		Internal:   true,
		Source:     common.SourceNameOpenSearchLogger,
	}
}

// InternalErrorEntryWithStack はスタックトレース付きのエラーログエントリを生成します
func (b *LoggerBase) InternalErrorEntryWithStack(msg, requestId string, err error, depth int) *logging.LogEntry {
	// 呼び出し元をdepthの数の深さまで取得
	var funcNames, sourceFiles []string
	var lines []int

	if depth > 0 {
		funcNames, sourceFiles, lines = logging.CollectCallerDetails(depth)
	}

	return &logging.LogEntry{
		RequestID:  requestId,
		OccurredAt: logging.ResolveOccurredAt(b.Clock),
		Level:      pkglogger.LogLevelError,
		Message:    msg,
		FuncName:   funcNames,
		File:       sourceFiles,
		Line:       lines,
		Err:        err,
		Internal:   true,
		Source:     common.SourceNameOpenSearchLogger,
	}
}

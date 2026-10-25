package file

import (
	"context"
	"errors"
	"log"

	applogging "github.com/go-jappe/heterolog/internal/application/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

// wrapAndDispatchErrorは、まだ操作名が付いていないfile logger内部errorをwrapし、
// 構築時に解決したinternal-log handlerへ委譲します。
// 呼び出し元は同じerrorを事前にwrapせず、この入口で一度だけwrapします。
func (l *FileLogger) wrapAndDispatchError(
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
) {
	l.dispatchError(ctx, l.wrapErrorByLogger(err, MsgDispatchError), entry)
}

// dispatchErrorは、既に操作名を付与したfile logger内部errorを、構築時に解決した
// internal-log handlerへ委譲します。file handlerも同じ経路を使うことで、
// 通常directory共有と専用internal-log directoryのどちらでも一度だけ記録します。
// 通知先へ渡すcontextにはこのFileLoggerの印を付けます（dispatchWarnなども同じです）。
func (l *FileLogger) dispatchError(
	ctx context.Context,
	wrappedErr error,
	entry *logging.LogEntry,
) {
	if l.OnError == nil {
		// 通知先がなければ再入も起きないため、印付けも記録もせず従来どおりの診断だけを残します。
		handler.CallErrorHandlerIfPresent(nil, ctx, wrappedErr, entry, l.Tracer, l.Meter)
		return
	}
	l.internalLogReentry.Dispatch(ctx, func(ctx context.Context) {
		handler.CallErrorHandlerIfPresent(l.OnError, ctx, wrappedErr, entry, l.Tracer, l.Meter)
	})
}

// dispatchWarnはwarn levelのinternal logを構築時に解決したhandlerへ委譲します。
func (l *FileLogger) dispatchWarn(
	ctx context.Context,
	entry *logging.LogEntry,
) {
	if l.OnWarn == nil {
		handler.CallWarnHandlerIfPresent(nil, ctx, entry, l.Tracer, l.Meter)
		return
	}
	l.internalLogReentry.Dispatch(ctx, func(ctx context.Context) {
		handler.CallWarnHandlerIfPresent(l.OnWarn, ctx, entry, l.Tracer, l.Meter)
	})
}

// dispatchInfoはinfo levelのinternal logを構築時に解決したhandlerへ委譲します。
func (l *FileLogger) dispatchInfo(
	ctx context.Context,
	entry *logging.LogEntry,
) {
	if l.OnInfo == nil {
		handler.CallInfoHandlerIfPresent(nil, ctx, entry, l.Tracer, l.Meter)
		return
	}
	l.internalLogReentry.Dispatch(ctx, func(ctx context.Context) {
		handler.CallInfoHandlerIfPresent(l.OnInfo, ctx, entry, l.Tracer, l.Meter)
	})
}

// dispatchDebugはdebug levelのinternal logを構築時に解決したhandlerへ委譲します。
func (l *FileLogger) dispatchDebug(
	ctx context.Context,
	entry *logging.LogEntry,
) {
	if l.OnDebug == nil {
		handler.CallDebugHandlerIfPresent(nil, ctx, entry, l.Tracer, l.Meter, l.config.Runtime().IsDebug())
		return
	}
	l.internalLogReentry.Dispatch(ctx, func(ctx context.Context) {
		handler.CallDebugHandlerIfPresent(l.OnDebug, ctx, entry, l.Tracer, l.Meter, l.config.Runtime().IsDebug())
	})
}

// reportWriteInternalLogは、書き込み処理（遅延・再試行・失敗・過負荷）が同期的に発生させた
// internal logをdispatchで通知します。このFileLoggerの通知の中から同じFileLoggerへ書き込んだ
// 場合（再入。通知へ渡したcontext、または通知を実行中の同じgoroutineで判別します）は、
// 同じhandlerを再び呼ばず、entryを同じfileへ直接記録します。background処理の通知は
// この判定を使いません。
// 呼び出し元はwriter lockを解放してから呼びます。
// handlerPresentがfalse（通知先がない）なら再入も起きないため、判定せずdispatchへ渡します。
func (l *FileLogger) reportWriteInternalLog(
	ctx context.Context,
	entry *logging.LogEntry,
	handlerPresent bool,
	dispatch func(),
) {
	if handlerPresent && l.internalLogReentry.Reentered(ctx) {
		l.writeReentrantInternalLog(ctx, entry)
		return
	}
	dispatch()
}

// wrapAndReportWriteErrorは、書き込み処理のerrorをreportWriteInternalLogの規則で
// wrapAndDispatchErrorへ渡すか、同じfileへ直接記録します。
func (l *FileLogger) wrapAndReportWriteError(
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
) {
	l.reportWriteInternalLog(ctx, entry, l.OnError != nil, func() {
		l.wrapAndDispatchError(ctx, err, entry)
	})
}

// writeReentrantInternalLogは、再入した書き込みのinternal logをwriteFallbackEntryで同じ
// 物理streamへ記録します。この書き込み自身のinternal logは記録も通知もしないため、再入は
// ここで終わります。internal logはこのstreamの状態を表すため、role（error・debugの分離）で
// 振り分けずにこのFileLoggerのfileへ書き込み、設定したlog levelより低いものだけを除きます。
// 過負荷のdropとClose開始後の拒否は通常のfallback書き込みと同じく報告しません。
// それ以外の記録失敗は、再入した書き込み自身の結果とは別に標準loggerへ残します。
func (l *FileLogger) writeReentrantInternalLog(
	ctx context.Context,
	entry *logging.LogEntry,
) {
	if entry == nil || entry.Level < l.config.Runtime().LogLevel() {
		return
	}
	localEntry := *entry
	logging.EnsureEntryOccurredAt(&localEntry, l.Clock)
	err := l.writeFallbackEntry(ctx, &localEntry)
	if err == nil || errors.Is(err, applogging.ErrLoggerClosed) || errors.Is(err, pkglogger.ErrLogDropped) {
		return
	}
	log.Printf(ErrorMsgWriteLog, err)
}

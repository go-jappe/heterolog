package fsarchivehandler

import (
	"context"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// ※ここで必ずラップするので、呼び出し元ではエラーをラップしない方針
// 内部エラー発生時、設定に応じて、別のロガーに転送する
func (f *FsArchiveHandler) delegateError(
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
) {
	// span開始
	ctx, span := f.Tracer.StartSpan(ctx, SpanDelegate)
	defer span.End()

	// 追加のエラーハンドラがあれば実行
	handler.CallErrorHandlerIfPresent(f.OnError, ctx, err, entry, f.Tracer, f.Meter)
}

// delegateWarnはwarn levelのinternal logを設定済みhandlerへ委譲します。
func (f *FsArchiveHandler) delegateWarn(
	ctx context.Context,
	entry *logging.LogEntry,
) {
	ctx, span := f.Tracer.StartSpan(ctx, SpanDelegate)
	defer span.End()

	handler.CallWarnHandlerIfPresent(f.OnWarn, ctx, entry, f.Tracer, f.Meter)
}

// デバッグログをデリゲートする必要があればそうする
// 自身でログするべき場合は自身で行う
func (f *FsArchiveHandler) delegateDebug(
	ctx context.Context,
	entry *logging.LogEntry,
) {

	// span開始
	ctx, span := f.Tracer.StartSpan(ctx, SpanDelegate)
	defer span.End()

	// 追加のエラーハンドラがあれば実行
	handler.CallDebugHandlerIfPresent(f.OnDebug, ctx, entry, f.Tracer, f.Meter, f.Config.IsDebug())
}

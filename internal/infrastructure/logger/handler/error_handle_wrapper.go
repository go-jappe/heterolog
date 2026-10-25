package handler

import (
	"context"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/formatter"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/timestamp"
	"log"
	"sync"
)

// diagnosticLogFormatterは、handler未設定時に標準loggerへ残す最終診断だけで使用します。
// applicationの保存先設定を持たない経路なので、UTC RFC3339をここで明示します。
var diagnosticLogFormatter = formatter.NewPlainTextFormatter(timestamp.RFC3339Formatter())

var onceErrorHandlerNilLog sync.Once
var onceWarnHandlerNilLog sync.Once
var onceInfoHandlerNilLog sync.Once
var onceDebugHandlerNilLog sync.Once

// ロガーのインターナルエラーのハンドラOnErrorを安全に実行する
func CallErrorHandlerIfPresent(
	handler func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter),
	ctx context.Context,
	err error,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	if handler != nil {
		handler(ctx, err, entry, tracer, meter)
		return
	}
	// OnErrorが未設定の場合の処理
	onceErrorHandlerNilLog.Do(func() {
		log.Printf("[INTERNAL][WARN] failed to handle OnError; OnError is nil;err type is [%v]: entry: %v\n", err, diagnosticLogFormatter.Format(entry))
	})
}

// ロガーのインターナルログのハンドラOnDebugを安全に実行する
func CallWarnHandlerIfPresent(
	handler func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter),
	ctx context.Context,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	if handler != nil {
		handler(ctx, entry, tracer, meter)
	} else {
		onceWarnHandlerNilLog.Do(func() {
			log.Printf("[INTERNAL][WARN] failed to handle OnWarn; OnWarn is nil; entry: %v\n", diagnosticLogFormatter.Format(entry))
		})
	}
}

// ロガーのインターナルログのハンドラOnDebugを安全に実行する
func CallInfoHandlerIfPresent(
	handler func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter),
	ctx context.Context,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
) {
	if handler != nil {
		handler(ctx, entry, tracer, meter)
	} else {
		onceInfoHandlerNilLog.Do(func() {
			log.Printf("[INTERNAL][WARN] failed to handle OnInfo; OnInfo is nil; entry: %v\n", diagnosticLogFormatter.Format(entry))
		})
	}
}

// ロガーのインターナルログのハンドラOnDebugを安全に実行する
func CallDebugHandlerIfPresent(
	handler func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter),
	ctx context.Context,
	entry *logging.LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
	isDebugMode bool,
) {
	if handler != nil {
		handler(ctx, entry, tracer, meter)
	} else if isDebugMode {
		onceDebugHandlerNilLog.Do(func() {
			log.Printf("[INTERNAL][WARN] failed to handle OnDebug; OnDebug is nil; entry: %v\n", diagnosticLogFormatter.Format(entry))
		})
	}
}

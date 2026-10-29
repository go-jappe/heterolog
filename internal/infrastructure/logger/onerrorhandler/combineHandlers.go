package onerrorhandler

import (
	"context"
	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/infrastructure/telemetry"
	"github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

// CombineErrorHandlersは、内部errorを選択済みの全handlerへ渡すhandlerを返します。
// entryなしで渡されたerrorも捨てず、そのerrorを持つ内部ERROR entryを作って渡します。
// entryとerrorの両方がnilの場合だけ、通知する内容がないため何もしません。
func CombineErrorHandlers(handlers ...logging.NamedLoggerErrorHandler) logging.LoggerErrorHandler {

	return func(ctx context.Context, err error, entry *logging.LogEntry, tracer pkgobserver.Tracer, meter pkgobserver.Meter) {
		if entry == nil {
			if err == nil {
				log.Printf("skip error handlers: nil log entry and nil error")
				return
			}
			entry = missingEntryErrorLogEntry(err)
		}
		telemetry.RecordInternalLog(ctx, meter, entry.Level)

		var wg sync.WaitGroup

		for _, handler := range handlers {

			if handler.Handler == nil {
				continue
			}

			handlerEntry := logging.CloneLogEntry(entry)
			wg.Add(1)
			go func(h logging.NamedLoggerErrorHandler, localEntry *logging.LogEntry) {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						log.Printf("panic in error handler %s: %v", h.Name, r)
					}
				}()
				handlerCtx, span := startNotifySpan(ctx, tracer, h.Name, localEntry.Level)
				defer span.End()
				h.Handler(handlerCtx, err, localEntry, tracer, meter)
			}(handler, handlerEntry)
		}

		wg.Wait()
	}
}

// missingEntryErrorLogEntryは、呼び出し元がentryを渡せなかったerrorの内部ERROR entryを
// 作ります。Clockを持たない経路なので発生時刻はここで一度だけ確定し、各handlerの
// cloneで同じ時刻を共有します。
func missingEntryErrorLogEntry(err error) *logging.LogEntry {
	return &logging.LogEntry{
		OccurredAt: time.Now().UTC(),
		Level:      logger.LogLevelError,
		Message:    MissingEntryErrorMessage,
		Err:        err,
		Internal:   true,
	}
}

// ハンドラーの内部ログを他のロガーで出力するための処理
func combineHandlers(
	expectedLevel logger.LogLevel,
	handlers []logging.NamedLoggerHandler,
) logging.LoggerHandlerFunc {
	return func(
		ctx context.Context,
		entry *logging.LogEntry,
		tracer pkgobserver.Tracer,
		meter pkgobserver.Meter,
	) {
		// fallback / internal handler は nil entry で落としてはいけない
		if entry == nil {
			log.Printf("skip handlers: nil log entry, expectedLevel=%v", expectedLevel)
			return
		}

		if entry.Level != expectedLevel {
			log.Printf(MismatchLevel+"\n", entry.Level, expectedLevel)
			return
		}
		telemetry.RecordInternalLog(ctx, meter, entry.Level)

		var wg sync.WaitGroup

		for _, handler := range handlers {
			if handler.Handler == nil {
				continue
			}

			handlerEntry := logging.CloneLogEntry(entry)
			wg.Add(1)

			go func(h logging.NamedLoggerHandler, localEntry *logging.LogEntry) {
				defer wg.Done()

				defer func() {
					if r := recover(); r != nil {
						log.Printf(
							"panic in handler %s: %v\n%s",
							h.Name,
							r,
							debug.Stack(),
						)
					}
				}()

				handlerCtx, span := startNotifySpan(ctx, tracer, h.Name, localEntry.Level)
				defer span.End()
				h.Handler(handlerCtx, localEntry, tracer, meter)
			}(handler, handlerEntry)
		}

		wg.Wait()
	}
}

// startNotifySpanは、内部ログをdestinationの通知先へ渡す処理のspanを始めます。通知先の
// panicをrecoverで止めるgoroutineの中で呼び、spanの開始で起きたpanicも同じrecoverで止めます。
func startNotifySpan(
	ctx context.Context,
	tracer pkgobserver.Tracer,
	destination string,
	level logger.LogLevel,
) (context.Context, pkgobserver.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	return observer.EnsureTracer(tracer).StartSpan(ctx, SpanInternalLogNotify,
		pkgobserver.Attr{Key: telemetry.SpanAttrDestination, Value: destination},
		pkgobserver.Attr{Key: telemetry.SpanAttrLevel, Value: telemetry.LevelName(level)},
	)
}

func CombineWarnHandlers(handlers ...logging.NamedLoggerHandler) logging.LoggerHandlerFunc {
	return combineHandlers(logger.LogLevelWarn, handlers)
}

func CombineInfoHandlers(handlers ...logging.NamedLoggerHandler) logging.LoggerHandlerFunc {
	return combineHandlers(logger.LogLevelInfo, handlers)
}

func CombineDebugHandlers(handlers ...logging.NamedLoggerHandler) logging.LoggerHandlerFunc {
	return combineHandlers(logger.LogLevelDebug, handlers)
}

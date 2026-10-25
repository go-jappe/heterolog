package logging

import (
	"context"
	"io"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/pkg/logger"
)

type LoggerErrorHandler func(
	ctx context.Context,
	err error,
	entry *LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
)

type NamedLoggerErrorHandler struct {
	// NameはHandlerConfigから選択するbackend名です。
	Name    string
	Handler LoggerErrorHandler
	// CloserはHandlerが使用するbufferやtimerなどの状態を終了します。
	// 終了処理が不要なstateless handlerではnilです。
	Closer io.Closer
}

type LoggerHandlerFunc func(
	ctx context.Context,
	entry *LogEntry,
	tracer observer.Tracer,
	meter observer.Meter,
)

type NamedLoggerHandler struct {
	// NameはHandlerConfigから選択するbackend名です。
	Name    string
	Handler LoggerHandlerFunc
	Level   logger.LogLevel
	// CloserはHandlerが使用するbufferやtimerなどの状態を終了します。
	// 終了処理が不要なstateless handlerではnilです。
	Closer io.Closer
}

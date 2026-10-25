package onerrorhandler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestCombinedHandlersReceiveIndependentLogEntries(t *testing.T) {
	original := &logging.LogEntry{
		Level:   pkglogger.LogLevelDebug,
		Message: "original",
		Fields:  map[string]any{"owner": "caller"},
	}
	received := make(chan *logging.LogEntry, 2)
	handler := func(
		_ context.Context,
		entry *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		entry.Message = "handler"
		entry.Fields["owner"] = "handler"
		received <- entry
	}

	CombineDebugHandlers(
		logging.NamedLoggerHandler{Name: "first", Handler: handler},
		logging.NamedLoggerHandler{Name: "second", Handler: handler},
	)(context.Background(), original, nil, nil)

	first := <-received
	second := <-received
	require.NotSame(t, original, first)
	require.NotSame(t, original, second)
	require.NotSame(t, first, second)
	require.Equal(t, "original", original.Message)
	require.Equal(t, map[string]any{"owner": "caller"}, original.Fields)
}

func TestCombinedErrorHandlersReceiveIndependentLogEntries(t *testing.T) {
	original := &logging.LogEntry{Message: "original"}
	received := make(chan *logging.LogEntry, 2)
	handler := func(
		_ context.Context,
		_ error,
		entry *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		entry.Message = "handler"
		received <- entry
	}

	CombineErrorHandlers(
		logging.NamedLoggerErrorHandler{Name: "first", Handler: handler},
		logging.NamedLoggerErrorHandler{Name: "second", Handler: handler},
	)(context.Background(), errors.New("trigger"), original, nil, nil)

	first := <-received
	second := <-received
	require.NotSame(t, original, first)
	require.NotSame(t, original, second)
	require.NotSame(t, first, second)
	require.Equal(t, "original", original.Message)
}

// entryなしで渡されたerrorも捨てず、そのerrorを持つ内部ERROR entryとして全handlerへ渡します。
func TestCombinedErrorHandlersReportErrorWithoutLogEntry(t *testing.T) {
	trigger := errors.New("trigger")
	received := make(chan *logging.LogEntry, 2)
	receivedErrs := make(chan error, 2)
	record := func(
		_ context.Context,
		err error,
		entry *logging.LogEntry,
		_ observer.Tracer,
		_ observer.Meter,
	) {
		receivedErrs <- err
		received <- entry
	}
	handler := CombineErrorHandlers(
		logging.NamedLoggerErrorHandler{Name: "first", Handler: record},
		logging.NamedLoggerErrorHandler{Name: "second", Handler: record},
	)

	require.NotPanics(t, func() {
		handler(context.Background(), trigger, nil, nil, nil)
	})
	require.Len(t, received, 2)
	var occurredAt []time.Time
	for range 2 {
		require.Same(t, trigger, <-receivedErrs)
		entry := <-received
		require.NotNil(t, entry)
		require.Equal(t, pkglogger.LogLevelError, entry.Level)
		require.Equal(t, MissingEntryErrorMessage, entry.Message)
		require.Same(t, trigger, entry.Err)
		require.True(t, entry.Internal)
		require.False(t, entry.OccurredAt.IsZero())
		occurredAt = append(occurredAt, entry.OccurredAt)
	}
	require.Equal(t, occurredAt[0], occurredAt[1], "handlers must share one occurrence time")
}

// entryもerrorもない呼び出しには通知する内容がないため、handlerを呼びません。
func TestCombinedErrorHandlersSkipNilLogEntryWithoutError(t *testing.T) {
	called := false
	handler := CombineErrorHandlers(logging.NamedLoggerErrorHandler{
		Name: "handler",
		Handler: func(
			context.Context,
			error,
			*logging.LogEntry,
			observer.Tracer,
			observer.Meter,
		) {
			called = true
		},
	})

	require.NotPanics(t, func() {
		handler(context.Background(), nil, nil, nil, nil)
	})
	require.False(t, called)
}

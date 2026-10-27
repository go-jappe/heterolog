package logger

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestFailureStoreInternalLogHandlerFactorySelectsOnlyConfiguredDestinations(t *testing.T) {
	var mu sync.Mutex
	calls := make(map[string]int)
	record := func(name string) {
		mu.Lock()
		defer mu.Unlock()
		calls[name]++
	}

	available := handlerStruct{
		OnError: []logging.NamedLoggerErrorHandler{
			{Name: "stdout", Handler: func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
				record("error:stdout")
			}},
			{Name: "slack", Handler: func(context.Context, error, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
				record("error:slack")
			}},
		},
		OnWarn: []logging.NamedLoggerHandler{
			{Name: "stdout", Handler: func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) { record("warn:stdout") }},
			{Name: "slack", Handler: func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) { record("warn:slack") }},
		},
		OnInfo: []logging.NamedLoggerHandler{
			{Name: "stdout", Handler: func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) { record("info:stdout") }},
		},
		OnDebug: []logging.NamedLoggerHandler{
			{Name: "stdout", Handler: func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
				record("debug:stdout")
			}},
		},
	}
	cfg := loggerhandler.NewConfigAdapter(pkgloggerhandler.HandlerConfig{
		OnErrorConfig: []string{" stdout ", "unknown", "stdout"},
		OnWarnConfig:  []string{"slack"},
		OnInfoConfig:  []string{"unknown", "stdout"},
		OnDebugConfig: []string{"STDOUT"},
	})

	resolved := failureStoreInternalLogHandlerFactory("opensearch", func(string) handlerStruct {
		return available
	})(pkgdurable.DurableTypeFileArchive, cfg)
	if resolved.OnError == nil || resolved.OnWarn == nil || resolved.OnInfo == nil || resolved.OnDebug == nil {
		t.Fatal("configured failure store callbacks must be resolved")
	}

	resolved.OnError(context.Background(), errors.New("archive failed"), &logging.LogEntry{Level: pkglogger.LogLevelError}, observer.NopTracer{}, observer.NopMeter{})
	resolved.OnWarn(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelWarn}, observer.NopTracer{}, observer.NopMeter{})
	resolved.OnInfo(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelInfo}, observer.NopTracer{}, observer.NopMeter{})
	resolved.OnDebug(context.Background(), &logging.LogEntry{Level: pkglogger.LogLevelDebug}, observer.NopTracer{}, observer.NopMeter{})

	want := map[string]int{"error:stdout": 1, "warn:slack": 1, "info:stdout": 1, "debug:stdout": 1}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != len(want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
	for name, count := range want {
		if calls[name] != count {
			t.Fatalf("calls[%q] = %d, want %d; all calls=%#v", name, calls[name], count, calls)
		}
	}
}

package logger

import (
	"context"
	"slices"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
)

func TestSelectConfiguredHandlersSelectsSourcesIndependentlyForEachLevel(t *testing.T) {
	errorHandler := func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {}
	levelHandler := func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) {}
	available := handlerStruct{
		OnError: []logging.NamedLoggerErrorHandler{
			{Name: " stdout ", Handler: errorHandler},
			{Name: "STDOUT", Handler: errorHandler},
			{Name: "slack"},
			{Name: "SLACK", Handler: errorHandler},
		},
		OnWarn: []logging.NamedLoggerHandler{
			{Name: "stdout", Handler: levelHandler, Level: pkglogger.LogLevelWarn},
			{Name: "slack", Handler: levelHandler, Level: pkglogger.LogLevelWarn},
		},
		OnInfo: []logging.NamedLoggerHandler{
			{Name: "loki", Handler: levelHandler, Level: pkglogger.LogLevelInfo},
			{Name: "stdout", Handler: levelHandler, Level: pkglogger.LogLevelInfo},
		},
		OnDebug: []logging.NamedLoggerHandler{
			{Name: "file", Handler: levelHandler, Level: pkglogger.LogLevelDebug},
			{Name: "stdout", Handler: levelHandler, Level: pkglogger.LogLevelDebug},
		},
	}
	cfg := loggerhandler.NewConfigAdapter(pkgloggerhandler.HandlerConfig{
		OnErrorConfig: []string{"SLACK", "stdout", "slack", "unknown"},
		OnWarnConfig:  []string{" stdout "},
		OnInfoConfig:  []string{"loki"},
		OnDebugConfig: []string{"FILE", "missing"},
	})

	selected := selectConfiguredHandlers(available, cfg)

	assertErrorHandlerNames(t, selected.OnError, []string{"SLACK", " stdout "})
	assertHandlerNames(t, selected.OnWarn, []string{"stdout"})
	assertHandlerNames(t, selected.OnInfo, []string{"loki"})
	assertHandlerNames(t, selected.OnDebug, []string{"file"})

	// 選択結果は新しいsliceなので、変更しても全候補を書き換えません。
	selected.OnWarn[0].Name = "changed"
	if available.OnWarn[0].Name != "stdout" {
		t.Fatalf("available OnWarn was mutated: %+v", available.OnWarn)
	}
}

func TestSelectConfiguredHandlersReturnsEmptySelectionForNilConfig(t *testing.T) {
	errorHandler := func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {}
	levelHandler := func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) {}
	available := handlerStruct{
		OnError: []logging.NamedLoggerErrorHandler{{Name: "stdout", Handler: errorHandler}},
		OnWarn:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: levelHandler}},
		OnInfo:  []logging.NamedLoggerHandler{{Name: "stdout", Handler: levelHandler}},
		OnDebug: []logging.NamedLoggerHandler{{Name: "stdout", Handler: levelHandler}},
	}

	selected := selectConfiguredHandlers(available, nil)

	if len(selected.OnError) != 0 ||
		len(selected.OnWarn) != 0 ||
		len(selected.OnInfo) != 0 ||
		len(selected.OnDebug) != 0 {
		t.Fatalf("selected handlers = %+v, want empty", selected)
	}
}

func TestFailureStoreInternalLogHandlerFactoryBuildsFreshCandidatesForEachStore(t *testing.T) {
	buildCount := 0
	var calledBuilds []int
	var sources []string
	newAvailable := func(source string) handlerStruct {
		buildCount++
		sources = append(sources, source)
		currentBuild := buildCount
		return handlerStruct{
			OnInfo: []logging.NamedLoggerHandler{{
				Name: "stdout",
				Handler: func(
					context.Context,
					*logging.LogEntry,
					observer.Tracer,
					observer.Meter,
				) {
					calledBuilds = append(calledBuilds, currentBuild)
				},
				Level: pkglogger.LogLevelInfo,
			}},
		}
	}
	cfg := loggerhandler.NewConfigAdapter(pkgloggerhandler.HandlerConfig{
		OnInfoConfig: []string{"stdout"},
	})
	create := failureStoreInternalLogHandlerFactory("opensearch", newAvailable)

	first := create(pkgdurable.DurableTypeSpool, cfg)
	second := create(pkgdurable.DurableTypeFileArchive, cfg)

	if buildCount != 2 {
		t.Fatalf("fallback candidate build count = %d, want 2", buildCount)
	}
	wantSources := []string{
		"opensearch.failure_store.spool",
		"opensearch.failure_store.fileArchive",
	}
	if !slices.Equal(sources, wantSources) {
		t.Fatalf("fallback handler sources = %v, want %v", sources, wantSources)
	}
	first.OnInfo(
		context.Background(),
		&logging.LogEntry{Level: pkglogger.LogLevelInfo},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	second.OnInfo(
		context.Background(),
		&logging.LogEntry{Level: pkglogger.LogLevelInfo},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	if len(calledBuilds) != 2 || calledBuilds[0] != 1 || calledBuilds[1] != 2 {
		t.Fatalf("called fallback candidate builds = %v, want [1 2]", calledBuilds)
	}
}

func assertErrorHandlerNames(
	t *testing.T,
	handlers []logging.NamedLoggerErrorHandler,
	want []string,
) {
	t.Helper()
	got := make([]string, 0, len(handlers))
	for _, handler := range handlers {
		got = append(got, handler.Name)
	}
	assertHandlerNameList(t, got, want)
}

func assertHandlerNames(
	t *testing.T,
	handlers []logging.NamedLoggerHandler,
	want []string,
) {
	t.Helper()
	got := make([]string, 0, len(handlers))
	for _, handler := range handlers {
		got = append(got, handler.Name)
	}
	assertHandlerNameList(t, got, want)
}

func assertHandlerNameList(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("handler names = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("handler names = %v, want %v", got, want)
		}
	}
}

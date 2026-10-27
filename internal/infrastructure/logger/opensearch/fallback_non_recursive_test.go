package opensearch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	infraopensearch "github.com/go-jappe/heterolog/internal/infrastructure/logbackend/opensearch"
	loggerfailure "github.com/go-jappe/heterolog/internal/infrastructure/logger/failure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/flushfailure"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
	pkgopensearch "github.com/go-jappe/heterolog/pkg/opensearch"
)

func TestOpenSearchFallbackBackendFailureDoesNotInvokeInternalOrFailureHandlers(t *testing.T) {
	cfg := NewMockOpenSearchConfigProvider()
	clientCfg := infraopensearch.NewOpenSearchClientConfigAdapter(&pkgopensearch.Config{
		Endpoint: "http://127.0.0.1:9200",
		Timeout:  time.Second,
		AuthConfig: pkgopensearch.AuthConfig{
			Type: pkgopensearch.AuthTypeNone,
		},
	})
	resourceProvider := NewMockResourceProvider()
	tracer, meter := newTestObserver()

	fallback, err := newFallbackLogger(
		cfg,
		clientCfg,
		nil,
		cfg,
		resourceProvider,
		tracer,
		meter,
	)
	if err != nil {
		t.Fatalf("create OpenSearch fallback logger: %v", err)
	}

	flushManager, ok := fallback.flushManager.(*OpenSearchFlushManager)
	if !ok {
		t.Fatalf("fallback flush manager type = %T, want *OpenSearchFlushManager", fallback.flushManager)
	}
	if flushManager.FailureHandler != nil || flushManager.FailureReportHandler != nil {
		t.Fatal("fallback flush manager must not wire a failure handler")
	}

	// production factoryはfailure handlerを配線しません。fallbackの送信経路が通常Flushの
	// failure処理へ戻った場合に検出できるよう、factoryが作ったmanagerへprobeを接続します。
	failureProbe := &openSearchFallbackFailureHandlerProbe{}
	failureHandler := mustNewOpenSearchFailureComposite(
		[]flushfailure.FailureHandler{failureProbe},
	)
	flushManager.FailureHandler = failureHandler
	backend := &MockFlushManagerClient{
		bulkErr: errors.New("OpenSearch backend is unavailable"),
	}
	flushManager.osClient = backend

	var internalHandlerCalls atomic.Int32
	fallback.OnError = func(
		context.Context,
		error,
		*logging.LogEntry,
		pkgobserver.Tracer,
		pkgobserver.Meter,
	) {
		internalHandlerCalls.Add(1)
	}
	levelHandler := func(
		context.Context,
		*logging.LogEntry,
		pkgobserver.Tracer,
		pkgobserver.Meter,
	) {
		internalHandlerCalls.Add(1)
	}
	fallback.OnWarn = levelHandler
	fallback.OnInfo = levelHandler
	fallback.OnDebug = levelHandler

	fallback.enqueueProcess(&logging.LogEntry{
		OccurredAt: fallback.Clock.Now(),
		Level:      pkglogger.LogLevelError,
		Message:    "fallback recursion guard test",
		Source:     "test",
		Internal:   true,
	})
	if err := fallback.Close(context.Background()); err != nil {
		t.Fatalf("close OpenSearch fallback logger: %v", err)
	}

	if backend.IndexDocumentsCallCount != 1 {
		t.Fatalf("OpenSearch bulk backend call count = %d, want 1", backend.IndexDocumentsCallCount)
	}
	if backend.IndexDocumentCallCount != 0 {
		t.Fatalf("OpenSearch single-document backend call count = %d, want 0", backend.IndexDocumentCallCount)
	}
	if err := failureHandler.Drain(context.Background()); err != nil {
		t.Fatalf("drain OpenSearch failure handler: %v", err)
	}
	if got := failureProbe.calls.Load(); got != 0 {
		t.Fatalf("failure handler call count = %d, want 0", got)
	}
	if got := internalHandlerCalls.Load(); got != 0 {
		t.Fatalf("On* handler call count = %d, want 0", got)
	}
}

type openSearchFallbackFailureHandlerProbe struct {
	calls atomic.Int32
}

func (h *openSearchFallbackFailureHandlerProbe) HandleFailure(
	context.Context,
	loggerfailure.SendResult,
) error {
	h.calls.Add(1)
	return nil
}

var _ flushfailure.FailureHandler = (*openSearchFallbackFailureHandlerProbe)(nil)

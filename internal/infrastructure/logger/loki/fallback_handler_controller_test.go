package loki

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloki "github.com/go-jappe/heterolog/pkg/loki"

	"github.com/stretchr/testify/require"
)

func TestNamedFallbackHandlersExposeCloser(t *testing.T) {
	cfg := newTestLokiLoggerConfig(100, time.Second, false)
	clientCfg := &pkgloki.Config{Endpoint: "http://127.0.0.1:3100"}
	fmCfg := newTestLokiFlushManagerConfig()
	resourceProvider := newTestLokiResourceProvider()

	closers := []io.Closer{
		NewNamedErrorFallbackHandler(cfg, clientCfg, fmCfg, resourceProvider, nil).Closer,
		NewNamedWarnFallbackHandler(cfg, clientCfg, fmCfg, resourceProvider, nil).Closer,
		NewNamedInfoFallbackHandler(cfg, clientCfg, fmCfg, resourceProvider, nil).Closer,
		NewNamedDebugFallbackHandler(cfg, clientCfg, fmCfg, resourceProvider, nil).Closer,
	}
	for index, closer := range closers {
		require.NotNilf(t, closer, "fallback handler[%d] must expose its Closer", index)
		require.NoError(t, closer.Close(), "Close before lazy initialization must succeed")
	}
}

func TestNamedFallbackHandlerCloserClosesOwnedLokiLogger(t *testing.T) {
	cfg := newTestLokiLoggerConfig(100, time.Second, false)
	named := NewNamedInfoFallbackHandler(
		cfg,
		&pkgloki.Config{Endpoint: "http://127.0.0.1:3100"},
		newTestLokiFlushManagerConfig(),
		newTestLokiResourceProvider(),
		nil,
	)
	controller, ok := named.Closer.(*fallbackHandlerController)
	require.True(t, ok)

	manager := newLifecycleLokiFlushManager(false)
	fallbackLogger := newLokiFallbackBatchTestLogger(cfg, manager)
	fallbackLogger.startFallbackWorker()

	controller.mu.Lock()
	controller.initializationAttempted = true
	controller.logger = fallbackLogger
	controller.mu.Unlock()

	entry := &logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "before close"}
	named.Handler(
		context.Background(),
		entry,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Equal(t, 0, manager.snapshot().immediateCalls, "handler must only enqueue")

	require.NoError(t, named.Closer.Close())
	require.Equal(t, 1, manager.snapshot().immediateCalls, "Close must drain the queued batch")
	require.Equal(t, 1, manager.snapshot().closeCalls)

	named.Handler(
		context.Background(),
		&logging.LogEntry{Level: pkglogger.LogLevelInfo, Message: "after close"},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.Equal(t, 1, manager.snapshot().immediateCalls, "handler must not send after Close")
	require.NoError(t, named.Closer.Close())
	require.Equal(t, 1, manager.snapshot().closeCalls, "underlying logger Close must remain idempotent")
}

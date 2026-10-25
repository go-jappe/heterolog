package cloudwatchlogs

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgcloudwatchlogs "github.com/go-jappe/heterolog/pkg/cloudwatchlogs"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"

	"github.com/stretchr/testify/require"
)

func TestNamedFallbackHandlersExposeCloser(t *testing.T) {
	cfg := newTestCloudWatchLogsLoggerConfig(100, time.Second, false)
	clientCfg := fallbackHandlerCloseCloudWatchLogsClientConfig()
	fmCfg := newTestCloudWatchLogsFlushManagerConfig()
	resourceProvider := newTestCloudWatchLogsResourceProvider()

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

func TestNamedFallbackHandlerCloserClosesOwnedCloudWatchLogsLogger(t *testing.T) {
	cfg := newTestCloudWatchLogsLoggerConfig(100, time.Second, false)
	named := NewNamedInfoFallbackHandler(
		cfg,
		fallbackHandlerCloseCloudWatchLogsClientConfig(),
		newTestCloudWatchLogsFlushManagerConfig(),
		newTestCloudWatchLogsResourceProvider(),
		nil,
	)
	controller, ok := named.Closer.(*fallbackHandlerController)
	require.True(t, ok)

	fallbackLogger := &FallbackLogger{
		CloudWatchLogsLogger: newLifecycleCloudWatchLogsLogger(t, time.Hour),
	}
	manager := newLifecycleCloudWatchLogsFlushManager(false)
	fallbackLogger.mu.Lock()
	fallbackLogger.flushManager = manager
	fallbackLogger.mu.Unlock()

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

func fallbackHandlerCloseCloudWatchLogsClientConfig() *pkgcloudwatchlogs.Config {
	return &pkgcloudwatchlogs.Config{
		Region:          "ap-northeast-1",
		Endpoint:        "http://127.0.0.1:4566",
		LogGroupName:    "/test/fallback",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
	}
}

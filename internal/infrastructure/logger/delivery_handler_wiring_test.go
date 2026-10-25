package logger

import (
	"context"
	"io"
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	clickhouselogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/clickhouse"
	cloudwatchlogslogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/cloudwatchlogs"
	filelogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/file"
	loggerhandler "github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	lokilogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/loki"
	opensearchlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/opensearch"
	slacklogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/slack"
	stdoutlogger "github.com/go-jappe/heterolog/internal/infrastructure/logger/stdout"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"

	"github.com/stretchr/testify/require"
)

func TestDeliveryFallbackHandlersKeepIndependentLevelControllers(t *testing.T) {
	t.Parallel()
	cfg := pkglogger.DeliveryLoggerConfig{
		Stdout: newActivationDeliveryStdoutConfig(),
		Slack:  &pkglogger.DeliverySlackLoggerConfig{},
	}
	// stdoutの通知先は、本番と同じくloggerの構築時に一度だけ作ります。
	stdoutTarget, err := newDeliveryStdoutNotificationTarget(cfg)
	require.NoError(t, err)
	// 設定providerを共有しても、levelと呼び出し元が違えばhandlerの状態は分離します。
	seen := make(map[io.Closer]struct{})
	for range 2 {
		handlers := getDeliveryHandler(cfg, streamrouting.None, nil, stdoutTarget)
		require.Len(t, handlers.OnError, 2)
		for _, named := range handlers.OnError {
			require.NotNil(t, named.Handler)
		}
		for _, group := range []struct {
			level pkglogger.LogLevel
			named []logging.NamedLoggerHandler
		}{
			{pkglogger.LogLevelWarn, handlers.OnWarn},
			{pkglogger.LogLevelInfo, handlers.OnInfo},
			{pkglogger.LogLevelDebug, handlers.OnDebug},
		} {
			require.Len(t, group.named, 2)
			for _, named := range group.named {
				require.Equal(t, group.level, named.Level)
				require.NotNil(t, named.Handler)
			}
		}
		for _, closer := range []io.Closer{
			handlers.OnError[1].Closer, handlers.OnWarn[1].Closer,
			handlers.OnInfo[1].Closer, handlers.OnDebug[1].Closer,
		} {
			require.NotNil(t, closer)
			_, duplicate := seen[closer]
			require.False(t, duplicate, "Slack fallback controller must not be shared")
			seen[closer] = struct{}{}
			require.NoError(t, closer.Close())
		}
	}
}

func TestDeliverySourceAdaptersSelectHandlerTargetsIndependentlyByLevel(t *testing.T) {
	handlerCfg := pkgloggerhandler.HandlerConfig{
		OnErrorConfig: []string{"stdout"},
		OnWarnConfig:  []string{"slack"},
		OnInfoConfig:  []string{"file"},
		OnDebugConfig: []string{"clickhouse"},
	}
	available := handlerStruct{
		OnError: []logging.NamedLoggerErrorHandler{{
			Name: "stdout",
			Handler: func(
				context.Context,
				error,
				*logging.LogEntry,
				observer.Tracer,
				observer.Meter,
			) {
			},
		}},
		OnWarn: []logging.NamedLoggerHandler{{
			Name:    "slack",
			Level:   pkglogger.LogLevelWarn,
			Handler: func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) {},
		}},
		OnInfo: []logging.NamedLoggerHandler{{
			Name:    "file",
			Level:   pkglogger.LogLevelInfo,
			Handler: func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) {},
		}},
		OnDebug: []logging.NamedLoggerHandler{{
			Name:    "clickhouse",
			Level:   pkglogger.LogLevelDebug,
			Handler: func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) {},
		}},
	}

	tests := []struct {
		name     string
		provider loggerhandler.ConfigProvider
	}{
		{
			name: "file",
			provider: filelogger.NewDeliveryFileLoggerConfigAdapter(
				pkglogger.DeliveryFileLoggerConfig{HandlerConfig: handlerCfg},
			).Handler(),
		},
		{
			name: "stdout",
			provider: stdoutlogger.NewDeliveryConfigAdapter(
				pkglogger.DeliveryStdoutLoggerConfig{HandlerConfig: handlerCfg},
			).Handler(),
		},
		{
			name: "slack",
			provider: slacklogger.NewDeliveryConfigAdapter(
				pkglogger.DeliverySlackLoggerConfig{HandlerConfig: handlerCfg},
			).Handler(),
		},
		{
			name: "opensearch",
			provider: opensearchlogger.NewDeliveryOpenSearchLoggerConfigAdapter(
				pkglogger.DeliveryOpenSearchLoggerBaseConfig{HandlerConfig: handlerCfg},
			).Handler(),
		},
		{
			name: "loki",
			provider: lokilogger.NewDeliveryLokiLoggerConfigAdapter(
				pkglogger.DeliveryLokiLoggerBaseConfig{HandlerConfig: handlerCfg},
			).Handler(),
		},
		{
			name: "cloudwatchlogs",
			provider: cloudwatchlogslogger.NewDeliveryCloudWatchLogsLoggerConfigAdapter(
				pkglogger.DeliveryCloudWatchLogsLoggerBaseConfig{HandlerConfig: handlerCfg},
			).Handler(),
		},
		{
			name: "clickhouse",
			provider: clickhouselogger.NewDeliveryClickHouseLoggerConfigAdapter(
				pkglogger.DeliveryClickHouseLoggerBaseConfig{HandlerConfig: handlerCfg},
			).Handler(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selected := selectConfiguredHandlers(available, tt.provider)
			require.Equal(t, "stdout", selected.OnError[0].Name)
			require.Equal(t, "slack", selected.OnWarn[0].Name)
			require.Equal(t, "file", selected.OnInfo[0].Name)
			require.Equal(t, "clickhouse", selected.OnDebug[0].Name)
		})
	}
}

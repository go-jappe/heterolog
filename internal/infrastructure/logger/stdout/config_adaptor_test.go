package stdout

import (
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/resource"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"
	"github.com/go-jappe/heterolog/pkg/otel"

	"github.com/stretchr/testify/require"
)

func TestDeliveryFallbackConfigAdapterPreservesSettings(t *testing.T) {
	t.Parallel()
	cfg := pkglogger.DeliveryStdoutLoggerConfig{
		TimeConfig: pkglogger.TimeConfig{TimeZone: "UTC"},
		RuntimeConfig: pkglogger.DeliveryRuntimeConfig{
			AppName: "delivery-fallback", IsDebug: true, Target: "test",
		},
		LogBodyConfig: pkglogger.LogBodyConfig{
			LogLineDateFormat: time.RFC3339Nano, ErrorTraceDepth: 2, ErrorTraceDepthDebugMode: 3,
		},
		HandlerConfig:          pkgloggerhandler.HandlerConfig{OnErrorConfig: []string{"slack"}},
		UseOpenTelemetryFormat: true,
	}
	adapter := NewDeliveryFallbackConfigAdapter(cfg)
	require.Equal(t, cfg.TimeConfig.TimeZone, adapter.Time().TimeZone())
	require.Equal(t, cfg.RuntimeConfig.AppName, adapter.cfg.RuntimeConfig.AppName)
	require.Equal(t, cfg.RuntimeConfig.IsDebug, adapter.Runtime().IsDebug())
	require.Equal(t, cfg.RuntimeConfig.Target, adapter.Runtime().Target())
	require.Equal(t, pkglogger.LogLevelDebug, adapter.Runtime().LogLevel())
	require.Equal(t, cfg.LogBodyConfig.LogLineDateFormat, adapter.LogBody().LogLineDateFormat())
	require.Equal(t, cfg.LogBodyConfig.ErrorTraceDepth, adapter.LogBody().ErrorTraceDepth())
	require.Equal(t, cfg.LogBodyConfig.ErrorTraceDepthDebugMode, adapter.LogBody().ErrorTraceDepthDebugMode())
	require.Equal(t, cfg.HandlerConfig.OnErrorConfig, adapter.Handler().OnError())
	require.True(t, adapter.UseOpenTelemetryFormat())
	require.False(t, adapter.Sampling().IsSamplingEnabled())
	require.False(t, adapter.Sampling().EnableLoggingByRequestID())

	// 通知先設定を保持していても、通知専用のinstance自身は別のhandlerへ再通知しません。
	target, err := NewNotificationTarget(adapter, resource.NewOtelResourceAdapter(&otel.Resource{}))
	require.NoError(t, err)
	fallback := target.newNotificationLogger()
	require.True(t, fallback.notificationOnly)
	require.Nil(t, fallback.OnError)
	require.Nil(t, fallback.OnWarn)
	require.Nil(t, fallback.OnInfo)
	require.Nil(t, fallback.OnDebug)
}

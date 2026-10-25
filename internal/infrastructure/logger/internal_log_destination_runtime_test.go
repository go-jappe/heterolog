package logger

import (
	"testing"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logbackend/cloudwatchlogs/streamrouting"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgloggerhandler "github.com/go-jappe/heterolog/pkg/logger/handler"

	"github.com/stretchr/testify/require"
)

func TestInternalLogDestinationRuntimeMakesDedicatedFileTerminalWithoutMutatingConfig(
	t *testing.T,
) {
	base := runnerFileConfigForInternalLogRuntime(t.TempDir(), "terminal-sync-base")
	internalFile := dedicatedInternalLogFileConfig(base, t.TempDir())
	internalFile.LifecycleHandlerConfig = pkgloggerhandler.HandlerConfig{
		OnErrorConfig: []string{pkglogger.LogOutputStdout.String()},
	}

	runtime, err := newInternalLogDestinationRuntime(
		pkglogger.LoggerConfig{
			File: base,
			InternalLogDestinations: &pkglogger.InternalLogDestinationConfig{
				File: internalFile,
			},
		},
		streamrouting.None,
		nil,
		syncInternalLogFileDirectoryScopeLabel,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	require.Equal(
		t,
		[]string{pkglogger.LogOutputStdout.String()},
		internalFile.LifecycleHandlerConfig.OnErrorConfig,
		"caller-owned config must not be mutated",
	)
	require.Empty(t, runtime.dedicatedFileConfig().LifecycleHandlerConfig.OnErrorConfig)
}

func TestDeliveryInternalLogDestinationRuntimeMakesDedicatedFileTerminalWithoutMutatingConfig(
	t *testing.T,
) {
	base := runnerDeliveryFileConfigForInternalLogRuntime(
		t.TempDir(),
		"terminal-delivery-base",
	)
	internalFile := dedicatedDeliveryInternalLogFileConfig(base, t.TempDir())
	internalFile.LifecycleRunnerSettings.LifecycleHandlerConfig =
		pkgloggerhandler.HandlerConfig{
			OnErrorConfig: []string{pkglogger.LogOutputStdout.String()},
		}

	runtime, err := newDeliveryInternalLogDestinationRuntime(
		pkglogger.DeliveryLoggerConfig{
			File: base,
			InternalLogDestinations: &pkglogger.DeliveryInternalLogDestinationConfig{
				File: internalFile,
			},
		},
		streamrouting.None,
		nil,
		deliveryInternalLogFileDirectoryScopeLabel,
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	require.Equal(
		t,
		[]string{pkglogger.LogOutputStdout.String()},
		internalFile.LifecycleRunnerSettings.LifecycleHandlerConfig.OnErrorConfig,
		"caller-owned config must not be mutated",
	)
	require.Empty(
		t,
		runtime.dedicatedFileConfig().
			LifecycleRunnerSettings.LifecycleHandlerConfig.OnErrorConfig,
	)
}

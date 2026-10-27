package slack

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/require"
)

func TestConstructorsConfigureInternalLogHandlers(t *testing.T) {
	t.Parallel()
	for _, delivery := range []bool{false, true} {
		name := "normal"
		if delivery {
			name = "delivery"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tracer, meter := observer.NopTracer{}, observer.NopMeter{}
			cause := errors.New("internal error")
			var calls []string
			onError := func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				require.ErrorIs(t, err, cause)
				calls = append(calls, "error")
			}
			handler := func(level string) logging.LoggerHandlerFunc {
				return func(context.Context, *logging.LogEntry, pkgobserver.Tracer, pkgobserver.Meter) {
					calls = append(calls, level)
				}
			}
			option := WithInternalLogHandlers(onError, handler("warn"), handler("info"), handler("debug"))
			cfg := normalSlackLifecycleConfig([]time.Duration{time.Second})
			var configured internalLogHandlers
			if delivery {
				logger, err := NewDeliverySlackLogger(ctx, NewDeliveryConfigAdapter(pkglogger.DeliverySlackLoggerConfig{
					TimeConfig: cfg.TimeConfig, RuntimeConfig: cfg.RuntimeConfig, LogBodyConfig: cfg.LogBodyConfig,
					Timeout: cfg.Timeout, Token: cfg.Token, Channel: cfg.Channel,
					ErrorChannel: cfg.ErrorChannel, WarnChannel: cfg.WarnChannel, InfoChannel: cfg.InfoChannel, DebugChannel: cfg.DebugChannel,
					MaxMessageLength: cfg.MaxMessageLength,
				}), tracer, meter, nil, option)
				require.NoError(t, err)
				configured = internalLogHandlers{logger.OnError, logger.OnWarn, logger.OnInfo, logger.OnDebug}
			} else {
				logger, err := NewSlackLogger(ctx, NewConfigAdapter(cfg), tracer, meter, nil, option)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, logger.Close()) })
				configured = internalLogHandlers{logger.OnError, logger.OnWarn, logger.OnInfo, logger.OnDebug}
			}
			configured.onError(ctx, cause, nil, tracer, meter)
			configured.onWarn(ctx, nil, tracer, meter)
			configured.onInfo(ctx, nil, tracer, meter)
			configured.onDebug(ctx, nil, tracer, meter)
			require.Equal(t, []string{"error", "warn", "info", "debug"}, calls)
		})
	}
}

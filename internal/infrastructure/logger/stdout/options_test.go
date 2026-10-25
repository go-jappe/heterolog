package stdout

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
			onError := func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				require.ErrorIs(t, err, cause)
				calls = append(calls, "error")
			}
			handler := func(level string) logging.LoggerHandlerFunc {
				return func(context.Context, *logging.LogEntry, observer.Tracer, observer.Meter) {
					calls = append(calls, level)
				}
			}
			option := WithInternalLogHandlers(onError, handler("warn"), handler("info"), handler("debug"))
			timeConfig := pkglogger.TimeConfig{TimeZone: "UTC"}
			logBodyConfig := pkglogger.LogBodyConfig{LogLineDateFormat: time.RFC3339}
			var configured internalLogHandlers
			if delivery {
				logger, err := NewDeliveryStdoutLogger(ctx, NewDeliveryConfigAdapter(pkglogger.DeliveryStdoutLoggerConfig{
					TimeConfig: timeConfig, LogBodyConfig: logBodyConfig,
				}), nil, tracer, meter, nil, option)
				require.NoError(t, err)
				configured = internalLogHandlers{logger.OnError, logger.OnWarn, logger.OnInfo, logger.OnDebug}
			} else {
				logger, err := NewStdoutLogger(ctx, NewConfigAdapter(pkglogger.StdoutLoggerConfig{
					TimeConfig: timeConfig, LogBodyConfig: logBodyConfig,
				}), nil, tracer, meter, nil, option)
				require.NoError(t, err)
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

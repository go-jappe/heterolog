package telemetry

import (
	"context"

	"github.com/go-jappe/heterolog/pkg/observer"
)

// Telemetry is what heterolog.NewTelemetry returns: a Tracer and a Meter whose
// spans and metrics are delivered to the configured destinations until
// Shutdown. The admin API takes a Telemetry to report its configuration.
type Telemetry interface {
	Tracer() observer.Tracer
	Meter() observer.Meter
	Shutdown(ctx context.Context) error
}

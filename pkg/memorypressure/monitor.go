package memorypressure

import "context"

// Monitor reports the shared memory pressure state that overload policies
// consult. Callers do not need to know whether an implementation reads lazily
// with a cache or polls in the background. heterolog.NewMemoryPressureMonitor
// returns one.
type Monitor interface {
	// Current returns the MemoryPressure to use now. An implementation may update
	// its internal state when needed.
	Current(ctx context.Context) (MemoryPressure, error)

	// IsCritical reports whether memory is in critical overload. Loggers use it
	// to decide whether to drop entries or shed load.
	IsCritical(ctx context.Context) (bool, error)

	// Last returns a snapshot of the latest internal state without reading the
	// provider, for debugging, logging and observation.
	Last() MonitorSnapshot

	// Reset clears the internal state of the monitor.
	Reset()

	// Start begins reading in the background and stops when ctx ends. Starting
	// a monitor again does not start a second loop and returns the result of the
	// first start. An implementation may read the current value once before it
	// starts polling.
	Start(ctx context.Context) error
}

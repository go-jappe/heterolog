package logspool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/observertest"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
)

// TestNewBackendRunnerSupervisorPassesObserversToWorkerは、再送のworkerへloggerの
// TracerとMeterを渡し、送り直しを計装できることを確認します。
func TestNewBackendRunnerSupervisorPassesObserversToWorker(t *testing.T) {
	t.Parallel()

	opts := BackendRuntimeOptions{
		Backend: "loki",
		SpoolConfig: pkgdurable.SpoolConfig{
			SpoolPath: t.TempDir(),
			Target:    "runtime-test",
			Dir:       "loki",
		},
		Sender: &backendRuntimeSenderStub{},
	}
	opts.RunnerConfig.Worker.Finalize.RetryDelay = time.Second
	opts.RunnerConfig.Worker.Finalize.MaxEntries = 100
	opts.RunnerConfig.LifeCycle.SizePurgeInterval = time.Second
	opts.RunnerConfig.Runner.Interval = time.Second
	opts.SupervisorConfig.RestartDelay = time.Second
	tracer := &observertest.Tracer{}
	meter := observertest.NewMeter()

	supervisor, err := NewBackendRunnerSupervisor(clock.NewUTCClock(), opts, tracer, meter)
	require.NoError(t, err)

	require.Same(t, tracer, supervisor.Runner.worker.Tracer)
	require.Same(t, meter, supervisor.Runner.worker.Meter)
}

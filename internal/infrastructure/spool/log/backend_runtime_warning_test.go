package logspool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	spoolpath "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkgdurable "github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestNewBackendRunnerSupervisorConnectsActiveStoreWarningHandler(t *testing.T) {
	t.Parallel()

	var warnings []*logging.LogEntry
	opts := BackendRuntimeOptions{
		Backend: "opensearch",
		SpoolConfig: pkgdurable.SpoolConfig{
			SpoolPath: t.TempDir(),
			Target:    "runtime-test",
			Dir:       "opensearch",
		},
		Sender: &backendRuntimeSenderStub{},
		OnWarn: func(_ context.Context, entry *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
			warnings = append(warnings, entry)
		},
	}
	opts.RunnerConfig.Worker.Finalize.RetryDelay = time.Second
	opts.RunnerConfig.Worker.Finalize.MaxEntries = 100
	opts.RunnerConfig.LifeCycle.SizePurgeInterval = time.Second
	opts.RunnerConfig.Runner.Interval = time.Second
	opts.SupervisorConfig.RestartDelay = time.Second
	supervisor, err := NewBackendRunnerSupervisor(clock.NewUTCClock(), opts, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	root, err := spoolpath.NewRootDir(opts.SpoolConfig.SpoolPath, opts.SpoolConfig.Target, opts.SpoolConfig.Dir)
	require.NoError(t, err)
	active := root.ActiveDir().Path()
	require.NoError(t, os.MkdirAll(filepath.Dir(active), 0o700))
	require.NoError(t, os.WriteFile(active, []byte("obstruction"), 0o600))

	// factoryが構築した再送用Storeにも、指定した保全通知先が接続されます。
	require.NoError(t, supervisor.Runner.worker.ActiveSpoolWriter.StoreActive(context.Background(), []pkgspool.SpoolRecord{{
		OccurredAt: time.Now().UTC(), Payload: []byte(`{"message":"retained"}`),
	}}))
	require.Len(t, warnings, 1)
	require.Equal(t, active, warnings[0].Fields["original_path"])
	require.Error(t, warnings[0].Err)
	retained, ok := warnings[0].Fields["retained_path"].(string)
	require.True(t, ok)
	require.Equal(t, filepath.Join(root.String(), ".preserved"), filepath.Dir(retained))
	content, err := os.ReadFile(retained)
	require.NoError(t, err)
	require.Equal(t, "obstruction", string(content))
	files, err := filepath.Glob(filepath.Join(active, "*", "*", "*", "*", "*", "*"+spoolpath.SpoolExt))
	require.NoError(t, err)
	require.Len(t, files, 1)
}

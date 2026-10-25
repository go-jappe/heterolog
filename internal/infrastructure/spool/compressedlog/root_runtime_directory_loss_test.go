package compressedlog

import (
	"context"
	"os"
	"testing"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/require"
)

func TestManagedRouteReportsDirectoryLossAndContinuesAfterExternalRestore(t *testing.T) {
	type result struct {
		report RunReport
		err    error
	}
	results := make(chan result)
	continueCycle := make(chan struct{})
	stopNotifications := make(chan struct{})
	upload := &replaytest.Uploader{}
	cfg := managedUploaderTestConfig(t.TempDir())
	cfg.RunnerInterval = time.Millisecond
	runtime := newTestRootRuntime(t, NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(cfg), objectstorage.UploadTargetS3))
	route, err := runtime.RegisterRoute(ManagedRouteConfig{
		FileName: replaytest.FileNamePattern("app_"),
		RunObserver: func(_ context.Context, report *RunReport, _ *objectstoragespool.TerminalPurgeReport, err error) {
			if report == nil {
				return
			}
			select {
			case results <- result{report: *report, err: err}:
			case <-stopNotifications:
				return
			}
			// Pause between real periodic runs, with no directory reader left
			// open when the test changes the filesystem on Windows.
			select {
			case <-continueCycle:
			case <-stopNotifications:
			}
		},
	}, upload)
	require.NoError(t, err)
	t.Cleanup(func() {
		close(stopNotifications)
		require.NoError(t, route.Close())
	})
	awaitCycle := func() result {
		t.Helper()
		select {
		case got := <-results:
			return got
		case <-time.After(hangDetectionTimeout):
			t.Fatal("shared spool coordinator stopped reporting periodic results")
			return result{}
		}
	}
	advance := func() {
		t.Helper()
		select {
		case continueCycle <- struct{}{}:
		case <-time.After(hangDetectionTimeout):
			t.Fatal("shared spool coordinator did not accept the next cycle")
		}
	}
	require.NoError(t, awaitCycle().err)
	store := route.store
	file := replaytest.SaveFile(t, store, "app_20260913_1.log.gz")
	payload, err := os.ReadFile(file.Path)
	require.NoError(t, err)
	processing := store.Root().ProcessingDir()
	require.NoError(t, os.Remove(processing))

	for attempt := 0; attempt < 2; attempt++ {
		advance()
		failed := awaitCycle()
		require.ErrorIs(t, failed.err, os.ErrNotExist)
		require.Empty(t, failed.report.CompressedLogFiles)
		require.NoDirExists(t, processing, "successful initialization must not recreate an externally removed directory")
		require.Zero(t, upload.CallCount())
		contents, err := os.ReadFile(file.Path)
		require.NoError(t, err)
		require.Equal(t, payload, contents)
	}

	// An external repair is enough for the same coordinator, runner and Store
	// to resume on the next tick; no reinitialization is requested.
	require.NoError(t, os.Mkdir(processing, 0o750))
	advance()
	recovered := awaitCycle()
	require.NoError(t, recovered.err)
	require.Len(t, recovered.report.CompressedLogFiles, 1)
	require.True(t, recovered.report.CompressedLogFiles[0].UploadSucceeded)
	require.Equal(t, pkgspool.StateDone, recovered.report.CompressedLogFiles[0].State)
	require.Equal(t, file.FileName, replaytest.OnlyFile(t, store, pkgspool.StateDone).FileName)
	require.Equal(t, 1, upload.CallCount())
}

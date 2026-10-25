package objectstoragespool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestStorePreserveFileDoesNotMoveAnyStateAndSurvivesTerminalPurge(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	require.NoError(t, store.EnsureLayout(context.Background()))
	const name = "app_20261003_1.log.custom"
	source := writeTestSource(t, t.TempDir(), name, "unaccepted source bytes")
	statePaths := make(map[pkgspool.State]string)
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		path, err := store.root.compressedLogFilePath(state, name)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, []byte("owned "+state), payloadMode))
		statePaths[state] = path
	}
	preserved, err := store.PreserveFile(context.Background(), source)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(store.Root().Path(), ".preserved"), filepath.Dir(preserved))
	retried, err := store.PreserveFile(context.Background(), source)
	require.NoError(t, err)
	require.Equal(t, preserved, retried)
	for state, path := range statePaths {
		got, err := os.ReadFile(path)
		require.NoError(t, err, "preservation must not interfere with an owned processing file")
		require.Equal(t, "owned "+string(state), string(got))
		old := time.Now().AddDate(0, 0, -10)
		require.NoError(t, os.Chtimes(path, old, old))
	}
	purger := newTestTerminalPurger(t, stores, enabledPurgeConfig(1, 1<<20))
	report, err := purger.Run(context.Background())
	require.NoError(t, err)
	for _, state := range terminalStates {
		require.Equal(t, 1, report.ByState[state].DeletedFileCount)
		require.NoFileExists(t, statePaths[state])
	}
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing} {
		require.FileExists(t, statePaths[state])
	}
	for _, path := range []string{source, preserved} {
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "unaccepted source bytes", string(got))
	}
	entries, err := os.ReadDir(filepath.Dir(preserved))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestStorePreserveFileCancelsWhileWaitingForOperationLock(t *testing.T) {
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "app_20261003_1.log.gz", "preserve")
	synctest.Test(t, func(t *testing.T) {
		store.operationMu.Lock()
		defer store.operationMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		path, err := store.PreserveFile(ctx, source)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Empty(t, path)
	})
	require.NoDirExists(t, filepath.Join(store.Root().Path(), ".preserved"))
	path, err := store.PreserveFile(context.Background(), source)
	require.NoError(t, err)
	require.FileExists(t, path)
	for _, directory := range []string{store.Root().ActiveDir(), store.Root().ProcessingDir(), store.Root().DoneDir(), store.Root().DeadDir(), store.Root().QuarantineDir()} {
		require.NoDirExists(t, directory, "preservation must not initialize normal state directories")
	}
}

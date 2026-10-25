package objectstoragespool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestDestinationStoresSeparateFilesAndKeepRootWideCapacityLimit(t *testing.T) {
	ctx := context.Background()
	stores, err := NewDestinationStores(newTestStore(t).Root())
	require.NoError(t, err)
	var files []CompressedLogFile
	for _, id := range []objectstorage.DestinationID{"a", "b"} {
		store, err := stores.ForDestination(id)
		require.NoError(t, err)
		same, err := stores.ForDestination(id)
		require.NoError(t, err)
		require.Same(t, store, same)
		file := saveTerminalPurgeTestFile(t, store, "app_20260929_1.log.gz", "data", pkgspool.StateDone)
		old := time.Now().Add(-time.Hour)
		require.NoError(t, os.Chtimes(file.Path, old, old))
		files = append(files, file)
	}
	require.NotEqual(t, files[0].Path, files[1].Path)
	wrongStore, err := stores.ForDestination("b")
	require.NoError(t, err)
	require.Error(t, wrongStore.RemoveTerminal(ctx, files[0]), "cannot delete another destination's file")
	purger, err := NewTerminalPurger(stores, NewPurgeConfigAdapter(TerminalPurgeConfig{
		Done: objectstorage.CompressedLogFileUploadSpoolPurgeConfig{Enabled: true, MaxFileAgeDays: 30, MaxDirSize: 4},
	}))
	require.NoError(t, err)
	report, err := purger.Run(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, report.ByState[pkgspool.StateDone].DeletedFileCount)
	remaining, err := stores.ListFiles(ctx, pkgspool.StateDone)
	require.NoError(t, err)
	require.Len(t, remaining, 1, "capacity is shared across destinations, not multiplied")
}

func TestDestinationRootRejectsUnsafeIDsAndLinkedDirectories(t *testing.T) {
	root := newTestStore(t).Root()
	for _, id := range []objectstorage.DestinationID{"", "..", "a/b", `a\b`, "A", "active", "a:b"} {
		_, err := root.ForDestination(id)
		require.Error(t, err, "id=%q", id)
	}
	stores, err := NewDestinationStores(root)
	require.NoError(t, err)
	require.NoError(t, stores.EnsureLayout(context.Background()))
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root.Path(), "linked")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	_, err = root.ForDestination("linked")
	require.Error(t, err)
	observed, err := stores.Existing(context.Background())
	require.Error(t, err)
	require.Empty(t, observed)
	entries, err := os.ReadDir(outside)
	require.NoError(t, err)
	require.Empty(t, entries)
}

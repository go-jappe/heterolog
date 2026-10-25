package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestSpoolStoreWritesToResolvedRootAfterAliasChanges(t *testing.T) {
	base, physical, replacement := t.TempDir(), resolvedTempDirForTest(t), t.TempDir()
	alias := filepath.Join(base, "alias")
	testutil.Symlink(t, physical, alias)
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	store := newTestSpoolStore(t, fixedClock{now: now}, testSpoolStoreConfig{spoolPath: alias, target: "target", dir: "record"})
	wantRoot := filepath.Join(physical, "target", "spool", "record")
	require.Equal(t, wantRoot, store.RootDir.String())
	entries, err := os.ReadDir(physical)
	require.NoError(t, err)
	require.Empty(t, entries)
	require.NoError(t, os.Remove(alias))
	testutil.Symlink(t, replacement, alias)
	require.NoError(t, store.StoreByState(context.Background(), pkgspool.StateActive, []pkgspool.SpoolRecord{{
		OccurredAt: now, Payload: json.RawMessage(`{"message":"fixed root"}`),
	}}))
	files := listSpoolFiles(t, filepath.Join(wantRoot, "active"))
	require.Len(t, files, 1)
	body, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Contains(t, string(body), "fixed root")
	entries, err = os.ReadDir(replacement)
	require.NoError(t, err)
	require.Empty(t, entries, "writes must not follow a retargeted configuration alias")
}

func TestSpoolStorePreservesTimePartitionDirectoryAliasesBeforeWriting(t *testing.T) {
	parts := []string{"2026", "09", "13", "12", "00"}
	for index := range parts {
		t.Run(filepath.Join(parts[:index+1]...), func(t *testing.T) {
			base, outside := t.TempDir(), t.TempDir()
			root := filepath.Join(base, "target", "spool", "record")
			path := filepath.Join(append([]string{root, "active"}, parts[:index+1]...)...)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
			testutil.Symlink(t, outside, path)
			now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
			store := newTestSpoolStore(t, fixedClock{now: now}, testSpoolStoreConfig{
				spoolPath: base, target: "target", dir: "record",
			})
			err := store.StoreByState(context.Background(), pkgspool.StateActive, []pkgspool.SpoolRecord{{
				OccurredAt: now, Payload: json.RawMessage(`{"message":"keep local"}`),
			}})
			require.NoError(t, err)
			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			require.Empty(t, entries, "no time partition directory or spool file may be created through the alias")
			info, err := os.Lstat(path)
			require.NoError(t, err)
			require.True(t, info.IsDir(), "the managed time partition must be recreated as a real directory")
			require.Zero(t, info.Mode()&os.ModeSymlink)
			preserved, err := os.ReadDir(filepath.Join(root, ".preserved"))
			require.NoError(t, err)
			require.Len(t, preserved, 1)
			retainedInfo, err := os.Lstat(filepath.Join(root, ".preserved", preserved[0].Name()))
			require.NoError(t, err)
			require.NotZero(t, retainedInfo.Mode()&os.ModeSymlink, "move the link itself, not its target")
			require.Len(t, listSpoolFiles(t, store.RootDir.ActiveDir().Path()), 1)
		})
	}
}

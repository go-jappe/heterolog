package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestNewFilesystemStoreRejectsMissingCheckpointDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "not-created", "checkpoints")
	store, err := NewFilesystemStore(directory, "source")
	require.ErrorIs(t, err, ErrInvalidDirectory)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Nil(t, store)
	require.NoDirExists(t, directory)
}

func TestFilesystemStoreUsesResolvedDirectoryAfterParentAliasChanges(t *testing.T) {
	for _, suffix := range []string{"checkpoints", "missing/checkpoints"} {
		t.Run(suffix, func(t *testing.T) {
			base, physical, replacement := t.TempDir(), resolvedTempDirForTest(t), t.TempDir()
			alias := filepath.Join(base, "alias")
			testutil.Symlink(t, physical, alias)
			want := filepath.Join(physical, suffix)
			require.NoError(t, os.MkdirAll(want, 0o700))
			store, err := NewFilesystemStore(filepath.Join(alias, suffix), "source")
			require.NoError(t, err)
			require.Equal(t, want, store.directory)
			entries, err := os.ReadDir(want)
			require.NoError(t, err)
			require.Len(t, entries, 2, "constructor initializes the document and marker")
			require.NoError(t, os.Remove(alias))
			testutil.Symlink(t, replacement, alias)
			_, err = store.Load(context.Background(), CheckpointKey{Generation: "20260913-1", Compressed: true})
			require.NoError(t, err)
			_, err = store.AdvanceCompletionCursor(context.Background(), CheckpointKey{Generation: "20260913-1", Compressed: true}, []objectstorage.DestinationID{"destination"})
			require.NoError(t, err)
			require.Equal(t, want, filepath.Dir(store.documentPath()))
			require.FileExists(t, store.documentPath())
			entries, err = os.ReadDir(replacement)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestNewFilesystemStoreRejectsFinalDirectorySymlinkBeforeInitialization(t *testing.T) {
	for _, kind := range []string{"directory", "dangling", "loop", "file"} {
		t.Run(kind, func(t *testing.T) {
			parent, external := t.TempDir(), t.TempDir()
			path := filepath.Join(parent, "checkpoints")
			target := external
			marker := filepath.Join(external, "preserved-state")
			require.NoError(t, os.WriteFile(marker, []byte("preserve checkpoint state"), 0o600))
			switch kind {
			case "dangling":
				target = filepath.Join(external, "missing")
			case "loop":
				target = path
			case "file":
				target = marker
			}
			testutil.Symlink(t, target, path)
			before, err := os.Lstat(path)
			require.NoError(t, err)
			for _, input := range []string{path, path + string(os.PathSeparator), path + string(os.PathSeparator) + "."} {
				store, err := NewFilesystemStore(input, "source")
				require.ErrorIs(t, err, ErrInvalidDirectory)
				require.Nil(t, store)
			}
			after, err := os.Lstat(path)
			require.NoError(t, err)
			require.True(t, os.SameFile(before, after))
			require.NotZero(t, after.Mode()&os.ModeSymlink)
			gotTarget, err := os.Readlink(path)
			require.NoError(t, err)
			require.Equal(t, target, gotTarget)
			body, err := os.ReadFile(marker)
			require.NoError(t, err)
			require.Equal(t, "preserve checkpoint state", string(body))
			entries, err := os.ReadDir(external)
			require.NoError(t, err)
			require.Len(t, entries, 1, "constructor must not create checkpoint state through the link")
			require.Equal(t, filepath.Base(marker), entries[0].Name())
		})
	}
}

func TestNewFilesystemStoreRejectsNonDirectoryBeforeInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, []byte("preserve"), 0o600))
	store, err := NewFilesystemStore(path, "source")
	require.Error(t, err)
	require.Nil(t, store)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(body))
}

func TestNewFilesystemStoreResolvesPreparedRelativeDirectory(t *testing.T) {
	for _, directory := range []string{".", "checkpoints", "missing/checkpoints"} {
		t.Run(directory, func(t *testing.T) {
			workingDirectory := resolvedTempDirForTest(t)
			t.Chdir(workingDirectory)
			require.NoError(t, os.MkdirAll(directory, 0o700))
			store, err := NewFilesystemStore(directory, "source")
			require.NoError(t, err)
			require.Equal(t, filepath.Join(workingDirectory, directory), store.directory)
			require.Equal(t, "source", store.state.Source)
			entries, err := os.ReadDir(directory)
			require.NoError(t, err)
			require.Len(t, entries, 2)
		})
	}
}

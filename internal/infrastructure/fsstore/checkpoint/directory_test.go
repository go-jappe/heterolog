package checkpoint_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestPrepareArchiveCheckpointDirectoryBeforeFirstUpload(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state", "archive-checkpoints")
	require.NoError(t, checkpointstore.PrepareDirectory(directory))
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, entries, "preparation must not create checkpoint records")
	// 一度準備した同じ保存先を、通常・非同期・Delivery・内部ログのStoreで使います。
	for _, source := range []string{"logger-default", "async-logger-default", "delivery-logger-default", "internal-logger"} {
		store, err := checkpointstore.NewFilesystemStore(directory, source)
		require.NoError(t, err)
		_, err = store.Load(context.Background(), checkpointstore.CheckpointKey{Generation: "20260917-1", Compressed: true})
		require.NoError(t, err)
	}
	// 再起動時には既存の保存先をそのまま利用できます。
	require.NoError(t, checkpointstore.PrepareDirectory(directory))
}

func TestPrepareArchiveCheckpointDirectoryRejectsBlankDirectory(t *testing.T) {
	for _, directory := range []string{"", " ", "\t"} {
		require.Error(t, checkpointstore.PrepareDirectory(directory))
	}
}

func TestPrepareArchiveCheckpointDirectoryAcceptsParentAliasButRejectsDestinationLink(t *testing.T) {
	root, physical := t.TempDir(), t.TempDir()
	alias := filepath.Join(root, "alias")
	testutil.Symlink(t, physical, alias)
	checkpointDirectory := filepath.Join(physical, "checkpoints")
	require.NoError(t, checkpointstore.PrepareDirectory(filepath.Join(alias, "checkpoints")))
	require.DirExists(t, checkpointDirectory)
	linkedDestination := filepath.Join(root, "linked-checkpoints")
	testutil.Symlink(t, checkpointDirectory, linkedDestination)
	require.Error(t, checkpointstore.PrepareDirectory(linkedDestination))
}

func TestPrepareArchiveCheckpointDirectoryReportsCreationFailureBeforeStoreConstruction(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(parent, []byte("preserve"), 0o600))
	err := checkpointstore.PrepareDirectory(filepath.Join(parent, "checkpoints"))
	require.Error(t, err)
	contents, err := os.ReadFile(parent)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(contents))
}

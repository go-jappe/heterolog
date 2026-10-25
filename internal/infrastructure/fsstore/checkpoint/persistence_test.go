package checkpoint

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestWriteAtomicallyPublishesAndReplacesPrivateCheckpoint(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "checkpoints")
	require.NoError(t, infrafsstore.EnsureDirectory(directory, 0o700))
	path := filepath.Join(directory, "source.json")
	for _, payload := range [][]byte{[]byte("first\n"), []byte("replacement\n")} {
		published, err := WriteAtomically(context.Background(), directory, path, payload)
		require.NoError(t, err)
		require.True(t, published)
		actual, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, payload, actual)
		testutil.RequirePrivatePermissions(t, path, 0o600)
		entries, err := os.ReadDir(directory)
		require.NoError(t, err)
		require.Len(t, entries, 1, "no unpublished temporary files should remain")
		require.Equal(t, filepath.Base(path), entries[0].Name())
	}
}

func TestWriteAtomicallyCanceledContextPreservesPublishedCheckpoint(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "source.json")
	previous := []byte("previous\n")
	require.NoError(t, os.WriteFile(path, previous, 0o600))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	published, err := WriteAtomically(ctx, directory, path, []byte("replacement\n"))
	require.False(t, published)
	require.ErrorIs(t, err, context.Canceled)
	actual, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, previous, actual)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestWriteAtomicallyFailedRenamePreservesDestinationAndRemovesTemporaryFile(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "source.json")
	// A nonempty directory cannot be replaced by the checkpoint file on either
	// Windows or Unix. This exercises the real failed-rename cleanup path.
	require.NoError(t, os.Mkdir(path, 0o700))
	marker := filepath.Join(path, "preserve")
	require.NoError(t, os.WriteFile(marker, []byte("untouched"), 0o600))

	published, err := WriteAtomically(context.Background(), directory, path, []byte("replacement\n"))
	require.False(t, published)
	require.Error(t, err)
	actual, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, []byte("untouched"), actual)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Base(path), entries[0].Name())
}

func TestWriteAtomicallyDoesNotCreateUnpreparedDirectory(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "not-initialized")
	path := filepath.Join(directory, "source.json")
	published, err := WriteAtomically(context.Background(), directory, path, []byte("checkpoint\n"))
	require.False(t, published)
	require.Error(t, err)
	_, statErr := os.Stat(directory)
	require.True(t, errors.Is(statErr, os.ErrNotExist), "directory preparation remains the caller's initialization responsibility")
}

func TestWriteAtomicallyDirectorySyncFailureReportsPublishedFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "source.json")
	require.NoError(t, os.WriteFile(path, []byte("previous\n"), fileMode))
	payload := []byte("published replacement\n")
	failure := errors.New("directory sync failed")
	published, err := writeAtomically(context.Background(), directory, path, payload, func(actual string) error {
		require.Equal(t, directory, actual)
		contents, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, payload, contents, "the error is injected after the replacement was published")
		return failure
	})
	require.True(t, published)
	require.ErrorIs(t, err, failure)
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, contents)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

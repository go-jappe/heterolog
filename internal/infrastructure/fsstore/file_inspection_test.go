package fsstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestInspectRegularFileReturnsMetadataWithoutVerifyingContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unverified.log.gz")
	content := []byte("not compressed content")
	modified := time.Date(2026, time.September, 24, 12, 0, 0, 0, time.UTC)
	require.NoError(t, os.WriteFile(path, content, 0o600))
	require.NoError(t, os.Chtimes(path, modified, modified))

	info, err := InspectRegularFile(path)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.True(t, info.Mode().IsRegular())
	require.Equal(t, filepath.Base(path), info.Name())
	require.EqualValues(t, len(content), info.Size())
	require.True(t, info.ModTime().Equal(modified), "modification time = %v, want %v", info.ModTime(), modified)
}

func TestInspectRegularFilePreservesFilesystemErrors(t *testing.T) {
	for _, kind := range []string{"missing", "invalid path"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing.log.gz")
			if kind == "invalid path" {
				path += "\x00"
			}

			info, err := InspectRegularFile(path)
			require.Error(t, err)
			require.Nil(t, info)
			require.NotErrorIs(t, err, ErrNotRegularFile)
			var pathErr *os.PathError
			require.ErrorAs(t, err, &pathErr, "error must identify the inspected path")
			require.Equal(t, path, pathErr.Path)
			if kind == "missing" {
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				require.NotErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

func TestInspectRegularFileRejectsNonRegularPaths(t *testing.T) {
	for _, kind := range []string{"directory", "symlink", "dangling symlink"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "inspected.log.gz")
			target := filepath.Join(dir, "target.log.gz")
			switch kind {
			case "directory":
				require.NoError(t, os.Mkdir(path, 0o700))
			case "symlink":
				require.NoError(t, os.WriteFile(target, []byte("regular target"), 0o600))
				testutil.Symlink(t, target, path)
			case "dangling symlink":
				testutil.Symlink(t, target, path)
			}

			info, err := InspectRegularFile(path)
			require.Nil(t, info)
			require.ErrorIs(t, err, ErrNotRegularFile)
			require.NotErrorIs(t, err, os.ErrNotExist)
			_, err = os.Lstat(path)
			require.NoError(t, err, "inspection must preserve the rejected path")
		})
	}
}

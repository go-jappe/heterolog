package failurearchive

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"

	"github.com/stretchr/testify/require"
)

func TestCompressOldFileWithResultRejectsNonRegularSourceWithoutPublishing(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"symlink", "dangling_symlink", "symlink_loop", "directory"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(dir, "source.jsonl")
			target := filepath.Join(t.TempDir(), "external.jsonl")
			require.NoError(t, os.WriteFile(target, []byte("preserve external source"), 0o600))
			linkTarget := target
			switch kind {
			case "directory":
				require.NoError(t, os.Mkdir(source, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(source, "child"), []byte("preserve child"), 0o600))
			case "dangling_symlink":
				linkTarget += ".missing"
			case "symlink_loop":
				linkTarget = source
			}
			if kind != "directory" {
				testutil.Symlink(t, linkTarget, source)
			}
			before, err := os.Lstat(source)
			require.NoError(t, err)
			cfg := newValidTestFileArchiveConfig(t)
			cfg.enableFileCompress = true
			store := FileStore{Config: cfg}

			returnedPath, err := store.CompressOldFileWithResult(context.Background(), source)
			require.Error(t, err)
			require.Empty(t, returnedPath, "a rejected source must not publish a compressed file")

			after, err := os.Lstat(source)
			require.NoError(t, err)
			require.True(t, os.SameFile(before, after), "source identity must be preserved")
			require.Equal(t, before.Mode(), after.Mode())
			if kind == "directory" {
				body, err := os.ReadFile(filepath.Join(source, "child"))
				require.NoError(t, err)
				require.Equal(t, "preserve child", string(body))
			} else {
				gotTarget, err := os.Readlink(source)
				require.NoError(t, err)
				require.Equal(t, linkTarget, gotTarget)
				if kind == "dangling_symlink" {
					_, err := os.Lstat(linkTarget)
					require.ErrorIs(t, err, os.ErrNotExist)
				}
			}
			body, err := os.ReadFile(target)
			require.NoError(t, err)
			require.Equal(t, "preserve external source", string(body))
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, 1, "rejection must leave no final, temporary, or pending source file")
			require.Equal(t, filepath.Base(source), entries[0].Name())
		})
	}
}

func TestCompressOldFileWithResultAcceptsRegularAndEmptySources(t *testing.T) {
	t.Parallel()
	for _, fixture := range []struct {
		name string
		body string
	}{
		{name: "regular", body: "ordinary source contents\n"},
		{name: "empty"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			dir := t.TempDir()
			source := filepath.Join(dir, "source.jsonl")
			require.NoError(t, os.WriteFile(source, []byte(fixture.body), 0o600))
			cfg := newValidTestFileArchiveConfig(t)
			cfg.enableFileCompress = true
			store := FileStore{Config: cfg}

			returnedPath, err := store.CompressOldFileWithResult(context.Background(), source)
			require.NoError(t, err)
			require.Equal(t, source+".gz", returnedPath)
			require.Equal(t, fixture.body, string(readGzipFile(t, returnedPath)))
			require.NoError(t, store.VerifyCompressedFileIntegrity(returnedPath))
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, entries, 1, "success must retire the source and remove temporary/pending files")
			require.Equal(t, filepath.Base(returnedPath), entries[0].Name())
		})
	}
}

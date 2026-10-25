package fsstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestCountPreservedFilesIncludesCopyAndRenameWithoutTouchingFiles(t *testing.T) {
	ctx := context.Background()
	// PreserveRegularFileはリンクを拒否するため、一時ディレクトリの上位のリンクを解決します。
	root := resolvedTempDirForTest(t)
	count, err := CountPreservedFiles(ctx, root)
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoDirExists(t, filepath.Join(root, PreservedDirectoryName))
	source := filepath.Join(t.TempDir(), "app_20261004_1.log.gz")
	require.NoError(t, os.WriteFile(source, []byte("copied bytes"), 0o600))
	copied, err := PreserveRegularFile(ctx, root, source)
	require.NoError(t, err)
	directory := filepath.Join(root, PreservedDirectoryName)
	renamed := filepath.Join(directory, "active__original-name.conflict")
	require.NoError(t, os.WriteFile(renamed, []byte("renamed bytes"), 0o600))
	temporary := filepath.Join(directory, ".preserve-unfinished.tmp")
	require.NoError(t, os.WriteFile(temporary, []byte("unfinished"), 0o600))
	count, err = CountPreservedFiles(ctx, root)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	for _, path := range []string{source, copied, renamed, temporary} {
		require.FileExists(t, path)
	}
}

func TestCountPreservedFilesCountsIsolatedEntriesWithoutFollowingOrChangingThem(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, PreservedDirectoryName)
	require.NoError(t, os.Mkdir(directory, 0o750))
	saved := filepath.Join(directory, "saved.conflict")
	require.NoError(t, os.WriteFile(saved, []byte("saved"), 0o600))
	outside := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(outside, []byte("outside"), 0o600))
	linked := filepath.Join(directory, "linked.conflict")
	temporaryLinked := filepath.Join(directory, ".preserve-linked.tmp")
	brokenLinked := filepath.Join(directory, "broken.conflict")
	missingTarget := filepath.Join(t.TempDir(), "missing")
	testutil.Symlink(t, outside, linked)
	testutil.Symlink(t, outside, temporaryLinked)
	testutil.Symlink(t, missingTarget, brokenLinked)
	retainedDirectory := filepath.Join(directory, "retained-directory")
	temporaryDirectory := filepath.Join(directory, ".preserve-directory.tmp")
	require.NoError(t, os.Mkdir(retainedDirectory, 0o700))
	require.NoError(t, os.Mkdir(temporaryDirectory, 0o700))
	nested := filepath.Join(retainedDirectory, "nested")
	require.NoError(t, os.Mkdir(nested, 0o700))
	child := filepath.Join(retainedDirectory, "child")
	grandchild := filepath.Join(nested, "child")
	temporaryChild := filepath.Join(temporaryDirectory, "child")
	temporary := filepath.Join(directory, ".preserve-unfinished.tmp")
	for _, path := range []string{child, grandchild, temporaryChild, temporary} {
		require.NoError(t, os.WriteFile(path, []byte("untouched"), 0o600))
	}
	before := make(map[string]os.FileInfo)
	for _, path := range []string{saved, outside, linked, temporaryLinked, brokenLinked, retainedDirectory, temporaryDirectory, nested, child, grandchild, temporaryChild, temporary} {
		info, err := os.Lstat(path)
		require.NoError(t, err)
		before[path] = info
	}
	count, err := CountPreservedFiles(context.Background(), root)
	require.NoError(t, err)
	require.Equal(t, 6, count)
	for path, info := range before {
		after, err := os.Lstat(path)
		require.NoError(t, err)
		require.True(t, os.SameFile(info, after), path)
		require.Equal(t, info.Mode(), after.Mode(), path)
	}
	for path, want := range map[string]string{saved: "saved", outside: "outside", child: "untouched", grandchild: "untouched", temporaryChild: "untouched", temporary: "untouched"} {
		body, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, string(body), path)
	}
	for path, want := range map[string]string{linked: outside, temporaryLinked: outside, brokenLinked: missingTarget} {
		target, err := os.Readlink(path)
		require.NoError(t, err)
		require.Equal(t, want, target, path)
	}
	require.NoFileExists(t, missingTarget)
}

func TestCountPreservedFilesRejectsLinkedPreservedDirectory(t *testing.T) {
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "saved.conflict"), []byte("saved"), 0o600))
	linkedRoot := t.TempDir()
	testutil.Symlink(t, directory, filepath.Join(linkedRoot, PreservedDirectoryName))
	count, err := CountPreservedFiles(context.Background(), linkedRoot)
	require.Zero(t, count)
	var obstruction *DirectoryObstructionError
	require.ErrorAs(t, err, &obstruction)
	require.Equal(t, filepath.Join(linkedRoot, PreservedDirectoryName), obstruction.Path)
	body, readErr := os.ReadFile(filepath.Join(directory, "saved.conflict"))
	require.NoError(t, readErr)
	require.Equal(t, "saved", string(body))
}

func TestCountPreservedFilesHonorsCancellationWithoutCreatingDirectory(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	count, err := CountPreservedFiles(ctx, root)
	require.Zero(t, count)
	require.ErrorIs(t, err, context.Canceled)
	require.NoDirExists(t, filepath.Join(root, PreservedDirectoryName))
}

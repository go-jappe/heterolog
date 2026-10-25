package compressedlogarchive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	fsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestReuseExistingFilePreservesBothConflictsAndPurgeRetainsThem(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	name := "app_" + time.Now().UTC().AddDate(0, 0, -3).Format("20060102") + "_1.log.gz"
	firstSource := writeTestCompressedLogIn(t, t.TempDir(), name, []byte("first"))
	first, err := store.Save(context.Background(), testSaveInput(firstSource, uploader.FailureCategoryRetryable))
	require.NoError(t, err)

	secondCanonical := filepath.Join(store.Root().PermanentDir(), name)
	require.NoError(t, os.WriteFile(secondCanonical, []byte("second"), archivedFileMode))
	newSource := writeTestCompressedLogIn(t, t.TempDir(), name, []byte("new"))

	preflight, found, err := store.ReuseExistingFile(context.Background(), newSource)
	require.NoError(t, err)
	require.False(t, found)
	require.Len(t, preflight.ConflictRenames, 2)
	for index, conflict := range preflight.ConflictRenames {
		category := archiveFailureCategories[index]
		require.Contains(t, filepath.Base(conflict.RetainedPath), "-"+string(category)+"-")
		require.Equal(t, filepath.Join(store.Root().Path(), fsstore.PreservedDirectoryName), filepath.Dir(conflict.RetainedPath))
		require.NoFileExists(t, conflict.OriginalPath)
	}
	assertFileBytes(t, preflight.ConflictRenames[0].RetainedPath, []byte("first"))
	assertFileBytes(t, preflight.ConflictRenames[1].RetainedPath, []byte("second"))

	saved, err := store.Save(context.Background(), testSaveInput(newSource, uploader.FailureCategoryRetryable))
	require.NoError(t, err)
	require.Equal(t, first.Path, saved.Path)
	assertFileBytes(t, saved.Path, []byte("new"))

	purger := newTestPurger(t, store, PurgeConfig{
		Retryable: enabledArchivePurgeConfig(1, 1),
		Permanent: enabledArchivePurgeConfig(1, 1),
	})
	report, err := purger.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, report.PreservedFileCount)
	for _, category := range archiveFailureCategories {
		categoryReport := report.ByFailureCategory[category]
		require.Equal(t, 0, categoryReport.RetainedUnsafeEntryCount)
		require.False(t, categoryReport.CapacityLimitExceeded)
	}
	for _, conflict := range preflight.ConflictRenames {
		require.FileExists(t, conflict.RetainedPath)
	}
}

func TestPreserveConflictingArchiveFileRenameFailureKeepsOriginal(t *testing.T) {
	// 保全先の作成はリンクを拒否するため、一時ディレクトリの上位のリンクを解決します。
	root := resolvedTempDirForTest(t)
	directory := filepath.Join(root, string(uploader.FailureCategoryRetryable))
	require.NoError(t, os.Mkdir(directory, directoryMode))
	path := filepath.Join(directory, "app_20260928_1.log.gz")
	require.NoError(t, os.WriteFile(path, []byte("old content"), archivedFileMode))
	renameErr := errors.New("rename rejected")
	conflict, err := preserveConflictingArchiveFile(
		context.Background(), root, uploader.FailureCategoryRetryable, path,
		func(_, _ string) error { return renameErr })
	require.ErrorIs(t, err, ErrCompressedLogAlreadyExists)
	require.ErrorIs(t, err, renameErr)
	require.Empty(t, conflict.RetainedPath)
	assertFileBytes(t, path, []byte("old content"))
}

func TestPreserveConflictingArchiveFileRejectsLinkedPreservationDirectory(t *testing.T) {
	// 試験が作る.preservedのリンクだけが拒否の原因になるよう、上位のリンクを解決します。
	root := resolvedTempDirForTest(t)
	directory := filepath.Join(root, string(uploader.FailureCategoryRetryable))
	require.NoError(t, os.Mkdir(directory, directoryMode))
	path := filepath.Join(directory, "app_20260928_1.log.gz")
	require.NoError(t, os.WriteFile(path, []byte("old content"), archivedFileMode))
	outside := t.TempDir()
	testutil.Symlink(t, outside, filepath.Join(root, fsstore.PreservedDirectoryName))

	conflict, err := preserveConflictingArchiveFile(
		context.Background(), root, uploader.FailureCategoryRetryable, path, fsstore.RenameFileNoReplace,
	)
	require.Error(t, err)
	require.Empty(t, conflict.RetainedPath)
	assertFileBytes(t, path, []byte("old content"))
	entries, readErr := os.ReadDir(outside)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestPurgerReportsPreservedFileWhenPurgeDisabled(t *testing.T) {
	store := newTestStore(t, pkglogger.LoggerContractSync)
	name := "app_20260928_1.log.gz"
	oldSource := writeTestCompressedLogIn(t, t.TempDir(), name, []byte("old"))
	_, err := store.Save(context.Background(), testSaveInput(oldSource, uploader.FailureCategoryRetryable))
	require.NoError(t, err)
	newSource := writeTestCompressedLogIn(t, t.TempDir(), name, []byte("new"))
	saved, err := store.Save(context.Background(), testSaveInput(newSource, uploader.FailureCategoryRetryable))
	require.NoError(t, err)
	require.Len(t, saved.ConflictRenames, 1)

	report, err := newTestPurger(t, store, PurgeConfig{}).Run(context.Background())
	require.NoError(t, err)
	retryable := report.ByFailureCategory[uploader.FailureCategoryRetryable]
	require.False(t, retryable.Enabled)
	require.Equal(t, 1, report.PreservedFileCount)
	require.Zero(t, retryable.ScannedEntryCount)
	require.Zero(t, retryable.RemainingBytes)
	require.Zero(t, retryable.DeletedFileCount)
	assertFileBytes(t, saved.Path, []byte("new"))
	assertFileBytes(t, saved.ConflictRenames[0].RetainedPath, []byte("old"))
}

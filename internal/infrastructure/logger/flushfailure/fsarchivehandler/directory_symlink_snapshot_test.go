package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFailureArchivePinsSymlinkRootAcrossRotationAndUpload(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent} {
		t.Run(string(failureCategory), func(t *testing.T) {
			realRoot, err := filepath.EvalSymlinks(t.TempDir())
			require.NoError(t, err)
			replacementRoot := t.TempDir()
			alias := filepath.Join(t.TempDir(), "root-alias")
			testutil.Symlink(t, realRoot, alias)
			now := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
			remote := newDestinationFailureArchiveUploader("primary")
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.archivePath = alias
				cfg.enableRotate = true
				cfg.enableFileCompress = true
				cfg.setArchiveUploader(failureCategory, remote)
			})
			handler := pipeline.handler
			directory := filepath.Join(realRoot, handler.Config.Target(), handler.Config.Dir(), string(failureCategory))
			pattern, err := handler.archiveFileNamePattern(failureCategory)
			require.NoError(t, err)
			require.Equal(t, directory, pattern.Dir)
			require.Same(t, appendlog.DirectoryGuardForPath(directory), handler.directoryForFailureCategory(failureCategory).guard)
			require.Same(t, appendlog.DirectorySizeTrackerForPath(directory), handler.directoryForFailureCategory(failureCategory).directorySizeTracker)
			oldPath, err := pipeline.ActiveArchivePath(failureCategory)
			require.NoError(t, err)
			activeFile := handler.Fsstore.FileRetryable
			if failureCategory == failurearchive.FailureCategoryPermanent {
				activeFile = handler.Fsstore.FilePermanent
			}
			_, err = activeFile.Write(make([]byte, handler.Config.MaxFileSize()+1))
			require.NoError(t, err)
			require.NoError(t, os.Remove(alias))
			testutil.Symlink(t, replacementRoot, alias)
			result := retryableArchiveResult(now, "archive after alias replacement")
			if failureCategory == failurearchive.FailureCategoryPermanent {
				result.DeadRecords = result.RetryableRecords
				result.RetryableRecords = nil
			}
			require.NoError(t, pipeline.HandleFailure(context.Background(), result))
			activePath, err := pipeline.ActiveArchivePath(failureCategory)
			require.NoError(t, err)
			require.NotEqual(t, oldPath, activePath)
			require.Equal(t, directory, filepath.Dir(activePath))
			require.NoError(t, shutdownFailureArchiveForTest(pipeline))
			require.Equal(t, 1, remote.attemptCount(oldPath+pkgfsstore.ExtensionCompressGzip, "primary"))
			require.NoFileExists(t, oldPath)
			require.NoFileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
			require.Len(t, readArchiveRecords(t, activePath), 1)
			require.NoDirExists(t, filepath.Join(replacementRoot, handler.Config.Target()))
		})
	}
}

func TestFailureArchiveOverloadCheckPinsSymlinkRoot(t *testing.T) {
	realRoot := t.TempDir()
	alias := filepath.Join(t.TempDir(), "root-alias")
	testutil.Symlink(t, realRoot, alias)
	pipeline := newTestArchiveHandler(t, time.Now(), func(cfg *testArchiveConfig) {
		cfg.archivePath = alias
		cfg.overload.minAvailableDiskSpace = ^uint64(0)
	})
	overloaded, err := pipeline.handler.isStorageOverloaded(context.Background())
	require.NoError(t, err)
	require.True(t, overloaded)
	require.NoError(t, os.Remove(alias))
	testutil.Symlink(t, filepath.Join(t.TempDir(), "missing"), alias)
	// alias側の容量確認は失敗しますが、稼働中writerの実体directoryは確認できます。
	overloaded, err = pipeline.handler.isStorageOverloaded(context.Background())
	require.NoError(t, err)
	require.True(t, overloaded)
}

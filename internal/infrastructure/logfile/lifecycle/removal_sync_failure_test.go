package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// 親ディレクトリも取り除くことで、権限や実行OSによらず、ファイルの不在確認後の
// SyncDirectoryだけを失敗させます。外部削除をサポートするための試験ではありません。
func removeRunnerFixtureDirectory(f logFileProcessingRecoveryFixture) error {
	directory := filepath.Dir(f.path)
	latest := filepath.Join(directory, "checkpoint-recovery_"+f.cursor.Period+"_3.log")
	if err := os.Remove(f.path); err != nil {
		return err
	}
	if err := os.Remove(latest); err != nil {
		return err
	}
	return os.Remove(directory)
}

func restoreRunnerFixtureDirectory(t *testing.T, f logFileProcessingRecoveryFixture) {
	t.Helper()
	directory := filepath.Dir(f.path)
	require.NoError(t, os.MkdirAll(directory, 0o750))
	writeFileSeriesCheckpointLog(t, directory, "checkpoint-recovery", f.cursor.Period, 3)
}

func TestUploadedRemovalSyncFailureKeepsCompletionCursor(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "normal run"
		if recovery {
			name = "existing file recovery"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			const destination = objectstorage.DestinationID("archive")
			f := newLogFileProcessingRecoveryFixture(t, destination)
			cacheVerifiedPeerForRemovalTest(t, f)
			directoryRemoval := make(chan error, 1)
			f.manager.objectStorageUpload.uploader = &destinationUploaderWithFirstSuccessHook{
				destinationUploaderRecorder: f.remote,
				afterFirstSuccess: func() {
					directoryRemoval <- removeRunnerFixtureDirectory(f)
				},
			}
			var report RunReport
			var runErr error
			if recovery {
				type notification struct {
					report RunReport
					err    error
				}
				notified := make(chan notification, 1)
				f.manager.logFileProcessingRecoveryReport = func(report RunReport, err error) {
					select {
					case notified <- notification{report: report, err: err}:
					default:
					}
				}
				f.store.failLoad.Store(true)
				_, err := f.lifecycle.RunOnce(ctx)
				require.ErrorIs(t, err, errRecoveryCheckpoint)
				require.True(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
				f.store.failLoad.Store(false)
				select {
				case result := <-notified:
					report, runErr = result.report, result.err
				case <-time.After(boundedwait.HangTimeout):
					t.Fatal("log file processing recovery did not report the removal sync failure")
				}
			} else {
				var result fileSeriesRunResult
				runErr = f.manager.directoryGuard.RunWithLifecycleLock(func() error {
					var err error
					result, err = f.manager.runFileSeriesCompressIfNeededThenUploadAndPurge(ctx)
					return err
				})
				report = result.report
				require.False(t, pathidentity.Contains(result.pendingPaths, f.path), "an absent file is not upload-pending")
			}
			select {
			case err := <-directoryRemoval:
				require.NoError(t, err)
			case <-time.After(boundedwait.HangTimeout):
				t.Fatal("upload did not reach the directory sync failure setup")
			}
			require.ErrorIs(t, runErr, pkgfsstore.ErrDurabilityUnconfirmed)
			require.Len(t, report.UploadedFileRemovals, 1)
			require.NoError(t, report.UploadedFileRemovals[0].Err, "a post-removal sync failure does not change the deletion result")
			require.NoFileExists(t, f.path)
			require.False(t, hasVerifiedPeerForRemovalTest(f.manager, f.path), "directory sync failure must not retain an absent peer's verification cache")
			require.True(t, f.manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(f.cursor, true)))
			require.False(t, f.manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))
			f.manager.directoryGuard.WithProtectedPaths(func(paths map[string]struct{}) {
				require.True(t, pathidentity.Contains(paths, f.path))
				require.True(t, pathidentity.Contains(paths, f.raw))
			})
			checkpoint, err := f.store.Load(ctx, fileCheckpointKey(f.cursor, true))
			require.NoError(t, err)
			require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)

			restoreRunnerFixtureDirectory(t, f)
			waitLogFileProcessingRecovery(t, f.manager, f.cursor)
			_, err = f.lifecycle.RunOnce(ctx)
			require.NoError(t, err)
			checkpoint, err = f.store.Load(ctx, fileCheckpointKey(f.cursor, true))
			require.NoError(t, err)
			require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs, "later scans retain the completion cursor")
			require.Equal(t, []string{f.path}, f.remote.pathsFor(destination), "sync failure must not cause another upload")
		})
	}
}

func TestAbsentDirectoryDoesNotEraseCursorOrStartPreparationRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const destination = objectstorage.DestinationID("archive")
	f := newLogFileProcessingRecoveryFixture(t, destination)
	_, err := f.store.AdvanceCompletionCursor(ctx, fileCheckpointKey(f.cursor, true), []objectstorage.DestinationID{destination})
	require.NoError(t, err)
	require.NoError(t, removeRunnerFixtureDirectory(f))
	err = f.manager.directoryGuard.RunWithLifecycleLock(func() error {
		_, err := f.manager.prepareObjectStorageUploadRun(ctx, nil, nil)
		return err
	})
	require.NoError(t, err)
	require.False(t, f.manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))
	checkpoint, err := f.store.Load(ctx, fileCheckpointKey(f.cursor, true))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)

	restoreRunnerFixtureDirectory(t, f)
	_, err = f.lifecycle.RunOnce(ctx)
	require.NoError(t, err)
	checkpoint, err = f.store.Load(ctx, fileCheckpointKey(f.cursor, true))
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{destination}, checkpoint.CompletedDestinationIDs)
	require.Empty(t, f.remote.pathsFor(destination))
}

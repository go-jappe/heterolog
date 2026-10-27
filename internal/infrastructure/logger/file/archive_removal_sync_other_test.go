//go:build !windows

package file

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestInlineRemovedArchiveSyncFailureRetainsCompletionCursor(t *testing.T) {
	for _, rotation := range []bool{false, true} {
		name := "pending-upload"
		if rotation {
			name = "rotation"
		}
		t.Run(name, func(t *testing.T) {
			synctestFromWallClock(t, func(t *testing.T) {
				ctx := context.Background()
				logger := newInlineCompressionCheckpointTestLogger(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), "remove-sync-"+name)
				store := &recoverableInlineCheckpointStore{Store: newCompressedLogCheckpointTestStore(t, t.TempDir(), "remove_sync")}
				logger.archiveUploadCheckpoints = store
				path := writePendingCompressedLog(t, logger, "20260920", 1, logger.Clock.Now())
				source := path
				if rotation {
					require.NoError(t, os.Remove(path))
					source = writeCanonicalRawLog(t, logger, "20260920", 1)
				}
				key := inlineCompressedLogCheckpointKey(t, logger, path)
				directory := logger.Fsstore.FileName.Dir
				restore := func() { require.NoError(t, os.Chmod(directory, 0o700)) }
				t.Cleanup(restore)
				// この環境で権限によるopen失敗を再現できることを先に確認します。
				denyArchiveRuntimeDirectoryAccess(t, directory, false)()
				attempts := 0
				remote := archiveUploaderFunc(func(context.Context, uploader.UploadRequest) error {
					attempts++
					// upload完了後だけ読取権限を外します。削除に必要な書込・検索権限は残します。
					return os.Chmod(directory, 0o300)
				})
				logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
				var notified []error
				logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
					if errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
						notified = append(notified, err)
					}
				}
				var retry bool
				if rotation {
					retry = logger.archiveRotatedFileWithNotifications(ctx, source, logger.Fsstore.FileName, runNotificationImmediately)
				} else {
					retry = logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately)
				}
				require.True(t, retry)
				require.NoFileExists(t, path)
				require.True(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
				require.Len(t, notified, 1)
				require.True(t, errors.Is(notified[0], pkgfsstore.ErrDurabilityUnconfirmed))
				checkpoint, err := store.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
				logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
					require.Contains(t, protected, path)
				})

				// 権限復旧後は通常writeなしで同期だけを完了し、cursorと送信成功を保持します。
				restore()
				requireArchiveRecoveryClosesForTest(t, logger)
				require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
				logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
					require.NotContains(t, protected, path)
				})
				owner := appendlog.NewDirectoryGuardToken()
				var retained []string
				err = logger.lifecycleGuard.RunWithLifecycleLock(func() error {
					_, err := logger.retainRemainingLogFilesForOwner(ctx, infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths([]string{logger.Fsstore.ActiveFilePath}), logger.Fsstore.FileName.Extension), logger.Fsstore.FileName, owner, &retained)
					return err
				})
				logger.lifecycleGuard.UnprotectPaths(owner, retained...)
				require.NoError(t, err)
				checkpoint, err = store.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
				require.Equal(t, 1, attempts)
			})
		})
	}
}

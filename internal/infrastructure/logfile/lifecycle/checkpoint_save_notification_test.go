package lifecycle

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestRestartWithOldDocumentDoesNotWaitForAlreadyRemovedCompletedFile(t *testing.T) {
	t.Parallel()
	// 受理済み処理の再試行・checkpoint保存の再試行は実時間で待たず、bubble内の時刻で進めます。
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		const destination = objectstorage.DestinationID("primary")
		const target = "checkpoint-restart"
		root := resolvedTempDirForTest(t)
		directory := filepath.Join(root, target, "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		period := currentLogDate()
		first := filepath.Join(directory, target+"_"+period+"_2.log.gz")
		writeGzipFile(t, first, []byte("completed before restart"))
		writeFileSeriesCheckpointLog(t, directory, target, period, 3)
		checkpointDirectory := newCheckpointDirectoryForTest(t, root)
		store, err := checkpointstore.NewFilesystemStore(checkpointDirectory, "restart")
		require.NoError(t, err)
		remote := newDestinationUploaderRecorder(destination)
		newManager := func(store *checkpointstore.FilesystemStore) (*FileSeriesManager, *DirectoryLifecycle) {
			manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 10, 0, 0, false),
				Uploader: remote, CheckpointStore: store,
			})
			lifecycle, err := NewDirectoryLifecycle(manager)
			require.NoError(t, err)
			return manager, lifecycle
		}
		manager, lifecycle := newManager(store)
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)
		path := filepath.Join(store.Directory(), store.SourceID()+".log-file-uploads.json")
		restore := blockRunnerCheckpointDocument(t, path)
		_, err = lifecycle.RunOnce(ctx)
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		require.NoFileExists(t, first)
		restore()
		require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		// 再起動時にdisk上のsnapshotが古い状況は、graceful closeとは別に再現します。
		// graceful closeは現在、返る前に受理済みのcheckpointをすべて永続化します。
		require.NoError(t, os.Rename(path+".before-save-failure", path))
		restarted, err := checkpointstore.NewFilesystemStore(store.Directory(), store.SourceID())
		require.NoError(t, err)
		// Storeとmanagerを作り直します。process内の完了記録は引き継ぎません。
		manager, lifecycle = newManager(restarted)
		second := strings.Replace(first, "_2.log.gz", "_3.log.gz", 1)
		require.NoError(t, os.Remove(strings.TrimSuffix(second, ".gz")))
		writeGzipFile(t, second, []byte("next generation after restart"))
		writeFileSeriesCheckpointLog(t, directory, target, period, 4)
		_, err = lifecycle.RunOnce(ctx)
		require.NoError(t, err)
		require.NoFileExists(t, second)
		require.Equal(t, []string{first, second}, remote.pathsFor(destination))
	})
}

func TestArchiveCheckpointFactoryReportsSaveRetryFailure(t *testing.T) {
	for _, closeFromReporter := range []bool{false, true} {
		name := "retry continues after notification"
		if closeFromReporter {
			name = "reporter can close manager"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				const destination = objectstorage.DestinationID("primary")
				const target = "checkpoint-retry-notification"
				root := resolvedTempDirForTest(t)
				directory := filepath.Join(root, target, "logs")
				require.NoError(t, os.MkdirAll(directory, 0o750))
				pattern := logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"}
				config := testFileSeriesManagerConfig(root, target, 10, 0, 0, false)
				checkpointConfig := pkgfsstore.ArchiveCheckpointConfig{
					Directory: newCheckpointDirectoryForTest(t, root), SourcePrefix: "retry-notification",
					AutoRecoverMissingCheckpoint: true,
				}
				sourceID := archiveCheckpointSourceID(checkpointConfig.SourcePrefix, "default", pattern)
				var manager *FileSeriesManager
				type notification struct {
					report        RunReport
					err, closeErr error
				}
				notifications := make(chan notification, 2)
				reporter := runReporterFunc(func(_ context.Context, report RunReport, err error) {
					result := notification{report: report, err: err}
					if closeFromReporter {
						result.closeErr = manager.Close()
					}
					boundedwait.SendOrReport(t, notifications, result, "notification")
				})
				remote := newDestinationUploaderRecorder(destination)
				store, err := newArchiveCheckpointStore(checkpointConfig, "default", pattern, config.Layout(), remote, reporter,
					archiveCheckpointFactoryOptions{activePeriod: currentLogDate()})
				require.NoError(t, err)
				manager = NewFileSeriesManager(FileSeriesManagerOptions{
					FileName: pattern, Config: config, Uploader: remote, CheckpointStore: store,
				})
				boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "manager.Close()", manager.Close, manager.ForceClose, nil)
				// RunOnceや宛先整合を呼ぶ前から、構築時の通知先が有効です。
				path := filepath.Join(checkpointConfig.Directory, sourceID+".log-file-uploads.json")
				restore := blockRunnerCheckpointDocument(t, path)
				key := checkpointstore.CheckpointKey{Generation: "20260924-2", Compressed: true}
				_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{destination})
				var pending *checkpointstore.PendingSaveError
				require.ErrorAs(t, err, &pending)
				synctest.Wait()
				require.Empty(t, notifications, "initial save errors are returned to the caller, not reported twice")
				time.Sleep(checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				if closeFromReporter {
					require.Empty(t, notifications, "reporter Close waits for accepted persistence")
					restore()
					time.Sleep(checkpointstore.SaveRecoveryInterval)
					synctest.Wait()
				}
				var result notification
				select {
				case result = <-notifications:
				default:
					t.Fatal("checkpoint save retry failure was not reported, or reporter Close did not return")
				}
				require.Equal(t, manager.fileName.Dir, result.report.DirectoryPath)
				require.Empty(t, result.report.ObjectStorageUploads)
				var retryErr *logfile.CheckpointSaveRetryError
				require.ErrorAs(t, result.err, &retryErr)
				require.Equal(t, sourceID, retryErr.SourceID)
				require.Equal(t, path, retryErr.Path)
				if closeFromReporter {
					require.NoError(t, result.closeErr)
				} else {
					restore()
					time.Sleep(checkpointstore.SaveRecoveryInterval)
					synctest.Wait()
					payload, err := os.ReadFile(path)
					require.NoError(t, err)
					var persisted struct {
						Cursors []struct {
							DestinationID objectstorage.DestinationID `json:"destination_id"`
							Generation    string                      `json:"generation"`
						} `json:"cursors"`
					}
					require.NoError(t, json.Unmarshal(payload, &persisted))
					require.Len(t, persisted.Cursors, 1)
					require.Equal(t, destination, persisted.Cursors[0].DestinationID)
					require.Equal(t, key.Generation, persisted.Cursors[0].Generation)
					require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
				}
				synctest.Wait()
				// 残留する通知workerがあればsynctestの終了時に検出します。
				time.Sleep(checkpointstore.SaveRecoveryInterval)
				synctest.Wait()
				require.Empty(t, notifications, "stopped recovery must not emit further notifications")
			})
		})
	}
}

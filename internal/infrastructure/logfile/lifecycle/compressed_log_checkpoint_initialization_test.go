package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestArchiveCheckpointConstructionRejectsCorruptDocument(t *testing.T) {
	t.Parallel()
	root := resolvedTempDirForTest(t)
	const target = "checkpoint-initialization"
	config := testFileSeriesManagerConfig(root, target, 10, 0, 0, false)
	fileName := logpattern.FileNamePattern{Dir: filepath.Join(root, target, "logs"), Target: target, Extension: "log"}
	directory := newCheckpointDirectoryForTest(t, root)
	cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: directory, SourcePrefix: "file", AutoRecoverMissingCheckpoint: true}
	source := archiveCheckpointSourceID(cfg.SourcePrefix, "default", fileName)
	initialized, err := checkpointstore.NewFilesystemStore(directory, source)
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "initialized.Close()", initialized.Close, boundedwait.ForceCloserOf(initialized)))
	checkpointPath := filepath.Join(directory, source+".log-file-uploads.json")
	corruptPayload := []byte("{broken")
	require.NoError(t, os.WriteFile(checkpointPath, corruptPayload, 0o600))
	remote := newDestinationUploaderRecorder("primary")
	store, err := newArchiveCheckpointStore(cfg, "default", fileName, config.Layout(), remote, nil,
		archiveCheckpointFactoryOptions{activePeriod: currentLogDate()})
	require.ErrorIs(t, err, checkpointstore.ErrCorruptCheckpoint)
	require.Nil(t, store)
	require.Empty(t, remote.pathsFor("primary"))
	payload, err := os.ReadFile(checkpointPath)
	require.NoError(t, err)
	require.Equal(t, corruptPayload, payload)

	// 管理者による修正後は新しく構築し直せます。失敗したStoreは公開しません。
	require.NoError(t, os.WriteFile(checkpointPath, []byte(
		"{\"source\":\""+source+"\",\"cursors\":[]}"), 0o600))
	store, err = newArchiveCheckpointStore(cfg, "default", fileName, config.Layout(), remote, nil,
		archiveCheckpointFactoryOptions{activePeriod: currentLogDate()})
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
	require.Empty(t, remote.pathsFor("primary"))
}

func TestObjectStorageUploadManagerUsesPreparedSingleCheckpointStore(t *testing.T) {
	t.Parallel()
	const source = "initial-checkpoint"
	directory := resolvedTempDirForTest(t)
	store, err := checkpointstore.NewFilesystemStore(directory, source, checkpointstore.WithMissingCheckpointRecovery(false))
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
	manager := newObjectStorageUploadManager(newDestinationUploaderRecorder("primary"), store)
	require.NoError(t, manager.validate())
	require.Same(t, store, manager.checkpointStore)
	record, err := store.Load(context.Background(), checkpointstore.CheckpointKey{Generation: "20260923-1", Compressed: true})
	require.NoError(t, err)
	require.Empty(t, record.CompletedDestinationIDs)
	require.FileExists(t, filepath.Join(directory, source+".log-file-uploads.json"))
	require.FileExists(t, filepath.Join(directory, source+".destinations.json"))
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 2, "one success document and separate destination registration metadata")
}

func TestArchiveCheckpointFactoryClosesNotificationsOnConstructionFailure(t *testing.T) {
	for _, corruptDocument := range []bool{true, false} {
		name := "destination scan fails after store construction"
		if corruptDocument {
			name = "store construction fails"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				root := resolvedTempDirForTest(t)
				const target = "checkpoint-notification-cleanup"
				config := testFileSeriesManagerConfig(root, target, 10, 0, 0, false)
				fileName := logpattern.FileNamePattern{Dir: filepath.Join(root, target, "logs"), Target: target, Extension: "log"}
				cfg := pkgfsstore.ArchiveCheckpointConfig{
					Directory: newCheckpointDirectoryForTest(t, root), SourcePrefix: "file", AutoRecoverMissingCheckpoint: true,
				}
				if corruptDocument {
					sourceID := archiveCheckpointSourceID(cfg.SourcePrefix, "default", fileName)
					store, err := checkpointstore.NewFilesystemStore(cfg.Directory, sourceID)
					require.NoError(t, err)
					require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, store.ForceClose))
					require.NoError(t, os.WriteFile(filepath.Join(cfg.Directory, sourceID+".log-file-uploads.json"), []byte("{broken"), 0o600))
				} else {
					require.NoError(t, os.MkdirAll(filepath.Dir(fileName.Dir), 0o750))
					require.NoError(t, os.WriteFile(fileName.Dir, []byte("not a directory"), 0o600))
				}
				reporter := runReporterFunc(func(context.Context, RunReport, error) {
					t.Error("construction errors must be returned, not reported as retry failures")
				})
				store, err := newArchiveCheckpointStore(cfg, "default", fileName, config.Layout(), newDestinationUploaderRecorder("primary"), reporter,
					archiveCheckpointFactoryOptions{activePeriod: currentLogDate()})
				if store != nil {
					// 期待に反してStoreが返された場合も通知workerを止め、bubbleのdeadlockではなく
					// この試験の失敗として報告します。
					t.Cleanup(func() {
						_ = boundedwait.CloseInBubble(t, "unexpected store.Close()", store.Close, boundedwait.ForceCloserOf(store))
					})
				}
				require.Nil(t, store)
				if corruptDocument {
					require.ErrorIs(t, err, checkpointstore.ErrCorruptCheckpoint)
				} else {
					require.ErrorIs(t, err, checkpointstore.ErrRegistrationFileScan)
				}
				// Storeを返せない場合も通知workerが終了することを確認します。
				// workerが残っていれば、このsynctestは終了時に失敗します。
				synctest.Wait()
			})
		})
	}
}

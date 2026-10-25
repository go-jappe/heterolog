package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

// 送信の準備（prepareRun）の後、送信待ちの宛先が残る圧縮fileや完了を確認していない圧縮fileは、
// 同じ周期の処理で失敗・保護したpathでなくても保持期限purgeから保護します
// （shouldProtectFromLifecyclePurge → uploadPending）。今回の周期の保護pathを空にし、
// 受理済み処理の保護もない状態で、送信待ちの判定だけが保護することを確認します。
// 全宛先へ送信済みと確認した同じ期間の世代は、通常どおり削除します。
func TestUploadPendingAloneProtectsCompressedFileFromAgePurge(t *testing.T) {
	for _, pendingObserved := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending_checkpoint_loaded=%t", pendingObserved), func(t *testing.T) {
			ctx := context.Background()
			root := resolvedTempDirForTest(t)
			const target = "upload-pending-guard"
			const destination = objectstorage.DestinationID("archive")
			directory := filepath.Join(root, target, "logs")
			require.NoError(t, os.MkdirAll(directory, 0o750))
			// 保持期間1日を過ぎた期間の世代です。
			period := logDateDaysBefore(10)
			delivered := fileGeneration{Period: period, Index: 1}
			waiting := fileGeneration{Period: period, Index: 2}
			deliveredPath := filepath.Join(directory, target+"_"+period+"_1.log.gz")
			waitingPath := filepath.Join(directory, target+"_"+period+"_2.log.gz")
			writeGzipFile(t, deliveredPath, []byte("delivered generation"))
			writeGzipFile(t, waitingPath, []byte("generation waiting for upload"))
			latest := writeFileSeriesCheckpointLog(t, directory, target, currentLogDate(), 1)
			store := newTestFileCheckpointStore(t)
			require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{destination}, nil))
			_, err := store.AdvanceCompletionCursor(ctx, fileCheckpointKey(delivered, true), []objectstorage.DestinationID{destination})
			require.NoError(t, err)
			manager := NewFileSeriesManager(FileSeriesManagerOptions{
				FileName: logpattern.FileNamePattern{Dir: directory, Target: target, Extension: "log"},
				Config:   testFileSeriesManagerConfig(root, target, 0, 1, 0, false),
				Uploader: newDestinationUploaderRecorder(destination), CheckpointStore: store,
			})
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "manager.Close()", manager.Close, manager.ForceClose, boundedwait.NoError)

			var result fileSeriesPurgeResult
			require.NoError(t, manager.directoryGuard.RunWithLifecycleLock(func() error {
				if _, err := manager.objectStorageUpload.prepareRun(ctx); err != nil {
					return err
				}
				if _, err := manager.objectStorageUpload.loadFileCheckpoint(ctx, deliveredPath, delivered, true); err != nil {
					return err
				}
				if pendingObserved {
					if _, err := manager.objectStorageUpload.loadFileCheckpoint(ctx, waitingPath, waiting, true); err != nil {
						return err
					}
				}
				var err error
				result, err = manager.purgeFileSeriesByAgeAndGeneration(ctx, nil)
				return err
			}))
			require.True(t, result.report.ByAge.Ran)
			require.Len(t, result.report.ByAge.RemovalResults, 1)
			require.Equal(t, deliveredPath, result.report.ByAge.RemovalResults[0].File.Path)
			require.NoError(t, result.report.ByAge.RemovalResults[0].Err)
			require.NoFileExists(t, deliveredPath)
			require.FileExists(t, waitingPath, "an upload-pending generation is not an age purge candidate")
			require.FileExists(t, latest)
		})
	}
}

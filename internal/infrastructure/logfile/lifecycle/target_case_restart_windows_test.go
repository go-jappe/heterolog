package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	spools3 "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/uploader/s3"
	compressedlogworker "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/worker"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestLifecycleWindowsTargetCaseRestartRetriesArchiveAndProtectsLatestRaw(t *testing.T) {
	// 失敗したuploadは次の通常周期で重ねて処理せず、受理済みの復旧が再開します。
	// 復旧の再試行間隔は実時間で待たず、bubble内の時刻で進めます。bubbleの時計を実時刻から
	// 始め、過去の期間を実行時の時計から作るため、bubbleの2000-01-01開始に依存しません。
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		root := resolvedTempDirForTest(t)
		directory := filepath.Join(root, "App", "logs")
		require.NoError(t, os.MkdirAll(directory, 0o750))
		// 過去期間の最新rawを残し、現在期間の推測だけでは保護できない再起動を再現します。
		period := logDateDaysBefore(1)
		compressedPath := filepath.Join(directory, "App_"+period+"_1.log.gz")
		rawPath := filepath.Join(directory, "App_"+period+"_2.log")
		writeGzipFile(t, compressedPath, []byte("old uppercase archive"))
		originalRaw := []byte("latest raw must not be uploaded, compressed, or purged")
		require.NoError(t, os.WriteFile(rawPath, originalRaw, 0o600))
		originalCompressed, err := os.ReadFile(compressedPath)
		require.NoError(t, err)
		remote := newDestinationUploaderRecorder("primary")
		wantErr := errors.New("retry once")
		remote.failNext("primary", wantErr)
		manager := newFileSeriesManagerWithExistingDestinations(t, FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: directory, Target: "app", Extension: "log"},
			Config:   testFileSeriesManagerConfig(root, "app", 10, 0, 0, true), Uploader: remote,
		})
		t.Cleanup(func() {
			require.NoError(t, boundedwait.CloseInBubble(t, "manager.Close()", manager.Close, manager.ForceClose))
		})
		report, err := manager.RunOnce(ctx)
		require.ErrorIs(t, err, wantErr)
		require.Empty(t, report.Compressed)
		require.Empty(t, report.UploadedFileRemovals)
		got, err := os.ReadFile(compressedPath)
		require.NoError(t, err)
		require.Equal(t, originalCompressed, got)
		got, err = os.ReadFile(rawPath)
		require.NoError(t, err)
		require.Equal(t, originalRaw, got)
		require.Len(t, remote.pathsFor("primary"), 1)
		require.Equal(t, filepath.Base(compressedPath), filepath.Base(remote.pathsFor("primary")[0]))

		// 復旧の再試行間隔（既定5秒）より長く時刻を進め、受理済みの再送と削除を完了させます。
		time.Sleep(time.Minute)
		synctest.Wait()
		require.False(t, manager.logFileProcessingRecovery.Has(
			logFileProcessingRecoveryKeyForFile(fileGeneration{Period: period, Index: 1}, true)))
		require.NoFileExists(t, compressedPath)
		require.Len(t, remote.pathsFor("primary"), 2)
		require.Equal(t, filepath.Base(compressedPath), filepath.Base(remote.pathsFor("primary")[1]))

		// 次の通常周期も最新rawを送信・圧縮・削除しません。
		report, err = manager.RunOnce(ctx)
		require.NoError(t, err)
		require.Empty(t, report.Compressed)
		require.Empty(t, report.UploadedFileRemovals)
		got, err = os.ReadFile(rawPath)
		require.NoError(t, err)
		require.Equal(t, originalRaw, got)
		require.NoFileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
		require.Len(t, remote.pathsFor("primary"), 2)
	})
}

func TestLifecycleWindowsTargetCaseRestartMatchesExistingSpool(t *testing.T) {
	for _, contract := range []pkglogger.LoggerContractKind{pkglogger.LoggerContractSync, pkglogger.LoggerContractDelivery} {
		t.Run(contract.String(), func(t *testing.T) {
			ctx := context.Background()
			root := resolvedTempDirForTest(t)
			source := filepath.Join(resolvedTempDirForTest(t), "App_20260101_1.log.gz")
			writeGzipFile(t, source, []byte("spooled with original target spelling"))
			original, err := os.ReadFile(source)
			require.NoError(t, err)
			oldRoot, err := objectstoragespool.NewRoot(root, "App", objectstorage.UploadTargetS3, contract)
			require.NoError(t, err)
			oldStore, err := objectstoragespool.NewStore(oldRoot)
			require.NoError(t, err)
			_, err = oldStore.SaveRetryableFailure(ctx, source)
			require.NoError(t, err)

			newRoot, err := objectstoragespool.NewRoot(root, "app", objectstorage.UploadTargetS3, contract)
			require.NoError(t, err)
			store, err := objectstoragespool.NewStore(newRoot)
			require.NoError(t, err)
			remote := newDestinationUploaderRecorder("primary")
			replayUploader, err := spools3.New(remote)
			require.NoError(t, err)
			worker, err := compressedlogworker.NewWorker(store, compressedlogworker.RouteRegistry{{
				FileName: logpattern.FileNamePattern{
					Dir: filepath.Dir(source), Target: "app", Extension: "log",
				},
				Uploader: replayUploader,
			}}, time.Minute)
			require.NoError(t, err)
			report, err := worker.RunOnce(ctx)
			require.NotErrorIs(t, err, compressedlogworker.ErrNoMatchingRoute)
			require.NoError(t, err)
			require.True(t, report.File.Claimed)
			require.True(t, report.File.UploadSucceeded)
			require.Equal(t, pkgspool.StateDone, report.File.State)
			done, err := store.ListFiles(ctx, pkgspool.StateDone)
			require.NoError(t, err)
			require.Len(t, done, 1)
			require.Equal(t, filepath.Base(source), done[0].FileName)
			got, err := os.ReadFile(done[0].Path)
			require.NoError(t, err)
			require.Equal(t, original, got)
			got, err = os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, original, got)
			require.Len(t, remote.pathsFor("primary"), 1)
			require.Equal(t, filepath.Base(source), filepath.Base(remote.pathsFor("primary")[0]))
		})
	}
}

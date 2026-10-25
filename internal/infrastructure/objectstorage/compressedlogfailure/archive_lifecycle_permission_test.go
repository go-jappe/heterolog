package compressedlogfailure

import (
	"context"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestArchiveLifecycleReportsPermissionFailureAndContinuesNextCycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		root, err := compressedlogarchive.NewRoot(t.TempDir(), objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
		require.NoError(t, err)
		root, err = root.ForDestination("destination-a")
		require.NoError(t, err)
		store, err := compressedlogarchive.NewStore(root)
		require.NoError(t, err)
		type purgeResult struct {
			report compressedlogarchive.PurgeReport
			err    error
		}
		results := make(chan purgeResult, 1)
		receiveResult := func() purgeResult {
			t.Helper()
			synctest.Wait()
			select {
			case result := <-results:
				return result
			default:
				t.Fatal("archive purge did not report its result")
				return purgeResult{}
			}
		}
		owner, _, err := acquireArchiveLifecycle(store, NewFileArchiveConfigAdapter(objectstorage.CompressedLogFileUploadFailureArchiveConfig{
			LifecycleInterval: time.Millisecond,
			Retryable: objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
				Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 1 << 20,
			},
		}), clock.NewUTCClock(), func(ctx context.Context, report compressedlogarchive.PurgeReport, err error) {
			select {
			case results <- purgeResult{report: report, err: err}:
			case <-ctx.Done():
			}
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, owner.close()) })
		// 初回purgeが待機状態に入るまで待ち、正常終了の結果も通知されることを確認します。
		require.NoError(t, receiveResult().err)
		fileDate := time.Now().UTC().AddDate(0, 0, -2).Format("20060102")
		source, payload := writeCompressedLog(t, t.TempDir(), "app_"+fileDate+"_1.log.gz")
		archived, err := store.Save(context.Background(), compressedlogarchive.SaveInput{
			SourcePath: source, FailureCategory: uploader.FailureCategoryRetryable,
		})
		require.NoError(t, err)
		old := time.Now().Add(-48 * time.Hour)
		require.NoError(t, os.Chtimes(archived.Path, old, old))
		restore := denyArchiveLifecycleDirectoryRead(t, root.RetryableDir())
		time.Sleep(time.Millisecond)
		failed := receiveResult()
		require.ErrorIs(t, failed.err, os.ErrPermission)
		require.Zero(t, failed.report.ByFailureCategory[uploader.FailureCategoryRetryable].DeletedFileCount)
		assertFileBytes(t, archived.Path, payload)

		restore()
		time.Sleep(time.Millisecond)
		// 権限復旧後は次の周期で削除し、削除件数と容量も通知されることを確認します。
		result := receiveResult()
		require.NoError(t, result.err)
		recovered := result.report.ByFailureCategory[uploader.FailureCategoryRetryable]
		require.Equal(t, 1, recovered.DeletedFileCount)
		require.Equal(t, uint64(len(payload)), recovered.DeletedBytes)
		require.Zero(t, recovered.RemainingBytes)
		require.NoFileExists(t, archived.Path)
		assertFileBytes(t, source, payload)
	})
}

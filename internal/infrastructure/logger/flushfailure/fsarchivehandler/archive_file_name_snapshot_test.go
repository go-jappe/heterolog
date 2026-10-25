package fsarchivehandler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFailureArchiveConsecutiveRotationsKeepFileNameSnapshots(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		t.Run(string(failureCategory), func(t *testing.T) {
			now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
			const destination objectstorage.DestinationID = "primary"
			remote := newDestinationFailureArchiveUploader(destination)
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.enableRotate = true
				cfg.enableFileCompress = true
				cfg.setArchiveUploader(failureCategory, remote)
			})
			handler := pipeline.handler
			result := retryableArchiveResult(now, "snapshot-race-record")
			if failureCategory == failurearchive.FailureCategoryPermanent {
				result.DeadRecords = result.RetryableRecords
				result.RetryableRecords = nil
			}

			// writer lock内でactive fileを上限まで埋め、公開入口の通常writeでrotationさせます。
			// 先行taskのcheckpoint更新とcleanupが進む間に次のrotationが命名情報を更新するため、
			// -raceでは不変fieldしか使わないhelperによるstruct全体の無保護な読み取りも検出します。
			var expectedArchives []string
			for index := 0; index < 35; index++ {
				handler.Fsstore.Mu.Lock()
				activeFile := handler.Fsstore.FileRetryable
				oldPath := handler.Fsstore.ActiveFilePathRetryable
				if failureCategory == failurearchive.FailureCategoryPermanent {
					activeFile = handler.Fsstore.FilePermanent
					oldPath = handler.Fsstore.ActiveFilePathPermanent
				}
				// append用handleのまま通常の追記で閾値を超えさせます。
				// WindowsではこのhandleによるTruncateは許可されません。
				_, err := activeFile.Write(make([]byte, handler.Config.MaxFileSize()+1))
				handler.Fsstore.Mu.Unlock()
				require.NoError(t, err)
				expectedArchives = append(expectedArchives, oldPath+pkgfsstore.ExtensionCompressGzip)
				require.NoError(t, pipeline.HandleFailure(context.Background(), result))
			}
			activePath, err := pipeline.ActiveArchivePath(failureCategory)
			require.NoError(t, err)
			// 古い世代の完了記録で順序待ちの世代を直ちに再開するため、Shutdownは再開間隔を
			// 世代数分待ちません。この期限は停止を検出するためだけの上限です。
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, pipeline.Shutdown(ctx))

			for _, path := range expectedArchives {
				require.Equal(t, 1, remote.attemptCount(path, destination), path)
				require.NoFileExists(t, path)
			}
			require.Len(t, readArchiveRecords(t, activePath), 1)
		})
	}
}

package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// rotationで一部の宛先だけ送信に失敗した世代は、成功した宛先を完了cursorへ記録したまま、
// Shutdownが受理済みの再開処理で失敗した宛先だけを再送して終えます。rawは送信後も残し、
// 圧縮fileは全宛先の完了後に削除します。
func TestFailureArchiveShutdownRetriesOnlyFailedDestination(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		for _, compress := range []bool{false, true} {
			name := string(category) + "/raw"
			if compress {
				name = string(category) + "/compressed"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					remote := newDestinationFailureArchiveUploader("primary", "secondary")
					remote.failNext("secondary", 1)
					pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
						cfg.enableFileCompress = compress
						cfg.setArchiveUploader(category, remote)
					})
					handler := pipeline.handler
					require.NotNil(t, handler.compressedArchiveUploadCheckpointStore(category))
					fileName, err := handler.archiveFileNamePattern(category)
					require.NoError(t, err)
					sourcePath := filepath.Join(fileName.Dir, "test-app_20260923_1.jsonl")
					require.NoError(t, os.WriteFile(sourcePath, []byte("accepted archive\n"), 0o600))
					uploadPath := sourcePath
					if compress {
						uploadPath += ".gz"
					}
					_, err = handler.processRotatedArchive(context.Background(), "active-file", sourcePath, category, fileName, nil)
					require.NoError(t, err)
					require.Equal(t, 1, remote.attemptCount(uploadPath, "primary"))
					require.Equal(t, 1, remote.attemptCount(uploadPath, "secondary"))
					require.FileExists(t, uploadPath)

					// 宛先ごとの成功を完了cursorに保持し、通常writeなしで未完了だけを終えます。
					require.NoError(t, pipeline.Shutdown(context.Background()))
					require.Equal(t, 1, remote.attemptCount(uploadPath, "primary"))
					require.Equal(t, 2, remote.attemptCount(uploadPath, "secondary"))
					if compress {
						require.NoFileExists(t, uploadPath)
						require.NoFileExists(t, sourcePath)
					} else {
						require.FileExists(t, sourcePath)
					}
				})
			})
		}
	}
}

// 送信失敗の通知がpanicしても、成功した宛先は完了cursorに残り、受理済みの再開処理が
// 失敗した宛先だけを再送します。
func TestFailureArchiveNotificationPanicPreservesCompletedDestinations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		category := failurearchive.FailureCategoryRetryable
		remote := newDestinationFailureArchiveUploader("primary", "secondary")
		remote.failNext("secondary", 1)
		pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
			cfg.enableFileCompress = true
			cfg.setArchiveUploader(category, remote)
		})
		handler := pipeline.handler
		require.NotNil(t, handler.compressedArchiveUploadCheckpointStore(category))
		fileName, err := handler.archiveFileNamePattern(category)
		require.NoError(t, err)
		sourcePath := filepath.Join(fileName.Dir, "test-app_20260923_1.jsonl")
		require.NoError(t, os.WriteFile(sourcePath, []byte("accepted archive\n"), 0o600))
		uploadPath := sourcePath + ".gz"

		const notificationPanic = "archive OnError panic"
		var notifications atomic.Int64
		handler.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
			if notifications.Add(1) == 1 {
				panic(notificationPanic)
			}
		}
		// 即時通知がpanicしても、既に成功したprimaryの記録は再開処理へ引き継ぎます。
		require.PanicsWithValue(t, notificationPanic, func() {
			_, _ = handler.processRotatedArchive(context.Background(), "active-file", sourcePath, category, fileName, nil)
		})
		require.EqualValues(t, 1, notifications.Load())
		require.Equal(t, 1, remote.attemptCount(uploadPath, "primary"))
		require.Equal(t, 1, remote.attemptCount(uploadPath, "secondary"))
		require.FileExists(t, uploadPath)

		require.NoError(t, pipeline.Shutdown(context.Background()))
		require.Equal(t, 1, remote.attemptCount(uploadPath, "primary"), "notification panic must not lose successful destinations")
		require.Equal(t, 2, remote.attemptCount(uploadPath, "secondary"))
		require.NoFileExists(t, uploadPath)
		require.NoFileExists(t, sourcePath)
	})
}

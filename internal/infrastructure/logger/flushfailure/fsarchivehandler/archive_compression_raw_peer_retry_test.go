package fsarchivehandler

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestInlineFailureArchiveBootstrapDefersCompressedPeerUntilRawRetirement(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		t.Run(string(failureCategory), func(t *testing.T) {
			// 受理済みの再試行は、再試行間隔を仮想時間で進めて完了させます。
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
				codec := &failureArchiveRecoveryCodec{}
				const destination objectstorage.DestinationID = "original-destination"
				remote := newDestinationFailureArchiveUploader(destination)
				// 宛先は構築時に登録し、その後に作られた世代を送信対象にします。
				pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
					cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
					cfg.enableFileCompress = true
					cfg.compressFormat = codec.Format()
					cfg.compressionCodec = codec
					cfg.setArchiveUploader(failureCategory, remote)
				})
				handler := pipeline.handler
				state := handler.directoryForFailureCategory(failureCategory)
				fileName, err := handler.archiveFileNamePattern(failureCategory)
				require.NoError(t, err)
				rawPath := filepath.Join(fileName.Dir, fileName.Prefix+fileName.Target+"_20260908_1."+fileName.Extension)
				compressedPath := rawPath + ".gz"
				readyPath := filepath.Join(fileName.Dir, fileName.Prefix+fileName.Target+"_20260908_2."+fileName.Extension+".gz")
				const content = "failure archive whose compression was published before a restart"
				require.NoError(t, os.WriteFile(rawPath, []byte(content), 0o600))
				writeFailureArchiveRecoveryGzip(t, compressedPath, content)
				writeFailureArchiveRecoveryGzip(t, readyPath, "already retired generation")

				// final公開後、raw retirement前に停止した後の起動と同じく、完了記録はまだありません。
				key := failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, compressedPath)
				checkpoint, err := state.uploadCheckpoints.Load(ctx, key)
				require.NoError(t, err)
				require.Empty(t, checkpoint.CompletedDestinationIDs)

				// 構築時に予約されたbootstrapのtarget-only scanはrawを回復しません。peerを先に送信・削除せず、
				// 次の自然なfull scanが同じ圧縮成果物を検証してrawをretireできるよう残します。
				require.NoError(t, handler.runInlineArchivePurgeIfNeeded(ctx, failureCategory))
				require.FileExists(t, rawPath)
				require.FileExists(t, compressedPath)
				require.Zero(t, remote.attemptCount(compressedPath, destination))
				// 後続世代も、古いrawの圧縮・送信が完了するまで待機します。
				require.FileExists(t, readyPath)
				require.Zero(t, remote.attemptCount(readyPath, destination))
				require.Zero(t, codec.verifyCalls.Load())
				require.Zero(t, codec.compressCalls.Load())

				require.NoError(t, handler.runInlineArchivePurgeIfNeeded(ctx, failureCategory))
				require.NoFileExists(t, rawPath)
				require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(rawPath))
				require.FileExists(t, compressedPath)
				require.Zero(t, remote.attemptCount(compressedPath, destination))
				require.EqualValues(t, 1, codec.verifyCalls.Load())
				require.Zero(t, codec.compressCalls.Load())

				handler.Clock = archiveFixedClock{now: now.Add(testUsageCacheTTL)}
				require.NoError(t, handler.runInlineArchivePurgeIfNeeded(ctx, failureCategory))
				drainArchiveRecoveryForTest(t, handler)
				require.NoFileExists(t, rawPath)
				require.NoFileExists(t, compressedPath)
				require.NoFileExists(t, readyPath)
				require.Equal(t, 1, remote.attemptCount(compressedPath, destination))
				require.Equal(t, 1, remote.attemptCount(readyPath, destination))
				require.Zero(t, codec.compressCalls.Load())
				scheduled, _ := archiveUploadRetryScheduleSnapshot(&state.uploadRetry)
				require.False(t, scheduled)
			})
		})
	}
}

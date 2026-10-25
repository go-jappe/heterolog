package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestInlineFailureArchiveFullScanDoesNotTreatCompressedPeerSymlinkAsCompleted(
	t *testing.T,
) {
	now := time.Date(2026, time.August, 30, 13, 15, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	compressedTargetPath := filepath.Join(t.TempDir(), "valid-target.jsonl.gz")
	writeFailureArchiveRecoveryGzip(t, compressedTargetPath, "unrelated valid compressed file")
	testutil.Symlink(t, compressedTargetPath, compressedPath)

	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))

	require.FileExists(t, rawPath)
	peerInfo, err := os.Lstat(compressedPath)
	require.NoError(t, err)
	require.NotZero(t, peerInfo.Mode()&os.ModeSymlink)
	require.Zero(t, codec.compressCalls.Load())
	require.Zero(t, codec.verifyCalls.Load())

	// symlinkは完了済みpeerではないためrawは残ります。path占有を解消した
	// 後のfull scanで、保持したrawを通常どおり圧縮します。
	require.NoError(t, os.Remove(compressedPath))
	pipeline.handler.Clock = archiveFixedClock{now: now.Add(testUsageCacheTTL)}
	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())
	peerInfo, err = os.Lstat(compressedPath)
	require.NoError(t, err)
	require.True(t, peerInfo.Mode().IsRegular())
}

func TestFailureArchiveRejectsCompressedLinkBeforeCheckpointOrCompression(t *testing.T) {
	for _, operation := range []string{"resolve", "full_scan", "target_upload", "rotation"} {
		t.Run(operation, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 18, 30, 0, 0, time.UTC)
			codec := &failureArchiveRecoveryCodec{}
			failureCategory := failurearchive.FailureCategoryRetryable
			checkpointFailure := errors.New("checkpoint must not be reached")
			var remote *recordingFailureArchiveUploader
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
				cfg.enableFileCompress = true
				cfg.compressFormat = codec.Format()
				cfg.compressionCodec = codec
				switch operation {
				case "resolve":
					// checkpointを読むと失敗する構成で、候補の検証がcheckpointより前に行われることを確認します。
					cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader("primary"))
					store := &transientArchiveCheckpointStore{
						loadFailures: 1 << 30, recordFailures: 1 << 30, failure: checkpointFailure,
					}
					cfg.embedArchiveCheckpointStore(failureCategory, &store.Store, store)
				case "target_upload":
					cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader("primary"))
				case "rotation":
					remote = newRecordingFailureArchiveUploader(nil, nil)
					cfg.setArchiveUploader(failureCategory, remote)
				}
			})
			handler := pipeline.handler
			state := handler.directoryForFailureCategory(failureCategory)
			rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260913", 1)
			gzipPath := rawPath + pkgfsstore.ExtensionCompressGzip
			writeFailureArchiveRecoveryGzip(t, gzipPath, "regular first candidate")
			linkPath := rawPath + pkgfsstore.ExtensionCompressZstd
			targetPath := filepath.Join(t.TempDir(), "outside.zst")
			require.NoError(t, os.WriteFile(targetPath, []byte("external file"), 0o600))
			testutil.Symlink(t, targetPath, linkPath)

			var operationErr error
			switch operation {
			case "resolve":
				_, _, operationErr = handler.findExistingCompressedArchivePath(rawPath)
			case "full_scan":
				// 回復中の候補拒否はOnErrorで通知します。checkpointの事前登録には依存しません。
				handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					operationErr = errors.Join(operationErr, err)
				}
				require.NoError(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory))
			case "target_upload":
				observation := fsstore.GroupFilesByCompressionSource(fsstore.InspectFilePaths([]string{gzipPath, linkPath}), "jsonl")
				protection, err := handler.retainArchivePathsForUpload(context.Background(), failureCategory,
					handler.Fsstore.FileNameRetryable, observation)
				operationErr = err
				require.Empty(t, protection.paths)
			case "rotation":
				reported := make(chan error, 4)
				handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
					reported <- err
				}
				handler.dispatchArchiveRotatedFile(context.Background(), handler.Fsstore.ActiveFilePathRetryable, rawPath, failureCategory)
				waitForFailureArchiveTasksForTest(t, handler)
				require.Empty(t, remote.inputs)
				select {
				case operationErr = <-reported:
				default:
					t.Fatal("compressed peer link must notify before starting an archive task")
				}
			}
			require.ErrorIs(t, operationErr, fsstore.ErrNotRegularFile)
			require.NotErrorIs(t, operationErr, checkpointFailure)
			require.FileExists(t, rawPath)
			require.FileExists(t, gzipPath)
			info, err := os.Lstat(linkPath)
			require.NoError(t, err)
			require.NotZero(t, info.Mode()&os.ModeSymlink)
			require.Zero(t, codec.compressCalls.Load())
			require.Zero(t, codec.verifyCalls.Load())
			assertArchivePathProtected(t, state.guard, rawPath, false)
		})
	}
}

package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgtime "github.com/go-jappe/heterolog/pkg/time"

	"github.com/stretchr/testify/require"
)

func TestFileLoggerPendingRetryWaitsForRawSourceRetirement(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		name := "memory-only"
		if persistent {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			synctestFromWallClock(t, func(t *testing.T) {
				ctx := context.Background()
				now := time.Date(2026, time.September, 9, 10, 0, 0, 0, time.UTC)
				codec := &inlineRecoveryCompressionCodec{}
				logger := newInlineCompressionRecoveryTestLogger(t, now, "unretired-peer-"+name, codec)
				remote := &recordingArchiveUploader{}
				failFirst := &sequenceArchiveUploader{errs: []error{errors.New("first generation upload failed")}}
				combined := archiveUploaderFunc(func(ctx context.Context, input uploader.UploadRequest) error {
					_ = remote.Upload(ctx, input)
					return failFirst.Upload(ctx, input)
				})
				logger.ArchiveUploader = newMultiArchiveUploader(t,
					uploader.Destination{ID: "required", Uploader: combined},
				)
				if persistent {
					logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, t.TempDir(), "unretired_peer")
				}
				counts := observeCleanupScans(logger)
				first := writePendingCompressedLog(t, logger, "20260820", 1, now)
				logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()
				require.NoError(t, purgeFileLoggerForTest(ctx, logger))
				require.Equal(t, []string{first}, archiveUploadInputPaths(remote.inputsSnapshot()))
				ttl := logger.inlinePurge.config.UsageCacheTTL()

				// 先行世代のupload失敗から次のretryまでの間に別世代をrotationし、
				// final公開後のsource retirementだけを既存codecの障害注入で失敗させます。
				logger.Clock = pkgclock.NewFixedClock(now.Add(ttl / 2))
				codec.createSourceRetirementConflictOnce.Store(true)
				raw := fillActiveFileLoggerForRotation(t, logger)
				require.NoError(t, rotateFileLoggerForTest(ctx, logger))
				waitBackgroundForTest(t, logger)
				peer := raw + pkgfsstore.ExtensionCompressGzip
				require.FileExists(t, raw)
				require.FileExists(t, peer)
				require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(raw))
				require.Equal(t, []string{first}, archiveUploadInputPaths(remote.inputsSnapshot()))
				fullScans := counts.all.Load()
				targetScans := counts.byTarget.Load()

				// 期限済みのtarget-only retryではrawが残る世代を保留します。
				// 取得済み一覧だけで判定し、同じ観測の別世代は通常どおり再送できます。
				logger.Clock = pkgclock.NewFixedClock(now.Add(ttl + time.Second))
				require.NoError(t, purgeFileLoggerForTest(ctx, logger))
				require.FileExists(t, raw)
				require.FileExists(t, peer)
				if persistent {
					require.FileExists(t, first, "受理済み送信は独立した再開処理が所有する")
					require.Equal(t, []string{first}, archiveUploadInputPaths(remote.inputsSnapshot()))
				} else {
					require.NoFileExists(t, first)
					require.Equal(t, []string{first, first}, archiveUploadInputPaths(remote.inputsSnapshot()))
				}
				require.Equal(t, fullScans, counts.all.Load())
				require.Equal(t, targetScans+1, counts.byTarget.Load())

				// 受理済み圧縮を独立した復旧が所有し、通常scanは横取りしません。
				verificationCount := codec.verifyCalls.Load()
				require.NoError(t, purgeFileLoggerForTest(ctx, logger))
				requireFileLoggerClosesForTest(t, logger)
				require.NoFileExists(t, raw)
				require.NoFileExists(t, peer)
				require.EqualValues(t, 1, codec.compressCalls.Load())
				require.Greater(t, codec.verifyCalls.Load(), verificationCount)
				require.Equal(t, []string{first, first, peer}, archiveUploadInputPaths(remote.inputsSnapshot()))
				require.NoFileExists(t, peer)
			})
		})
	}
}

type unverifiableArchiveCompressionCodec struct {
	inlineRecoveryCompressionCodec
}

func (c *unverifiableArchiveCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	c.verifyCalls.Add(1)
	return errors.New("compressed peer temporarily unreadable")
}

func TestFileLoggerUnretiredGenerationSurvivesPurgeAfterRepeatedRecoveryFailure(t *testing.T) {
	for _, mode := range []string{"age", "generation", "directory size"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
			codec := &unverifiableArchiveCompressionCodec{}
			logger := newInlineCompressionRecoveryTestLogger(t, now, "unretired-purge", codec)
			old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
			var files []infrafsstore.FileInfo
			for index := 0; index <= logger.inlinePurge.config.MaxFileBackups(); index++ {
				raw := writeInlineRecoveryRawArchive(t, logger, "20260820", uint64(index+1))
				peer := raw + pkgfsstore.ExtensionCompressGzip
				writeGzipFile(t, peer, "published before source retirement failed")
				for _, path := range []string{raw, peer} {
					require.NoError(t, os.Chtimes(path, old, old))
					files = append(files, fileInfoForPendingTest(t, path, logger.inlinePurge.config.MaxDirSize()+1, old))
				}
			}

			// 既存peerの完全性検証が繰り返し失敗しても、終了時にtask保護を
			// 解放したrawとpeerを、後続の各purgeが削除対象へ戻してはいけません。
			for attempt := 0; attempt < 2; attempt++ {
				previousVerifications := codec.verifyCalls.Load()
				logger.inlinePurge.invalidateDirectorySizeEstimate()
				require.NoError(t, purgeFileLoggerForTest(ctx, logger))
				require.Greater(t, codec.verifyCalls.Load(), previousVerifications)
			}
			switch mode {
			case "age":
				result := logger.purgeLogsByAge(ctx, logger.inlinePurge, files, newPurgeFileNames(logger.Fsstore.FileName), pkgtime.StartOfDay(now.AddDate(0, 0, 1)), nil, runNotificationImmediately)
				require.True(t, result.Ran)
				require.Empty(t, result.RemovalResults)
				require.Equal(t, files, result.Remaining)
			case "generation":
				result := logger.purgeLogsByGeneration(ctx, logger.inlinePurge, files, newPurgeFileNames(logger.Fsstore.FileName), nil, runNotificationImmediately)
				require.Empty(t, result.RemovalResults)
				require.Equal(t, files, result.Remaining)
			case "directory size":
				remaining := logger.purgeByDirectorySize(ctx, logger.inlinePurge, files, newPurgeFileNames(logger.Fsstore.FileName), nil, runNotificationImmediately)
				require.Equal(t, files, remaining)
			}
			for _, file := range files {
				require.FileExists(t, file.Path)
			}
		})
	}
}

func TestFileLoggerRecoveredPendingRetirementSchedulesTargetOnlyUploadAfterBootstrap(
	t *testing.T,
) {
	now := time.Date(2026, time.August, 30, 16, 30, 0, 0, time.UTC)
	logger := newInlineCompressionRecoveryTestLogger(
		t,
		now,
		"pending-retirement-upload-retry",
		&inlineRecoveryCompressionCodec{},
	)
	runtime := logger.inlinePurge
	upload := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, upload)
	runtime.seriesUploadRetry.scheduleImmediateRetry()
	counts := observeCleanupScans(logger)

	// process起動時のupload bootstrapを、pending archiveがない最初のfull scanで
	// 消費します。以降のupload予約はretirement回復自身が作る必要があります。
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.EqualValues(t, 1, counts.all.Load())
	require.Zero(t, counts.byTarget.Load())
	require.Empty(t, upload.inputsSnapshot())
	require.False(t, runtime.rawArchiveRecoveryBootstrapPending.Load())
	scheduled, _ := fileSeriesUploadRetryScheduleSnapshot(runtime)
	require.False(t, scheduled)

	fileName := logger.Fsstore.FileName
	rawPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260829_1."+fileName.Extension,
	)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	pendingPath := logpattern.CompressionSourceDeletePendingPath(rawPath)
	writeGzipFile(t, compressedPath, "already committed archive")
	require.NoError(t, os.WriteFile(pendingPath, []byte("retired raw source"), 0o600))

	// retirementの残骸はfull scanで発見してdurably削除します。ただし、そのscanのplanは
	// upload retryを含まないためremote I/Oを開始せず、TTL後の予約だけを作ります。
	runtime.invalidateDirectorySizeEstimate()
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, pendingPath)
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.Empty(t, upload.inputsSnapshot())
	require.EqualValues(t, 2, counts.all.Load())
	require.Zero(t, counts.byTarget.Load())

	scheduled, retryAt := fileSeriesUploadRetryScheduleSnapshot(runtime)
	require.True(t, scheduled)
	require.Equal(t, now.Add(runtime.config.UsageCacheTTL()), retryAt)

	// TTL後の次の自然なcleanupは、このphysical seriesだけをtarget-only走査します。
	// 検証済み圧縮fileを一度だけuploadし、成功後にlocal copyを削除します。
	logger.Clock = pkgclock.NewFixedClock(retryAt)
	logger.triggerCleanup(context.Background(), logger.Fsstore.FileName)
	waitBackgroundForTest(t, logger)

	require.Equal(
		t,
		[]string{compressedPath},
		archiveUploadInputPaths(upload.inputsSnapshot()),
	)
	require.NoFileExists(t, compressedPath)
	require.EqualValues(t, 2, counts.all.Load())
	require.EqualValues(t, 1, counts.byTarget.Load())
	scheduled, _ = fileSeriesUploadRetryScheduleSnapshot(runtime)
	require.False(t, scheduled)
}

package file

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestRemainingLogProcessingKeepsSelectedFileNameSnapshot(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 9, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "selected-name-snapshot")
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, t.TempDir(), "selected_snapshot")
	remote := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, remote)
	path := writePendingCompressedLog(t, logger, "20260820", 3, now)
	fileName := logger.Fsstore.FileName
	owner := appendlog.NewDirectoryGuardToken()
	var protection []string
	var groups []archiveUploadGroup
	defer func() { logger.lifecycleGuard.UnprotectPaths(owner, protection...) }()
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		var err error
		groups, err = logger.retainRemainingLogFilesForOwner(ctx,
			fsstore.GroupFilesByCompressionSource(fsstore.InspectFilePaths([]string{path}), fileName.Extension),
			fileName, owner, &protection)
		return err
	}))
	require.Len(t, groups, 1)

	// 選択後にlogger側の命名情報が変わっても、取得した候補の系列と世代で処理します。
	logger.Fsstore.FileName.Target = "changed-after-selection"
	defer func() { logger.Fsstore.FileName = fileName }()
	require.False(t, logger.processRemainingLogFiles(ctx, groups, runNotificationImmediately))
	require.Equal(t, []string{path}, archiveUploadInputPaths(remote.inputsSnapshot()))
	require.NoFileExists(t, path)
	completed, err := logger.archiveUploadCheckpoints.Load(ctx, groups[0].key)
	require.NoError(t, err)
	require.True(t, completed.Complete(logger.ArchiveUploader.DestinationIDs()))
}

func TestFileLoggerConsecutiveRotationsKeepArchiveFileNameSnapshots(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		now := time.Date(2026, time.September, 9, 10, 0, 0, 0, time.UTC)
		logger := newInlineCompressionCheckpointTestLogger(t, now, "archive-name-snapshots")
		logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(
			t, t.TempDir(), "archive_name_snapshots",
		)
		remote := &recordingArchiveUploader{}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "primary", Uploader: remote},
		)

		// writer lock内でactive fileを上限まで埋め、通常writeからrotationさせます。
		// 先行archive taskが動いている間に次のrotationがFileNameを更新するため、
		// -raceでは、系列を識別する不変fieldだけを使うhelperによるstruct全体の
		// 読み取りも含めて競合を検出できます。
		var expectedArchives []string
		for index := 0; index < 40; index++ {
			oldPath := fillActiveFileLoggerForRotation(t, logger)
			expectedArchives = append(expectedArchives, oldPath+pkgfsstore.ExtensionCompressGzip)
			require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
				OccurredAt: now,
				Level:      pkglogger.LogLevelInfo,
				Message:    "rotate while previous archive checkpoints are being updated",
			}))
		}
		waitBackgroundForTest(t, logger)

		// Later rotations wait behind earlier in-flight generations until the next retry.
		logger.Clock = pkgclock.NewFixedClock(now.Add(2 * logger.inlinePurge.config.UsageCacheTTL()))
		require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
		requireArchiveRecoveryClosesForTest(t, logger)
		require.ElementsMatch(t, expectedArchives, archiveUploadInputPaths(remote.inputsSnapshot()))
		for _, path := range expectedArchives {
			require.NoFileExists(t, path)
		}
		require.FileExists(t, logger.Fsstore.ActiveFilePath)
	})
}

func TestFileLoggerRecoveryAndPendingRetriesKeepFileNameSnapshotsDuringRotation(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		now := time.Date(2026, time.September, 9, 11, 0, 0, 0, time.UTC)
		logger := newInlineCompressionCheckpointTestLogger(t, now, "recovery-name-snapshots")
		logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(
			t, t.TempDir(), "recovery_name_snapshots",
		)
		remote := &recordingArchiveUploader{}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "primary", Uploader: remote},
		)

		// process停止後に残ったinactiveなrawと、世代ごとに圧縮形式が異なるfileを用意します。
		// writerがrotationを進める間も、通常cleanupが開始時の命名情報を使って
		// raw回復と圧縮fileの再送を完了することを確認します。
		recoveryRaw := writeCanonicalRawLog(t, logger, "20260820", 1)
		expectedArchives := []string{recoveryRaw + pkgfsstore.ExtensionCompressGzip}
		for index := 2; index <= 9; index++ {
			raw := writeCanonicalRawLog(t, logger, "20260820", index)
			suffix := pkgfsstore.ExtensionCompressGzip
			if index%2 == 0 {
				suffix = pkgfsstore.ExtensionCompressZstd
			}
			path := raw + suffix
			require.NoError(t, os.WriteFile(path, []byte("pending archive"), 0o600))
			expectedArchives = append(expectedArchives, path)
			require.NoError(t, os.Remove(raw))
		}

		for phase := 0; phase < 2; phase++ {
			if phase == 1 {
				// 初回の通常走査でrawを回復し、次のTTLへuploadを予約します。
				// 全background taskの完了後にだけClockを進めます。
				require.NoFileExists(t, recoveryRaw)
				require.FileExists(t, recoveryRaw+pkgfsstore.ExtensionCompressGzip)
				now = now.Add(logger.inlinePurge.config.UsageCacheTTL() + time.Second)
				logger.Clock = pkgclock.NewFixedClock(now)
			}
			for index := 0; index < 16; index++ {
				oldPath := fillActiveFileLoggerForRotation(t, logger)
				expectedArchives = append(expectedArchives, oldPath+pkgfsstore.ExtensionCompressGzip)
				require.NoError(t, logger.HandleLogEntry(context.Background(), &logging.LogEntry{
					OccurredAt: now,
					Level:      pkglogger.LogLevelInfo,
					Message:    "rotate while cleanup processes an earlier series snapshot",
				}))
			}
			waitBackgroundForTest(t, logger)
		}

		// Later rotations wait behind earlier in-flight generations until the next retry.
		logger.Clock = pkgclock.NewFixedClock(now.Add(2 * logger.inlinePurge.config.UsageCacheTTL()))
		require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
		requireArchiveRecoveryClosesForTest(t, logger)
		require.ElementsMatch(t, expectedArchives, archiveUploadInputPaths(remote.inputsSnapshot()))
		for _, path := range expectedArchives {
			require.NoFileExists(t, path)
		}
		require.FileExists(t, logger.Fsstore.ActiveFilePath)
	})
}

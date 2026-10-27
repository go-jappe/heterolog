package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/require"
)

func TestCompressedArchiveUploadSuccessDeletesLocalFileAndInvalidatesEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "uploaded-compressed-cleanup", "uploaded-compressed-cleanup")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	sourcePath := filepath.Join(logger.Fsstore.FileName.Dir, "uploaded-compressed-rotated.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("upload payload"), 0o600))
	runtime := logger.inlinePurge
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(14)

	recorder := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, recorder)
	logger.tryArchiveRotatedFile(context.Background(), sourcePath, logger.Fsstore.FileName)

	compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
	require.Len(t, recorder.inputsSnapshot(), 1)
	require.NoFileExists(t, sourcePath)
	require.NoFileExists(t, compressedPath)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, false))
}

func TestCompressedArchiveUploadFailureKeepsLocalFileAndInvalidatesForCompression(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 30, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "failed-compressed-upload", "failed-compressed-upload")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })

	sourcePath := filepath.Join(logger.Fsstore.FileName.Dir, "failed-upload-rotated.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("failed upload"), 0o600))
	runtime := logger.inlinePurge
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(13)

	logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploadErrorUploader{err: errors.New("upload failed")})
	logger.tryArchiveRotatedFile(context.Background(), sourcePath, logger.Fsstore.FileName)
	require.NoFileExists(t, sourcePath)
	require.FileExists(t, sourcePath+pkgfsstore.ExtensionCompressGzip)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, false))
}

func TestCompletedCompressedLogRemovalFailureContinuesAndInvalidatesEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 29, 11, 30, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "uploaded-cleanup-remove-failure")
	runtime := logger.inlinePurge

	compressedPath := writePendingCompressedLog(t, logger, "20260828", 1, now)
	require.NoError(t, os.Remove(compressedPath))
	require.NoError(t, os.Mkdir(compressedPath, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(compressedPath, "child"), []byte("zip"), 0o600))
	removablePath := writePendingCompressedLog(t, logger, "20260828", 2, now)
	recorder := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, recorder)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, true))
	runtime.directorySizeTracker.FinishObservation(3)

	// 削除失敗を再試行対象にしても、後続ファイルの処理と容量見積もりの無効化は続けます。
	require.True(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{compressedPath, removablePath}, logger.Fsstore.FileName), runNotificationImmediately))
	require.DirExists(t, compressedPath)
	require.NoFileExists(t, removablePath)
	require.Len(t, recorder.inputsSnapshot(), 2)
	require.True(t, runtime.directorySizeTracker.TryStartObservation(now, runtime.config, false))
}

func TestRemoveCompletedCompressedLogFileNotifiesOnlyOnFailure(t *testing.T) {
	for _, tc := range []struct {
		name           string
		removalBlocked bool
		alreadyAbsent  bool
		parentAbsent   bool
	}{
		{name: "removed"},
		{name: "already absent", alreadyAbsent: true},
		{name: "removal failure", removalBlocked: true},
		{name: "directory sync failure", parentAbsent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := newInlineUsageTestLogger(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), "completed-log-removal")
			directory := t.TempDir()
			if tc.parentAbsent {
				directory = filepath.Join(directory, "missing-parent")
			}
			path := filepath.Join(directory, "completed.log.gz")
			if tc.removalBlocked {
				require.NoError(t, os.Mkdir(path, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(path, "child"), []byte("preserve"), 0o600))
			} else if !tc.alreadyAbsent && !tc.parentAbsent {
				require.NoError(t, os.WriteFile(path, []byte("completed"), 0o600))
			}

			var reported []error
			logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
				reported = append(reported, err)
			}
			notifications := &backgroundTaskNotificationQueue{}
			err := logger.removeCompletedCompressedLogFile(context.Background(), path, notifications.enqueue)
			require.Empty(t, reported, "通知は呼び出し元が保留を解除してから実行する")
			if tc.removalBlocked || tc.parentAbsent {
				require.Error(t, err)
				require.Len(t, notifications.notifications, 1)
				notifications.runDeferredNotifications()
				require.Len(t, reported, 1)
				require.ErrorIs(t, reported[0], err)
				if tc.parentAbsent {
					// ファイルが既に不在でも、親ディレクトリの同期失敗を返して再試行を促します。
					require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
				}
			} else {
				require.NoError(t, err)
				notifications.runDeferredNotifications()
				require.Empty(t, reported)
			}
			if tc.removalBlocked {
				require.DirExists(t, path)
			} else {
				require.NoFileExists(t, path)
			}
		})
	}
}

type archiveUploadErrorUploader struct {
	err error
}

func (u archiveUploadErrorUploader) Upload(context.Context, uploader.UploadRequest) error {
	return u.err
}

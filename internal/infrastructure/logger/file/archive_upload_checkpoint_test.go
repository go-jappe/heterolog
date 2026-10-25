package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// restartInlineRecoveryForTestは再起動時に失われるメモリ上の再開処理を停止します。
// Storeとuploaderを差し替える前に実行中callbackの終了と保護解除を待ちます。
func restartInlineRecoveryForTest(t *testing.T, logger *FileLogger) {
	t.Helper()
	_ = logger.archiveLogFileProcessingRecovery.ForceClose()
	if err := closeBoundedForTest(t, boundedwait.HangTimeout, "archive log file processing recovery Close", &logger.archiveLogFileProcessingRecovery); err != nil {
		require.ErrorIs(t, err, shutdown.ErrForcedClose)
	}
	receiveForTest(t, logger.archiveLogFileProcessingRecovery.NotificationsDone(), "archive log file processing recovery notifications")
	logger.archiveLogFileProcessingRecovery = logfile.LogFileProcessingRecovery[archiveLogFileProcessingRecoveryKey]{}
}

// closeInlineFixtureForTestは未完了を意図する試験の復旧timerを停止してから資源を片付けます。
// graceful drainを検証する試験は、その検証箇所で明示的にCloseを呼びます。
// 試験が既に失敗していれば、試験が解除しなかった待機や再試行を待たずに中止して片付けます
// （closeWithinBoundForTest）。成功した試験ではCloseがerrorを返さないことも確認します。
func closeInlineFixtureForTest(t *testing.T, logger *FileLogger) {
	t.Helper()
	if !t.Failed() && !t.Skipped() {
		waitBackgroundForTest(t, logger)
		restartInlineRecoveryForTest(t, logger)
	}
	closeWithinBoundForTest(t, boundedwait.HangTimeout, "inline fixture logger Close", logger, true)
}

func TestInlineLoggerSkipsCheckpointStoreWithoutUploadOrExistingState(t *testing.T) {
	cfg := newTestFileLoggerConfig(t.TempDir(), "checkpoint-constructor", "checkpoint-constructor")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{
		Directory:    t.TempDir(),
		SourcePrefix: "logger_file_test",
	}

	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)
	require.Nil(t, logger.archiveUploadCheckpoints)
	entries, err := os.ReadDir(cfg.ArchiveCheckpointConfig.Directory)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestInlineCompressedLogCheckpointRetriesOnlyMissingDestinationAfterRestart(t *testing.T) {
	now := time.Date(2026, time.September, 3, 9, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "compressed-log-checkpoint-restart")
	path := writePendingCompressedLog(t, logger, "20260820", 1, now.AddDate(0, 0, -2))
	checkpointDirectory := t.TempDir()
	const sourceID = "inline_partial-restart"
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(
		t,
		checkpointDirectory,
		sourceID,
	)

	primary := &sequenceArchiveUploader{}
	secondary := &sequenceArchiveUploader{errs: []error{errors.New("secondary unavailable")}}
	logger.ArchiveUploader = newMultiArchiveUploader(
		t,
		uploader.Destination{ID: "primary", Uploader: primary},
		uploader.Destination{ID: "secondary", Uploader: secondary},
	)
	logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.FileExists(t, path)
	require.Equal(t, 1, primary.attemptCount())
	require.Equal(t, 1, secondary.attemptCount())

	// process restartを模してStoreとuploaderを作り直します。primaryの永続済み成功を
	// checkpointから読み、失敗したsecondaryだけを今回の送信対象にします。
	restartInlineRecoveryForTest(t, logger)
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(
		t,
		checkpointDirectory,
		sourceID,
	)
	restartedUploader := &checkpointDestinationRecordingArchiveUploader{
		destinationIDs: []objectstorage.DestinationID{"primary", "secondary"},
	}
	logger.ArchiveUploader = restartedUploader
	logger.Clock = pkgclock.NewFixedClock(now.Add(2 * time.Hour))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))

	require.NoFileExists(t, path)
	require.Equal(t, 1, primary.attemptCount())
	require.Equal(t, 1, secondary.attemptCount())
	require.Equal(t, []objectstorage.DestinationID{"secondary"}, restartedUploader.requested)
}

func TestInlineCompressedLogCheckpointKeepsCursorForDisappearedCompressedLog(t *testing.T) {
	now := time.Date(2026, time.September, 3, 15, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "prune-disappeared-compressed-log-file")
	checkpointDirectory := t.TempDir()
	const sourceID = "inline_prune-disappeared"
	store := newCompressedLogCheckpointTestStore(t, checkpointDirectory, sourceID)
	logger.archiveUploadCheckpoints = store
	path := filepath.Join(
		logger.Fsstore.FileName.Dir,
		logger.Fsstore.FileName.Target+"_20260903_6.log.gz",
	)
	key := inlineCompressedLogCheckpointKey(t, logger, path)
	_, err := store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{"old"})
	require.NoError(t, err)

	require.Empty(t, retainPendingArchiveGroupsForTest(t, logger, nil))
	restarted := newCompressedLogCheckpointTestStore(t, checkpointDirectory, sourceID)
	checkpoint, err := restarted.Load(context.Background(), key)
	require.NoError(t, err)
	require.Equal(
		t,
		[]objectstorage.DestinationID{"old"},
		checkpoint.CompletedDestinationIDs,
	)
}

func TestInlineRotationReusesPreviousCompressionFormatWithoutAddingAnother(t *testing.T) {
	rotationTime := time.Date(2026, time.September, 3, 16, 0, 0, 0, time.UTC)
	logger := newInlineCompressionCheckpointTestLogger(t, rotationTime, "rotation-compressed-log-group")
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(
		t,
		t.TempDir(),
		"inline_rotation-compressed-log-group",
	)
	oldPath := logger.Fsstore.ActiveFilePath
	gzipPath := oldPath + pkgfsstore.ExtensionCompressGzip
	zstdPath := oldPath + pkgfsstore.ExtensionCompressZstd
	// 以前の形式で同じ世代を圧縮済みの、元ログと同じ内容のpeerです。
	oldContent, err := os.ReadFile(oldPath)
	require.NoError(t, err)
	writeArchiveFileWithCodec(t, zstdPath, pkgfsstore.CompressZstd, string(oldContent))

	remote := newBlockingRecordingArchiveUploader()
	releaseUpload := releaseOnCleanupForTest(t, remote.release)
	logger.ArchiveUploader = newMultiArchiveUploader(
		t,
		uploader.Destination{ID: "primary", Uploader: remote},
	)
	require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
	receiveForTest(t, remote.entered, "rotation compressed log file upload start")

	// 旧zstdを再利用してupload中はrawキーと実際のzstdを保護します。
	// cleanupが同じfileを二重送信してはいけません。
	require.Empty(t, retainPendingArchiveGroupsForTest(
		t,
		logger,
		[]string{zstdPath},
	))
	releaseUpload()
	waitBackgroundForTest(t, logger)

	require.Equal(t, []string{zstdPath}, remote.pathsSnapshot())
	require.NoFileExists(t, oldPath)
	require.NoFileExists(t, gzipPath)
	require.NoFileExists(t, zstdPath)
}

func TestInlineRotationDefersArchiveWhileAnotherTaskOwnsSameGeneration(t *testing.T) {
	now := time.Date(2026, time.September, 3, 16, 30, 0, 0, time.UTC)
	logger := newInlineCompressionCheckpointTestLogger(t, now, "rotation-retry-owner")
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(
		t,
		t.TempDir(),
		"inline_rotation-retry-owner",
	)
	remote := &recordingArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(
		t,
		uploader.Destination{ID: "primary", Uploader: remote},
	)
	oldRawPath := logger.Fsstore.ActiveFilePath
	// productionで同一period内のrotationが起きる条件を再現します。上限未満の
	// active fileは同じpathを継続利用する契約なので、手動Rotateだけでは新世代に
	// 切り替わりません。
	require.Equal(t, oldRawPath, fillActiveFileLoggerForRotation(t, logger))
	require.NoError(t, logger.Fsstore.File.Sync())
	compressedPath := oldRawPath + pkgfsstore.ExtensionCompressGzip

	// 先行pending retryが生成予定のcompressed pathを所有している状態を模します。
	// owner tokenはactive writerとも、新しいrotation archive taskとも共有しません。
	pendingRetryToken := appendlog.NewDirectoryGuardToken()
	logger.lifecycleGuard.ProtectPaths(pendingRetryToken, compressedPath)

	require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
	require.NotEqual(t, oldRawPath, logger.Fsstore.ActiveFilePath)
	require.Empty(t, remote.inputsSnapshot())
	require.FileExists(t, oldRawPath)
	require.NoFileExists(t, compressedPath)

	// 先行taskの終了後、rotation側の競合延期で無効化されたcacheにより、次の
	// 自然な全体観測がrawを再圧縮します。TTL後はそのcompressed log fileだけをuploadします。
	logger.lifecycleGuard.UnprotectPaths(pendingRetryToken, compressedPath)
	ttl := logger.inlinePurge.config.UsageCacheTTL()
	logger.Clock = pkgclock.NewFixedClock(now.Add(ttl + time.Second))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, oldRawPath)
	require.FileExists(t, compressedPath)
	logger.Clock = pkgclock.NewFixedClock(now.Add(2*ttl + 2*time.Second))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, compressedPath)
	require.Equal(
		t,
		[]string{compressedPath},
		archiveUploadInputPaths(remote.inputsSnapshot()),
	)
}

func TestInlineCompletedCheckpointDeletesOnlyFirstSelectedFileAfterRestart(t *testing.T) {
	now := time.Date(2026, time.September, 3, 18, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "group-local-delete-restart")
	checkpointDirectory := t.TempDir()
	const sourceID = "inline_group-local-delete-restart"
	store := newCompressedLogCheckpointTestStore(t, checkpointDirectory, sourceID)
	logger.archiveUploadCheckpoints = store
	gzipPath := writePendingCompressedLog(t, logger, "20260820", 10, now)
	zstdPath := gzipPath[:len(gzipPath)-len(pkgfsstore.ExtensionCompressGzip)] +
		pkgfsstore.ExtensionCompressZstd
	writeArchiveFileWithCodec(t, zstdPath, pkgfsstore.CompressZstd, "unselected candidate")
	key := inlineCompressedLogCheckpointKey(t, logger, gzipPath)
	required := []objectstorage.DestinationID{"primary"}
	_, err := store.Load(context.Background(), key)
	require.NoError(t, err)
	_, err = store.AdvanceCompletionCursor(context.Background(), key, required)
	require.NoError(t, err)
	logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: &recordingArchiveUploader{}})
	// restart後に完了記録を読み直しても、今回選んだ最初のfileだけを削除します。
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(
		t,
		checkpointDirectory,
		sourceID,
	)
	retained := retainPendingArchiveGroupsForTest(t, logger, []string{gzipPath, zstdPath})
	require.Equal(t, []string{gzipPath}, archiveUploadGroupPathsForTest(retained))
	require.False(t, logger.processRemainingLogFiles(context.Background(), retained, runNotificationImmediately))
	logger.releasePendingCompressedArchivesForTest(retained)
	require.NoFileExists(t, gzipPath)
	require.FileExists(t, zstdPath)
}

func newInlineCompressionCheckpointTestLogger(
	t *testing.T,
	now time.Time,
	name string,
) *FileLogger {
	t.Helper()
	cfg := newTestFileLoggerConfig(t.TempDir(), name, name)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableRotate = true
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	logger, err := newStandaloneFileLogger(context.Background(), NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })
	return logger
}

func newCompressedLogCheckpointTestStore(
	t *testing.T,
	directory string,
	sourceID string,
) *checkpointstore.FilesystemStore {
	t.Helper()
	require.NoError(t, checkpointstore.PrepareDirectory(directory))
	store, err := checkpointstore.NewFilesystemStore(directory, sourceID)
	require.NoError(t, err)
	return store
}

func inlineCompressedLogCheckpointKey(t *testing.T, logger *FileLogger, path string) checkpointstore.CheckpointKey {
	t.Helper()
	key, err := checkpointstore.ArchiveCheckpointGenerationKeyForPath(path, logger.Fsstore.FileName)
	require.NoError(t, err)
	return checkpointstore.CheckpointKey{Generation: key, Compressed: true}
}

func newMultiArchiveUploader(
	t *testing.T,
	destinations ...uploader.Destination,
) uploader.DestinationUploader {
	t.Helper()
	multi, err := infraobjectstorage.NewMultiUploader(destinations...)
	require.NoError(t, err)
	return multi
}

// newSingleArchiveUploaderは単一の送信fixtureへ宛先IDを付け、FileLoggerの契約へ合わせます。
// Closeを持つfixtureでは、その終了処理もFileLoggerから呼べるように引き継ぎます。
func newSingleArchiveUploader(t *testing.T, remote uploader.Uploader) uploader.DestinationUploader {
	t.Helper()
	if remote == nil {
		return nil
	}
	destinations := newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
	if closer, ok := remote.(interface{ Close() error }); ok {
		return &singleArchiveUploaderWithClose{DestinationUploader: destinations, closer: closer}
	}
	return destinations
}

type singleArchiveUploaderWithClose struct {
	uploader.DestinationUploader
	closer interface{ Close() error }
}

func (u *singleArchiveUploaderWithClose) Close() error {
	return u.closer.Close()
}

func writeCanonicalRawLog(
	t *testing.T,
	logger *FileLogger,
	date string,
	index int,
) string {
	t.Helper()
	fileName := logger.Fsstore.FileName
	path := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_"+date+"_"+
			strconv.Itoa(index)+"."+fileName.Extension,
	)
	require.NoError(t, os.WriteFile(path, []byte("raw archive"), 0o600))
	return path
}

func retainPendingArchiveGroupsForTest(
	t *testing.T,
	logger *FileLogger,
	paths []string,
) []archiveUploadGroup {
	t.Helper()
	var retained []archiveUploadGroup
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		var err error
		retained, err = logger.retainPendingCompressedArchivePathsWithContext(
			context.Background(),
			paths,
			logger.Fsstore.FileName,
		)
		return err
	}))
	return retained
}

type failingCompressedLogCheckpointStore struct {
	err       error
	loadCalls int
}

func (s *failingCompressedLogCheckpointStore) RegisterDestinations(context.Context, []objectstorage.DestinationID, []checkpointstore.CheckpointKey) error {
	return s.err
}

func (s *failingCompressedLogCheckpointStore) Load(
	context.Context,
	checkpointstore.CheckpointKey,
) (checkpointstore.Checkpoint, error) {
	s.loadCalls++
	return checkpointstore.Checkpoint{}, s.err
}

func (s *failingCompressedLogCheckpointStore) AdvanceCompletionCursor(
	context.Context,
	checkpointstore.CheckpointKey,
	[]objectstorage.DestinationID,
) (checkpointstore.Checkpoint, error) {
	return checkpointstore.Checkpoint{}, s.err
}

func (s *failingCompressedLogCheckpointStore) DestinationCursors(context.Context) ([]checkpointstore.DestinationCursor, error) {
	return nil, s.err
}

func (s *failingCompressedLogCheckpointStore) LoadUploadProgress(context.Context, checkpointstore.CheckpointKey, objectstorage.DestinationID) (checkpointstore.UploadProgress, bool, error) {
	return checkpointstore.UploadProgress{}, false, s.err
}

func (s *failingCompressedLogCheckpointStore) SaveUploadProgress(context.Context, checkpointstore.UploadProgress) error {
	return s.err
}

func (s *failingCompressedLogCheckpointStore) Close() error {
	return nil
}

type blockingRecordingArchiveUploader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu    sync.Mutex
	paths []string
}

type checkpointDestinationRecordingArchiveUploader struct {
	destinationIDs []objectstorage.DestinationID
	requested      []objectstorage.DestinationID
}

func (u *checkpointDestinationRecordingArchiveUploader) DestinationIDs() []objectstorage.DestinationID {
	return append([]objectstorage.DestinationID(nil), u.destinationIDs...)
}

func (u *checkpointDestinationRecordingArchiveUploader) Upload(
	ctx context.Context,
	input uploader.UploadRequest,
) error {
	return u.UploadDestinations(ctx, input, u.DestinationIDs()).Err()
}

func (u *checkpointDestinationRecordingArchiveUploader) UploadDestinations(
	_ context.Context,
	_ uploader.UploadRequest,
	destinationIDs []objectstorage.DestinationID,
) uploader.UploadReport {
	u.requested = append([]objectstorage.DestinationID(nil), destinationIDs...)
	results := make([]uploader.DestinationResult, 0, len(destinationIDs))
	for _, destinationID := range destinationIDs {
		results = append(results, uploader.DestinationResult{DestinationID: destinationID})
	}
	return uploader.UploadReport{Results: results}
}

func newBlockingRecordingArchiveUploader() *blockingRecordingArchiveUploader {
	return &blockingRecordingArchiveUploader{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (u *blockingRecordingArchiveUploader) Upload(
	_ context.Context,
	input uploader.UploadRequest,
) error {
	u.mu.Lock()
	u.paths = append(u.paths, input.FilePath())
	u.mu.Unlock()
	u.once.Do(func() { close(u.entered) })
	<-u.release
	return nil
}

func (u *blockingRecordingArchiveUploader) pathsSnapshot() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.paths...)
}

func TestInlineCheckpointExcludesOldFilesFromNewDestinationsAfterRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "current-destinations")
	path := writePendingCompressedLog(t, logger, "20260820", 1, now)
	directory := t.TempDir()
	const sourceID = "current_destinations"
	store := newCompressedLogCheckpointTestStore(t, directory, sourceID)
	require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{"primary"}, nil))
	key := inlineCompressedLogCheckpointKey(t, logger, path)
	_, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, directory, sourceID)
	remote := &checkpointDestinationRecordingArchiveUploader{destinationIDs: []objectstorage.DestinationID{"primary", "new"}}
	require.NoError(t, logger.archiveUploadCheckpoints.RegisterDestinations(ctx, remote.destinationIDs, []checkpointstore.CheckpointKey{key}))
	logger.ArchiveUploader = remote
	retained := retainPendingArchiveGroupsForTest(t, logger, []string{path})
	require.Equal(t, []string{path}, archiveUploadGroupPathsForTest(retained))
	require.False(t, logger.processRemainingLogFiles(ctx, retained, runNotificationImmediately))
	logger.releasePendingCompressedArchivesForTest(retained)
	require.Empty(t, remote.requested)
	require.NoFileExists(t, path)
}

func TestInlineCheckpointWithoutActiveDestinationsDoesNotDeleteUploadedFile(t *testing.T) {
	for _, withUploader := range []bool{false, true} {
		name := "nil-uploader"
		if withUploader {
			name = "empty-destinations"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
			logger := newInlineUsageTestLogger(t, now, "inactive-destinations")
			path := writePendingCompressedLog(t, logger, "20260820", 1, now)
			store := newCompressedLogCheckpointTestStore(t, t.TempDir(), "inactive_destinations")
			logger.archiveUploadCheckpoints = store
			key := inlineCompressedLogCheckpointKey(t, logger, path)
			_, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"old"})
			require.NoError(t, err)
			if withUploader {
				logger.ArchiveUploader = &checkpointDestinationRecordingArchiveUploader{}
			}
			require.Empty(t, retainPendingArchiveGroupsForTest(t, logger, []string{path}))
			require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
			require.FileExists(t, path)
			checkpoint, err := store.Load(ctx, key)
			require.NoError(t, err)
			require.Equal(t, []objectstorage.DestinationID{"old"}, checkpoint.CompletedDestinationIDs)
			require.False(t, checkpoint.Complete(nil))
			protected := logger.remainingLogFileProtectedPaths(infrafsstore.InspectFilePaths([]string{path}).RegularFiles, newPurgeFileNames(logger.Fsstore.FileName), nil)
			require.NotContains(t, protected, path, "inactive archives remain subject to ordinary retention")
		})
	}
}

func TestInlineCompressionWithoutDestinationsRecordsNoCompletionAndUploadsAfterActivation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
	logger := newInlineCompressionCheckpointTestLogger(t, now, "late-activation")
	directory := t.TempDir()
	const sourceID = "late_activation"
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, directory, sourceID)
	raw := writeCanonicalRawLog(t, logger, "20260820", 1)
	path := raw + pkgfsstore.ExtensionCompressGzip
	require.False(t, logger.archiveRotatedFileWithNotifications(ctx, raw, logger.Fsstore.FileName, runNotificationImmediately))
	require.NoFileExists(t, raw)
	require.FileExists(t, path)
	checkpoint, err := logger.archiveUploadCheckpoints.Load(ctx, inlineCompressedLogCheckpointKey(t, logger, path))
	require.NoError(t, err)
	require.Empty(t, checkpoint.CompletedDestinationIDs)
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, directory, sourceID)
	remote := &recordingArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "later", Uploader: remote})
	retained := retainPendingArchiveGroupsForTest(t, logger, []string{path})
	require.False(t, logger.processRemainingLogFiles(ctx, retained, runNotificationImmediately))
	logger.releasePendingCompressedArchivesForTest(retained)
	require.Equal(t, []string{path}, archiveUploadInputPaths(remote.inputsSnapshot()))
	require.NoFileExists(t, path)
}

func TestRotationCompressesBeforeCheckpointLoadFailure(t *testing.T) {
	t.Parallel()
	rotationTime := time.Now().UTC().Add(48 * time.Hour)
	logger := newInlineCompressionCheckpointTestLogger(t, rotationTime, "checkpoint-load-failure")
	oldPath := logger.Fsstore.ActiveFilePath
	failure := &recoverableInlineCheckpointStore{Store: newCompressedLogCheckpointTestStore(t, t.TempDir(), "rotation_load_recovery")}
	failure.failLoad.Store(true)
	t.Cleanup(func() { failure.failLoad.Store(false) })
	logger.archiveUploadCheckpoints = failure
	remote := &sequenceArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
	require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
	waitBackgroundForTest(t, logger)
	require.Positive(t, failure.loads.Load())
	require.NoFileExists(t, oldPath)
	require.FileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
	require.Zero(t, remote.attemptCount())
}

func TestInlineRecoveryIgnoresCheckpointFailureUntilUpload(t *testing.T) {
	for _, peerExists := range []bool{false, true} {
		name := "raw-only"
		if peerExists {
			name = "valid-peer"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
			logger := newInlineCompressionCheckpointTestLogger(t, now, "checkpoint-independent-recovery")
			raw := writeCanonicalRawLog(t, logger, "20260820", 1)
			path := raw + pkgfsstore.ExtensionCompressGzip
			paths := []string{raw}
			if peerExists {
				// 元ログと同じ内容の、圧縮済みpeerです。
				rawContent, err := os.ReadFile(raw)
				require.NoError(t, err)
				writeGzipFile(t, path, string(rawContent))
				paths = append(paths, path)
			}
			failure := &recoverableInlineCheckpointStore{Store: newCompressedLogCheckpointTestStore(t, t.TempDir(), "raw_load_recovery")}
			failure.failLoad.Store(true)
			t.Cleanup(func() { failure.failLoad.Store(false) })
			logger.archiveUploadCheckpoints = failure
			remote := &sequenceArchiveUploader{}
			logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
			owner := appendlog.NewDirectoryGuardToken()
			var protection []string
			require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
				mutated, err := logger.recoverObservedRawArchivesForOwner(ctx, infrafsstore.GroupFilesByCompressionSource(infrafsstore.InspectFilePaths(paths), logger.Fsstore.FileName.Extension),
					logger.Fsstore.FileName, "", &protection, owner, runNotificationImmediately)
				require.True(t, mutated)
				return err
			}))
			logger.lifecycleGuard.UnprotectPaths(owner, protection...)
			require.Zero(t, failure.loads.Load())
			require.NoFileExists(t, raw)
			require.FileExists(t, path)
			require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
			require.FileExists(t, path)
			require.Zero(t, remote.attemptCount())
		})
	}
}

func TestInlineCheckpointSaveFailureKeepsFileAndRecoversWithoutReupload(t *testing.T) {
	t.Parallel()
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
		logger := newInlineUsageTestLogger(t, now, "success-save-failure")
		path := writePendingCompressedLog(t, logger, "20260820", 1, now)
		key := inlineCompressedLogCheckpointKey(t, logger, path)
		directory := t.TempDir()
		const sourceID = "success_save_failure"
		store := newCompressedLogCheckpointTestStore(t, directory, sourceID)
		recovering := &recoverableInlineCheckpointStore{Store: store}
		recovering.failRecord.Store(true)
		logger.archiveUploadCheckpoints = recovering
		remote := &sequenceArchiveUploader{}
		logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
		require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		require.Equal(t, 1, remote.attemptCount())
		require.FileExists(t, path)
		checkpoint, err := store.Load(ctx, key)
		require.NoError(t, err)
		require.Empty(t, checkpoint.CompletedDestinationIDs)
		recovering.failRecord.Store(false)
		// 新しいwrite、purge、upload試行を起こさなくても、再試行間隔の経過後に保存とlocal削除まで進みます。
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		require.Equal(t, 1, remote.attemptCount(), "the successful upload is retained across checkpoint failure")
		require.NoFileExists(t, path)
	})
}

func TestInlineArchiveDurableSaveDeadlineKeepsSourceWithoutRecordingUploadSuccess(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
		logger := newInlineUsageTestLogger(t, now, "durable-save-deadline")
		path := writePendingCompressedLog(t, logger, "20260820", 1, now)
		store := newCompressedLogCheckpointTestStore(t, t.TempDir(), "durable_save_deadline")
		logger.archiveUploadCheckpoints = store
		key := inlineCompressedLogCheckpointKey(t, logger, path)
		// 保存専用contextの期限切れも、uploaderの失敗として呼び出し元へ返ります。
		// 親contextが生存していても、その試行を送信済みと記録してはいけません。
		remote := &sequenceArchiveUploader{errs: []error{
			errors.Join(errors.New("durable save exceeded its budget"), context.DeadlineExceeded),
		}}
		logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})

		require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		require.NoError(t, ctx.Err())
		require.FileExists(t, path)
		checkpoint, err := store.Load(ctx, key)
		require.NoError(t, err)
		require.Empty(t, checkpoint.CompletedDestinationIDs)
		require.Equal(t, 1, remote.attemptCount())

		// 次回の試行が成功して初めて成功記録を確定し、元の圧縮ファイルを削除できます。
		requireArchiveRecoveryClosesForTest(t, logger)
		require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		require.Equal(t, 2, remote.attemptCount())
		require.NoFileExists(t, path)
	})
}

type uploadActivationCompressionCodec struct {
	pkgfsstore.CompressionCodec
	afterCompress func()
}

func (c *uploadActivationCompressionCodec) Compress(in, out *os.File) error {
	if err := c.CompressionCodec.Compress(in, out); err != nil {
		return err
	}
	c.afterCompress()
	return nil
}

func TestInlineRotationSnapshotsDestinationsAfterCompression(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
	realCodec, err := infrafsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)
	codec := &uploadActivationCompressionCodec{CompressionCodec: realCodec}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "upload-start-snapshot", codec)
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, t.TempDir(), "upload_start_snapshot")
	raw := writeCanonicalRawLog(t, logger, "20260820", 1)
	oldRemote, newRemote := &recordingArchiveUploader{}, &recordingArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "old", Uploader: oldRemote})
	codec.afterCompress = func() {
		logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "new", Uploader: newRemote})
	}
	require.False(t, logger.archiveRotatedFileWithNotifications(ctx, raw, logger.Fsstore.FileName, runNotificationImmediately))
	require.Empty(t, oldRemote.inputsSnapshot())
	require.Equal(t, []string{raw + pkgfsstore.ExtensionCompressGzip}, archiveUploadInputPaths(newRemote.inputsSnapshot()))
	require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressGzip)
}

func TestInlineEmptyDestinationUploaderWithoutCheckpointDoesNotRetainOrDelete(t *testing.T) {
	now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "empty-destinations-no-checkpoint")
	path := writePendingCompressedLog(t, logger, "20260820", 1, now)
	logger.ArchiveUploader = &checkpointDestinationRecordingArchiveUploader{}
	require.Nil(t, logger.archiveUploadCheckpoints)
	require.Empty(t, retainPendingArchiveGroupsForTest(t, logger, []string{path}))
	require.False(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
	require.FileExists(t, path)
	protected := logger.remainingLogFileProtectedPaths(infrafsstore.InspectFilePaths([]string{path}).RegularFiles, newPurgeFileNames(logger.Fsstore.FileName), nil)
	require.NotContains(t, protected, path)
}

type fixedReportArchiveUploader struct {
	checkpointDestinationRecordingArchiveUploader
	report uploader.UploadReport
}

func (u *fixedReportArchiveUploader) UploadDestinations(
	context.Context,
	uploader.UploadRequest,
	[]objectstorage.DestinationID,
) uploader.UploadReport {
	return u.report
}

func TestInlineCheckpointRejectsMalformedUploadReportWithoutSavingOrDeleting(t *testing.T) {
	for _, test := range []struct {
		name    string
		results []uploader.DestinationResult
	}{
		{name: "unrequested", results: []uploader.DestinationResult{{DestinationID: "primary"}, {DestinationID: "unknown"}}},
		{name: "unrequested-extra", results: []uploader.DestinationResult{{DestinationID: "primary"}, {DestinationID: "secondary"}, {DestinationID: "unknown"}}},
		{name: "duplicate", results: []uploader.DestinationResult{{DestinationID: "primary"}, {DestinationID: "primary"}}},
		{name: "missing", results: []uploader.DestinationResult{{DestinationID: "primary"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, time.September, 15, 10, 0, 0, 0, time.UTC)
			logger := newInlineUsageTestLogger(t, now, "invalid-upload-report")
			path := writePendingCompressedLog(t, logger, "20260820", 1, now)
			directory := t.TempDir()
			const sourceID = "invalid_upload_report"
			store := newCompressedLogCheckpointTestStore(t, directory, sourceID)
			logger.archiveUploadCheckpoints = store
			logger.ArchiveUploader = &fixedReportArchiveUploader{
				checkpointDestinationRecordingArchiveUploader: checkpointDestinationRecordingArchiveUploader{
					destinationIDs: []objectstorage.DestinationID{"primary", "secondary"},
				},
				report: uploader.UploadReport{Results: test.results},
			}
			require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
			require.FileExists(t, path)
			checkpoint, err := store.Load(ctx, inlineCompressedLogCheckpointKey(t, logger, path))
			require.NoError(t, err)
			require.Empty(t, checkpoint.CompletedDestinationIDs)
		})
	}
}

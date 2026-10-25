package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgtime "github.com/go-jappe/heterolog/pkg/time"

	"github.com/stretchr/testify/require"
)

func TestInlineCleanupRetriesPendingCompressedArchiveAndDeletesAfterEveryDestinationSucceeds(
	t *testing.T,
) {
	now := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "pending-compressed-retry")
	path := writePendingCompressedLog(t, logger, "20260820", 1, now.AddDate(0, 0, -2))

	primary := &sequenceArchiveUploader{}
	secondary := &sequenceArchiveUploader{errs: []error{errors.New("secondary unavailable")}}
	multi, err := infraobjectstorage.NewMultiUploader(
		uploader.Destination{ID: "primary", Uploader: primary},
		uploader.Destination{ID: "secondary", Uploader: secondary},
	)
	require.NoError(t, err)
	logger.ArchiveUploader = multi
	logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()

	// 初回観測では一宛先が失敗するため、age/generation/directory purgeを一巡しても
	// local compressed fileを保持します。
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.FileExists(t, path)
	require.Equal(t, 1, primary.attemptCount())
	require.Equal(t, 1, secondary.attemptCount())

	// UsageCacheTTL後の自然なdirectory観測で再試行し、全宛先が成功した時点でだけ
	// local compressed fileを削除します。
	logger.Clock = pkgclock.NewFixedClock(now.Add(2 * time.Hour))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, path)
	require.Equal(t, 2, primary.attemptCount())
	require.Equal(t, 2, secondary.attemptCount())
}

func TestInlineCleanupRetriesUploadedCompressedArchiveAfterLocalDeleteFailure(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 30, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "pending-compressed-delete-retry")
	fileName := logger.Fsstore.FileName
	path := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_20260820_1.log.gz",
	)
	require.NoError(t, os.WriteFile(path, []byte("payload"), 0o600))
	// 候補の型検査後、upload中に非空directoryへ差し替えてlocal削除失敗を再現します。
	// 次回の候補選択には再び通常fileを用意し、非通常fileをuploadしない契約も維持します。
	var attempts int
	upload := archiveUploaderFunc(func(_ context.Context, input uploader.UploadRequest) error {
		attempts++
		if attempts == 1 {
			require.NoError(t, os.Remove(input.FilePath()))
			require.NoError(t, os.Mkdir(input.FilePath(), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(input.FilePath(), "child"), []byte("payload"), 0o600))
		}
		return nil
	})
	logger.ArchiveUploader = newSingleArchiveUploader(t, upload)
	logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.DirExists(t, path)
	require.Equal(t, 1, attempts)

	// local削除を可能にした次の自然な観測で再uploadし、その成功後に削除します。
	require.NoError(t, os.Remove(filepath.Join(path, "child")))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, []byte("payload"), 0o600))
	logger.Clock = pkgclock.NewFixedClock(now.Add(2 * time.Hour))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoDirExists(t, path)
	require.Equal(t, 2, attempts)
}

func TestPendingCompressedUploadRetryIgnoresNonCanonicalGeneration(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 45, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "pending-compressed-canonical-only")
	fileName := logger.Fsstore.FileName
	canonicalPath := writePendingCompressedLog(t, logger, "20260820", 1, now)
	nonCanonicalPath := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_memo_1."+fileName.Extension+pkgfsstore.ExtensionCompressGzip,
	)
	require.NoError(t, os.WriteFile(nonCanonicalPath, []byte("unowned user file"), 0o600))

	upload := &sequenceArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, upload)
	files := []fsstore.FileInfo{
		fileInfoForPendingTest(t, nonCanonicalPath, 17, now),
		fileInfoForPendingTest(t, canonicalPath, 26, now),
	}
	protected := logger.remainingLogFileProtectedPaths(files, newPurgeFileNames(fileName), nil)
	require.Contains(t, protected, filepath.Clean(canonicalPath))
	require.NotContains(t, protected, filepath.Clean(nonCanonicalPath))

	var retained []archiveUploadGroup
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		var err error
		retained, err = logger.retainPendingCompressedArchives(files, fileName)
		return err
	}))
	require.Equal(t, []string{canonicalPath}, archiveUploadGroupPathsForTest(retained))
	defer logger.releasePendingCompressedArchivesForTest(retained)

	require.False(t, logger.processRemainingLogFiles(context.Background(), retained, runNotificationImmediately))
	require.Equal(t, 1, upload.attemptCount())
	require.NoFileExists(t, canonicalPath)
	require.FileExists(t, nonCanonicalPath,
		"a target-like non-canonical file must not be uploaded or deleted as a completed archive")
}

func TestRemainingLogFileProtectionRefreshesGuardForParsedGeneration(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 50, 0, 0, time.UTC)
	logger := newInlineCompressionRecoveryTestLogger(
		t, now, "parsed-generation-protection", &inlineRecoveryCompressionCodec{},
	)
	gzipPath := writePendingCompressedLog(t, logger, "20260820", 1, now)
	rawPath := gzipPath[:len(gzipPath)-len(pkgfsstore.ExtensionCompressGzip)]
	zstdPath := rawPath + pkgfsstore.ExtensionCompressZstd
	require.NoError(t, os.WriteFile(zstdPath, []byte("compressed peer"), 0o600))
	require.NoFileExists(t, rawPath)
	files := []fsstore.FileInfo{
		fileInfoForPendingTest(t, gzipPath, 1, now),
		fileInfoForPendingTest(t, zstdPath, 1, now),
	}
	names := newPurgeFileNames(logger.Fsstore.FileName)
	deletionCandidates := func() []string {
		var candidates []string
		logger.lifecycleGuard.RunPurgeWithProtection(
			files,
			func(protected map[string]struct{}) map[string]struct{} {
				return logger.remainingLogFileProtectedPaths(files, names, protected)
			},
			func(purge *appendlog.Purge) {
				for _, file := range purge.DeletionCandidates(files) {
					candidates = append(candidates, file.Path)
				}
			},
		)
		return candidates
	}

	// 名前を解析した後に別の所有者が保護を登録・解除しても、同世代の全形式へ反映します。
	require.ElementsMatch(t, []string{gzipPath, zstdPath}, deletionCandidates())
	owner := appendlog.NewDirectoryGuardToken()
	t.Cleanup(func() { logger.lifecycleGuard.UnprotectPaths(owner, gzipPath) })
	logger.lifecycleGuard.ProtectPaths(owner, gzipPath)
	require.Empty(t, deletionCandidates(), "fresh guard protection must cover every compressed peer")
	logger.lifecycleGuard.UnprotectPaths(owner, gzipPath)
	require.ElementsMatch(t, []string{gzipPath, zstdPath}, deletionCandidates(),
		"reusing parsed filenames must not retain a released generation")
}

func TestUploadPendingCompressedArchiveIsExcludedFromEveryInlinePurge(t *testing.T) {
	now := time.Date(2026, time.August, 29, 11, 0, 0, 0, time.UTC)

	t.Run("age", func(t *testing.T) {
		logger := newInlineUsageTestLogger(t, now, "pending-compressed-age")
		logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploadErrorUploader{err: errors.New("upload failed")})
		old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
		path := writePendingCompressedLog(t, logger, "20260820", 1, old)
		files := []fsstore.FileInfo{fileInfoForPendingTest(t, path, 1, old)}

		result := logger.purgeLogsByAge(
			context.Background(),
			logger.inlinePurge,
			files,
			newPurgeFileNames(logger.Fsstore.FileName),
			pkgtime.StartOfDay(now),
			nil,
			runNotificationImmediately,
		)

		require.True(t, result.Ran)
		require.Empty(t, result.RemovalResults)
		require.Equal(t, files, result.Remaining)
		require.FileExists(t, path)
	})

	t.Run("generation", func(t *testing.T) {
		logger := newInlineUsageTestLogger(t, now, "pending-compressed-generation")
		logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploadErrorUploader{err: errors.New("upload failed")})
		maxBackups := logger.inlinePurge.config.MaxFileBackups()
		files := make([]fsstore.FileInfo, 0, maxBackups+1)
		for index := 0; index < maxBackups+1; index++ {
			modifiedAt := now.Add(time.Duration(index) * time.Minute)
			path := writePendingCompressedLog(t, logger, "20260820", index+1, modifiedAt)
			files = append(files, fileInfoForPendingTest(t, path, 1, modifiedAt))
		}

		result := logger.purgeLogsByGeneration(
			context.Background(),
			logger.inlinePurge,
			files,
			newPurgeFileNames(logger.Fsstore.FileName),
			nil,
			runNotificationImmediately,
		)

		require.False(t, result.Ran)
		require.Empty(t, result.RemovalResults)
		require.Equal(t, files, result.Remaining)
		for _, file := range files {
			require.FileExists(t, file.Path)
		}
	})

	t.Run("directory size", func(t *testing.T) {
		logger := newInlineUsageTestLogger(t, now, "pending-compressed-directory")
		logger.ArchiveUploader = newSingleArchiveUploader(t, archiveUploadErrorUploader{err: errors.New("upload failed")})
		pendingPath := writePendingCompressedLog(t, logger, "20260820", 1, now)
		deletablePath := filepath.Join(
			logger.Fsstore.FileName.Dir,
			logger.Fsstore.FileName.Target+"_20260819_1.log",
		)
		require.NoError(t, os.WriteFile(deletablePath, []byte("x"), 0o600))
		old := now.Add(-time.Hour)
		require.NoError(t, os.Chtimes(deletablePath, old, old))
		files := []fsstore.FileInfo{
			fileInfoForPendingTest(t, deletablePath, 1, old),
			fileInfoForPendingTest(
				t,
				pendingPath,
				logger.inlinePurge.config.MaxDirSize()+1,
				now,
			),
		}

		remaining := logger.purgeByDirectorySize(
			context.Background(),
			logger.inlinePurge,
			files,
			newPurgeFileNames(logger.Fsstore.FileName),
			nil,
			runNotificationImmediately,
		)

		require.NoFileExists(t, deletablePath)
		require.FileExists(t, pendingPath)
		require.Len(t, remaining, 1)
		require.Equal(t, pendingPath, remaining[0].Path)
	})
}

func TestDirectoryPurgeProtectsUploadEnabledLazyRoleCompressedArchive(t *testing.T) {
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	cfg := newTestFileLoggerConfig(t.TempDir(), "lazy-role-pending", "lazy-role-pending")
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	// default/errorを同じphysical directoryへ置き、default loggerだけを生成します。
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "logger Close", func() closerForTest { return logger }, true)

	configured := NewConfigAdapter(cfg)
	configured.ObjectStorageUploadConfig = objectstorage.NewLogFileUploadConfigAdapter(objectstorage.LogFileUploadConfig{
		Enabled: true,
		Error:   objectstorage.UploadTargetConfig{Enabled: true},
	})
	logger.config = configured

	pendingErrorPath := filepath.Join(
		logger.Fsstore.FileName.Dir,
		cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix+
			cfg.FsstoreConfig.Layout.Target+"_20260820_1.log.gz",
	)
	require.NoError(t, os.WriteFile(pendingErrorPath, []byte("pending"), 0o600))
	deletableDefaultPath := filepath.Join(
		logger.Fsstore.FileName.Dir,
		cfg.FsstoreConfig.Layout.Target+"_20260819_1.log",
	)
	require.NoError(t, os.WriteFile(deletableDefaultPath, []byte("x"), 0o600))
	old := now.Add(-time.Hour)
	require.NoError(t, os.Chtimes(deletableDefaultPath, old, old))
	files := []fsstore.FileInfo{
		fileInfoForPendingTest(t, deletableDefaultPath, 1, old),
		fileInfoForPendingTest(
			t,
			pendingErrorPath,
			logger.inlinePurge.config.MaxDirSize()+1,
			now,
		),
	}

	remaining := logger.purgeByDirectorySize(
		context.Background(),
		logger.inlinePurge,
		files,
		newPurgeFileNames(logger.Fsstore.FileName),
		nil,
		runNotificationImmediately,
	)

	require.NoFileExists(t, deletableDefaultPath)
	require.FileExists(t, pendingErrorPath)
	require.Len(t, remaining, 1)
	require.Equal(t, pendingErrorPath, remaining[0].Path)
}

func TestPendingRetrySkipsInFlightRotationAndDoesNotHoldLifecycleLockDuringUpload(
	t *testing.T,
) {
	now := time.Date(2026, time.August, 29, 13, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "pending-compressed-lock")
	path := writePendingCompressedLog(t, logger, "20260820", 1, now)
	files := []fsstore.FileInfo{fileInfoForPendingTest(t, path, 1, now)}

	// rotation taskが同じpathをすでにretainしている場合、cleanupは二重upload権を
	// 取得しません。
	rotationToken := appendlog.NewDirectoryGuardToken()
	logger.lifecycleGuard.ProtectPaths(rotationToken, path)
	var retained []archiveUploadGroup
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		var err error
		retained, err = logger.retainPendingCompressedArchives(files, logger.Fsstore.FileName)
		return err
	}))
	require.Empty(t, retained)
	logger.lifecycleGuard.UnprotectPaths(rotationToken, path)

	blocking := newBlockingArchiveUploader()
	// 待機を検出して失敗した場合も、止めたuploadを解除してからloggerを閉じます。
	releaseUpload := releaseOnCleanupForTest(t, blocking.release)
	logger.ArchiveUploader = newSingleArchiveUploader(t, blocking)
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		var err error
		retained, err = logger.retainPendingCompressedArchives(files, logger.Fsstore.FileName)
		return err
	}))
	require.Equal(t, []string{path}, archiveUploadGroupPathsForTest(retained))
	defer logger.releasePendingCompressedArchivesForTest(retained)

	uploadDone := make(chan struct{})
	go func() {
		logger.processRemainingLogFiles(context.Background(), retained, runNotificationImmediately)
		close(uploadDone)
	}()
	receiveForTest(t, blocking.entered, "pending upload did not start")

	// remote upload待機中も、同じdirectoryの短いlifecycle処理は進められます。
	lockDone := make(chan struct{})
	go func() {
		_ = logger.lifecycleGuard.RunWithLifecycleLock(func() error { return nil })
		close(lockDone)
	}()
	receiveForTest(t, lockDone, "pending remote upload held the directory lifecycle lock")

	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Contains(t, protected, filepath.Clean(path))
	})
	releaseUpload()
	receiveForTest(t, uploadDone, "pending upload did not finish")
	require.NoFileExists(t, path)
}

func TestInlineCleanupKeepsPendingPathRetainedUntilUploadReturns(t *testing.T) {
	now := time.Date(2026, time.August, 29, 13, 30, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "pending-cleanup-ownership")
	path := writePendingCompressedLog(t, logger, "20260820", 1, now)
	blocking := newBlockingArchiveUploader()
	// 待機を検出して失敗した場合も、止めたuploadを解除してからloggerを閉じます。
	releaseUpload := releaseOnCleanupForTest(t, blocking.release)
	logger.ArchiveUploader = newSingleArchiveUploader(t, blocking)
	logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()

	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- purgeFileLoggerForTest(context.Background(), logger)
	}()
	receiveForTest(t, blocking.entered, "pending upload did not start")

	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Contains(t, protected, filepath.Clean(path),
			"cleanup must retain the path until remote upload returns")
	})
	releaseUpload()
	err := receiveForTest(t, cleanupDone, "pending cleanup did not finish")
	require.NoError(t, err)
	require.NoFileExists(t, path)
}

func TestInlineCleanupReschedulesSeriesRetryAndUnprotectsPathWhenUploaderPanics(t *testing.T) {
	now := time.Date(2026, time.August, 29, 14, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "pending-cleanup-uploader-panic")
	path := writePendingCompressedLog(t, logger, "20260820", 1, now)
	logger.ArchiveUploader = panickingDestinationArchiveUploader{}
	logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()

	require.Panics(t, func() {
		_ = purgeFileLoggerForTest(context.Background(), logger)
	})
	require.FileExists(t, path)

	scheduled, retryAt := fileSeriesUploadRetryScheduleSnapshot(logger.inlinePurge)
	require.True(t, scheduled)
	require.Equal(t, now.Add(logger.inlinePurge.config.UsageCacheTTL()), retryAt)
	logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) {
		for _, protectionPath := range fsstore.CompressionProtectionPaths(path, logger.Fsstore.FileName.Extension) {
			require.NotContains(t, protected, filepath.Clean(protectionPath),
				"panic cleanup must release both the file and its source protection key")
		}
	})
}

func writePendingCompressedLog(
	t *testing.T,
	logger *FileLogger,
	date string,
	index int,
	modifiedAt time.Time,
) string {
	t.Helper()
	fileName := logger.Fsstore.FileName
	path := filepath.Join(
		fileName.Dir,
		fileName.Prefix+fileName.Target+"_"+date+"_"+
			strconv.Itoa(index)+"."+fileName.Extension+pkgfsstore.ExtensionCompressGzip,
	)
	require.NoError(t, os.WriteFile(path, []byte("pending compressed archive"), 0o600))
	require.NoError(t, os.Chtimes(path, modifiedAt, modifiedAt))
	return path
}

func fileInfoForPendingTest(
	t *testing.T,
	path string,
	size uint64,
	modifiedAt time.Time,
) fsstore.FileInfo {
	t.Helper()
	return fsstore.FileInfo{Path: path, Size: size, ModTime: modifiedAt}
}

type sequenceArchiveUploader struct {
	mu       sync.Mutex
	errIndex int
	errs     []error
	attempts int
}

// panickingDestinationArchiveUploaderは宛先別の送信境界そのもののpanicを再現します。
// MultiUploaderのchild panic回収を経由せず、FileLoggerの復旧・保護解除を検証します。
type panickingDestinationArchiveUploader struct{}

func (panickingDestinationArchiveUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"primary"}
}

func (u panickingDestinationArchiveUploader) UploadDestinations(ctx context.Context, request uploader.UploadRequest, _ []objectstorage.DestinationID) uploader.UploadReport {
	_ = u.Upload(ctx, request)
	return uploader.UploadReport{}
}

func (panickingDestinationArchiveUploader) Upload(context.Context, uploader.UploadRequest) error {
	panic("archive uploader panic")
}

func (u *sequenceArchiveUploader) Upload(context.Context, uploader.UploadRequest) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.attempts++
	if u.errIndex >= len(u.errs) {
		return nil
	}
	err := u.errs[u.errIndex]
	u.errIndex++
	return err
}

func (u *sequenceArchiveUploader) attemptCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attempts
}

type blockingArchiveUploader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingArchiveUploader() *blockingArchiveUploader {
	return &blockingArchiveUploader{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (u *blockingArchiveUploader) Upload(context.Context, uploader.UploadRequest) error {
	u.once.Do(func() { close(u.entered) })
	<-u.release
	return nil
}

// archiveUploadGroupsForTestは、明示的なパスを使う試験の入力を本番と同じ解析済み候補へ変換します。
func archiveUploadGroupsForTest(t *testing.T, paths []string, fileName logpattern.FileNamePattern) []archiveUploadGroup {
	t.Helper()
	var groups []archiveUploadGroup
	seen := make(map[string]struct{})
	for _, path := range paths {
		group, err := parseArchiveUploadGroup(path, fileName)
		require.NoError(t, err)
		if _, exists := seen[group.key.Generation]; exists {
			continue
		}
		seen[group.key.Generation] = struct{}{}
		groups = append(groups, group)
	}
	return groups
}

// archiveUploadGroupPathsForTestは、処理候補を変更せず、パス一覧の検証に使います。
func archiveUploadGroupPathsForTest(groups []archiveUploadGroup) []string {
	paths := make([]string, len(groups))
	for index, group := range groups {
		paths[index] = group.path
	}
	return paths
}

// 以下のretain helperは、既存の同期unit testが明示的に解放するwriter tokenへ
// ownershipを置きます。production cleanupは必ずtask固有tokenを
// retainRemainingLogFilesForOwnerへ渡します。
func (l *FileLogger) retainPendingCompressedArchives(
	files []fsstore.FileInfo,
	fileNameSnapshot logpattern.FileNamePattern,
) ([]archiveUploadGroup, error) {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.Path)
	}
	return l.retainPendingCompressedArchivePathsWithContext(
		context.Background(),
		paths,
		fileNameSnapshot,
	)
}

func (l *FileLogger) retainPendingCompressedArchivePathsWithContext(
	ctx context.Context,
	paths []string,
	fileNameSnapshot logpattern.FileNamePattern,
) ([]archiveUploadGroup, error) {
	if l == nil {
		return nil, nil
	}
	var protection []string
	return l.retainRemainingLogFilesForOwner(
		ctx,
		fsstore.GroupFilesByCompressionSource(fsstore.InspectFilePaths(paths), fileNameSnapshot.Extension),
		fileNameSnapshot,
		l.lifecycleToken,
		&protection,
	)
}

func (l *FileLogger) releasePendingCompressedArchivesForTest(groups []archiveUploadGroup) {
	var protection []string
	for _, group := range groups {
		protection = append(protection, fsstore.CompressionProtectionPaths(group.path, group.fileName.Extension)...)
	}
	l.lifecycleGuard.UnprotectPaths(l.lifecycleToken, protection...)
}

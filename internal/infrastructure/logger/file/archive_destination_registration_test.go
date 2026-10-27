package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestInlineDestinationRegistrationKeepsInitialScopeAcrossRestartAndDisable(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "destination-registration")
	oldPath := writePendingCompressedLog(t, logger, "20260920", 1, now)
	rawPath := filepath.Join(logger.Fsstore.FileName.Dir, logger.Fsstore.FileName.Target+"_20260919_1.log")
	require.NoError(t, os.WriteFile(rawPath, []byte("old raw"), 0o600))
	cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "scope", AutoRecoverMissingCheckpoint: true}
	remote := &checkpointDestinationRecordingArchiveUploader{destinationIDs: []objectstorage.DestinationID{"archive"}}
	store, err := newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, logger.Fsstore.FileName, true, remote.DestinationIDs(), nil)
	require.NoError(t, err)
	logger.archiveUploadCheckpoints, logger.ArchiveUploader = store, remote
	// 登録前から存在した圧縮fileは送信せず、upload完了を理由に削除もしません。
	completed, retry, _ := logger.completeArchiveGroupUpload(ctx, archiveUploadGroupsForTest(t, []string{oldPath}, logger.Fsstore.FileName)[0], runNotificationImmediately, logger.archiveUploadDestinationsSnapshot(), nil)
	require.Empty(t, completed)
	require.False(t, retry)
	require.Empty(t, remote.requested)
	require.FileExists(t, oldPath)

	newPath := writePendingCompressedLog(t, logger, "20260921", 1, now)
	// 登録時に対象外のraw世代は、後から圧縮しても送信・削除しません。
	compressedRawPath := rawPath + ".gz"
	require.NoError(t, os.WriteFile(compressedRawPath, []byte("new compressed output"), 0o600))
	require.NoError(t, os.Remove(rawPath))
	require.NoError(t, store.Close())
	disabled, err := newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, logger.Fsstore.FileName, false, nil, nil)
	require.NoError(t, err)
	require.Nil(t, disabled, "disabled upload leaves the saved cursor for re-enable")
	store, err = newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, logger.Fsstore.FileName, true, remote.DestinationIDs(), nil)
	require.NoError(t, err)
	logger.archiveUploadCheckpoints = store
	for _, path := range []string{newPath} {
		completed, retry, _ = logger.completeArchiveGroupUpload(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName)[0], runNotificationImmediately, logger.archiveUploadDestinationsSnapshot(), nil)
		require.Equal(t, path, completed)
		require.False(t, retry)
		require.Equal(t, remote.destinationIDs, remote.requested)
	}
	remote.requested = nil
	for _, path := range []string{compressedRawPath, oldPath} {
		completed, retry, _ = logger.completeArchiveGroupUpload(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName)[0], runNotificationImmediately, logger.archiveUploadDestinationsSnapshot(), nil)
		require.Empty(t, completed)
		require.False(t, retry)
		require.Empty(t, remote.requested)
	}
	// 送信も削除もしない世代はupload待ちとして保護せず、通常の保持・purgeへ任せます。
	retained := retainPendingArchiveGroupsForTest(t, logger, []string{oldPath, compressedRawPath})
	defer logger.releasePendingCompressedArchivesForTest(retained)
	require.Empty(t, retained)
	require.False(t, logger.processRemainingLogFiles(ctx, retained, runNotificationImmediately))
	require.FileExists(t, oldPath)
	require.FileExists(t, compressedRawPath)
	require.Empty(t, remote.requested)
}

func TestLoggerSetRegistersLazyDestinationsBeforeFirstRoleOpens(t *testing.T) {
	ctx := context.Background()
	var (
		cfg                                                            pkglogger.FileLoggerConfig
		dir, old, pastRaw, currentRaw, recoveryRaw, recoveryCompressed string
	)
	// 登録境界は、writerの既定の時計と同じUTCの現在の期間で決まります。過去の期間の最新rawは
	// writerが戻って追記しないため登録前に完結した世代です。現在の期間の最新rawは
	// まだ追記され得るため、新しい宛先の対象に残します。
	// 最初の登録の前後でUTCの日付が変わった場合は、登録に使った期間を特定できないため、新しい
	// 保存先で準備からやり直します。日付は一日に一度しか変わらないため、やり直しは一回で足ります。
	for attempt := 0; ; attempt++ {
		day := time.Now().UTC().Format(pkgfsstore.FileDateFormat)
		// 共有directoryのpurge判定は実体pathで行うため、symlinkを含む一時directoryでも
		// 同じpathで比較できるよう、解決済みのpathをログの保存先にします。
		cfg = newTestFileLoggerConfig(resolvedTempDirForTest(t), "logs", "lazy-scope")
		cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
		cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
		cfg.FsstoreConfig.Layout.DebugLogDir = cfg.FsstoreConfig.Layout.LogDir
		cfg.ArchiveCheckpointConfig = pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "scope"}
		require.NoError(t, checkpointstore.PrepareDirectory(cfg.ArchiveCheckpointConfig.Directory))
		target := objectstorage.UploadTargetConfig{
			Enabled: true, Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
			S3: objectstorage.S3Config{Region: "ap-northeast-1", Endpoint: "https://s3.example.com", AccessKeyID: "test", SecretAccessKey: "test", Bucket: "archive"},
		}
		cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{Enabled: true, Default: target, Error: target, Debug: target}
		dir = filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir)
		require.NoError(t, os.MkdirAll(dir, 0o750))
		old = filepath.Join(dir, "error_lazy-scope_20260920_1.log.gz")
		require.NoError(t, os.WriteFile(old, []byte("old compressed"), 0o600))
		pastRaw = filepath.Join(dir, "debug_lazy-scope_20260919_1.log")
		require.NoError(t, os.WriteFile(pastRaw, []byte("month-old raw"), 0o600))
		currentRaw = filepath.Join(dir, "error_lazy-scope_"+day+"_1.log")
		require.NoError(t, os.WriteFile(currentRaw, []byte("current period raw"), 0o600))
		recoveryRaw = filepath.Join(dir, "error_lazy-scope_20260918_2.log")
		recoveryCompressed = recoveryRaw + ".gz"
		require.NoError(t, os.WriteFile(recoveryRaw, []byte("retained recovery source"), 0o600))
		require.NoError(t, os.WriteFile(recoveryCompressed, []byte("old invalid peer"), 0o600))
		set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		require.Empty(t, set.loggers)
		require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "set Close", set))
		if time.Now().UTC().Format(pkgfsstore.FileDateFormat) == day {
			break
		}
		require.Zero(t, attempt, "the UTC date changed during two consecutive destination registrations")
	}
	// 以前の圧縮peerだけが削除され、rawが残った状態で再起動します。
	// 再圧縮後も、その世代をlazy roleのupload待ちとして保護しません。
	require.NoError(t, os.Remove(recoveryCompressed))
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	require.Empty(t, set.loggers)
	// 起動後、lazy roleの生成前に別roleがrawを回復した場合も、登録時の対象は変わりません。
	// 現在の期間のrawの圧縮fileは送信対象で、過去の期間のrawの圧縮fileは送信も削除もしません。
	pastCompressed := pastRaw + ".gz"
	require.NoError(t, os.WriteFile(pastCompressed, []byte("month-old compressed"), 0o600))
	require.NoError(t, os.Remove(pastRaw))
	currentCompressed := currentRaw + ".gz"
	require.NoError(t, os.WriteFile(currentCompressed, []byte("current period compressed"), 0o600))
	require.NoError(t, os.Remove(currentRaw))
	require.NoError(t, os.WriteFile(recoveryCompressed, []byte("recovered compressed"), 0o600))
	require.NoError(t, os.Remove(recoveryRaw))
	// 登録後に作られた世代はlazy roleでも送信対象です。
	newCompressed := filepath.Join(dir, "debug_lazy-scope_20260922_1.log.gz")
	require.NoError(t, os.WriteFile(newCompressed, []byte("new compressed"), 0o600))
	defaultLogger, err := set.NewFileLogger(ctx)
	require.NoError(t, err)
	names := newPurgeFileNames(defaultLogger.Fsstore.FileName)
	require.False(t, defaultLogger.requiresUploadProtection(old, names))
	require.False(t, defaultLogger.requiresUploadProtection(recoveryCompressed, names))
	require.False(t, defaultLogger.requiresUploadProtection(pastCompressed, names))
	require.True(t, defaultLogger.requiresUploadProtection(currentCompressed, names))
	require.True(t, defaultLogger.requiresUploadProtection(newCompressed, names))
	// lazy roleの新しいrawは、それを所有するroleがdeliveryを完了するまで保護されたままです。
	completedRaw := filepath.Join(dir, "error_lazy-scope_20260921_1.log")
	require.NoError(t, os.WriteFile(completedRaw, []byte("new raw generation"), 0o600))
	require.False(t, cfg.FsstoreConfig.Lifecycle.EnableFileCompress)
	require.True(t, defaultLogger.requiresUploadProtection(completedRaw, names))
	for _, tc := range []struct {
		role     loggerType
		path     string
		eligible bool
	}{
		{ErrorLogger, old, false}, {DebugLogger, pastCompressed, false}, {ErrorLogger, currentCompressed, true},
		{DebugLogger, newCompressed, true}, {ErrorLogger, recoveryCompressed, false},
	} {
		logger, err := set.fileLogger(ctx, tc.role)
		require.NoError(t, err)
		generation, err := checkpointstore.ArchiveCheckpointGenerationKeyForPath(tc.path, logger.Fsstore.FileName)
		require.NoError(t, err)
		key := checkpointstore.CheckpointKey{Generation: generation, Compressed: true}
		checkpoint, err := logger.archiveUploadCheckpoints.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, tc.eligible, !checkpoint.Complete(logger.archiveUploadDestinationsSnapshot().destinationIDs), tc.path)
	}
	owner, err := set.NewErrorFileLogger(ctx)
	require.NoError(t, err)
	_, err = owner.archiveUploadCheckpoints.AdvanceCompletionCursor(ctx,
		checkpointstore.CheckpointKey{Generation: "20260921-1"}, owner.archiveUploadDestinationsSnapshot().destinationIDs)
	require.NoError(t, err)
	require.FileExists(t, completedRaw)
	require.False(t, defaultLogger.requiresUploadProtection(completedRaw, names),
		"shared-directory purge must read the materialized peer's current cursor, not its initial cursor")
	require.True(t, defaultLogger.requiresUploadProtection(newCompressed, names),
		"one role's completion must not release another role's pending generation")
	require.NoError(t, owner.closeOwnedFileResource())
	require.True(t, defaultLogger.requiresUploadProtection(completedRaw, names),
		"a closing peer must be protected without reusing its closed checkpoint store")
}

func TestInlineInitiallyCompletedArchiveUsesNormalAgeRetention(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "excluded-retention")
	path := writePendingCompressedLog(t, logger, "20200101", 1, now)
	remote := &checkpointDestinationRecordingArchiveUploader{destinationIDs: []objectstorage.DestinationID{"new"}}
	cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "scope"}
	store, err := newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, logger.Fsstore.FileName, true, remote.DestinationIDs(), nil)
	require.NoError(t, err)
	logger.ArchiveUploader, logger.archiveUploadCheckpoints = remote, store
	logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	// 初期cursorで完了済みの圧縮fileを、送信せずcleanupできます。
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.NoFileExists(t, path)
	require.Empty(t, remote.requested)
}

func TestInlineNewDestinationPreservesExistingDestinationRetry(t *testing.T) {
	// Close時の再試行間隔は実時間で待たず、bubble内の時刻で進めます。
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
		logger := newInlineUsageTestLogger(t, now, "existing-destination-retry")
		primary := &sequenceArchiveUploader{errs: []error{errors.New("retry existing destination")}}
		secondary := &sequenceArchiveUploader{}
		first := newMultiArchiveUploader(t, uploader.Destination{ID: "existing", Uploader: primary})
		cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "scope"}
		store, err := newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, logger.Fsstore.FileName, true, first.DestinationIDs(), nil)
		require.NoError(t, err)
		path := writePendingCompressedLog(t, logger, "20260920", 1, now)
		require.NoError(t, store.Close())
		remote := newMultiArchiveUploader(t,
			uploader.Destination{ID: "existing", Uploader: primary},
			uploader.Destination{ID: "new", Uploader: secondary},
		)
		store, err = newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, logger.Fsstore.FileName, true, remote.DestinationIDs(), nil)
		require.NoError(t, err)
		logger.ArchiveUploader, logger.archiveUploadCheckpoints = remote, store
		completed, retry, _ := logger.completeArchiveGroupUpload(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName)[0], runNotificationImmediately, logger.archiveUploadDestinationsSnapshot(), nil)
		require.Empty(t, completed)
		require.True(t, retry)
		require.FileExists(t, path)
		require.Equal(t, 1, primary.attemptCount())
		require.Zero(t, secondary.attemptCount())
		// Closeは既存宛先の再試行を完了させてから戻ります。戻らない場合は期限後に止めて失敗させます。
		closed := make(chan error, 1)
		go func() { closed <- logger.Close() }()
		requireStartedCloseCompletesForTest(t, logger, closed)
		require.NoFileExists(t, path)
		require.Equal(t, 2, primary.attemptCount())
		require.Zero(t, secondary.attemptCount())
	})
}

func TestInlineOutOfScopeCompressedLogReturnsToOrdinaryPurge(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 23, 12, 0, 0, 0, time.UTC)
	logger := newInlineUsageTestLogger(t, now, "out-of-scope-purge")
	expired := writePendingCompressedLog(t, logger, "20200101", 1, now)
	retained := writePendingCompressedLog(t, logger, "20260922", 1, now)
	cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: t.TempDir(), SourcePrefix: "scope"}
	remote := &checkpointDestinationRecordingArchiveUploader{destinationIDs: []objectstorage.DestinationID{"archive"}}
	store, err := newArchiveCompressedLogCheckpointStore(cfg, DefaultLogger, logger.Fsstore.FileName, true, remote.DestinationIDs(), nil)
	require.NoError(t, err)
	logger.archiveUploadCheckpoints, logger.ArchiveUploader = store, remote

	// 登録前から存在した圧縮ログは送信せず、送信待ちとして保護もしません。
	// 保持期限を過ぎたものだけを、同じcleanupの通常purgeで削除します。
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	require.NoFileExists(t, expired)
	require.FileExists(t, retained)
	require.FileExists(t, logger.Fsstore.ActiveFilePath)
	require.Empty(t, remote.requested)
}

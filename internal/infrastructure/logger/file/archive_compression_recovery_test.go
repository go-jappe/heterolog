package file

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"

	"github.com/stretchr/testify/require"
)

var errInlineRecoveryCompression = errors.New("inline recovery compression failed")

type inlineRecoveryCompressionCodec struct {
	failCompression                    atomic.Bool
	createSourceRetirementConflictOnce atomic.Bool
	compressCalls                      atomic.Int32
	verifyCalls                        atomic.Int32
}

func (*inlineRecoveryCompressionCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressGzip
}

func (c *inlineRecoveryCompressionCodec) Compress(in, out *os.File) error {
	c.compressCalls.Add(1)
	if c.failCompression.Load() {
		return errInlineRecoveryCompression
	}
	writer := gzip.NewWriter(out)
	if _, err := io.Copy(writer, in); err != nil {
		_ = writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if c.createSourceRetirementConflictOnce.CompareAndSwap(true, false) {
		// retirement失敗後に同内容の残骸を片付けるfixture。内容不一致の隔離は別testで扱う。
		content, err := os.ReadFile(in.Name())
		if err != nil {
			return err
		}
		return os.WriteFile(
			logpattern.CompressionSourceDeletePendingPath(in.Name()),
			content,
			0o600,
		)
	}
	return nil
}

func (c *inlineRecoveryCompressionCodec) VerifyCompressedIntegrity(in *os.File) error {
	c.verifyCalls.Add(1)
	codec, err := infrafsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	if err != nil {
		return err
	}
	return codec.VerifyCompressedIntegrity(in)
}

func TestInlineFirstFullScanRecoversInactiveRawArchiveAfterRestart(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	const name = "restart-raw-recovery"
	logRoot := t.TempDir()
	cfg := newInlineCompressionRecoveryTestConfig(logRoot, name, codec)
	directory, err := infrafsstore.GetFileSystemStoreDirectoryPath(logRoot, name, name)
	require.NoError(t, err)
	rawPath := filepath.Join(directory, name+"_20260829_1.log")
	// process終了後のfilesystem状態を先に作り、その後で新しいFileLoggerを構築します。
	require.NoError(t, os.WriteFile(rawPath, []byte("raw left by the previous process"), 0o600))
	logger := newInlineCompressionRecoveryLoggerFromConfig(t, now, cfg)

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))

	require.NoFileExists(t, rawPath)
	require.FileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(rawPath))
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFullScanRetriesCompressionFailureAfterCacheTTL(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 30, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	codec.failCompression.Store(true)
	logger := newInlineCompressionRecoveryTestLogger(t, now, "failed-raw-recovery", codec)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.FileExists(t, rawPath)
	require.NoFileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 1, codec.compressCalls.Load())

	codec.failCompression.Store(false)
	logger.Clock = pkgclock.NewFixedClock(now.Add(logger.inlinePurge.config.UsageCacheTTL()))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))

	require.NoFileExists(t, rawPath)
	require.FileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 2, codec.compressCalls.Load())
}

func TestInlineFullScanRefreshesAfterCompressedPeerPublishedBeforeSourceRetirement(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 45, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	codec.createSourceRetirementConflictOnce.Store(true)
	logger := newInlineCompressionRecoveryTestLogger(t, now, "published-peer-recovery", codec)
	scans := observeCleanupScans(logger)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	pendingPath := logpattern.CompressionSourceDeletePendingPath(rawPath)

	// 検証済みfinal peerの公開後に、注入したpending pathとの競合でcanonical sourceの
	// retirementだけが失敗します。新しいpeerは走査済みsnapshotに含まれていません。
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.FileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.FileExists(t, pendingPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())

	// 公開済みpeerを検出した時点で古いsnapshotを無効化します。cache TTLを待たず、
	// 次のcleanupでpendingだけを削除し、同じ観測でpeerを検証してsourceをretireします。
	// pendingの削除は観測済みraw/peerを変えないため、間に追加の走査を挟みません。
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.NoFileExists(t, pendingPath)
	require.EqualValues(t, 2, scans.all.Load())
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFullScanRetiresSourceWhenValidCompressedPeerExists(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "valid-peer-recovery", codec)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	// 圧縮先の公開後、元ログの削除前に停止した状態です。peerは同じ内容の正しい圧縮ファイルです。
	rawContent, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	writeGzipFile(t, compressedPath, string(rawContent))

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))

	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(rawPath))
	require.Zero(t, codec.compressCalls.Load(), "a verified peer must not be overwritten")
	require.EqualValues(t, 1, codec.verifyCalls.Load())
}

func TestInlineRestartDoesNotRecreateExternallyDeletedArchiveAfterSuccessfulCompression(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 30, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "deleted-committed-archive", codec)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.NoError(t, os.Remove(compressedPath))
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))

	// process再起動を模してFileLoggerとそのphysical series用runtimeを作り直します。
	// directory共有Trackerに以前のbaselineが残っていても、新しいloggerの初回full scanは
	// canonical rawがretire済みであることを確認し、外部削除された成果物を再生成しません。
	restarted, err := newStandaloneFileLogger(
		context.Background(),
		logger.config,
		nil,
		nil,
	)
	require.NoError(t, err)
	restarted.Clock = pkgclock.NewFixedClock(now.Add(time.Minute))
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "restarted Close", func() closerForTest { return restarted }, true)

	require.NoError(t, purgeFileLoggerForTest(context.Background(), restarted))
	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, compressedPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFullScanDoesNotMistakeStaleCompressionTempForCompletedPeer(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 45, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "stale-compression-temp", codec)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	temporaryPath := compressedPath + ".tmp"
	require.NoError(t, os.WriteFile(temporaryPath, []byte("interrupted compression"), 0o600))

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))

	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.NoFileExists(t, temporaryPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFullScanProtectsLazySharedRoleRawUntilOwningRoleRecoversIt(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 50, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	codec.failCompression.Store(true)
	const name = "shared-role-raw-recovery"
	cfg := newInlineCompressionRecoveryTestConfig(t.TempDir(), name, codec)
	// default/errorを同じphysical directoryへ置き、error loggerは最初のscan後まで
	// lazy未生成のままにします。
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir

	loggerSet, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "loggerSet Close", func() closerForTest { return loggerSet }, true)
	defaultLogger, err := loggerSet.NewFileLogger(context.Background())
	require.NoError(t, err)
	defaultLogger.Clock = pkgclock.NewFixedClock(now)

	errorRawPath := filepath.Join(
		defaultLogger.Fsstore.FileName.Dir,
		cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix+
			cfg.FsstoreConfig.Layout.Target+"_20260829_1.log",
	)
	require.NoError(t, os.WriteFile(errorRawPath, []byte("uncompressed error archive"), 0o600))
	require.NoError(t, os.Truncate(
		errorRawPath,
		int64(defaultLogger.inlinePurge.config.MaxDirSize()+1),
	))
	old := now.Add(-time.Hour)
	require.NoError(t, os.Chtimes(errorRawPath, old, old))

	// default loggerのfull scanも共通codecでlazy error系列のraw回復を試みます。
	// 圧縮が失敗した場合は、directory-size超過でも未完了rawをpurgeしません。
	require.NoError(t, purgeFileLoggerForTest(context.Background(), defaultLogger))
	require.FileExists(t, errorRawPath)
	require.NoFileExists(t, errorRawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 1, codec.compressCalls.Load())

	// error roleが初めて生成された時点で、そのphysical series自身の初回full scanが
	// 共有Trackerのbaselineを無視してrawを再試行します。
	codec.failCompression.Store(false)
	errorLogger, err := loggerSet.NewErrorFileLogger(context.Background())
	require.NoError(t, err)
	errorLogger.Clock = pkgclock.NewFixedClock(now.Add(time.Minute))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), errorLogger))
	require.NoFileExists(t, errorRawPath)
	require.FileExists(t, errorRawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 2, codec.compressCalls.Load())
}

func TestInlineUploaderBootstrapFirstWritePerformsRawRecoveryFullScan(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 55, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	const name = "uploader-bootstrap-raw-recovery"
	logRoot := t.TempDir()
	cfg := newInlineCompressionRecoveryTestConfig(logRoot, name, codec)
	cfg.ObjectStorageUploadConfig = objectstorage.LogFileUploadConfig{
		Enabled: true,
		Default: objectstorage.UploadTargetConfig{
			Enabled: true,
			Targets: []objectstorage.UploadTarget{objectstorage.UploadTargetS3},
			S3: objectstorage.S3Config{
				Region:          "ap-northeast-1",
				Endpoint:        "https://s3.example.com",
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
				Bucket:          "archive-bucket",
			},
		},
	}
	directory, err := infrafsstore.GetFileSystemStoreDirectoryPath(logRoot, name, name)
	require.NoError(t, err)
	rawPath := filepath.Join(directory, name+"_20260829_1.log")
	require.NoError(t, os.WriteFile(rawPath, []byte("raw before constructor"), 0o600))

	logger := newInlineCompressionRecoveryLoggerFromConfig(t, now, cfg)
	require.NotNil(t, logger.ArchiveUploader)
	counts := observeCleanupScans(logger)

	// 構築時に即時の再試行が予約されていても、productionの最初のwriteが
	// 起動するcleanupはtarget-onlyで終わらず、同じ回にdirectory full scanを取得します。
	require.NoError(t, logger.testInfo(context.Background(), "first natural write"))
	waitBackgroundForTest(t, logger)
	require.EqualValues(t, 1, counts.all.Load())
	require.Zero(t, counts.byTarget.Load())
	require.NoFileExists(t, rawPath)
	require.FileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFullScanRecoversKnownDisabledRolePrefixInSharedDirectory(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 57, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	const name = "disabled-role-prefix-recovery"
	cfg := newInlineCompressionRecoveryTestConfig(t.TempDir(), name, codec)
	cfg.FsstoreConfig.Layout.IsSeparateDebugLogsEnabled = false
	logger := newInlineCompressionRecoveryLoggerFromConfig(t, now, cfg)

	legacyDebugRaw := filepath.Join(
		logger.Fsstore.FileName.Dir,
		cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix+
			cfg.FsstoreConfig.Layout.Target+"_20260829_1.log",
	)
	require.NoError(t, os.WriteFile(legacyDebugRaw, []byte("raw from previous split config"), 0o600))

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, legacyDebugRaw)
	require.FileExists(t, legacyDebugRaw+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlinePostRotationFullScanDoesNotPurgeRawAfterCompressionFailure(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		now := time.Date(2026, time.August, 30, 13, 59, 0, 0, time.UTC)
		codec := &inlineRecoveryCompressionCodec{}
		codec.failCompression.Store(true)
		logger := newInlineCompressionRecoveryTestLogger(t, now, "rotation-failed-raw-protection", codec)
		counts := observeCleanupScans(logger)
		oldPath := logger.Fsstore.ActiveFilePath
		require.NoError(t, os.Truncate(
			oldPath,
			int64(logger.inlinePurge.config.MaxDirSize()+1),
		))

		// rotation archiveで一度失敗したsourceは、直後の強制full scanでは再圧縮せず
		// 一回分retainします。これにより同じerror通知への自己再入を避けながら、
		// directory-size超過を解消するために未圧縮rawを削除しません。
		require.NoError(t, rotateFileLoggerForTest(context.Background(), logger))
		// rotation後のarchiveとfull scanはbackgroundで実行されるため、完了を待ってから確認します。
		synctest.Wait()
		require.EqualValues(t, 1, counts.all.Load())
		require.False(t, logger.inlinePurge.cleanupRunning.Load())

		require.FileExists(t, oldPath)
		require.NoFileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
		require.EqualValues(t, 1, codec.compressCalls.Load())

		// post-rotation観測で確定したbaselineのTTL後は通常回復へ戻り、正常圧縮を
		// 完了するまでrawを保持したまま再試行します。
		codec.failCompression.Store(false)
		logger.Clock = pkgclock.NewFixedClock(now.Add(logger.inlinePurge.config.UsageCacheTTL()))
		require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
		// 受理済み圧縮は通常観測に依存せず終了時に完了します。
		requireFileLoggerClosesForTest(t, logger)
		require.NoFileExists(t, oldPath)
		require.FileExists(t, oldPath+pkgfsstore.ExtensionCompressGzip)
		require.EqualValues(t, 2, codec.compressCalls.Load())
	})
}

func TestInlineFullScanRemovesInvalidPeerWithoutPurgingSource(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "invalid-peer-recovery", codec)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	require.NoError(t, os.WriteFile(compressedPath, []byte("not a gzip stream"), 0o600))

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.FileExists(t, rawPath)
	require.NoFileExists(t, compressedPath)
	require.Zero(t, codec.compressCalls.Load())

	// invalid peerの削除で観測を無効化したため、古いbaselineのTTLを待たず、次の
	// 自然なcleanupがfreshなfull scanを取得できます。
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFullScanExcludesRetainedAndNonCanonicalRawPaths(t *testing.T) {
	now := time.Date(2026, time.August, 30, 14, 30, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "protected-raw-recovery", codec)
	retainedRawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
	nonCanonicalPath := filepath.Join(
		logger.Fsstore.FileName.Dir,
		logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_memo.log",
	)
	require.NoError(t, os.WriteFile(nonCanonicalPath, []byte("not a rotation generation"), 0o600))

	owner := appendlog.NewDirectoryGuardToken()
	logger.lifecycleGuard.ProtectPaths(owner, retainedRawPath)
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.FileExists(t, retainedRawPath)
	require.FileExists(t, nonCanonicalPath)
	require.Zero(t, codec.compressCalls.Load())

	logger.lifecycleGuard.UnprotectPaths(owner, retainedRawPath)
	logger.Clock = pkgclock.NewFixedClock(now.Add(logger.inlinePurge.config.UsageCacheTTL()))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, retainedRawPath)
	require.FileExists(t, nonCanonicalPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineRecoveredCompressedArchiveUsesExistingUploadRetryOutsideLifecycleLock(t *testing.T) {
	now := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "recovered-archive-upload", codec)
	upload := &recordingArchiveUploader{}
	logger.ArchiveUploader = newSingleArchiveUploader(t, upload)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.FileExists(t, compressedPath)
	require.Empty(t, upload.inputsSnapshot(), "a full observation must not perform remote I/O")
	scheduled, retryAt := fileSeriesUploadRetryScheduleSnapshot(logger.inlinePurge)
	require.True(t, scheduled)
	require.Equal(t, now.Add(logger.inlinePurge.config.UsageCacheTTL()), retryAt)

	logger.Clock = pkgclock.NewFixedClock(retryAt)
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.Equal(t, []string{compressedPath}, archiveUploadInputPaths(upload.inputsSnapshot()))
	require.NoFileExists(t, compressedPath)
}

func TestInlineFullScanRemovesRetiredSourcePendingPathWithoutRecompression(t *testing.T) {
	now := time.Date(2026, time.August, 30, 15, 30, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "pending-source-recovery", codec)
	canonicalPath := filepath.Join(
		logger.Fsstore.FileName.Dir,
		logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_20260829_1.log",
	)
	pendingPath := logpattern.CompressionSourceDeletePendingPath(canonicalPath)
	require.NoError(t, os.WriteFile(pendingPath, []byte("already compressed source"), 0o600))

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, pendingPath)
	require.NoFileExists(t, canonicalPath)
	require.NoFileExists(t, canonicalPath+pkgfsstore.ExtensionCompressGzip)
	require.Zero(t, codec.compressCalls.Load())
}

func TestInlineFullScanFailsClosedWhenRetiredSourcePendingPathCannotBeRemoved(t *testing.T) {
	now := time.Date(2026, time.August, 30, 16, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "pending-source-remove-failure", codec)
	canonicalPath := filepath.Join(
		logger.Fsstore.FileName.Dir,
		logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_20260829_1.log",
	)
	pendingPath := logpattern.CompressionSourceDeletePendingPath(canonicalPath)
	require.NoError(t, os.Mkdir(pendingPath, 0o700))
	childPath := filepath.Join(pendingPath, "child")
	require.NoError(t, os.WriteFile(childPath, []byte("blocks directory removal"), 0o600))

	require.Error(t, purgeFileLoggerForTest(context.Background(), logger))
	require.DirExists(t, pendingPath)
	require.NoFileExists(t, canonicalPath)
	require.Zero(t, codec.compressCalls.Load())

	require.NoError(t, os.Remove(childPath))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoDirExists(t, pendingPath)
	require.NoFileExists(t, canonicalPath)
	require.Zero(t, codec.compressCalls.Load())
}

func TestInlinePendingSourceConflictReportsAndPreservesConflictingBytes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	cfg := newInlineCompressionRecoveryTestConfig(t.TempDir(), "pending-conflict", codec)
	cfg.FsstoreConfig.Duration.CompressWarnThresholdDuration = time.Hour
	cfg.FsstoreConfig.Duration.CompressErrorThresholdDuration = time.Hour
	logger := newInlineCompressionRecoveryLoggerFromConfig(t, now, cfg)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260909", 1)
	rawContent, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	pendingPath := logpattern.CompressionSourceDeletePendingPath(rawPath)
	pendingContent := []byte("different pending content must remain recoverable")
	require.NoError(t, os.WriteFile(pendingPath, pendingContent, 0o600))
	old := now.AddDate(0, 0, -30)
	require.NoError(t, os.Chtimes(pendingPath, old, old))
	reported := make(chan error, 4)
	logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
		reported <- err
	}
	fileName := logger.Fsstore.FileName
	logger.triggerCleanup(ctx, fileName)
	var reportedErr error
	reportedErr = receiveForTest(t, reported, "pending conflict was not reported through OnError")
	waitBackgroundForTest(t, logger)
	require.ErrorIs(t, reportedErr, appendlog.ErrCompressionSourceDeletePendingConflict)
	preserved := preservedFilesForTest(t, filepath.Dir(rawPath))
	require.Len(t, preserved, 1)
	// 通知されたerrorから退避先を取り出し、同じfileを指すことを確認します。
	var conflictErr *appendlog.CompressionSourceDeletePendingConflictError
	require.ErrorAs(t, reportedErr, &conflictErr)
	reportedPreserved, err := os.Stat(conflictErr.PreservedPath)
	require.NoError(t, err)
	actualPreserved, err := os.Stat(preserved[0])
	require.NoError(t, err)
	require.True(t, os.SameFile(reportedPreserved, actualPreserved), "OnError must identify the preserved file")
	require.NoFileExists(t, pendingPath)
	gotRaw, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	require.Equal(t, rawContent, gotRaw)
	gotPending, err := os.ReadFile(preserved[0])
	require.NoError(t, err)
	require.Equal(t, pendingContent, gotPending)
	require.Zero(t, codec.compressCalls.Load(), "the conflict must abort this full scan")

	// 保全で停止した観測を次の通常要求で再開し、rawの回復を一度だけ行う。
	logger.triggerCleanup(ctx, fileName)
	waitBackgroundForTest(t, logger)
	require.NoFileExists(t, rawPath)
	require.FileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 1, codec.compressCalls.Load())

	// age purgeを再度期限にし、古い保全fileが通常lifecycleやCloseで失われないことを確認する。
	logger.Clock = pkgclock.NewFixedClock(now.AddDate(0, 0, 2))
	logger.triggerCleanup(ctx, fileName)
	waitBackgroundForTest(t, logger)
	require.NoError(t, closeBoundedForTest(t, boundedwait.HangTimeout, "logger Close", logger))
	gotPending, err = os.ReadFile(preserved[0])
	require.NoError(t, err)
	require.Equal(t, pendingContent, gotPending)
	select {
	case err := <-reported:
		t.Fatalf("unexpected repeated cleanup error: %v", err)
	default:
	}
}

func newInlineCompressionRecoveryTestLogger(
	t *testing.T,
	now time.Time,
	name string,
	codec pkgfsstore.CompressionCodec,
) *FileLogger {
	t.Helper()
	cfg := newInlineCompressionRecoveryTestConfig(t.TempDir(), name, codec)
	return newInlineCompressionRecoveryLoggerFromConfig(t, now, cfg)
}

func newInlineCompressionRecoveryTestConfig(
	logRoot string,
	name string,
	codec pkgfsstore.CompressionCodec,
) pkglogger.FileLoggerConfig {
	cfg := newTestFileLoggerConfig(logRoot, name, name)
	cfg.LifecycleMode = pkgfsstore.FileLifecycleModeInline
	cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
	cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
	cfg.FsstoreConfig.CompressionCodec = codec
	return cfg
}

func newInlineCompressionRecoveryLoggerFromConfig(
	t *testing.T,
	now time.Time,
	cfg pkglogger.FileLoggerConfig,
) *FileLogger {
	t.Helper()
	logger, err := newStandaloneFileLogger(
		context.Background(),
		NewConfigAdapter(cfg),
		nil,
		nil,
	)
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	t.Cleanup(func() { closeInlineFixtureForTest(t, logger) })
	return logger
}

func writeInlineRecoveryRawArchive(
	t *testing.T,
	logger *FileLogger,
	period string,
	index uint64,
) string {
	t.Helper()
	path := filepath.Join(
		logger.Fsstore.FileName.Dir,
		logger.Fsstore.FileName.Prefix+logger.Fsstore.FileName.Target+"_"+
			period+"_"+strconv.FormatUint(index, 10)+"."+logger.Fsstore.FileName.Extension,
	)
	require.NoError(t, os.WriteFile(path, []byte("inactive raw archive"), 0o600))
	return path
}

func writeGzipFile(t *testing.T, path string, content string) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	writer := gzip.NewWriter(file)
	_, writeErr := writer.Write([]byte(content))
	closeWriterErr := writer.Close()
	closeFileErr := file.Close()
	require.NoError(t, writeErr)
	require.NoError(t, closeWriterErr)
	require.NoError(t, closeFileErr)
}

func archiveUploadInputPaths(inputs []uploader.UploadRequest) []string {
	paths := make([]string, len(inputs))
	for index := range inputs {
		paths[index] = inputs[index].FilePath()
	}
	return paths
}

var _ pkgfsstore.CompressionCodec = (*inlineRecoveryCompressionCodec)(nil)

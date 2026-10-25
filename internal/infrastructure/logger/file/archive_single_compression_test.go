package file

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type countingArchiveCompressionCodec struct {
	pkgfsstore.CompressionCodec
	compressCalls atomic.Int32
}

func (c *countingArchiveCompressionCodec) Compress(in, out *os.File) error {
	c.compressCalls.Add(1)
	return c.CompressionCodec.Compress(in, out)
}

func TestFileLoggerRecoveryReusesGzipAfterChangingToZstd(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		name := "without-checkpoint"
		if checkpoint {
			name = "with-checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			codec, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressZstd, nil)
			require.NoError(t, err)
			counted := &countingArchiveCompressionCodec{CompressionCodec: codec}
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			logger := newInlineCompressionRecoveryTestLogger(t, now, "old-gzip-recovery", counted)
			if checkpoint {
				logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, t.TempDir(), "old_gzip_recovery")
			}
			raw := writeInlineRecoveryRawArchive(t, logger, "20260820", 1)
			gzip := raw + pkgfsstore.ExtensionCompressGzip
			// 形式変更前にcommitした、元ログと同じ内容のgzipです。
			rawContent, err := os.ReadFile(raw)
			require.NoError(t, err)
			writeGzipFile(t, gzip, string(rawContent))
			before, err := os.ReadFile(gzip)
			require.NoError(t, err)
			require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
			require.NoFileExists(t, raw)
			require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressZstd)
			require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(raw))
			// 同じ内容のpeerを再利用した元ログは退避ではなく削除されます。
			require.NoDirExists(t, filepath.Join(filepath.Dir(raw), fsstore.PreservedDirectoryName))
			after, err := os.ReadFile(gzip)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Zero(t, counted.compressCalls.Load())
			if checkpoint {
				state, err := logger.archiveUploadCheckpoints.Load(context.Background(), inlineCompressedLogCheckpointKey(t, logger, gzip))
				require.NoError(t, err)
				require.Empty(t, state.CompletedDestinationIDs)
			}
		})
	}
}

func TestFileLoggerUnknownPreviousCodecKeepsRawAndPeerDuringPurge(t *testing.T) {
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "unknown-old-codec", codec)
	raw := writeInlineRecoveryRawArchive(t, logger, "20260820", 1)
	peer := raw + ".previous-custom"
	content := []byte("old custom format must be kept")
	require.NoError(t, os.WriteFile(peer, content, 0o600))
	old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
	for _, path := range []string{raw, peer} {
		require.NoError(t, os.Chtimes(path, old, old))
	}
	var notified bool
	logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		if errors.Is(err, fsstore.ErrUnsupportedCompressionFormat) {
			notified = true
		}
	}
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.True(t, notified)
	require.FileExists(t, raw)
	require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressGzip)
	got, err := os.ReadFile(peer)
	require.NoError(t, err)
	require.Equal(t, content, got)
	require.Zero(t, codec.compressCalls.Load())
}

func TestFileLoggerMultipleCompressedPeersUseFirstCandidateWithoutUploader(t *testing.T) {
	for _, rawExists := range []bool{false, true} {
		name := "compressed-only"
		if rawExists {
			name = "with-raw"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			codec := &inlineRecoveryCompressionCodec{}
			logger := newInlineCompressionRecoveryTestLogger(t, now, "multiple-peers", codec)
			raw := writeInlineRecoveryRawArchive(t, logger, "20260913", 1)
			if !rawExists {
				require.NoError(t, os.Remove(raw))
			}
			gzip, zstd := raw+pkgfsstore.ExtensionCompressGzip, raw+pkgfsstore.ExtensionCompressZstd
			writeGzipFile(t, gzip, "first published file")
			writeArchiveFileWithCodec(t, zstd, pkgfsstore.CompressZstd, "second published file")
			for _, path := range []string{gzip, zstd} {
				require.NoError(t, os.Chtimes(path, now, now))
			}
			require.Nil(t, logger.ArchiveUploader)
			require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
			require.FileExists(t, gzip)
			require.FileExists(t, zstd)
			if rawExists {
				require.NoFileExists(t, raw)
			}
			require.Zero(t, codec.compressCalls.Load())
		})
	}
}

func TestFileLoggerCompressedOnlyRetryRetainsRawKeyAcrossFormats(t *testing.T) {
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	logger := newInlineCompressionRecoveryTestLogger(t, now, "compressed-raw-key", &inlineRecoveryCompressionCodec{})
	logger.ArchiveUploader = newSingleArchiveUploader(t, &recordingArchiveUploader{})
	raw := writeInlineRecoveryRawArchive(t, logger, "20260820", 1)
	require.NoError(t, os.Remove(raw))
	peer := raw + pkgfsstore.ExtensionCompressZstd
	writeArchiveFileWithCodec(t, peer, pkgfsstore.CompressZstd, "previous-format archive")
	owner, competing := appendlog.NewDirectoryGuardToken(), appendlog.NewDirectoryGuardToken()
	var protection []string
	var uploadPaths []archiveUploadGroup
	require.NoError(t, logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		var err error
		uploadPaths, err = logger.retainRemainingLogFilesForOwner(
			context.Background(), fsstore.GroupFilesByCompressionSource(fsstore.InspectFilePaths([]string{peer}), logger.Fsstore.FileName.Extension), logger.Fsstore.FileName, owner, &protection,
		)
		return err
	}))
	defer logger.lifecycleGuard.UnprotectPaths(owner, protection...)
	require.Equal(t, []string{peer}, archiveUploadGroupPathsForTest(uploadPaths), "raw protection must not become an upload input")
	require.ElementsMatch(t, []string{raw, peer}, protection)
	require.False(t, logger.lifecycleGuard.ProtectPathsIfNoOtherOwnerConflict(
		competing, nil, []string{raw, raw + pkgfsstore.ExtensionCompressGzip},
	))
	logger.lifecycleGuard.UnprotectPaths(owner, protection...)
	require.True(t, logger.lifecycleGuard.ProtectPathsIfNoOtherOwnerConflict(
		competing, nil, []string{raw, raw + pkgfsstore.ExtensionCompressGzip},
	))
	logger.lifecycleGuard.UnprotectPaths(competing, raw, raw+pkgfsstore.ExtensionCompressGzip)
}

func TestFileLoggerDuplicateArchivesDoNotBlockLazyForeignRoleRetention(t *testing.T) {
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	cfg := newInlineCompressionRecoveryTestConfig(t.TempDir(), "foreign-duplicate-peers", &inlineRecoveryCompressionCodec{})
	cfg.FsstoreConfig.Layout.ErrorLogDir = cfg.FsstoreConfig.Layout.LogDir
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	// error roleは一度も生成せず、uploadも無効のままdefaultの全体走査を行います。
	logger, err := set.NewFileLogger(context.Background())
	require.NoError(t, err)
	logger.Clock = pkgclock.NewFixedClock(now)
	require.Nil(t, logger.ArchiveUploader)
	raw := filepath.Join(logger.Fsstore.FileName.Dir,
		cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix+cfg.FsstoreConfig.Layout.Target+"_20260820_1.log",
	)
	gzip, zstd := raw+pkgfsstore.ExtensionCompressGzip, raw+pkgfsstore.ExtensionCompressZstd
	writeGzipFile(t, gzip, "foreign gzip")
	writeArchiveFileWithCodec(t, zstd, pkgfsstore.CompressZstd, "foreign zstd")
	require.NoError(t, os.Truncate(gzip, int64(logger.inlinePurge.config.MaxDirSize()+1)))
	old := now.AddDate(0, 0, -30)
	for _, path := range []string{gzip, zstd} {
		require.NoError(t, os.Chtimes(path, old, old))
	}
	require.NoFileExists(t, raw)
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, gzip)
	require.FileExists(t, zstd)
}

func TestFileLoggerPendingRetryDeletesOnlyFirstObservedCompressedPeer(t *testing.T) {
	for _, name := range []string{"upload-required", "completed-checkpoint"} {
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			logger := newInlineCompressionRecoveryTestLogger(t, now, "first-observed-peer", &inlineRecoveryCompressionCodec{})
			logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, t.TempDir(), "first_observed_peer")
			remote := &recordingArchiveUploader{}
			logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "required", Uploader: remote})
			raw := writeInlineRecoveryRawArchive(t, logger, "20260820", 1)
			require.NoError(t, os.Remove(raw))
			gzip, zstd := raw+pkgfsstore.ExtensionCompressGzip, raw+pkgfsstore.ExtensionCompressZstd
			writeGzipFile(t, gzip, "second candidate")
			writeArchiveFileWithCodec(t, zstd, pkgfsstore.CompressZstd, "first candidate")
			paths := []string{zstd, gzip}
			key := inlineCompressedLogCheckpointKey(t, logger, zstd)
			required := []objectstorage.DestinationID{"required"}
			if name != "upload-required" {
				_, err := logger.archiveUploadCheckpoints.Load(context.Background(), key)
				require.NoError(t, err)
				_, err = logger.archiveUploadCheckpoints.AdvanceCompletionCursor(context.Background(), key, required)
				require.NoError(t, err)
			}
			retained := retainPendingArchiveGroupsForTest(t, logger, paths)
			defer logger.releasePendingCompressedArchivesForTest(retained)
			require.Equal(t, []string{zstd}, archiveUploadGroupPathsForTest(retained), "selection follows input order, not codec or lexical order")
			require.False(t, logger.processRemainingLogFiles(context.Background(), retained, runNotificationImmediately))
			require.NoFileExists(t, zstd)
			require.FileExists(t, gzip, "selected candidate completion must not delete another compressed file")
			if name != "upload-required" {
				require.Empty(t, remote.inputsSnapshot())
			} else {
				require.Equal(t, []string{zstd}, archiveUploadInputPaths(remote.inputsSnapshot()))
			}
			state, err := logger.archiveUploadCheckpoints.Load(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, required, state.CompletedDestinationIDs, "completion cursor remains after removing the selected file")
			require.True(t, state.Complete(required))
		})
	}
}

func TestFileLoggerGenerationCheckpointUsesObservedOldCodecAfterRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	codec, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressZstd, nil)
	require.NoError(t, err)
	counted := &countingArchiveCompressionCodec{CompressionCodec: codec}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "generation-checkpoint-old-codec", counted)
	directory := t.TempDir()
	const sourceID = "generation_checkpoint_old_codec"
	store := newCompressedLogCheckpointTestStore(t, directory, sourceID)
	raw := writeInlineRecoveryRawArchive(t, logger, "20260820", 1)
	require.NoError(t, os.Remove(raw))
	actualPath := raw + pkgfsstore.ExtensionCompressGzip
	writeGzipFile(t, actualPath, "previously compressed using gzip")
	key := inlineCompressedLogCheckpointKey(t, logger, actualPath)
	_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)

	// 再起動後のcodecはzstdでも、成功は世代keyで照合し、送信・削除には走査したgzipを使います。
	logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, directory, sourceID)
	primary, secondary := &recordingArchiveUploader{}, &recordingArchiveUploader{}
	logger.ArchiveUploader = newMultiArchiveUploader(t,
		uploader.Destination{ID: "primary", Uploader: primary},
		uploader.Destination{ID: "secondary", Uploader: secondary},
	)
	retained := retainPendingArchiveGroupsForTest(t, logger, []string{actualPath})
	require.Equal(t, []string{actualPath}, archiveUploadGroupPathsForTest(retained))
	require.False(t, logger.processRemainingLogFiles(ctx, retained, runNotificationImmediately))
	logger.releasePendingCompressedArchivesForTest(retained)
	require.Empty(t, primary.inputsSnapshot())
	require.Equal(t, []string{actualPath}, archiveUploadInputPaths(secondary.inputsSnapshot()))
	require.Zero(t, counted.compressCalls.Load())
	require.NoFileExists(t, actualPath)
	require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressZstd)
	checkpoint, err := logger.archiveUploadCheckpoints.Load(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"primary", "secondary"}, checkpoint.CompletedDestinationIDs)
}

func writeArchiveFileWithCodec(t *testing.T, path string, format pkgfsstore.CompressFormat, content string) {
	t.Helper()
	source := filepath.Join(t.TempDir(), "source")
	require.NoError(t, os.WriteFile(source, []byte(content), 0o600))
	in, err := os.Open(source)
	require.NoError(t, err)
	defer in.Close()
	out, err := os.Create(path)
	require.NoError(t, err)
	defer out.Close()
	codec, err := fsstore.ResolveCompressionCodec(format, nil)
	require.NoError(t, err)
	require.NoError(t, codec.Compress(in, out))
}

package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

func TestFileLoggerCandidateInspectionKeepsNormalPeerWhenAnotherPeerIsNotRegular(t *testing.T) {
	for _, rawExists := range []bool{false, true} {
		for _, kind := range []string{"symlink", "dangling", "loop", "directory"} {
			name := "compressed-only/" + kind
			if rawExists {
				name = "with-raw/" + kind
			}
			t.Run(name, func(t *testing.T) {
				now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
				codec := &inlineRecoveryCompressionCodec{}
				logger := newInlineCompressionRecoveryTestLogger(t, now, "candidate-inspection", codec)
				raw := writeInlineRecoveryRawArchive(t, logger, "20260820", 1)
				if !rawExists {
					require.NoError(t, os.Remove(raw))
				}
				normalPeer := raw + pkgfsstore.ExtensionCompressGzip
				writeGzipFile(t, normalPeer, "first normal candidate must not be consumed")
				normalBefore, err := os.ReadFile(normalPeer)
				require.NoError(t, err)
				old := now.AddDate(0, 0, -logger.inlinePurge.config.MaxFileAge()-1)
				require.NoError(t, os.Chtimes(normalPeer, old, old))
				if rawExists {
					require.NoError(t, os.Chtimes(raw, old, old))
				}
				invalidPeer := raw + pkgfsstore.ExtensionCompressZstd
				target := filepath.Join(t.TempDir(), "external.zst")
				var targetBefore []byte
				switch kind {
				case "directory":
					require.NoError(t, os.Mkdir(invalidPeer, 0o700))
				case "loop":
					testutil.Symlink(t, invalidPeer, invalidPeer)
				case "dangling":
					testutil.Symlink(t, target, invalidPeer)
				case "symlink":
					writeArchiveFileWithCodec(t, target, pkgfsstore.CompressZstd, "external archive")
					targetBefore, err = os.ReadFile(target)
					require.NoError(t, err)
					testutil.Symlink(t, target, invalidPeer)
				}
				var notified bool
				logger.OnError = func(context.Context, error, *logging.LogEntry, observer.Tracer, observer.Meter) {
					notified = true
				}
				remote := &recordingArchiveUploader{}
				logger.ArchiveUploader = newSingleArchiveUploader(t, remote)
				logger.inlinePurge.seriesUploadRetry.scheduleImmediateRetry()
				require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
				require.True(t, notified)
				require.Empty(t, remote.inputsSnapshot())
				require.Zero(t, codec.compressCalls.Load())
				require.Zero(t, codec.verifyCalls.Load())
				normalAfter, err := os.ReadFile(normalPeer)
				require.NoError(t, err)
				require.Equal(t, normalBefore, normalAfter)
				if rawExists {
					require.FileExists(t, raw)
				}
				info, err := os.Lstat(invalidPeer)
				require.NoError(t, err)
				require.False(t, info.Mode().IsRegular())
				if kind == "symlink" {
					targetAfter, err := os.ReadFile(target)
					require.NoError(t, err)
					require.Equal(t, targetBefore, targetAfter)
				}
			})
		}
	}
}

func TestFileLoggerRotationInspectionRejectsOldPeerBeforeCreatingAnotherFormat(t *testing.T) {
	for _, kind := range []string{"dangling", "directory"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			codec := &inlineRecoveryCompressionCodec{}
			logger := newInlineCompressionRecoveryTestLogger(t, now, "rotation-candidate-inspection", codec)
			raw := writeInlineRecoveryRawArchive(t, logger, "20260820", 1)
			peer := raw + pkgfsstore.ExtensionCompressZstd
			if kind == "directory" {
				require.NoError(t, os.Mkdir(peer, 0o700))
			} else {
				testutil.Symlink(t, filepath.Join(t.TempDir(), "missing"), peer)
			}
			path, compressed := logger.tryCompressOldFile(context.Background(), raw, runNotificationImmediately, nil)
			require.False(t, compressed)
			require.Empty(t, path)
			require.FileExists(t, raw)
			require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressGzip)
			_, err := os.Lstat(peer)
			require.NoError(t, err)
			require.Zero(t, codec.compressCalls.Load())
			require.Zero(t, codec.verifyCalls.Load())
		})
	}
}

func TestInlineFullScanPreservesPreviousFormatSymlinkAfterCodecChange(t *testing.T) {
	for _, dangling := range []bool{false, true} {
		name := "valid-target"
		if dangling {
			name = "dangling"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			codec, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressZstd, nil)
			require.NoError(t, err)
			counted := &countingArchiveCompressionCodec{CompressionCodec: codec}
			logger := newInlineCompressionRecoveryTestLogger(t, now, "previous-peer-symlink", counted)
			raw := writeInlineRecoveryRawArchive(t, logger, "20260820", 1)
			peer := raw + pkgfsstore.ExtensionCompressGzip
			target := filepath.Join(t.TempDir(), "old-target.log.gz")
			var before []byte
			if !dangling {
				writeGzipFile(t, target, "unrelated valid compressed data")
				before, err = os.ReadFile(target)
				require.NoError(t, err)
			}
			testutil.Symlink(t, target, peer)
			require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
			require.FileExists(t, raw)
			require.NoFileExists(t, raw+pkgfsstore.ExtensionCompressZstd)
			info, err := os.Lstat(peer)
			require.NoError(t, err)
			require.NotZero(t, info.Mode()&os.ModeSymlink)
			require.Zero(t, counted.compressCalls.Load())
			if !dangling {
				after, err := os.ReadFile(target)
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})
	}
}

func TestInlineFullScanDoesNotTreatCompressedPeerSymlinkAsCompleted(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 15, 0, 0, time.UTC)
	codec := &inlineRecoveryCompressionCodec{}
	logger := newInlineCompressionRecoveryTestLogger(t, now, "peer-symlink-recovery", codec)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	compressedTargetPath := filepath.Join(t.TempDir(), "valid-target.log.gz")
	writeGzipFile(t, compressedTargetPath, "unrelated valid compressed file")
	testutil.Symlink(t, compressedTargetPath, compressedPath)

	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))

	require.FileExists(t, rawPath)
	peerInfo, err := os.Lstat(compressedPath)
	require.NoError(t, err)
	require.NotZero(t, peerInfo.Mode()&os.ModeSymlink)
	require.Zero(t, codec.compressCalls.Load())
	require.Zero(t, codec.verifyCalls.Load())

	// symlinkは正常圧縮の証明にならないためrawは残ります。path占有を
	// 解消した後のfull scanで、保持したrawを圧縮できます。
	require.NoError(t, os.Remove(compressedPath))
	logger.Clock = pkgclock.NewFixedClock(now.Add(logger.inlinePurge.config.UsageCacheTTL()))
	require.NoError(t, purgeFileLoggerForTest(context.Background(), logger))
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())
	peerInfo, err = os.Lstat(compressedPath)
	require.NoError(t, err)
	require.True(t, peerInfo.Mode().IsRegular())
}

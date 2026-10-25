package file

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// reportedErrorsForTestは、OnErrorへ通知されたerrorを試験の確認まで保持します。
func reportedErrorsForTest(logger *FileLogger) <-chan error {
	reported := make(chan error, 32)
	logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		select {
		case reported <- err:
		default:
		}
	}
	return reported
}

// preservedFilesForTestは、log directoryの.preservedへ退避されたfileを名前順に返します。
// 退避先は.preservedのdirectoryだけを前提にし、退避fileの命名には依存せず走査します。
func preservedFilesForTest(t *testing.T, logDir string) []string {
	t.Helper()
	directory := filepath.Join(logDir, infrafsstore.PreservedDirectoryName)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	var paths []string
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			paths = append(paths, filepath.Join(directory, entry.Name()))
		}
	}
	return paths
}

// requireReportedContentMismatchは、通知済みerrorのうち内容不一致の報告が退避先を持つことを確認します。
func requireReportedContentMismatch(t *testing.T, reported <-chan error, preservedPath string) {
	t.Helper()
	for {
		select {
		case err := <-reported:
			if !errorsIsContentMismatch(err) {
				continue
			}
			var preserved *PreservedSourceError
			require.ErrorAs(t, err, &preserved)
			require.Equal(t, preservedPath, preserved.PreservedPath)
			return
		default:
			t.Fatal("mismatched compressed peer was not reported through OnError")
		}
	}
}

func errorsIsContentMismatch(err error) bool {
	return errors.Is(err, infrafsstore.ErrCompressedContentMismatch)
}

func TestInlineFullScanPreservesRawWhenCompressedPeerContentDiffers(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)
	codec, err := infrafsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)
	logger := newInlineCompressionRecoveryTestLogger(t, now, "mismatched-peer-recovery", codec)
	reported := reportedErrorsForTest(logger)
	rawPath := writeInlineRecoveryRawArchive(t, logger, "20260909", 1)
	rawContent, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	// 完全なgzipですが、元ログとは別の内容です。
	writeGzipFile(t, compressedPath, "records that are not in the raw archive")
	peerBefore, err := os.ReadFile(compressedPath)
	require.NoError(t, err)

	require.NoError(t, purgeFileLoggerForTest(ctx, logger))

	// 元ログは削除せず.preservedへ退避し、完全なpeerは削除も再圧縮もしません。
	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(rawPath))
	preserved := preservedFilesForTest(t, filepath.Dir(rawPath))
	require.Len(t, preserved, 1)
	requireReportedContentMismatch(t, reported, preserved[0])
	gotRaw, err := os.ReadFile(preserved[0])
	require.NoError(t, err)
	require.Equal(t, rawContent, gotRaw)
	peerAfter, err := os.ReadFile(compressedPath)
	require.NoError(t, err)
	require.Equal(t, peerBefore, peerAfter)

	// 退避後の観測は残ったpeerだけを扱い、同じ不一致を繰り返し通知しません。
	logger.Clock = pkgclock.NewFixedClock(now.Add(logger.inlinePurge.config.UsageCacheTTL()))
	require.NoError(t, purgeFileLoggerForTest(ctx, logger))
	for {
		select {
		case err := <-reported:
			require.False(t, errorsIsContentMismatch(err), "the preserved source must not be reported again: %v", err)
			continue
		default:
		}
		break
	}
	require.FileExists(t, compressedPath)
	require.Equal(t, preserved, preservedFilesForTest(t, filepath.Dir(rawPath)))
}

func TestInlineRotationPreservesRawAndUploadsIntactPeerWhenContentDiffers(t *testing.T) {
	// 受理済み処理の再試行間隔は仮想時間で進めます。
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, time.September, 3, 17, 0, 0, 0, time.UTC)
		logger := newInlineCompressionCheckpointTestLogger(t, now, "rotation-mismatched-peer")
		logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(
			t,
			t.TempDir(),
			"inline_rotation-mismatched-peer",
		)
		remote := &recordingArchiveUploader{}
		logger.ArchiveUploader = newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
		reported := reportedErrorsForTest(logger)
		oldPath := logger.Fsstore.ActiveFilePath
		rawContent := []byte("records written before rotation\n")
		logger.Fsstore.Mu.Lock()
		_, err := logger.Fsstore.File.Write(rawContent)
		logger.Fsstore.Mu.Unlock()
		require.NoError(t, err)
		compressedPath := oldPath + pkgfsstore.ExtensionCompressGzip
		// 同じ名前の完全なgzipが既にありますが、このactive fileの内容ではありません。
		writeGzipFile(t, compressedPath, "records of another file with the same name")

		require.NoError(t, rotateFileLoggerForTest(ctx, logger))
		require.NotEqual(t, oldPath, logger.Fsstore.ActiveFilePath)

		// 受理済みの処理が終わった結果を確認します。rotation時の圧縮でも元ログを削除せず一度だけ退避し、
		// 内容の不一致をerrorで通知したうえで、残った完全なpeerをその世代の圧縮済みfileとして送信します。
		requireFileLoggerClosesForTest(t, logger)
		require.NoFileExists(t, oldPath)
		preserved := preservedFilesForTest(t, filepath.Dir(oldPath))
		require.Len(t, preserved, 1)
		requireReportedContentMismatch(t, reported, preserved[0])
		gotRaw, err := os.ReadFile(preserved[0])
		require.NoError(t, err)
		require.Equal(t, rawContent, gotRaw)
		inputs := remote.inputsSnapshot()
		require.Len(t, inputs, 1)
		require.Equal(t, compressedPath, inputs[0].FilePath())
	})
}

// requireFileLoggerClosesForTestは、受理済みの処理が終わりLoggerのCloseが完了することを確認します。
// 完了しない場合は仮想時間の期限後にForceCloseで止め、試験自体が待ち続けないようにします。
func requireFileLoggerClosesForTest(t *testing.T, logger *FileLogger) {
	t.Helper()
	closed := make(chan error, 1)
	go func() { closed <- logger.Close() }()
	requireStartedCloseCompletesForTest(t, logger, closed)
}

func TestInlineRotationRemovesCorruptPeerAndRecompressesBeforeClose(t *testing.T) {
	// 受理済み処理の再試行間隔は仮想時間で進めます。
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Date(2026, time.September, 3, 17, 30, 0, 0, time.UTC)
		logger := newInlineCompressionCheckpointTestLogger(t, now, "rotation-corrupt-peer")
		oldPath := logger.Fsstore.ActiveFilePath
		rawContent := []byte("records written before rotation\n")
		logger.Fsstore.Mu.Lock()
		_, err := logger.Fsstore.File.Write(rawContent)
		logger.Fsstore.Mu.Unlock()
		require.NoError(t, err)
		compressedPath := oldPath + pkgfsstore.ExtensionCompressGzip
		require.NoError(t, os.WriteFile(compressedPath, []byte("not a gzip stream"), 0o600))

		require.NoError(t, rotateFileLoggerForTest(ctx, logger))

		// 破損したpeerを残さず元ログから再圧縮し、同じ破損の再試行を待ち続けずにCloseが完了します。
		requireFileLoggerClosesForTest(t, logger)
		require.NoFileExists(t, oldPath)
		require.Equal(t, rawContent, readGzipFileForContentTest(t, compressedPath))
		require.Empty(t, preservedFilesForTest(t, filepath.Dir(oldPath)))
	})
}

func readGzipFileForContentTest(t *testing.T, path string) []byte {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	reader, err := gzip.NewReader(file)
	require.NoError(t, err)
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	return content
}

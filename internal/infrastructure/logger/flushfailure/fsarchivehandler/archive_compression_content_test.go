package fsarchivehandler

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// newContentVerifyingFailureArchivePipelineは、組み込みgzip codecで既存peerを展開し、
// 元ファイルとの内容一致まで確認するfailure archiveのpipelineを作ります。
func newContentVerifyingFailureArchivePipeline(t *testing.T, now time.Time) (*FailureArchivePipeline, <-chan error) {
	t.Helper()
	codec, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	reported := make(chan error, 32)
	pipeline.handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		select {
		case reported <- err:
		default:
		}
	}
	return pipeline, reported
}

func writeFailureArchivePeerForContentTest(t *testing.T, path string, content []byte) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	writer := gzip.NewWriter(file)
	_, writeErr := writer.Write(content)
	require.NoError(t, errors.Join(writeErr, writer.Close(), file.Close()))
}

func mismatchedFailureArchiveSourcePreservations(t *testing.T, directory string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(directory, fsstore.PreservedDirectoryName, "*.compressed-content-mismatch-*.preserved"))
	require.NoError(t, err)
	return paths
}

// requireFailureArchiveContentMismatchReportedは、内容不一致の通知が退避先を示すことを確認します。
func requireFailureArchiveContentMismatchReported(t *testing.T, reported <-chan error, preservedPath string) {
	t.Helper()
	for {
		select {
		case err := <-reported:
			if !errors.Is(err, fsstore.ErrCompressedContentMismatch) {
				continue
			}
			var preserved *processing.CompressionSourcePreservedError
			require.ErrorAs(t, err, &preserved)
			require.Equal(t, preservedPath, preserved.PreservedPath)
			return
		default:
			t.Fatal("mismatched compressed failure archive was not reported through OnError")
		}
	}
}

func TestInlineFailureArchiveFullScanVerifiesCompressedPeerContent(t *testing.T) {
	for _, peer := range []string{"matching", "different", "corrupt"} {
		t.Run(peer, func(t *testing.T) {
			now := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
			pipeline, reported := newContentVerifyingFailureArchivePipeline(t, now)
			rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
			rawContent, err := os.ReadFile(rawPath)
			require.NoError(t, err)
			compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
			switch peer {
			case "matching":
				writeFailureArchivePeerForContentTest(t, compressedPath, rawContent)
			case "different":
				// 完全なgzipですが、元ファイルとは別の内容です。
				writeFailureArchivePeerForContentTest(t, compressedPath, []byte("records that are not in the failure archive"))
			case "corrupt":
				require.NoError(t, os.WriteFile(compressedPath, []byte("not a gzip stream"), 0o600))
			}
			peerBefore, err := os.ReadFile(compressedPath)
			require.NoError(t, err)

			require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
				context.Background(),
				failurearchive.FailureCategoryRetryable,
			))

			directory := filepath.Dir(rawPath)
			require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(rawPath))
			switch peer {
			case "matching":
				// 同じ内容のpeerを再利用し、元ファイルだけを片付けます。
				require.NoFileExists(t, rawPath)
				require.Empty(t, mismatchedFailureArchiveSourcePreservations(t, directory))
			case "different":
				// 元ファイルは削除せず.preservedへ退避し、完全なpeerは削除も再圧縮もしません。
				require.NoFileExists(t, rawPath)
				preserved := mismatchedFailureArchiveSourcePreservations(t, directory)
				require.Len(t, preserved, 1)
				requireFailureArchiveContentMismatchReported(t, reported, preserved[0])
				got, err := os.ReadFile(preserved[0])
				require.NoError(t, err)
				require.Equal(t, rawContent, got)
			case "corrupt":
				// 破損peerは従来どおり削除し、元ファイルを次の圧縮へ残します。
				got, err := os.ReadFile(rawPath)
				require.NoError(t, err)
				require.Equal(t, rawContent, got)
				require.NoFileExists(t, compressedPath)
				require.Empty(t, mismatchedFailureArchiveSourcePreservations(t, directory))
				return
			}
			peerAfter, err := os.ReadFile(compressedPath)
			require.NoError(t, err)
			require.Equal(t, peerBefore, peerAfter)
		})
	}
}

func TestFailureArchiveRotationPreservesRawWhenCompressedPeerContentDiffers(t *testing.T) {
	// 受理済み処理の再試行間隔は仮想時間で進めます。
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, time.August, 30, 15, 30, 0, 0, time.UTC)
		pipeline, reported := newContentVerifyingFailureArchivePipeline(t, now)
		rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
		rawContent, err := os.ReadFile(rawPath)
		require.NoError(t, err)
		compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
		writeFailureArchivePeerForContentTest(t, compressedPath, []byte("records of another archive with the same name"))
		peerBefore, err := os.ReadFile(compressedPath)
		require.NoError(t, err)

		pipeline.handler.dispatchArchiveRotatedFile(
			context.Background(),
			pipeline.handler.Fsstore.ActiveFilePathRetryable,
			rawPath,
			failurearchive.FailureCategoryRetryable,
		)
		pipeline.handler.archiveTaskMu.Lock()
		taskDone := pipeline.handler.archiveTaskGeneration
		pipeline.handler.archiveTaskMu.Unlock()
		select {
		case <-taskDone:
		case <-time.After(time.Second):
			t.Fatal("rotation archive task did not complete")
		}

		// rotation時の圧縮でも元ファイルを削除せず退避し、内容の不一致を通知します。
		require.NoFileExists(t, rawPath)
		preserved := mismatchedFailureArchiveSourcePreservations(t, filepath.Dir(rawPath))
		require.Len(t, preserved, 1)
		requireFailureArchiveContentMismatchReported(t, reported, preserved[0])
		got, err := os.ReadFile(preserved[0])
		require.NoError(t, err)
		require.Equal(t, rawContent, got)

		// 受理済みの再開処理は同じ不一致を繰り返さず、残った完全なpeerを再利用して完了します。
		drainArchiveRecoveryForTest(t, pipeline.handler)
		for {
			select {
			case err := <-reported:
				require.NotErrorIs(t, err, fsstore.ErrCompressedContentMismatch)
				continue
			default:
			}
			break
		}
		peerAfter, err := os.ReadFile(compressedPath)
		require.NoError(t, err)
		require.Equal(t, peerBefore, peerAfter)
		require.Equal(t, preserved, mismatchedFailureArchiveSourcePreservations(t, filepath.Dir(rawPath)))
	})
}

func TestFailureArchiveRotationRemovesCorruptPeerAndRecompressesBeforeClose(t *testing.T) {
	// 受理済み処理の再試行間隔は仮想時間で進めます。
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, time.August, 30, 16, 0, 0, 0, time.UTC)
		pipeline, _ := newContentVerifyingFailureArchivePipeline(t, now)
		rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
		rawContent, err := os.ReadFile(rawPath)
		require.NoError(t, err)
		compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
		require.NoError(t, os.WriteFile(compressedPath, []byte("not a gzip stream"), 0o600))

		pipeline.handler.dispatchArchiveRotatedFile(
			context.Background(),
			pipeline.handler.Fsstore.ActiveFilePathRetryable,
			rawPath,
			failurearchive.FailureCategoryRetryable,
		)
		pipeline.handler.archiveTaskMu.Lock()
		taskDone := pipeline.handler.archiveTaskGeneration
		pipeline.handler.archiveTaskMu.Unlock()
		select {
		case <-taskDone:
		case <-time.After(time.Second):
			t.Fatal("rotation archive task did not complete")
		}
		// rotation時の圧縮は破損したpeerだけを削除し、元ファイルを次の試行へ残します。
		require.FileExists(t, rawPath)
		require.NoFileExists(t, compressedPath)

		// 受理済みの再開処理は同じ破損を繰り返さず、元ファイルから再圧縮して完了します。
		closed := make(chan error, 1)
		go func() { closed <- pipeline.handler.logFileProcessingRecovery.Close() }()
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(10 * time.Minute):
			_ = pipeline.handler.logFileProcessingRecovery.ForceClose()
			<-closed
			t.Fatal("accepted failure archive processing kept retrying the corrupt peer")
		}
		<-pipeline.handler.logFileProcessingRecovery.NotificationsDone()
		pipeline.handler.logFileProcessingRecovery = &logfile.LogFileProcessingRecovery[archiveLogFileProcessingRecoveryKey]{}
		require.NoFileExists(t, rawPath)
		file, err := os.Open(compressedPath)
		require.NoError(t, err)
		defer file.Close()
		reader, err := gzip.NewReader(file)
		require.NoError(t, err)
		recompressed, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.Equal(t, rawContent, recompressed)
		require.Empty(t, mismatchedFailureArchiveSourcePreservations(t, filepath.Dir(rawPath)))
	})
}

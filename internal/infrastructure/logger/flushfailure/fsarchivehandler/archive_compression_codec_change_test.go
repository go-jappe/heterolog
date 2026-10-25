package fsarchivehandler

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
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestFailureArchiveReusesPublishedGzipAfterZstdConfigurationChange(t *testing.T) {
	for _, operation := range []string{"full_scan", "rotation"} {
		t.Run(operation, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 16, 0, 0, 0, time.UTC)
			failureCategory := failurearchive.FailureCategoryRetryable
			// 圧縮の再利用がcheckpointへ完了を記録しないことを、宛先を持たない送信設定で確認します。
			initial := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.enableFileCompress = true
				cfg.compressFormat = pkgfsstore.CompressGzip
				cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader())
			})
			rawPath := filepath.Join(initial.handler.Fsstore.FileNameRetryable.Dir, "test-app_20260913_1.jsonl")
			const payload = "published before configuration changed"
			require.NoError(t, os.WriteFile(rawPath, []byte(payload), 0o600))
			gzipPath := rawPath + pkgfsstore.ExtensionCompressGzip
			writeFailureArchiveRecoveryGzip(t, gzipPath, payload)
			original, err := os.ReadFile(gzipPath)
			require.NoError(t, err)
			// 設定は構築後に変更できないため、圧縮形式をzstdへ変えて再起動します。
			codec := &unexpectedFailureArchiveCompressionCodec{format: pkgfsstore.CompressZstd}
			pipeline := restartTestArchiveHandler(t, initial, now, func(cfg *testArchiveConfig) {
				cfg.compressFormat, cfg.compressionCodec = pkgfsstore.CompressZstd, codec
				cfg.setArchiveUploader(failureCategory, newDestinationFailureArchiveUploader())
			})
			handler := pipeline.handler
			state := handler.directoryForFailureCategory(failureCategory)
			require.NotNil(t, state.uploadCheckpoints)
			if operation == "full_scan" {
				// 構築時のtarget-only予約を消費し、今回はdirectory全体の観測を通します。
				require.True(t, state.uploadRetry.claimForAttempt(now, testUsageCacheTTL))
				require.NoError(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory))
			} else {
				handler.dispatchArchiveRotatedFile(context.Background(), handler.Fsstore.ActiveFilePathRetryable, rawPath, failureCategory)
				waitForFailureArchiveTasksForTest(t, handler)
			}
			require.NoFileExists(t, rawPath)
			require.NoFileExists(t, rawPath+pkgfsstore.ExtensionCompressZstd)
			actual, err := os.ReadFile(gzipPath)
			require.NoError(t, err)
			require.Equal(t, original, actual)
			require.Zero(t, codec.compressCalls.Load())
			require.Zero(t, codec.verifyCalls.Load(), "old gzip must not be passed to the current zstd codec")
			checkpoint, err := state.uploadCheckpoints.Load(context.Background(), failureArchiveCompressedLogCheckpointKey(t, handler, failureCategory, gzipPath))
			require.NoError(t, err)

			require.Empty(t, checkpoint.CompletedDestinationIDs)
			assertArchivePathProtected(t, state.guard, rawPath, false)
		})
	}
}

func TestFailureArchiveRecoveryPreservesUnverifiableFilesThroughPurge(t *testing.T) {
	for _, reason := range []string{"unknown_codec", "verification_io"} {
		t.Run(reason, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 17, 0, 0, 0, time.UTC)
			codec := &unexpectedFailureArchiveCompressionCodec{format: pkgfsstore.CompressGzip, verifyErr: os.ErrPermission}
			errorsReported := make(chan error, 8)
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.enableFileCompress = true
				cfg.compressionCodec = codec
			}, InternalLogHandlers{OnError: func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				errorsReported <- err
			}})
			handler := pipeline.handler
			failureCategory := failurearchive.FailureCategoryRetryable
			rawPath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260801_1.jsonl")
			originals := map[string][]byte{rawPath: []byte("raw must survive all purge phases")}
			switch reason {
			case "unknown_codec":
				originals[rawPath+".legacy"] = []byte("unknown codec output")
			case "verification_io":
				originals[rawPath+".gz"] = []byte("unreadable compressed file")
			}
			old := now.AddDate(0, 0, -30)
			for path, contents := range originals {
				require.NoError(t, os.WriteFile(path, contents, 0o600))
				require.NoError(t, os.Chtimes(path, old, old))
			}
			// upload無効でも、回復が安全に完了していないraw/peerはage purgeへ渡しません。
			_ = handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory)
			require.NotEmpty(t, errorsReported, "unsafe recovery must notify an error")
			for path, contents := range originals {
				actual, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, contents, actual)
			}
			require.Zero(t, codec.compressCalls.Load())
			if reason == "verification_io" {
				require.EqualValues(t, 1, codec.verifyCalls.Load())
			}
		})
	}
}

func TestFailureArchiveRecoverySelectsFirstPublishedCompressedFile(t *testing.T) {
	for _, operation := range []string{"full_scan", "rotation"} {
		t.Run(operation, func(t *testing.T) {
			now := time.Date(2026, 9, 14, 17, 30, 0, 0, time.UTC)
			codec := &unexpectedFailureArchiveCompressionCodec{format: pkgfsstore.CompressZstd}
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.enableFileCompress = true
				cfg.compressionCodec = codec
			})
			handler := pipeline.handler
			failureCategory := failurearchive.FailureCategoryRetryable
			rawPath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260913_1.jsonl")
			require.NoError(t, os.WriteFile(rawPath, []byte("published archive"), 0o600))
			gzipPath := rawPath + pkgfsstore.ExtensionCompressGzip
			writeFailureArchiveRecoveryGzip(t, gzipPath, "published archive")
			gzipContents, err := os.ReadFile(gzipPath)
			require.NoError(t, err)
			zstdPath := rawPath + pkgfsstore.ExtensionCompressZstd
			const unselectedContents = "unselected file must not be verified or deleted"
			require.NoError(t, os.WriteFile(zstdPath, []byte(unselectedContents), 0o600))

			if operation == "full_scan" {
				require.NoError(t, handler.runInlineArchivePurgeIfNeeded(context.Background(), failureCategory))
			} else {
				handler.dispatchArchiveRotatedFile(context.Background(), handler.Fsstore.ActiveFilePathRetryable, rawPath, failureCategory)
				waitForFailureArchiveTasksForTest(t, handler)
			}
			require.NoFileExists(t, rawPath)
			actual, err := os.ReadFile(gzipPath)
			require.NoError(t, err)
			require.Equal(t, gzipContents, actual)
			actual, err = os.ReadFile(zstdPath)
			require.NoError(t, err)
			require.Equal(t, unselectedContents, string(actual))
			require.Zero(t, codec.compressCalls.Load())
			require.Zero(t, codec.verifyCalls.Load())
		})
	}
}

// codec変更前に公開済みの圧縮fileを再利用するrotationは、送信と削除が終わるまで実際の旧codecのpathを保護します。
func TestFailureArchiveUploadProtectsActualOldCodecPath(t *testing.T) {
	now := time.Date(2026, 9, 14, 18, 0, 0, 0, time.UTC)
	codec := &unexpectedFailureArchiveCompressionCodec{format: pkgfsstore.CompressZstd}
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	remote := newRecordingFailureArchiveUploader(release, nil)
	failureCategory := failurearchive.FailureCategoryRetryable
	pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.enableFileCompress = true
		cfg.compressionCodec = codec
		cfg.setArchiveUploader(failureCategory, remote)
	})
	handler := pipeline.handler
	state := handler.directoryForFailureCategory(failureCategory)
	require.NotNil(t, state.uploadCheckpoints)
	rawPath := filepath.Join(handler.Fsstore.FileNameRetryable.Dir, "test-app_20260913_1.jsonl")
	gzipPath := rawPath + pkgfsstore.ExtensionCompressGzip
	require.NoError(t, os.WriteFile(rawPath, []byte("retained archive"), 0o600))
	writeFailureArchiveRecoveryGzip(t, gzipPath, "retained archive")
	handler.dispatchArchiveRotatedFile(context.Background(), handler.Fsstore.ActiveFilePathRetryable, rawPath, failureCategory)
	require.Equal(t, gzipPath, receiveArchiveUploadInput(t, remote).FilePath())
	require.NoFileExists(t, rawPath)
	assertArchivePathProtected(t, state.guard, rawPath, true)
	assertArchivePathProtected(t, state.guard, gzipPath, true)
	require.Empty(t, handler.archiveFileInfosExcludingProtected([]fsstore.FileInfo{{Path: gzipPath}}, failureCategory))
	require.Zero(t, codec.compressCalls.Load())
	close(release)
	waitForFailureArchiveTasksForTest(t, handler)
	require.NoFileExists(t, gzipPath)
	assertArchivePathProtected(t, state.guard, rawPath, false)
	assertArchivePathProtected(t, state.guard, gzipPath, false)
}

type unexpectedFailureArchiveCompressionCodec struct {
	format        pkgfsstore.CompressFormat
	verifyErr     error
	compressCalls atomic.Int32
	verifyCalls   atomic.Int32
}

func (c *unexpectedFailureArchiveCompressionCodec) Format() pkgfsstore.CompressFormat {
	return c.format
}
func (c *unexpectedFailureArchiveCompressionCodec) Compress(*os.File, *os.File) error {
	c.compressCalls.Add(1)
	return errors.New("unexpected recompression")
}
func (c *unexpectedFailureArchiveCompressionCodec) VerifyCompressedIntegrity(*os.File) error {
	c.verifyCalls.Add(1)
	if c.verifyErr != nil {
		return c.verifyErr
	}
	return errors.New("unexpected verification with the current codec")
}

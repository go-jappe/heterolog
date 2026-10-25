package fsarchivehandler

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"

	"github.com/stretchr/testify/require"
)

var errFailureArchiveRecoveryCompression = errors.New("failure archive recovery compression failed")

type failureArchiveRecoveryCodec struct {
	failCompression atomic.Bool
	compressCalls   atomic.Int32
	verifyCalls     atomic.Int32
}

func (*failureArchiveRecoveryCodec) Format() pkgfsstore.CompressFormat {
	return pkgfsstore.CompressGzip
}

func (c *failureArchiveRecoveryCodec) Compress(in, out *os.File) error {
	c.compressCalls.Add(1)
	if c.failCompression.Load() {
		return errFailureArchiveRecoveryCompression
	}
	writer := gzip.NewWriter(out)
	if _, err := io.Copy(writer, in); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func (c *failureArchiveRecoveryCodec) VerifyCompressedIntegrity(in *os.File) error {
	c.verifyCalls.Add(1)
	codec, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	if err != nil {
		return err
	}
	return codec.VerifyCompressedIntegrity(in)
}

type errorThenPanicArchiveCodec struct {
	pkgfsstore.CompressionCodec
	calls      int
	firstError error
	panicValue string
}

func (c *errorThenPanicArchiveCodec) Compress(*os.File, *os.File) error {
	c.calls++
	if c.calls == 1 {
		return c.firstError
	}
	panic(c.panicValue)
}

func TestInlineFailureArchiveCompressionErrorSurvivesLaterCandidatePanic(t *testing.T) {
	base, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
	require.NoError(t, err)
	firstError := errors.New("first archive compression failed")
	const panicValue = "second archive compression panic"
	codec := &errorThenPanicArchiveCodec{CompressionCodec: base, firstError: firstError, panicValue: panicValue}
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC), codec)
	firstPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
	secondPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 2)
	handler := pipeline.handler
	state := &handler.retryableDirectory
	var reported int
	handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
		if !errors.Is(err, firstError) {
			t.Errorf("unexpected compression notification: %v", err)
			return
		}
		reported++
		// 後続候補のpanicで排他区間を抜けても、先行した失敗の通知を失いません。
		if codec.calls != 2 {
			t.Errorf("compression calls during notification = %d, want 2", codec.calls)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := state.guard.RunWithLifecycleLockContext(ctx, func() error { return nil }); err != nil {
			t.Errorf("compression failure notified before releasing lifecycle lock: %v", err)
		}
	}
	require.PanicsWithValue(t, panicValue, func() {
		_ = handler.runInlineArchivePurgeIfNeeded(context.Background(), failurearchive.FailureCategoryRetryable)
	})
	require.Equal(t, 1, reported)
	require.Equal(t, 2, codec.calls)
	for _, path := range []string{firstPath, secondPath} {
		require.FileExists(t, path)
		assertArchivePathProtected(t, state.guard, path, false)
	}
}

func TestInlineFailureArchiveFirstFullScanRecoversInactiveRawAfterRestart(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)

	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))

	require.NoFileExists(t, rawPath)
	require.FileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(rawPath))
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFailureArchiveFullScanRetriesCompressionFailureAfterCacheTTL(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 30, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	codec.failCompression.Store(true)
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)

	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))
	require.FileExists(t, rawPath)
	require.NoFileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 1, codec.compressCalls.Load())

	codec.failCompression.Store(false)
	pipeline.handler.Clock = archiveFixedClock{now: now.Add(testUsageCacheTTL)}
	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))

	require.NoFileExists(t, rawPath)
	require.FileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 2, codec.compressCalls.Load())
}

func TestFailureArchivePostRotationObservationDefersFailedSourceUntilNextNaturalScan(
	t *testing.T,
) {
	// 受理済み圧縮の再試行間隔は仮想時間で進めます。
	synctest.Test(t, func(t *testing.T) {
		now := time.Date(2026, time.August, 30, 12, 40, 0, 0, time.UTC)
		codec := &failureArchiveRecoveryCodec{}
		codec.failCompression.Store(true)
		pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
		rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)

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

		// rotation本体で一度失敗したsourceは、同じtaskのpost-observationでは保護だけされ、
		// 即時に同じ圧縮を繰り返しません。
		require.FileExists(t, rawPath)
		require.NoFileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
		require.EqualValues(t, 1, codec.compressCalls.Load())

		codec.failCompression.Store(false)
		pipeline.handler.Clock = archiveFixedClock{now: now.Add(testUsageCacheTTL)}
		drainArchiveRecoveryForTest(t, pipeline.handler)
		require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
			context.Background(),
			failurearchive.FailureCategoryRetryable,
		))
		require.NoFileExists(t, rawPath)
		require.FileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
		require.EqualValues(t, 2, codec.compressCalls.Load())
	})
}

func TestFailureArchivePostRotationObservationStillRecoversOtherRestartRaw(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 42, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	deferredRaw := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
	restartRaw := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 2)

	require.NoError(
		t,
		pipeline.handler.runInlineArchivePurgeWithOptions(
			context.Background(),
			failurearchive.FailureCategoryRetryable,
			false,
			deferredRaw,
		),
	)

	require.FileExists(t, deferredRaw)
	require.NoFileExists(t, deferredRaw+pkgfsstore.ExtensionCompressGzip)
	require.NoFileExists(t, restartRaw)
	require.FileExists(t, restartRaw+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFailureArchiveCompressionFailureProtectsRawFromSameScanPurge(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 45, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	codec.failCompression.Store(true)
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260801", 1)
	old := now.AddDate(0, 0, -30)
	require.NoError(t, os.Chtimes(rawPath, old, old))

	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))

	// rawはage purge対象になる古さですが、正常圧縮が完了していない同じscanでは
	// lifecycle保護され、次回の再圧縮に残ります。
	require.FileExists(t, rawPath)
	require.NoFileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFailureArchiveFullScanRetiresSourceWhenValidPeerExists(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	// 圧縮先の公開後、元ログの削除前に停止した状態です。peerは同じ内容の正しい圧縮ファイルです。
	rawContent, err := os.ReadFile(rawPath)
	require.NoError(t, err)
	writeFailureArchiveRecoveryGzip(t, compressedPath, string(rawContent))

	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))

	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.NoFileExists(t, logpattern.CompressionSourceDeletePendingPath(rawPath))
	require.Zero(t, codec.compressCalls.Load(), "a verified peer must not be overwritten")
	require.EqualValues(t, 1, codec.verifyCalls.Load())
	require.Equal(t, rawContent, readFailureArchiveRecoveryGzip(t, compressedPath), "the retained peer must hold the retired source content")
}

func TestInlineFailureArchiveFullScanDoesNotSkipRawForInvalidPeer(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 30, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip
	require.NoError(t, os.WriteFile(compressedPath, []byte("not a gzip stream"), 0o600))

	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))
	require.FileExists(t, rawPath)
	require.NoFileExists(t, compressedPath)
	require.Zero(t, codec.compressCalls.Load())

	// invalid peerの削除でbaselineは無効化されるため、次の自然なcleanupがrawを再圧縮します。
	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFailureArchiveDoesNotRecompressAfterCommittedArchiveIsExternallyDeleted(
	t *testing.T,
) {
	now := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
	compressedPath := rawPath + pkgfsstore.ExtensionCompressGzip

	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))
	require.NoFileExists(t, rawPath)
	require.FileExists(t, compressedPath)
	require.NoError(t, os.Remove(compressedPath))

	pipeline.handler.Clock = archiveFixedClock{now: now.Add(testUsageCacheTTL)}
	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))
	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, compressedPath)
	require.EqualValues(t, 1, codec.compressCalls.Load())
}

func TestInlineFailureArchivePendingRetiredSourceIsNotRecompressedWhenPeerWasDeleted(
	t *testing.T,
) {
	now := time.Date(2026, time.August, 30, 14, 30, 0, 0, time.UTC)
	codec := &failureArchiveRecoveryCodec{}
	pipeline := newInlineFailureArchiveRecoveryPipeline(t, now, codec)
	rawPath := writeFailureArchiveRecoveryRaw(t, pipeline, "20260829", 1)
	pendingPath := logpattern.CompressionSourceDeletePendingPath(rawPath)
	require.NoError(t, os.Rename(rawPath, pendingPath))

	// source retirement後にcompressed peerだけが外部削除された状態を再現します。
	require.NoError(t, pipeline.handler.runInlineArchivePurgeIfNeeded(
		context.Background(),
		failurearchive.FailureCategoryRetryable,
	))

	require.NoFileExists(t, rawPath)
	require.NoFileExists(t, pendingPath)
	require.NoFileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
	require.Zero(t, codec.compressCalls.Load())
}

func TestInlineFailureArchivePendingSourceConflictReportsAndPreservesSource(t *testing.T) {
	for _, failureCategory := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable,
		failurearchive.FailureCategoryPermanent,
	} {
		t.Run(string(failureCategory), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC)
			codec := &failureArchiveRecoveryCodec{}
			var reported []error
			pipeline := newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
				cfg.enableFileCompress = true
				cfg.compressionCodec = codec
			}, InternalLogHandlers{OnError: func(_ context.Context, err error, _ *logging.LogEntry, _ observer.Tracer, _ observer.Meter) {
				reported = append(reported, err)
			}})
			fileName, err := pipeline.handler.archiveFileNamePattern(failureCategory)
			require.NoError(t, err)
			rawPath := filepath.Join(fileName.Dir, "test-app_20260909_1.jsonl")
			rawContent := []byte("canonical failure archive")
			pendingContent := []byte("different pending failure archive must remain recoverable")
			pendingPath := logpattern.CompressionSourceDeletePendingPath(rawPath)
			require.NoError(t, os.WriteFile(rawPath, rawContent, 0o600))
			require.NoError(t, os.WriteFile(pendingPath, pendingContent, 0o600))
			old := now.AddDate(0, 0, -30)
			require.NoError(t, os.Chtimes(pendingPath, old, old))
			result := retryableArchiveResult(now, "pending-conflict")
			if failureCategory == failurearchive.FailureCategoryPermanent {
				result.DeadRecords = result.RetryableRecords
				result.RetryableRecords = nil
			}
			err = pipeline.HandleFailure(ctx, result)
			require.ErrorIs(t, err, appendlog.ErrCompressionSourceDeletePendingConflict)
			require.Len(t, reported, 1)
			require.ErrorIs(t, reported[0], appendlog.ErrCompressionSourceDeletePendingConflict)
			preserved, err := filepath.Glob(filepath.Join(fileName.Dir, fsstore.PreservedDirectoryName, "*.preserved"))
			require.NoError(t, err)
			require.Len(t, preserved, 1)
			var conflict *appendlog.CompressionSourceDeletePendingConflictError
			require.ErrorAs(t, reported[0], &conflict)
			require.True(t, conflict.ContentsDiffer)
			require.Equal(t, preserved[0], conflict.PreservedPath)
			require.NoFileExists(t, pendingPath)
			gotRaw, err := os.ReadFile(rawPath)
			require.NoError(t, err)
			require.Equal(t, rawContent, gotRaw)
			gotPending, err := os.ReadFile(preserved[0])
			require.NoError(t, err)
			require.Equal(t, pendingContent, gotPending)
			require.Zero(t, codec.compressCalls.Load(), "the conflict must abort this full scan")

			// 公開入口の次の通常要求でraw回復を再開し、同じ競合通知は繰り返さない。
			require.NoError(t, pipeline.HandleFailure(ctx, result))
			require.NoFileExists(t, rawPath)
			require.FileExists(t, rawPath+pkgfsstore.ExtensionCompressGzip)
			require.EqualValues(t, 1, codec.compressCalls.Load())
			pipeline.handler.Clock = archiveFixedClock{now: now.AddDate(0, 0, 2)}
			require.NoError(t, pipeline.HandleFailure(ctx, result))
			require.NoError(t, shutdownFailureArchiveForTest(pipeline))
			gotPending, err = os.ReadFile(preserved[0])
			require.NoError(t, err)
			require.Equal(t, pendingContent, gotPending)
			require.Len(t, reported, 1)
		})
	}
}

func newInlineFailureArchiveRecoveryPipeline(
	t *testing.T,
	now time.Time,
	codec pkgfsstore.CompressionCodec,
) *FailureArchivePipeline {
	t.Helper()
	return newTestArchiveHandler(t, now, func(cfg *testArchiveConfig) {
		cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
		cfg.enableFileCompress = true
		cfg.compressFormat = codec.Format()
		cfg.compressionCodec = codec
	})
}

func writeFailureArchiveRecoveryRaw(
	t *testing.T,
	pipeline *FailureArchivePipeline,
	period string,
	index uint64,
) string {
	t.Helper()
	fileName := pipeline.handler.Fsstore.FileNameRetryable
	path := filepath.Join(
		fileName.Dir,
		fmt.Sprintf(
			"%s%s_%s_%d.%s",
			fileName.Prefix,
			fileName.Target,
			period,
			index,
			fileName.Extension,
		),
	)
	require.NoError(t, os.WriteFile(path, []byte("inactive failure archive"), 0o600))
	return path
}

func writeFailureArchiveRecoveryGzip(t *testing.T, path string, content string) {
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

func readFailureArchiveRecoveryGzip(t *testing.T, path string) []byte {
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

var _ pkgfsstore.CompressionCodec = (*failureArchiveRecoveryCodec)(nil)

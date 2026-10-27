package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

type firstPanicArchiveCodec struct {
	pkgfsstore.CompressionCodec
	calls atomic.Int32
}

const (
	firstArchiveCompressionPanic = "one-time accepted compression panic"
	firstArchiveUploadPanic      = "one-time accepted upload panic"
)

func (c *firstPanicArchiveCodec) Compress(source, destination *os.File) error {
	if c.calls.Add(1) == 1 {
		panic(firstArchiveCompressionPanic)
	}
	return c.CompressionCodec.Compress(source, destination)
}

type firstPanicArchiveUploader struct {
	panicFirst bool
	calls      atomic.Int32
}

func (u *firstPanicArchiveUploader) Upload(_ context.Context, request uploader.UploadRequest) error {
	if u.calls.Add(1) == 1 && u.panicFirst {
		panic(firstArchiveUploadPanic)
	}
	_, err := os.Stat(request.FilePath())
	return err
}

func TestFailureArchiveShutdownDrainsInitialRotationPanic(t *testing.T) {
	for _, category := range []failurearchive.FailureCategory{
		failurearchive.FailureCategoryRetryable, failurearchive.FailureCategoryPermanent,
	} {
		for _, stage := range []string{"compression", "compressed-upload", "raw-upload"} {
			t.Run(string(category)+"/"+stage, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					delegate, err := fsstore.ResolveCompressionCodec(pkgfsstore.CompressGzip, nil)
					require.NoError(t, err)
					codec := &firstPanicArchiveCodec{CompressionCodec: delegate}
					remote := &firstPanicArchiveUploader{panicFirst: stage != "compression"}
					pipeline := newTestArchiveHandler(t, time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
						cfg.lifecycleMode = pkgfsstore.FileLifecycleModeInline
						cfg.enableFileCompress = stage != "raw-upload"
						cfg.compressFormat = pkgfsstore.CompressGzip
						if stage == "compression" {
							cfg.compressionCodec = codec
						}
						cfg.setArchiveUploader(category, remote)
					})
					reported := make(chan error, 1)
					pipeline.handler.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
						var internalErr *FailureArchiveInternalError
						if errors.As(err, &internalErr) && internalErr.Op == OperationRotationArchive {
							select {
							case reported <- err:
							default:
							}
						}
					}
					fileName, err := pipeline.handler.archiveFileNamePattern(category)
					require.NoError(t, err)
					sourcePath := filepath.Join(fileName.Dir, "test-app_20261006_1.jsonl")
					require.NoError(t, os.WriteFile(sourcePath, []byte("accepted before shutdown\n"), 0o600))
					pipeline.handler.dispatchArchiveRotatedFile(context.Background(), "active-file", sourcePath, category)
					select {
					case err := <-reported:
						wantPanic := firstArchiveUploadPanic
						if stage == "compression" {
							wantPanic = firstArchiveCompressionPanic
						}
						var panicErr *ArchiveTaskPanicError
						require.ErrorAs(t, err, &panicErr)
						require.Equal(t, wantPanic, panicErr.Value)
					case <-time.After(time.Second):
						t.Fatal("initial rotation panic was not reported")
					}
					require.Contains(t, pipeline.handler.protectedArchivePaths(category), sourcePath)
					pendingPath := sourcePath
					if stage == "compressed-upload" {
						pendingPath += ".gz"
					}
					require.FileExists(t, pendingPath)
					// 通常writeや再走査を行わず、初回panicした受理済み処理だけを終了します。
					require.NoError(t, pipeline.Shutdown(context.Background()))
					if stage == "compression" {
						require.EqualValues(t, 2, codec.calls.Load())
						require.EqualValues(t, 1, remote.calls.Load())
					} else {
						require.EqualValues(t, 2, remote.calls.Load())
					}
					if stage == "raw-upload" {
						require.FileExists(t, sourcePath)
					} else {
						require.NoFileExists(t, sourcePath)
						require.NoFileExists(t, sourcePath+".gz")
					}
					require.NotContains(t, pipeline.handler.protectedArchivePaths(category), sourcePath)
				})
			})
		}
	}
}

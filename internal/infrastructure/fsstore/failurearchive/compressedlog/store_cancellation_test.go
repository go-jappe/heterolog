package compressedlogarchive

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/stretchr/testify/require"
)

func TestArchiveCopyCancellationStopsBeforeReadOrWrite(t *testing.T) {
	t.Parallel()
	for _, cancelAt := range []string{"before read", "after read", "after write"} {
		t.Run(cancelAt, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cancelAt == "before read" {
				cancel()
			}
			readCalls, writeCalls := 0, 0
			reader := archiveCopyReader(func(buffer []byte) (int, error) {
				readCalls++
				if cancelAt == "after read" {
					cancel()
				}
				return copy(buffer, "payload"), io.EOF
			})
			writer := archiveCopyWriter(func(buffer []byte) (int, error) {
				writeCalls++
				if cancelAt == "after write" {
					cancel()
				}
				return len(buffer), nil
			})
			written, err := copyWithContext(ctx, writer, reader)
			require.ErrorIs(t, err, context.Canceled)
			switch cancelAt {
			case "before read":
				require.Zero(t, readCalls)
				require.Zero(t, writeCalls)
				require.Zero(t, written)
			case "after read":
				require.Equal(t, 1, readCalls)
				require.Zero(t, writeCalls)
				require.Zero(t, written)
			case "after write":
				require.Equal(t, 1, readCalls)
				require.Equal(t, 1, writeCalls)
				require.Equal(t, int64(len("payload")), written)
			}
		})
	}
}

func TestArchivePublishCanceledDuringCopyCleansOnlyItsTemporaryFileAndCanRetry(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, pkglogger.LoggerContractSync)
	require.NoError(t, store.EnsureDirectories(context.Background()))
	previousSource := writeTestCompressedLog(t, "previous.log.gz", []byte("previous successful archive"))
	previous, err := store.Save(context.Background(), testSaveInput(previousSource, uploader.FailureCategoryPermanent))
	require.NoError(t, err)

	body := bytes.Repeat([]byte("retained source "), 32*1024)
	source := writeTestCompressedLog(t, "cancel-copy.log.gz", body)
	input := testSaveInput(source, uploader.FailureCategoryRetryable)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	copyContext := archiveCancelOnStateContext{Context: ctx, cancel: cancel, ready: func() bool {
		entries, err := os.ReadDir(store.Root().RetryableDir())
		require.NoError(t, err)
		for _, entry := range entries {
			info, err := entry.Info()
			require.NoError(t, err)
			if info.Size() > 0 {
				return true
			}
		}
		return false
	}}

	_, err = store.Save(copyContext, input)
	require.ErrorIs(t, err, context.Canceled)
	assertNoArchiveTemporaryFiles(t, store)
	assertDirectoryEmpty(t, store.Root().RetryableDir())
	assertFileBytes(t, source, body)
	assertFileBytes(t, previous.Path, []byte("previous successful archive"))

	archived, err := store.Save(context.Background(), input)
	require.NoError(t, err)
	assertFileBytes(t, archived.Path, body)
	assertNoArchiveTemporaryFiles(t, store)
}

func TestArchiveCancellationAfterPublicationPreservesFinalForRetry(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, pkglogger.LoggerContractSync)
	body := []byte("completed bytes must survive cancellation before directory sync")
	source := writeTestCompressedLog(t, "published.log.gz", body)
	input := testSaveInput(source, uploader.FailureCategoryRetryable)
	finalPath := filepath.Join(store.Root().RetryableDir(), filepath.Base(source))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publicationContext := archiveCancelOnStateContext{Context: ctx, cancel: cancel, ready: func() bool {
		_, err := os.Lstat(finalPath)
		return err == nil
	}}

	_, err := store.Save(publicationContext, input)
	require.ErrorIs(t, err, context.Canceled)
	assertFileBytes(t, finalPath, body)
	assertFileBytes(t, source, body)
	assertNoArchiveTemporaryFiles(t, store)
	beforeRetry, err := os.Stat(finalPath)
	require.NoError(t, err)

	archived, err := store.Save(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, finalPath, archived.Path)
	afterRetry, err := os.Stat(finalPath)
	require.NoError(t, err)
	require.True(t, os.SameFile(beforeRetry, afterRetry), "retry must reuse the already published file")
	assertFileBytes(t, finalPath, body)
}

func TestArchivePublishCancellationWhileWaitingForStoreLocks(t *testing.T) {
	t.Parallel()
	for _, lockName := range []string{"operation", "layout"} {
		t.Run(lockName, func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			source := writeTestCompressedLog(t, "waiting.log.gz", []byte("source remains"))
			input := testSaveInput(source, uploader.FailureCategoryRetryable)
			mutex := &store.operationMu
			if lockName == "layout" {
				mutex = &store.directorySetupMu
			}
			mutex.Lock()
			unlock := sync.OnceFunc(mutex.Unlock)
			defer unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			returned := make(chan error, 1)
			go func() {
				_, err := store.Save(ctx, input)
				returned <- err
			}()
			select {
			case err := <-returned:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(2 * time.Second):
				t.Fatal("Save did not stop waiting for the held store lock")
			}
			_, err := os.Stat(store.Root().Path())
			require.ErrorIs(t, err, os.ErrNotExist)
			assertFileBytes(t, source, []byte("source remains"))
			unlock()
			// nil contextは既存のBackground相当の契約を維持します。
			_, err = store.Save(nil, input)
			require.NoError(t, err)
		})
	}
}

func TestArchiveTemporaryCleanupReturnsRemovalFailure(t *testing.T) {
	t.Parallel()
	temporaryPath := filepath.Join(t.TempDir(), "temporary.tmp")
	require.NoError(t, os.Mkdir(temporaryPath, 0o700))
	child := filepath.Join(temporaryPath, "retained")
	require.NoError(t, os.WriteFile(child, []byte("do not remove"), 0o600))
	err := removeTemporaryCompressedLog(temporaryPath)
	var pathErr *os.PathError
	require.ErrorAs(t, err, &pathErr)
	require.Equal(t, temporaryPath, pathErr.Path)
	assertFileBytes(t, child, []byte("do not remove"))
}

type archiveCopyReader func([]byte) (int, error)

func (read archiveCopyReader) Read(buffer []byte) (int, error) { return read(buffer) }

type archiveCopyWriter func([]byte) (int, error)

func (write archiveCopyWriter) Write(buffer []byte) (int, error) { return write(buffer) }

// 実ファイルの処理段階でcancelし、時間やcopy速度に依存せず境界を確認します。
type archiveCancelOnStateContext struct {
	context.Context
	cancel context.CancelFunc
	ready  func() bool
}

func (ctx archiveCancelOnStateContext) Err() error {
	if ctx.ready() {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

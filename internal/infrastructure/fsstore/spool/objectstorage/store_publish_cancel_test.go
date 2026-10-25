package objectstoragespool

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStorePublishCancellationRemovesOnlyItsUnpublishedStagingFile(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		name := "retryable"
		if permanent {
			name = "permanent"
		}
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.EnsureLayout(context.Background()); err != nil {
				t.Fatal(err)
			}
			body := strings.Repeat("compressed bytes", 32*1024)
			source := writeTestSource(t, t.TempDir(), "app_20260915_1.log.gz", body)
			otherStaging := filepath.Join(store.Root().stagingDir(), stagingFilePrefix+"previous"+stagingFileSuffix)
			if err := os.WriteFile(otherStaging, []byte("other worker"), payloadMode); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var partialSize int64
			observed := &publishObservedContext{Context: ctx, beforeErr: func() {
				entries, err := os.ReadDir(store.Root().stagingDir())
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if entry.Name() == filepath.Base(otherStaging) {
						continue
					}
					// Windowsのdirectory entryでは書き込み中のサイズが古い場合が
					// あるため、開いたhandleから現在のコピー量を取得します。
					file, err := os.Open(filepath.Join(store.Root().stagingDir(), entry.Name()))
					if err != nil {
						t.Fatal(err)
					}
					info, statErr := file.Stat()
					closeErr := file.Close()
					if err := errors.Join(statErr, closeErr); err != nil {
						t.Fatal(err)
					}
					if info.Size() > 0 {
						partialSize = info.Size()
						cancel()
					}
				}
			}}
			publish := store.SaveRetryableFailure
			if permanent {
				publish = store.SavePermanentFailure
			}
			if _, err := publish(observed, source); !errors.Is(err, context.Canceled) {
				t.Fatalf("Publish() error = %v, want context.Canceled", err)
			}
			if partialSize <= 0 || partialSize >= int64(len(body)) {
				t.Fatalf("cancelled staged bytes = %d, want a partial copy of %d bytes", partialSize, len(body))
			}
			entries, err := os.ReadDir(store.Root().stagingDir())
			if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(otherStaging) {
				t.Fatalf("staging entries after cancellation = %v, error = %v, want only other worker's file", entries, err)
			}
			if saved, err := os.ReadFile(otherStaging); err != nil || string(saved) != "other worker" {
				t.Fatalf("other staging file = %q, error = %v", saved, err)
			}
			assertDirectoryEmpty(t, store.Root().ActiveDir())
			assertDirectoryEmpty(t, store.Root().DeadDir())
			if raw, err := os.ReadFile(source); err != nil || string(raw) != body {
				t.Fatalf("source changed by cancellation: bytes = %d, error = %v", len(raw), err)
			}
			published, err := publish(context.Background(), source)
			if err != nil {
				t.Fatalf("retry Publish(): %v", err)
			}
			if saved, err := os.ReadFile(published.Path); err != nil || string(saved) != body {
				t.Fatalf("retry published bytes = %d, error = %v", len(saved), err)
			}
		})
	}
}

func TestStoreCanceledPublishKeepsExistingPublishedFileForRetry(t *testing.T) {
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "app_20260915_2.log.gz", "already stored")
	published, err := store.SaveRetryableFailure(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.SavePermanentFailure(ctx, source); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled SavePermanentFailure() error = %v", err)
	}
	if saved, err := os.ReadFile(published.Path); err != nil || string(saved) != "already stored" {
		t.Fatalf("published file changed by cancellation: %q, %v", saved, err)
	}
	retried, err := store.SavePermanentFailure(context.Background(), source)
	if err != nil || !reflect.DeepEqual(retried, published) {
		t.Fatalf("retry SavePermanentFailure() = %+v, %v, want existing %+v", retried, err, published)
	}
	assertDirectoryEmpty(t, store.Root().DeadDir())
	assertDirectoryEmpty(t, store.Root().stagingDir())
}

func TestStoreCanceledPublishAfterRenameKeepsPublishedFileForRetry(t *testing.T) {
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "app_20260915_5.log.gz", "published before cancellation")
	finalPath := filepath.Join(store.Root().ActiveDir(), filepath.Base(source))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &publishObservedContext{Context: ctx, beforeErr: func() {
		if _, err := os.Lstat(finalPath); err == nil {
			cancel()
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}}
	if _, err := store.SaveRetryableFailure(observed, source); !errors.Is(err, context.Canceled) {
		t.Fatalf("SaveRetryableFailure() error = %v, want cancellation after rename", err)
	}
	if saved, err := os.ReadFile(finalPath); err != nil || string(saved) != "published before cancellation" {
		t.Fatalf("final file after cancellation = %q, %v", saved, err)
	}
	beforeRetry, err := os.Stat(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	assertDirectoryEmpty(t, store.Root().stagingDir())
	if saved, err := os.ReadFile(source); err != nil || string(saved) != "published before cancellation" {
		t.Fatalf("source after cancellation = %q, %v", saved, err)
	}
	retried, err := store.SaveRetryableFailure(context.Background(), source)
	if err != nil || retried.Path != finalPath {
		t.Fatalf("retry SaveRetryableFailure() = %+v, %v, want %q", retried, err, finalPath)
	}
	afterRetry, err := os.Stat(finalPath)
	if err != nil || !os.SameFile(beforeRetry, afterRetry) {
		t.Fatalf("retry replaced the already published file: %v", err)
	}
}

func TestStorePublishCanCancelWhileWaitingForLocks(t *testing.T) {
	for _, lockName := range []string{"operation", "layout"} {
		t.Run(lockName, func(t *testing.T) {
			store := newTestStore(t)
			source := writeTestSource(t, t.TempDir(), "app_20260915_3.log.gz", "source")
			lock := &store.operationMu
			if lockName == "layout" {
				lock = &store.layoutMu
			}
			lock.Lock()
			defer lock.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waiting := make(chan struct{})
			observed := &publishWaitingContext{Context: ctx, waiting: waiting}
			result := make(chan error, 1)
			go func() {
				_, err := store.SaveRetryableFailure(observed, source)
				result <- err
			}()
			select {
			case <-waiting:
			case <-time.After(3 * time.Second):
				t.Fatal("Publish did not reach the cancellable lock wait")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Publish() error = %v, want context.Canceled", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Publish waited for the held lock after cancellation")
			}
		})
	}
}

func TestStorePublishRejectsNilContext(t *testing.T) {
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "app_20260915_4.log.gz", "source")
	if _, err := store.SaveRetryableFailure(nil, source); !errors.Is(err, ErrNilContext) {
		t.Fatalf("SaveRetryableFailure(nil) error = %v, want %v", err, ErrNilContext)
	}
}

func TestCopyWithContextDoesNotWriteBytesReadAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &cancelAfterRead{Reader: strings.NewReader("not written"), cancel: cancel}
	var destination bytes.Buffer
	written, err := copyWithContext(ctx, &destination, source)
	if !errors.Is(err, context.Canceled) || written != 0 || destination.Len() != 0 {
		t.Fatalf("copyWithContext() = %d, %v, destination = %q", written, err, destination.String())
	}
}

func TestCopyWithContextReportsCancellationDuringLastWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := spoolCopyReaderFunc(func(buffer []byte) (int, error) {
		return copy(buffer, "last bytes"), io.EOF
	})
	var destination bytes.Buffer
	writer := spoolCopyWriterFunc(func(buffer []byte) (int, error) {
		n, err := destination.Write(buffer)
		cancel()
		return n, err
	})
	written, err := copyWithContext(ctx, writer, source)
	if !errors.Is(err, context.Canceled) || written != int64(len("last bytes")) {
		t.Fatalf("copyWithContext() = %d, %v, want cancellation after final write", written, err)
	}
}

// publishObservedContextは実際のcopy進捗を見てキャンセルを発生させます。
// syscallを差し替えず、sleepやファイルサイズ依存のタイミング競争も使いません。
type publishObservedContext struct {
	context.Context
	mu        sync.Mutex
	beforeErr func()
}

func (c *publishObservedContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.beforeErr()
	return c.Context.Err()
}

type cancelAfterRead struct {
	io.Reader
	cancel context.CancelFunc
}

// Doneはキャンセル対応lockの取得を開始したときに参照されます。
// 取得が実際に待ち列へ入ったかどうかは、この通知だけでは判定しません。
type publishWaitingContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *publishWaitingContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func (r *cancelAfterRead) Read(buffer []byte) (int, error) {
	n, err := r.Reader.Read(buffer)
	r.cancel()
	return n, err
}

type spoolCopyReaderFunc func([]byte) (int, error)

func (f spoolCopyReaderFunc) Read(buffer []byte) (int, error) { return f(buffer) }

type spoolCopyWriterFunc func([]byte) (int, error)

func (f spoolCopyWriterFunc) Write(buffer []byte) (int, error) { return f(buffer) }

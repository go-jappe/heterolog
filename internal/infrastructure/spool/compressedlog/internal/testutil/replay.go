// Package replaytest provides fixtures shared by compressed log runner and worker tests.
package replaytest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	infras3 "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage/s3"
	"github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// Uploaderは成功・失敗・停止待ちを再現し、呼び出し回数と最後の送信パスを記録します。
// フィールドは開始前に設定します。開始後の変更にはSetError・SetPanicを使います。
type Uploader struct {
	Err           error
	PanicValue    any
	Block         <-chan struct{}
	Ready         chan<- struct{}
	IgnoreContext bool
	DestinationID objectstorage.DestinationID

	mu        sync.Mutex
	calls     int
	lastInput uploader.UploadRequest
}

func (*Uploader) ClassifyRetry(ctx context.Context, err error) retryable.RetryDecision {
	return infras3.ClassifyRetry(ctx, err)
}

func (u *Uploader) Upload(ctx context.Context, input uploader.UploadRequest) error {
	u.mu.Lock()
	u.calls++
	u.lastInput = input
	ready, block, ignoreContext := u.Ready, u.Block, u.IgnoreContext
	err, panicValue := u.Err, u.PanicValue
	u.mu.Unlock()
	if panicValue != nil {
		panic(panicValue)
	}
	if ready != nil {
		select {
		case ready <- struct{}{}:
		default:
		}
	}
	if block != nil {
		if ignoreContext {
			<-block
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-block:
			}
		}
	}
	return err
}

func (u *Uploader) DestinationIDs() []objectstorage.DestinationID {
	if u.DestinationID == "" {
		return []objectstorage.DestinationID{"s3"}
	}
	return []objectstorage.DestinationID{u.DestinationID}
}

func (u *Uploader) UploadDestinations(ctx context.Context, input uploader.UploadRequest, ids []objectstorage.DestinationID) uploader.UploadReport {
	err := u.Upload(ctx, input)
	var category uploader.FailureCategory
	if err != nil {
		category = infras3.ClassifyDurableUploadFailure(ctx, err).FailureCategory
	}
	results := make([]uploader.DestinationResult, len(ids))
	for index, id := range ids {
		results[index] = uploader.DestinationResult{DestinationID: id, Err: err, FailureCategory: category}
	}
	return uploader.UploadReport{Results: results}
}

func (u *Uploader) CallCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

func (u *Uploader) SetError(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.Err = err
}

func (u *Uploader) SetPanic(value any) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.PanicValue = value
}

func (u *Uploader) LastPath() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastInput.FilePath()
}

func FileNamePattern(prefix string) logpattern.FileNamePattern {
	return logpattern.FileNamePattern{Target: strings.TrimSuffix(prefix, "_"), Extension: "log"}
}

func NewStore(t *testing.T) *objectstoragespool.Store {
	t.Helper()
	root, err := objectstoragespool.NewRoot(t.TempDir(), "test-app", objectstorage.UploadTargetS3, logger.LoggerContractSync)
	if err != nil {
		t.Fatal(err)
	}
	store, err := objectstoragespool.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func NewDestinationStores(t *testing.T) (*objectstoragespool.DestinationStores, *objectstoragespool.Store) {
	t.Helper()
	root, err := objectstoragespool.NewRoot(t.TempDir(), "test-app", objectstorage.UploadTargetS3, logger.LoggerContractSync)
	if err != nil {
		t.Fatal(err)
	}
	stores, err := objectstoragespool.NewDestinationStores(root)
	if err != nil {
		t.Fatal(err)
	}
	store, err := stores.ForDestination("s3")
	if err != nil {
		t.Fatal(err)
	}
	return stores, store
}

func SaveFile(t *testing.T, store *objectstoragespool.Store, fileName string) objectstoragespool.CompressedLogFile {
	t.Helper()
	sourcePath := WriteSource(t, fileName)
	file, err := store.SaveRetryableFailure(context.Background(), sourcePath)
	if err != nil {
		t.Fatalf("Store.SaveRetryableFailure() error = %v", err)
	}
	return file
}

func WriteSource(t *testing.T, fileName string) string {
	t.Helper()
	sourcePath := filepath.Join(t.TempDir(), fileName)
	if err := os.WriteFile(sourcePath, []byte("compressed log "+fileName), 0o640); err != nil {
		t.Fatal(err)
	}
	return sourcePath
}

func OnlyFile(t *testing.T, store interface {
	ListFiles(context.Context, pkgspool.State) ([]objectstoragespool.CompressedLogFile, error)
}, state pkgspool.State) objectstoragespool.CompressedLogFile {
	t.Helper()
	files, err := store.ListFiles(context.Background(), state)
	if err != nil {
		t.Fatalf("Store.ListFiles(%q) error = %v", state, err)
	}
	if len(files) != 1 {
		t.Fatalf("Store.ListFiles(%q) count = %d, want 1", state, len(files))
	}
	return files[0]
}

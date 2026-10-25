package compressedlogarchive

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestStoreRejectsChangedCopyBeforePublication(t *testing.T) {
	for _, category := range []uploader.FailureCategory{uploader.FailureCategoryRetryable, uploader.FailureCategoryPermanent} {
		for _, extension := range []string{"gz", "custom"} {
			t.Run(string(category)+"/"+extension, func(t *testing.T) {
				store := newTestStore(t, pkglogger.LoggerContractSync)
				if err := store.EnsureDirectories(context.Background()); err != nil {
					t.Fatal(err)
				}
				body := append(bytes.Repeat([]byte{0x00, 0xff, 0x42}, 128*1024), 0x01)
				source := writeTestCompressedLog(t, "app_20260925_1.log."+extension, body)
				input := testSaveInput(source, category)
				directory, err := store.Root().FailureCategoryDir(category)
				if err != nil {
					t.Fatal(err)
				}
				injected := false
				ctx := &archiveCopyObservedContext{Context: context.Background(), beforeErr: func() {
					if injected {
						return
					}
					entries, err := os.ReadDir(directory)
					if err != nil {
						t.Fatal(err)
					}
					if len(entries) != 1 {
						return
					}
					path := filepath.Join(directory, entries[0].Name())
					info, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if info.Size() != int64(len(body)) {
						return
					}
					// 元ファイルが残ったcopy完了時に、metadataを変えない内容破損を注入します。
					file, err := os.OpenFile(path, os.O_WRONLY, 0)
					if err != nil {
						t.Fatal(err)
					}
					_, writeErr := file.WriteAt([]byte{0x02}, info.Size()-1)
					closeErr := file.Close()
					if writeErr != nil || closeErr != nil {
						t.Fatalf("inject copy corruption: %v, %v", writeErr, closeErr)
					}
					if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
						t.Fatal(err)
					}
					injected = true
				}}
				result, err := store.Save(ctx, input)
				if !injected || !errors.Is(err, fsstore.ErrCopyDiffersFromSource) || result.Path != "" {
					t.Fatalf("Save(changed copy) = %+v, %v; injected=%t", result, err, injected)
				}
				for _, directory := range []string{store.root.RetryableDir(), store.root.PermanentDir()} {
					assertDirectoryEmpty(t, directory)
				}
				assertFileBytes(t, source, body)
				// 独自形式にもcodecは不要で、元ファイルからの再試行が可能です。
				result, err = store.Save(context.Background(), input)
				if err != nil || result.FailureCategory != category {
					t.Fatalf("Save retry = %+v, %v", result, err)
				}
				assertFileBytes(t, result.Path, body)
			})
		}
	}
}

// ファイルのcopy進捗を同じgoroutineから観測し、sleepや競合なしで異常を再現します。
type archiveCopyObservedContext struct {
	context.Context
	mu        sync.Mutex
	beforeErr func()
}

func (c *archiveCopyObservedContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.beforeErr()
	return c.Context.Err()
}

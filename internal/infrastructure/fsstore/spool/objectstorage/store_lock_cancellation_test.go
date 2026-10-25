package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestStoreQueueOperationsCancelWhileWaitingForLocks(t *testing.T) {
	operations := []struct {
		name   string
		layout bool
		run    func(context.Context, *Store) error
	}{
		{"claim", true, func(ctx context.Context, store *Store) error {
			_, _, err := store.ClaimOldestActive(ctx)
			return err
		}},
		{"recover", true, func(ctx context.Context, store *Store) error {
			_, err := store.RecoverProcessing(ctx)
			return err
		}},
		{"clean staging", true, func(ctx context.Context, store *Store) error {
			_, err := store.RemoveAbandonedStagingFiles(ctx)
			return err
		}},
		{"requeue", false, func(ctx context.Context, store *Store) error {
			_, err := store.RequeueProcessing(ctx, CompressedLogFile{State: pkgspool.StateProcessing})
			return err
		}},
		{"transition", false, func(ctx context.Context, store *Store) error {
			_, err := store.TransitionProcessing(ctx, CompressedLogFile{State: pkgspool.StateProcessing}, pkgspool.StateDone)
			return err
		}},
		{"remove terminal", false, func(ctx context.Context, store *Store) error {
			return store.RemoveTerminal(ctx, CompressedLogFile{State: pkgspool.StateDone})
		}},
	}
	for _, operation := range operations {
		for _, lockName := range []string{"operation", "layout"} {
			if lockName == "layout" && !operation.layout {
				continue
			}
			t.Run(operation.name+"/"+lockName, func(t *testing.T) {
				store := newTestStore(t)
				synctest.Test(t, func(t *testing.T) {
					var held sync.Locker = &store.operationMu
					if lockName == "layout" {
						held = &store.layoutMu
					}
					held.Lock()
					defer held.Unlock()
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := operation.run(ctx, store); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("waiting operation = %v", err)
					}
				})
				if _, err := os.Lstat(store.Root().ActiveDir()); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("canceled operation initialized layout: %v", err)
				}
				// 入れ子の初期化待ちで取消されても、外側のoperation lockは解放されます。
				// 上限はlockが残った場合のhangを検出するためだけのもので、bubble外の実時間で数えます。
				// layoutの初期化はdirectoryごとに同期するため、負荷の高いdiskでは1秒を超えることがあります。
				ctx, cancel := context.WithTimeout(context.Background(), boundedwait.HangTimeout)
				defer cancel()
				if _, _, err := store.ClaimOldestActive(ctx); err != nil {
					t.Fatalf("claim after canceled operation: %v", err)
				}
			})
		}
	}
}

func TestStoreListFilesCancelsLayoutWait(t *testing.T) {
	store := newTestStore(t)
	synctest.Test(t, func(t *testing.T) {
		store.layoutMu.Lock()
		defer store.layoutMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := store.ListFiles(ctx, pkgspool.StateActive); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("list waiting for layout = %v", err)
		}
	})
}

func TestStoreCanceledRemovalRetainsTerminalFile(t *testing.T) {
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "app_20260916_1.log.gz", "retained file")
	file, err := store.SavePermanentFailure(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	synctest.Test(t, func(t *testing.T) {
		store.operationMu.Lock()
		defer store.operationMu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := store.RemoveTerminal(ctx, file); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("remove waiting for operation lock = %v", err)
		}
	})
	if data, err := os.ReadFile(file.Path); err != nil || string(data) != "retained file" {
		t.Fatalf("canceled removal changed file: %q, %v", data, err)
	}
	if err := store.RemoveTerminal(context.Background(), file); err != nil {
		t.Fatalf("retry removal: %v", err)
	}
}

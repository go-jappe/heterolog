package objectstoragespool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestStoreEnsureLayoutDoesNotRecreateRemovedDirectories(t *testing.T) {
	for _, test := range []struct {
		name      string
		directory func(Root) string
		operate   func(*Store, string) error
	}{
		{
			name: "active",
			directory: func(root Root) string {
				return root.ActiveDir()
			},
			operate: func(store *Store, _ string) error {
				_, _, err := store.ClaimOldestActive(context.Background())
				return err
			},
		},
		{
			name: "processing",
			directory: func(root Root) string {
				return root.ProcessingDir()
			},
			operate: func(store *Store, _ string) error {
				_, err := store.RecoverProcessing(context.Background())
				return err
			},
		},
		{
			name: "staging",
			directory: func(root Root) string {
				return root.stagingDir()
			},
			operate: func(store *Store, source string) error {
				_, err := store.SaveRetryableFailure(context.Background(), source)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			if err := store.EnsureLayout(context.Background()); err != nil {
				t.Fatal(err)
			}
			directory := test.directory(store.Root())
			// 一時領域に作成した空のstate directoryだけを削除します。
			if err := os.Remove(directory); err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				if err := store.EnsureLayout(context.Background()); err != nil {
					t.Fatalf("EnsureLayout() after initialization %d: %v", attempt, err)
				}
				if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("EnsureLayout() recreated removed directory: %v", err)
				}
			}
			source := writeTestSource(t, t.TempDir(), "app_20260913_1.log.gz", "source preserved")
			if err := test.operate(store, source); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("operation with removed %s directory: %v, want not-exist error", test.name, err)
			}
			if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("operation recreated removed directory: %v", err)
			}
			if data, err := os.ReadFile(source); err != nil || string(data) != "source preserved" {
				t.Fatalf("source after failed operation = %q, error = %v", data, err)
			}
			// 初期化済みの記録はStoreごとなので、新しいStoreはlayoutを再度用意します。
			restarted, err := NewStore(store.Root())
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.EnsureLayout(context.Background()); err != nil {
				t.Fatalf("new Store initialization: %v", err)
			}
			if info, err := os.Stat(directory); err != nil || !info.IsDir() {
				t.Fatalf("new Store did not recreate missing directory: info = %v, error = %v", info, err)
			}
		})
	}
}

func TestStoreEnsureLayoutRetriesAfterPartialFailure(t *testing.T) {
	store := newTestStore(t)
	if err := os.MkdirAll(store.Root().Path(), directoryMode); err != nil {
		t.Fatal(err)
	}
	blocked := store.Root().ProcessingDir()
	if err := os.WriteFile(blocked, []byte("blocking fixture"), payloadMode); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := store.EnsureLayout(context.Background()); err == nil {
			t.Fatalf("EnsureLayout() attempt %d accepted a file as a state directory", attempt)
		}
	}
	if info, err := os.Stat(store.Root().ActiveDir()); err != nil || !info.IsDir() {
		t.Fatalf("earlier state directory was not created: info = %v, error = %v", info, err)
	}
	if _, err := os.Stat(store.Root().stagingDir()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("layout continued past invalid state directory: %v", err)
	}
	if err := os.Rename(blocked, filepath.Join(t.TempDir(), "saved-blocking-fixture")); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureLayout(context.Background()); err != nil {
		t.Fatalf("EnsureLayout() after conflicting file moved: %v", err)
	}
	source := writeTestSource(t, t.TempDir(), "app_20260913_2.log.gz", "published after retry")
	file, err := store.SaveRetryableFailure(context.Background(), source)
	if err != nil {
		t.Fatalf("SaveRetryableFailure() after layout retry: %v", err)
	}
	if data, err := os.ReadFile(file.Path); err != nil || string(data) != "published after retry" {
		t.Fatalf("published bytes = %q, error = %v", data, err)
	}
}

func TestStoreConcurrentLayoutAndQueueOperations(t *testing.T) {
	store := newTestStore(t)
	source := writeTestSource(t, t.TempDir(), "app_20260913_3.log.gz", "concurrent payload")
	ctx := context.Background()
	operations := []struct {
		name string
		run  func() error
	}{
		{name: "ensure", run: func() error { return store.EnsureLayout(ctx) }},
		{name: "publish", run: func() error {
			_, err := store.SaveRetryableFailure(ctx, source)
			return err
		}},
		{name: "recover", run: func() error {
			_, err := store.RecoverProcessing(ctx)
			return err
		}},
		{name: "claim", run: func() error {
			_, _, err := store.ClaimOldestActive(ctx)
			return err
		}},
	}
	start := make(chan struct{})
	results := make(chan error, len(operations))
	for _, operation := range operations {
		go func() {
			<-start
			for range 8 {
				if err := operation.run(); err != nil {
					results <- fmt.Errorf("%s: %w", operation.name, err)
					return
				}
			}
			results <- nil
		}()
	}
	close(start)
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for range operations {
		select {
		case err := <-results:
			if err != nil {
				t.Error(err)
			}
		case <-timeout.C:
			t.Fatal("layout and queue operations did not complete; possible lock deadlock")
		}
	}
	active, err := store.ListFiles(ctx, pkgspool.StateActive)
	if err != nil {
		t.Fatal(err)
	}
	processing, err := store.ListFiles(ctx, pkgspool.StateProcessing)
	if err != nil {
		t.Fatal(err)
	}
	files := append(active, processing...)
	if len(files) != 1 {
		t.Fatalf("concurrent operations left %d files across active/processing, want one", len(files))
	}
	if data, err := os.ReadFile(files[0].Path); err != nil || string(data) != "concurrent payload" {
		t.Fatalf("stored bytes after concurrent operations = %q, error = %v", data, err)
	}
	assertDirectoryEmpty(t, store.Root().stagingDir())
}

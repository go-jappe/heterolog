package fsstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnsureDirectoryCancellationBeforeCreation(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "canceled"
		if expired {
			name = "deadline exceeded"
		}
		t.Run(name, func(t *testing.T) {
			// リンクを拒否するため、macOSの/varなど一時ディレクトリの上位のリンクを解決します。
			directory := filepath.Join(resolvedTempDirForTest(t), "new-directory")
			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled
			if expired {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				want = context.DeadlineExceeded
			}
			cancel()
			if err := EnsureDirectoryNoSymlinksContext(ctx, directory, 0o700); !errors.Is(err, want) {
				t.Fatalf("canceled initialization = %v, want %v", err, want)
			}
			if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("canceled initialization created a directory: %v", err)
			}
			if err := EnsureDirectoryNoSymlinksContext(context.Background(), directory, 0o700); err != nil {
				t.Fatalf("retry initialization: %v", err)
			}
		})
	}
}

func TestEnsureDirectoryCancellationStillSyncsCreatedParents(t *testing.T) {
	base := resolvedTempDirForTest(t)
	first := filepath.Join(base, "first")
	last := filepath.Join(first, "last")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &directoryCreationCancellationContext{Context: ctx, createdPath: first, cancel: cancel}
	var synced []string
	err := ensureDirectoryWithPolicy(observed, last, 0o700, directorySetupPolicy{rejectSymlinks: true}, func(path string) error {
		synced = append(synced, path)
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("partial initialization = %v", err)
	}
	if len(synced) != 1 || synced[0] != base {
		t.Fatalf("created directory parents synced = %v, want [%s]", synced, base)
	}
	if _, err := os.Lstat(last); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("initialization continued after cancel: %v", err)
	}
	if err := EnsureDirectoryNoSymlinksContext(context.Background(), last, 0o700); err != nil {
		t.Fatalf("retry initialization: %v", err)
	}
}

type directoryCreationCancellationContext struct {
	context.Context
	createdPath string
	cancel      context.CancelFunc
}

func (c *directoryCreationCancellationContext) Err() error {
	if _, err := os.Lstat(c.createdPath); err == nil {
		c.cancel()
	}
	return c.Context.Err()
}

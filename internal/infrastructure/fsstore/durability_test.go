package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestSyncDirectory(t *testing.T) {
	t.Run("existing directory", func(t *testing.T) {
		if err := SyncDirectory(t.TempDir()); err != nil {
			t.Fatalf("SyncDirectory() error = %v", err)
		}
	})
	t.Run("missing directory", func(t *testing.T) {
		err := SyncDirectory(filepath.Join(t.TempDir(), "missing"))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("SyncDirectory() error = %v, want os.ErrNotExist", err)
		}
		if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
			t.Fatalf("SyncDirectory() error = %v, want durability unconfirmed", err)
		}
	})
}

func TestSyncDirectoryAfterRenameAndRemove(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.log")
	pending := filepath.Join(directory, "source.delete-pending.tmp")
	if err := os.WriteFile(source, []byte("sync test"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := SyncDirectory(directory); err != nil {
		t.Fatalf("sync after create: %v", err)
	}
	if err := os.Rename(source, pending); err != nil {
		t.Fatal(err)
	}
	if err := SyncDirectory(filepath.Dir(pending)); err != nil {
		t.Fatalf("sync after rename: %v", err)
	}
	if err := os.Remove(pending); err != nil {
		t.Fatal(err)
	}
	if err := SyncDirectory(filepath.Dir(pending)); err != nil {
		t.Fatalf("sync after remove: %v", err)
	}
	// APIの正常完了を確認するtestであり、電源断後の永続化を試験しません。
}

//go:build !windows

package testutil_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestDenyFileRemovalDeniesOnlyRemovalWithoutLeavingProbe(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "target.log")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	// 権限の制限が効かない実行環境では、ここで試験をskipします。
	restore := testutil.DenyFileRemoval(t, path)
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("directory entries during removal denial = %v; want only the fixture", entries)
	}
	if err := os.Remove(path); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("remove denied file = %v; want permission denied", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "keep" {
		t.Fatalf("read denied-removal file = %q, %v", got, err)
	}
	restore()
	after, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if after.Mode().Perm() != before.Mode().Perm() {
		t.Fatalf("restored directory permissions = %o, want %o", after.Mode().Perm(), before.Mode().Perm())
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove target after restore: %v", err)
	}
}

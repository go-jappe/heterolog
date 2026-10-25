//go:build !windows

package fsstore

import (
	"os"
	"path/filepath"
	"testing"
)

func makePurgeStatFailure(t *testing.T) (string, func()) {
	t.Helper()
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(realDirectory, "app_20260824_1.log"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	// 最終要素のリンクはLstatで調べられるため、親の循環リンクで情報取得を失敗させます。
	alias := filepath.Join(root, "loop")
	if err := os.Symlink(filepath.Base(alias), alias); err != nil {
		t.Fatalf("create symlink loop: %v", err)
	}
	restored := false
	restore := func() {
		t.Helper()
		if restored {
			return
		}
		if err := os.Remove(alias); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(realDirectory, alias); err != nil {
			t.Fatal(err)
		}
		restored = true
	}
	t.Cleanup(restore)
	return filepath.Join(alias, "app_20260824_1.log"), restore
}

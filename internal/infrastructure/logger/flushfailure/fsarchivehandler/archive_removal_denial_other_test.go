//go:build !windows

package fsarchivehandler

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// denyArchiveFileRemovalは、実際の圧縮fileを残したまま削除を拒否する試験準備です。
// rootなど権限確認を迂回する実行環境では削除を拒否できないため、同じdirectoryへの
// 作成で拒否が有効か確認し、無効なら試験をskipします。
func denyArchiveFileRemoval(t testing.TB, path string) {
	t.Helper()
	directory := filepath.Dir(path)
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	restore := func() {
		t.Helper()
		if err := os.Chmod(directory, info.Mode().Perm()); err != nil {
			t.Fatalf("restore archive directory permissions: %v", err)
		}
	}
	t.Cleanup(restore)
	if err := os.Chmod(directory, info.Mode().Perm()&^0o222); err != nil {
		t.Fatalf("deny archive file removal: %v", err)
	}
	// 削除と作成はどちらも親directoryの書込権限で判定されます。archiveの命名規則に
	// 一致しない名前で確認し、作成できた場合はすぐに取り除きます。
	probe, err := os.CreateTemp(directory, ".archive-removal-denial-probe-*")
	if err == nil {
		probePath := probe.Name()
		if err := errors.Join(probe.Close(), os.Remove(probePath)); err != nil {
			t.Fatal(err)
		}
		t.Skip("current user or filesystem bypasses directory write permissions")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("probe denied archive file removal: %v", err)
	}
}

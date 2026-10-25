//go:build !windows

package testutil

import (
	"errors"
	"os"
	"testing"
)

// DenyDirectorySyncは、配下の項目を名前で参照する操作と名前変更は許可したまま、
// 同期用にディレクトリを開けなくします。ディレクトリ内の一覧を取得する前に、
// 権限を復元してください。戻り値の関数とテスト終了時の後片付けは、
// どちらも元の権限を復元します。
func DenyDirectorySync(t *testing.T, directory string) func() {
	t.Helper()
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	restored := false
	restore := func() {
		t.Helper()
		if restored {
			return
		}
		if err := os.Chmod(directory, info.Mode().Perm()); err != nil {
			t.Fatalf("restore directory sync fixture permissions: %v", err)
		}
		restored = true
	}
	t.Cleanup(restore)
	if err := os.Chmod(directory, 0o300); err != nil {
		t.Fatalf("deny directory sync: %v", err)
	}
	probe, err := os.Open(directory)
	if err == nil {
		if err := probe.Close(); err != nil {
			t.Fatal(err)
		}
		t.Skip("current user or filesystem bypasses directory read permissions")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("probe denied directory sync: %v", err)
	}
	return restore
}

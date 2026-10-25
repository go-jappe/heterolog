// Package testutilは、ファイルシステム関連の回帰試験で共用する補助関数を提供します。
package testutil

import (
	"errors"
	"os"
	"runtime"
	"syscall"
	"testing"
)

// Symlinkは、試験用のシンボリックリンクを作成します。
// Windowsでリンク作成の特権が不足している場合に限り、呼び出し元の試験をスキップします。
// パスの不正やその他の予期しないエラーは、試験の失敗として扱います。
// リンク以外の検証も含む試験では、リンクを扱うサブテスト内から呼び出してください。
func Symlink(t testing.TB, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		if runtime.GOOS == "windows" && errors.Is(err, syscall.Errno(1314)) {
			t.Skipf("Windows symlink fixture needs symlink creation privilege: %v", err)
		}
		t.Fatalf("create symlink fixture %q -> %q: %v", link, target, err)
	}
}

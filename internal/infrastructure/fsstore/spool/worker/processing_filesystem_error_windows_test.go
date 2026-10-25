package worker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/testutil"
	"golang.org/x/sys/windows"
)

// processingPathWithLstatFailureは、属性の読み取りを拒否したspool fileのpathを返します。
// Windowsのsymlinkは作成権限が必要で一般userではskipになるため、symlinkの循環ではなく
// 権限不足で存在を確認できない状態を作ります。親directoryの一覧権限は子の属性の読み取りを
// 暗黙に許可するため、spool fileの属性の読み取りと親directoryの一覧を両方拒否します。
// 拒否は所有者と管理者にも適用されるため、実行userの権限に依存せず失敗を再現できます。
func processingPathWithLstatFailure(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "blocked"+spooldir.SpoolExt)
	if err := os.WriteFile(path, []byte("spool body"), 0o600); err != nil {
		t.Fatal(err)
	}
	testutil.DenyWindowsAccess(t, path, windows.FILE_READ_ATTRIBUTES)
	testutil.DenyWindowsAccess(t, dir, windows.FILE_LIST_DIRECTORY)
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("Lstat of access-denied fixture %q = %v, want permission error", path, err)
	}
	return path
}

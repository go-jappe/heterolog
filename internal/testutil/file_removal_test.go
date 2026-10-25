package testutil_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
)

// DenyFileRemovalが全OSで保証する動作だけを確認します。対象ファイルは読めるまま削除が
// 権限エラーになり、権限を戻した後は削除できます。同じdirectoryの他のentryへの影響は
// OSごとに異なる仕様外の動作のため、この試験では確認しません。
func TestDenyFileRemovalDeniesTargetRemovalUntilRestore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "target.log")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	// 権限の制限が効かない実行環境では、ここで試験をskipします。
	restore := testutil.DenyFileRemoval(t, path)
	if err := os.Remove(path); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("remove denied file = %v; want permission denied", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "keep" {
		t.Fatalf("read denied-removal file = %q, %v; want the original content", got, err)
	}
	restore()
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove target after restore: %v", err)
	}
}

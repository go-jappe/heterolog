package checkpoint

import (
	"path/filepath"
	"testing"
)

// resolvedTempDirForTestは、t.TempDir()をsymlinkを含まない実体pathで返します。
// macOSの/var→/private/varのように一時directoryの祖先がlinkでも、Storeが解決して使う
// 物理pathと試験の期待値を同じ表記に揃えます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return directory
}

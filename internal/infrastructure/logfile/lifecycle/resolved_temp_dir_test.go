package lifecycle

import (
	"path/filepath"
	"testing"
)

// resolvedTempDirForTestは、t.TempDir()をsymlinkを含まない実体pathで返します。
// macOSの/var→/private/varのように一時directoryの祖先がlinkでも、production側が
// 解決して使う物理pathと試験のfixture・期待値を同じ表記に揃えます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return directory
}

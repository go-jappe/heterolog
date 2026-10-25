package processing

import (
	"path/filepath"
	"testing"
)

// resolvedTempDirForTestは、t.TempDir()をsymlinkを含まない実体pathで返します。
// productionはlog directoryを実体pathへ解決してから元ログを渡すため、macOSの
// /var→/private/varのような一時directoryのlinkを入力側で解決して揃えます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return directory
}

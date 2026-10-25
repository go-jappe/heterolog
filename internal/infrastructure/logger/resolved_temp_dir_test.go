package logger

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// resolvedTempDirForTestは、symlinkを解決した物理pathのtest用temporary directoryを返します。
// macOSのt.TempDirは/varから/private/varへのsymlinkを経由し、productionは保存先を物理path
// へ解決してから扱います。期待値を同じpathから組み立てるため、入力側で解決しておきます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return directory
}

//go:build !windows

package file

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// denyLogDirectoryOperationForTestは、directoryに対するoperationを実行userに拒否し、試験の終了時に
// 元の権限へ戻します。拒否を確かめられない環境ではskipします
// （requireLogDirectoryOperationDeniedForTest）。
func denyLogDirectoryOperationForTest(t *testing.T, directory string, operation logDirectoryOperationForTest) {
	t.Helper()
	info, err := os.Stat(directory)
	require.NoError(t, err)
	// 0500は列挙を許してentryの作成を拒否し、0300はentryの作成を許して列挙を拒否します。
	mode := os.FileMode(0o500)
	if operation == listEntriesForTest {
		mode = 0o300
	}
	t.Cleanup(func() { require.NoError(t, os.Chmod(directory, info.Mode().Perm())) })
	require.NoError(t, os.Chmod(directory, mode))
	requireLogDirectoryOperationDeniedForTest(t, directory, operation)
}

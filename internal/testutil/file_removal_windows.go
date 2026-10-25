package testutil

import (
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// DenyFileRemovalは、pathの通常ファイルを読めるまま、そのファイルの削除を権限エラー
// （errors.Is(err, os.ErrPermission)）で失敗させます。返却関数またはtest cleanupで元の
// 権限へ戻し、戻した後は削除できます。全OSで保証するのはこの対象ファイルについての
// 動作だけです。同じdirectoryの他のentryの作成・削除・renameへの影響はOSごとに異なるため、
// 呼び出し側は依存しないでください。
//
// Windowsではfileの DELETE と親directoryの FILE_DELETE_CHILD を拒否します。
func DenyFileRemoval(t testing.TB, path string) func() {
	t.Helper()
	const fileDeleteChild = 0x0040
	// 親の継承ACLを変更する前に、file自身へ明示的なACLを設定します。
	restoreFile := DenyWindowsAccess(t, path, windows.DELETE)
	restoreDirectory := DenyWindowsAccess(t, filepath.Dir(path), fileDeleteChild)
	return func() {
		restoreDirectory()
		restoreFile()
	}
}

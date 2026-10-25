package fsarchivehandler

import (
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil"
)

// WindowsのchmodはUnixのdirectory書込禁止を再現しません。file自身のDELETEと
// 親directoryのFILE_DELETE_CHILDを両方拒否し、実際のRemove失敗を発生させます。
// 読取は許可したままにして、削除失敗後も元の圧縮データが残ることを確認します。
func denyArchiveFileRemoval(t testing.TB, path string) {
	t.Helper()
	// x/sys/windowsに定義のないWinNT.hのFILE_DELETE_CHILD。
	// https://learn.microsoft.com/en-us/windows/win32/fileio/file-access-rights-constants
	const fileDeleteChild = 0x0040
	// 親の継承ACLを変える前に、fileへ明示的な保護DACLを設定します。
	// 逆順では親の変更でfileの継承権限も失われ、障害注入の準備自体が失敗します。
	testutil.DenyWindowsAccess(t, path, windows.DELETE)
	testutil.DenyWindowsAccess(t, filepath.Dir(path), fileDeleteChild)
}

package testutil

import (
	"fmt"
	"runtime"
	"testing"

	"golang.org/x/sys/windows"
)

// DenyWindowsAccess は、access で指定された権限を所有者を含む全員に対して拒否する、保護された DACL を設定する。
// それ以外の権限は、実効ユーザー、SYSTEM、Administrators に許可する。
// 返された関数の明示的な呼び出し、またはテスト終了時のクリーンアップで元の権限を復元する。
// 復元するのは元の DACL の ACE と継承からの保護の有無であり、SetSecurityInfo は復元後の DACL を
// 自動継承形式（SE_DACL_AUTO_INHERITED）にする。この形式の bit は ACE の評価と保護を変えない。
// テスト用の設定に失敗した場合はエラーとして扱い、テストをスキップする理由にはしない。
func DenyWindowsAccess(t testing.TB, path string, access uint32) func() {
	t.Helper()
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	// WRITE_DAC を拒否してもクリーンアップできるよう、元のハンドルを保持する。
	// os.ReadDir のハンドルは削除アクセスを共有しないため、DELETE は要求しない。
	// DELETE 権限を要求したハンドルを保持すると、ディレクトリの列挙時に共有違反が発生する。
	handle, err := windows.CreateFile(name, windows.READ_CONTROL|windows.WRITE_DAC,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		t.Fatalf("open access-denial fixture %q: %v", path, err)
	}
	closed := false
	closeHandle := func() {
		if closed {
			return
		}
		closed = true
		if err := windows.CloseHandle(handle); err != nil {
			t.Errorf("close access-denial fixture %q: %v", path, err)
		}
	}
	t.Cleanup(closeHandle)
	original, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	originalACL, _, err := original.DACL()
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := original.Control()
	if err != nil {
		t.Fatal(err)
	}
	restoreFlags := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION | windows.UNPROTECTED_DACL_SECURITY_INFORMATION)
	if control&windows.SE_DACL_PROTECTED != 0 {
		restoreFlags = windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION
	}
	restored := false
	restore := func() {
		t.Helper()
		if restored {
			return
		}
		err := windows.SetSecurityInfo(handle, windows.SE_FILE_OBJECT, restoreFlags, nil, nil, originalACL, nil)
		runtime.KeepAlive(original)
		if err != nil {
			t.Fatalf("restore access-denial fixture %q: %v", path, err)
		}
		restored = true
		// 明示的に復元した時点でハンドルも解放し、同じ名前の削除・再作成を可能にする。
		closeHandle()
	}
	t.Cleanup(restore)

	user, err := windows.GetCurrentThreadEffectiveToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	// OWNER RIGHTS ACE は、所有者に暗黙的に付与される WRITE_DAC 権限を抑制する。
	// これにより、テストがオブジェクトの所有者でも、明示的な拒否が適用される。
	denied, err := windows.SecurityDescriptorFromString(fmt.Sprintf(
		"D:P(D;;0x%x;;;WD)(A;;RC;;;OW)(A;;FA;;;%s)(A;;FA;;;SY)(A;;FA;;;BA)",
		access, user.User.Sid.String()))
	if err != nil {
		t.Fatal(err)
	}
	deniedACL, _, err := denied.DACL()
	if err != nil {
		t.Fatal(err)
	}
	// MAXIMUM_ALLOWED で開いた一時ハンドルは、拒否 DACL による既存の子への ACE 伝播を抑止する。
	// DELETE も含むため、os.ReadDir と競合しないよう拒否の適用直後に閉じる。
	// https://learn.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setsecurityinfo
	updateHandle, err := windows.CreateFile(name, windows.MAXIMUM_ALLOWED,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		t.Fatalf("open access-denial update fixture %q: %v", path, err)
	}
	err = windows.SetSecurityInfo(updateHandle, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, deniedACL, nil)
	runtime.KeepAlive(denied)
	closeErr := windows.CloseHandle(updateHandle)
	if err != nil {
		t.Fatalf("deny access to fixture %q: %v", path, err)
	}
	if closeErr != nil {
		t.Fatalf("close access-denial update fixture %q: %v", path, closeErr)
	}
	return restore
}

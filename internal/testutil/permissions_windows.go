package testutil

import (
	"os"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// RequirePrivatePermissionsは、Windowsの実際のDACL（アクセス権の一覧）を検査します。
// FileMode.Permでは、実行ユーザー・SYSTEM・Administratorsに対する保存先の
// アクセス権設定を表現できないため、DACLを直接確認します。
func RequirePrivatePermissions(t testing.TB, path string, _ os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatalf("read DACL of %q: %v", path, err)
	}
	control, _, err := descriptor.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatalf("DACL of %q is not protected from inheritance: control=%#x, error=%v", path, control, err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil {
		t.Fatalf("DACL of %q is missing or unrestricted: %v", path, err)
	}
	user, err := windows.GetCurrentThreadEffectiveToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		user.User.Sid.String(): false,
		"S-1-5-18":             false,
		"S-1-5-32-544":         false,
	}
	var wantFlags uint8
	if info.IsDir() {
		wantFlags = windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE
	}
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil {
			t.Fatal(err)
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || ace.Header.AceFlags != wantFlags || ace.Mask != 0x1f01ff {
			t.Fatalf("unexpected ACE %d in %q: type=%d, flags=%#x, access=%#x", index, path, ace.Header.AceType, ace.Header.AceFlags, ace.Mask)
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart)).String()
		if _, allowed := want[sid]; !allowed {
			t.Fatalf("DACL of %q grants access to unexpected identity %s", path, sid)
		}
		want[sid] = true
	}
	for sid, present := range want {
		if !present {
			t.Fatalf("DACL of %q lacks required identity %s", path, sid)
		}
	}
}

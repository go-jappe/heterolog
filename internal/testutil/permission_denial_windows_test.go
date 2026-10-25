package testutil_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestDenyWindowsAccessRestoreReleasesHandle(t *testing.T) {
	for _, directory := range []bool{false, true} {
		t.Run(fmt.Sprintf("directory=%t", directory), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "fixture")
			create := func() {
				t.Helper()
				var err error
				if directory {
					err = os.Mkdir(path, 0o700)
				} else {
					err = os.WriteFile(path, []byte("fixture"), 0o600)
				}
				if err != nil {
					t.Fatalf("create fixture: %v", err)
				}
			}
			create()
			before := fixtureWindowsDACLEntries(t, path)
			restore := testutil.DenyWindowsAccess(t, path, windows.FILE_WRITE_EA|windows.WRITE_DAC)
			requireWindowsAccessDenied(t, path, windows.FILE_WRITE_EA)
			requireWindowsAccessDenied(t, path, windows.WRITE_DAC)
			restore()
			if got := fixtureWindowsDACLEntries(t, path); got != before {
				t.Fatalf("restored DACL entries differ: before %s, after %s", before, got)
			}
			if err := os.Remove(path); err != nil {
				t.Fatalf("remove restored fixture: %v", err)
			}
			// 削除だけの成功では、ハンドルが残る delete-pending 状態を検出できない。
			create()
			// 2回目の呼び出しは何も書き込まないため、形式のbitも含めて記述子全体を比べます。
			recreated := fixtureWindowsDACL(t, path).String()
			restore()
			if got := fixtureWindowsDACL(t, path).String(); got != recreated {
				t.Fatalf("repeated restore changed recreated fixture: before %s, after %s", recreated, got)
			}
		})
	}
}

func TestDenyWindowsAccessDirectoryPreservesChildren(t *testing.T) {
	directory := fixtureWindowsInheritedDirectory(t)
	subdirectory := filepath.Join(directory, "child")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	files := []string{filepath.Join(directory, "first.txt"), filepath.Join(directory, "sibling.txt"), filepath.Join(subdirectory, "nested.txt")}
	paths := append([]string{subdirectory}, files...)
	for _, path := range files {
		if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := make(map[string]string, len(paths))
	for _, path := range paths {
		descriptor := fixtureWindowsDACL(t, path)
		control, _, err := descriptor.Control()
		if err != nil || control&windows.SE_DACL_PROTECTED != 0 {
			t.Fatalf("child must inherit its DACL: path=%q, control=%#x, error=%v", path, control, err)
		}
		dacl, _, err := descriptor.DACL()
		if err != nil || dacl == nil || dacl.AceCount == 0 {
			t.Fatalf("child must have inherited ACEs: path=%q, error=%v", path, err)
		}
		for index := uint32(0); index < uint32(dacl.AceCount); index++ {
			var ace *windows.ACCESS_ALLOWED_ACE
			if err := windows.GetAce(dacl, index, &ace); err != nil {
				t.Fatal(err)
			}
			if ace.Header.AceFlags&windows.INHERITED_ACE == 0 {
				t.Fatalf("child ACE must be inherited: path=%q, index=%d", path, index)
			}
		}
		before[path] = descriptor.String()
	}
	checkChildren := func() {
		t.Helper()
		for _, path := range paths {
			if got := fixtureWindowsDACL(t, path).String(); got != before[path] {
				t.Fatalf("directory denial changed child DACL: path=%q, before %s, after %s", path, before[path], got)
			}
		}
		for _, path := range files {
			if err := os.WriteFile(path, []byte("updated"), 0o600); err != nil {
				t.Fatalf("write existing child %q: %v", path, err)
			}
			if got, err := os.ReadFile(path); err != nil || string(got) != "updated" {
				t.Fatalf("read existing child %q = %q, %v", path, got, err)
			}
		}
		if entries, err := os.ReadDir(directory); err != nil || len(entries) != 3 {
			t.Fatalf("list directory with denial handle = %v, %v", entries, err)
		}
		if entries, err := os.ReadDir(subdirectory); err != nil || len(entries) != 1 {
			t.Fatalf("list existing child directory = %v, %v", entries, err)
		}
	}
	restore := testutil.DenyWindowsAccess(t, directory, windows.FILE_WRITE_EA|windows.WRITE_DAC)
	requireWindowsAccessDenied(t, directory, windows.FILE_WRITE_EA)
	requireWindowsAccessDenied(t, directory, windows.WRITE_DAC)
	checkChildren()
	restore()
	checkChildren()
}

// DenyFileRemovalの契約は対象ファイルの削除失敗だけで、同じdirectoryの他のentryの
// 作成・削除はOSごとに異なるため確認しません。Windowsの実装に固有の性質として、拒否の
// 適用と復元が親directory・兄弟fileのDACLを書き換えたまま残さないことを確認します。
func TestDenyFileRemovalRestoresWindowsDACLs(t *testing.T) {
	directory := fixtureWindowsInheritedDirectory(t)
	path := filepath.Join(directory, "target.txt")
	sibling := filepath.Join(directory, "sibling.txt")
	for _, name := range []string{path, sibling} {
		if err := os.WriteFile(name, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	before := map[string]string{
		directory: fixtureWindowsDACL(t, directory).String(),
		path:      fixtureWindowsDACL(t, path).String(),
		sibling:   fixtureWindowsDACL(t, sibling).String(),
	}
	restore := testutil.DenyFileRemoval(t, path)
	if err := os.Remove(path); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("remove denied file = %v; want permission denied", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "keep" {
		t.Fatalf("read denied-removal file = %q, %v", got, err)
	}
	// 親directoryの拒否ACEは既存の兄弟fileへ伝播させません。
	if got := fixtureWindowsDACL(t, sibling).String(); got != before[sibling] {
		t.Fatalf("file removal denial changed sibling DACL: before %s, after %s", before[sibling], got)
	}
	restore()
	for name, want := range before {
		if got := fixtureWindowsDACL(t, name).String(); got != want {
			t.Fatalf("restored DACL of %q differs: before %s, after %s", name, want, got)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove target after restore: %v", err)
	}
	if err := os.WriteFile(path, []byte("recreated"), 0o600); err != nil {
		t.Fatalf("recreate target after restore: %v", err)
	}
}

func fixtureWindowsDACL(t *testing.T, path string) *windows.SECURITY_DESCRIPTOR {
	t.Helper()
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatalf("read fixture DACL %q: %v", path, err)
	}
	return descriptor
}

// fixtureWindowsDACLEntriesは、DenyWindowsAccessが復元するDACLのACEと継承からの保護の有無
// （SE_DACL_PROTECTED）だけを持つ記述子を作り直し、SDDLで返します。
// 復元に使うSetSecurityInfoはDACLを常に自動継承形式（SE_DACL_AUTO_INHERITED、SDDLの"AI"）で書き込むため、
// 旧形式のDACLを持つfile（GitHub ActionsのWindows Server 2025 runnerの一時directoryの子など）では復元後に
// "AI"が付きます。"AI"はACEによる許可・拒否も保護も変えず、保護の有無は元の値で戻すため比較から除きます。
// 旧形式のDACLでは、GetNamedSecurityInfoが要求していない所有者・groupも返す場合があるため、それらも除きます。
func fixtureWindowsDACLEntries(t *testing.T, path string) string {
	t.Helper()
	descriptor := fixtureWindowsDACL(t, path)
	control, _, err := descriptor.Control()
	if err != nil {
		t.Fatalf("read fixture DACL control %q: %v", path, err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatalf("read fixture DACL entries %q: %v", path, err)
	}
	entries, err := windows.NewSecurityDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	if err := entries.SetDACL(dacl, true, false); err != nil {
		t.Fatal(err)
	}
	if err := entries.SetControl(windows.SE_DACL_PROTECTED, control&windows.SE_DACL_PROTECTED); err != nil {
		t.Fatal(err)
	}
	sddl := entries.String()
	// entriesは、descriptorの中にあるDACLを参照します。
	runtime.KeepAlive(descriptor)
	if sddl == "" {
		t.Fatalf("convert fixture DACL entries %q to SDDL", path)
	}
	return sddl
}

func fixtureWindowsInheritedDirectory(t *testing.T) string {
	t.Helper()
	user, err := windows.GetCurrentThreadEffectiveToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := windows.SecurityDescriptorFromString(
		"D:P(A;OICI;FA;;;" + user.User.Sid.String() + ")(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)")
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "inherited")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("create inheritable fixture directory: %v", err)
	}
	// SetNamedSecurityInfoは自動継承形式（SE_DACL_AUTO_INHERITED）のDACLを設定するため、以後に作る
	// 子も自動継承形式の継承ACEを持ちます。CreateDirectoryへ渡した記述子ではこのbitが付かず、子は旧形式の
	// DACLになります。旧形式の子はGetNamedSecurityInfoが親の現在のDACLと照合して継承ACEを表示し直すため、
	// 親のDACLを一時的に変えるだけで、子のDACLを変更していなくても変わったように見えます。
	if err := windows.SetNamedSecurityInfo(directory, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		t.Fatalf("set inheritable fixture DACL: %v", err)
	}
	runtime.KeepAlive(descriptor)
	control, _, err := fixtureWindowsDACL(t, directory).Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 || control&windows.SE_DACL_AUTO_INHERITED == 0 {
		t.Fatalf("fixture directory must have a protected auto-inherited DACL: control=%#x, error=%v", control, err)
	}
	return directory
}

func requireWindowsAccessDenied(t *testing.T, path string, access uint32) {
	t.Helper()
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(name, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err == nil {
		windows.CloseHandle(handle)
	}
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("open denied fixture %q with access %#x = %v; want access denied", path, access, err)
	}
}

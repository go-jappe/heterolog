//go:build windows

package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil"
)

const privateTestFullAccess = windows.STANDARD_RIGHTS_REQUIRED | windows.SYNCHRONIZE | 0x1ff

func TestPrivateDirectoryDACLIsRestrictedAtCreation(t *testing.T) {
	parent := privateTestBroadDirectory(t)
	before := privateTestSecurity(t, parent).String()
	child := filepath.Join(parent, "checkpoints")
	if err := CreatePrivateDirectory(child, 0o750); err != nil {
		t.Fatal(err)
	}
	privateTestAssertDACL(t, child, true, true)
	if got := privateTestSecurity(t, parent).String(); got != before {
		t.Fatalf("creation changed shared parent DACL: before %s, after %s", before, got)
	}
	if err := CreatePrivateDirectory(child, 0o700); !errors.Is(err, os.ErrExist) {
		t.Fatalf("create existing directory error = %v, want ErrExist", err)
	}

	untrusted := privateTestUntrustedToken(t)
	privateTestCheckAccess(t, privateTestSecurity(t, parent), untrusted, true)
	privateTestCheckAccess(t, privateTestSecurity(t, child), untrusted, false)
}

func TestPrivateExistingDirectoryChangesOnlySelectedLeaf(t *testing.T) {
	parent := privateTestBroadDirectory(t)
	leaf := filepath.Join(parent, "archive")
	if err := os.Mkdir(leaf, 0o777); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(leaf, "existing")
	if err := os.WriteFile(existing, []byte("legacy payload"), 0o666); err != nil {
		t.Fatal(err)
	}
	parentBefore := privateTestSecurity(t, parent).String()
	fileBefore := privateTestSecurity(t, existing).String()
	if err := SetPrivateDirectoryPermissions(leaf, 0o750); err != nil {
		t.Fatal(err)
	}
	privateTestAssertDACL(t, leaf, true, true)
	if got := privateTestSecurity(t, parent).String(); got != parentBefore {
		t.Fatalf("changed shared parent DACL: before %s, after %s", parentBefore, got)
	}
	if got := privateTestSecurity(t, existing).String(); got != fileBefore {
		t.Fatalf("recursively changed existing child DACL: before %s, after %s", fileBefore, got)
	}
	// 新しい子は新しい方針を継承します。親がprivateでも、既存のcheckpointの内容は
	// 起動時に読む前に明示的に保護しなければなりません。
	data, err := ReadPrivateFile(existing, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "legacy payload" {
		t.Fatalf("read contents = %q", data)
	}
	privateTestAssertDACL(t, existing, false, true)
}

func TestPrivateTempFileInheritanceAndReplacement(t *testing.T) {
	parent := privateTestBroadDirectory(t)
	directory := filepath.Join(parent, "spool")
	if err := CreatePrivateDirectory(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	untrusted := privateTestUntrustedToken(t)
	final := filepath.Join(directory, "checkpoint.json")
	for _, contents := range []string{"first checkpoint", "replacement checkpoint"} {
		file, err := os.CreateTemp(directory, "checkpoint-*.tmp")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		// これはsetterを呼ぶ前、payloadを書き込む前に実行します。継承したDACLが、
		// この時点ですでに無関係な読み手を拒否していなければなりません。
		privateTestAssertDACL(t, file.Name(), false, false)
		privateTestCheckAccess(t, privateTestSecurity(t, file.Name()), untrusted, false)
		if err := SetPrivateFilePermissions(file, 0o640); err != nil {
			t.Fatal(err)
		}
		privateTestAssertDACL(t, file.Name(), false, true)
		before := privateTestSecurity(t, file.Name()).String()
		if _, err := file.WriteString(contents); err != nil {
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(file.Name(), final); err != nil {
			t.Fatal(err)
		}
		privateTestAssertDACL(t, final, false, true)
		if got := privateTestSecurity(t, final).String(); got != before {
			t.Fatalf("rename changed file security: before %s, after %s", before, got)
		}
		privateTestCheckAccess(t, privateTestSecurity(t, final), untrusted, false)
		if data, err := os.ReadFile(final); err != nil || string(data) != contents {
			t.Fatalf("checkpoint = %q, error = %v", data, err)
		}
	}
}

func TestPrivateFileSetterUsesOpenObjectAfterRename(t *testing.T) {
	directory := privateTestBroadDirectory(t)
	original := filepath.Join(directory, "original")
	name, err := privateWindowsPath(original)
	if err != nil {
		t.Fatal(err)
	}
	// os.CreateTempのWindows handleと異なり、このfixtureは開いたままのrenameを許可します。
	// これにより、権限設定がfile.Name()を開き直さないことを試験で証明できます。
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.CREATE_NEW, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(handle), original)
	defer file.Close()
	moved := filepath.Join(directory, "moved")
	if err := os.Rename(original, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(original, []byte("unrelated replacement"), 0o666); err != nil {
		t.Fatal(err)
	}
	unrelatedBefore := privateTestSecurity(t, original).String()
	if err := SetPrivateFilePermissions(file, 0o600); err != nil {
		t.Fatal(err)
	}
	privateTestAssertDACL(t, moved, false, true)
	if got := privateTestSecurity(t, original).String(); got != unrelatedBefore {
		t.Fatalf("setter changed replacement at old pathname: before %s, after %s", unrelatedBefore, got)
	}
}

func TestPrivatePermissionsRejectWrongObjectTypesAndMissingPaths(t *testing.T) {
	root := t.TempDir()
	if _, err := privateWindowsPath(""); !errors.Is(err, os.ErrInvalid) {
		t.Fatalf("empty private path error = %v, want ErrInvalid", err)
	}
	regular := filepath.Join(root, "file")
	if err := os.WriteFile(regular, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := SetPrivateDirectoryPermissions(regular, 0o700); err == nil {
		t.Fatal("directory permissions accepted a regular file")
	}
	directory, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	if err := SetPrivateFilePermissions(directory, 0o600); err == nil {
		t.Fatal("file permissions accepted a directory")
	}
	if err := SetPrivateFilePermissions(nil, 0o600); err == nil {
		t.Fatal("file permissions accepted nil")
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	if err := SetPrivateFilePermissions(write, 0o600); err == nil {
		t.Fatal("file permissions accepted a pipe")
	}
	missing := filepath.Join(root, "missing")
	if err := SetPrivateDirectoryPermissions(missing, 0o700); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing directory error = %v", err)
	}
	if _, err := ReadPrivateFile(missing, 0o600); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing read error = %v", err)
	}
	if _, err := ReadPrivateFile(root, 0o600); err == nil {
		t.Fatal("private read accepted a directory")
	}
}

func TestPrivatePermissionsRejectReparsePoints(t *testing.T) {
	root := t.TempDir()
	targetDirectory := filepath.Join(root, "target-directory")
	if err := os.Mkdir(targetDirectory, 0o777); err != nil {
		t.Fatal(err)
	}
	targetFile := filepath.Join(root, "target-file")
	if err := os.WriteFile(targetFile, []byte("untouched"), 0o666); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{targetDirectory, targetFile} {
		t.Run(filepath.Base(target), func(t *testing.T) {
			link := target + "-link"
			testutil.Symlink(t, target, link)
			before := privateTestSecurity(t, target).String()
			if err := SetPrivateDirectoryPermissions(link, 0o700); err == nil {
				t.Fatal("directory permissions accepted a reparse point")
			}
			if _, err := ReadPrivateFile(link, 0o600); err == nil {
				t.Fatal("private read accepted a reparse point")
			}
			if got := privateTestSecurity(t, target).String(); got != before {
				t.Fatalf("reparse target changed: before %s, after %s", before, got)
			}
		})
	}
}

func TestPrivatePermissionsPreserveLongPaths(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, strings.Repeat("a", 100), strings.Repeat("b", 100), strings.Repeat("c", 100))
	if err := os.MkdirAll(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(parent, "checkpoints")
	if err := CreatePrivateDirectory(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := SetPrivateDirectoryPermissions(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	privateTestAssertDACL(t, directory, true, true)
	file, err := os.CreateTemp(directory, "checkpoint-*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := SetPrivateFilePermissions(file, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPrivateFile(file.Name(), 0o600); err != nil {
		t.Fatal(err)
	}
	privateTestAssertDACL(t, file.Name(), false, true)
}

func TestPrivatePermissionsRejectForeignOwners(t *testing.T) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{user.User.Sid.String(), "S-1-5-18", "S-1-5-32-544", "S-1-5-21-123-456-789-1001"} {
		t.Run(owner, func(t *testing.T) {
			// 実際のfileに他者のownerを設定するには特権が必要です。
			// 実際のWindows descriptorを使って、同じownerの判定を確かめます。
			sd, err := windows.SecurityDescriptorFromString("O:" + owner + "D:P(A;;FA;;;WD)")
			if err != nil {
				t.Fatal(err)
			}
			before := sd.String()
			err = validatePrivateOwner(sd, user.User.Sid)
			foreign := owner == "S-1-5-21-123-456-789-1001"
			if (err != nil) != foreign {
				t.Fatalf("owner validation error = %v, foreign = %t", err, foreign)
			}
			if got := sd.String(); got != before {
				t.Fatalf("owner validation mutated descriptor: before %s, after %s", before, got)
			}
		})
	}
}

func TestPrivatePermissionsPropagateAccessDeniedWithoutMutation(t *testing.T) {
	for _, directory := range []bool{false, true} {
		name := "file"
		if directory {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			if directory {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte("unchanged payload"), 0o600); err != nil {
				t.Fatal(err)
			}
			pathName, err := privateWindowsPath(path)
			if err != nil {
				t.Fatal(err)
			}
			// 後始末で権限を戻すため、この元のhandleを保持します。後でDACLを変更しても、
			// すでに付与されたhandleの権限は取り消されません。
			handle, err := windows.CreateFile(pathName, windows.READ_CONTROL|windows.WRITE_DAC,
				windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
				nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer windows.CloseHandle(handle)
			original := privateTestSecurity(t, path)
			originalACL, _, err := original.DACL()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := windows.SetSecurityInfo(handle, windows.SE_FILE_OBJECT,
					windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
					nil, nil, originalACL, nil); err != nil {
					t.Errorf("restore fixture permissions: %v", err)
				}
				runtime.KeepAlive(original)
			}()
			// OWNER RIGHTSがownerの暗黙の権限を明示的に制限するため、
			// 試験自身のaccountでもWRITE_DACで開き直せません。
			denied, err := windows.SecurityDescriptorFromString("D:P(D;;WD;;;WD)(A;;RC;;;OW)(A;;FA;;;WD)")
			if err != nil {
				t.Fatal(err)
			}
			deniedACL, _, err := denied.DACL()
			if err != nil {
				t.Fatal(err)
			}
			if err := windows.SetSecurityInfo(handle, windows.SE_FILE_OBJECT,
				windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
				nil, nil, deniedACL, nil); err != nil {
				t.Fatal(err)
			}
			runtime.KeepAlive(denied)
			before := privateTestSecurity(t, path).String()
			if directory {
				err = SetPrivateDirectoryPermissions(path, 0o700)
			} else {
				file, openErr := os.OpenFile(path, os.O_RDWR, 0)
				if openErr != nil {
					t.Fatal(openErr)
				}
				defer file.Close()
				err = SetPrivateFilePermissions(file, 0o600)
				if data, readErr := ReadPrivateFile(path, 0o600); !errors.Is(readErr, windows.ERROR_ACCESS_DENIED) || len(data) != 0 {
					t.Fatalf("private read = %q, error %v, want no data and access denied", data, readErr)
				}
				if data, readErr := os.ReadFile(path); readErr != nil || string(data) != "unchanged payload" {
					t.Fatalf("fixture contents = %q, error = %v", data, readErr)
				}
			}
			if !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
				t.Fatalf("permission setup error = %v, want access denied", err)
			}
			if got := privateTestSecurity(t, path).String(); got != before {
				t.Fatalf("failed permission setup changed DACL: before %s, after %s", before, got)
			}
		})
	}
}

func privateTestBroadDirectory(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	sd, err := windows.SecurityDescriptorFromString("D:P(A;OICI;FA;;;WD)")
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil); err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(sd)
	return path
}

func privateTestSecurity(t *testing.T, path string) *windows.SECURITY_DESCRIPTOR {
	t.Helper()
	name, err := privateWindowsPath(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(name, windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(handle)
	sd, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	return sd
}

func privateTestAssertDACL(t *testing.T, path string, directory, protected bool) {
	t.Helper()
	sd := privateTestSecurity(t, path)
	control, _, err := sd.Control()
	if err != nil {
		t.Fatal(err)
	}
	if protected && control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatalf("%q has unprotected DACL: %s", path, sd.String())
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{user.User.Sid.String(): false, "S-1-5-18": false, "S-1-5-32-544": false}
	dacl, _, err := sd.DACL()
	if err != nil || dacl == nil {
		t.Fatalf("%q has missing/null DACL: %v", path, err)
	}
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, index, &ace); err != nil {
			t.Fatal(err)
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || ace.Mask != privateTestFullAccess {
			t.Fatalf("%q unexpected ACE type/mask: %+v", path, ace)
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart)).String()
		if _, found := want[sid]; !found {
			t.Fatalf("%q grants unexpected principal %s: %s", path, sid, sd.String())
		}
		want[sid] = true
		flags := uint8(0)
		if directory {
			flags = windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE
		}
		if !protected {
			flags |= windows.INHERITED_ACE
		}
		if ace.Header.AceFlags != flags {
			t.Fatalf("%q ACE flags = %#x, want %#x", path, ace.Header.AceFlags, flags)
		}
	}
	for sid, present := range want {
		if !present {
			t.Fatalf("%q missing required principal %s", path, sid)
		}
	}
	runtime.KeepAlive(sd)
}

// application userと、許可listにある管理用の二つのprincipalを無効にします。
// tokenはEveryone/Usersを保持するため、権限の広い親での陽性対照により、拒否が
// 保存されたprivate DACLによるものだと証明できます。別のaccountや試験threadの
// impersonationは必要ありません。これは実際のWindowsのrestricted tokenです。
func privateTestUntrustedToken(t *testing.T) windows.Token {
	t.Helper()
	var process, impersonation windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_DUPLICATE|windows.TOKEN_QUERY, &process); err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	if err := windows.DuplicateTokenEx(process, windows.TOKEN_DUPLICATE|windows.TOKEN_QUERY,
		nil, windows.SecurityImpersonation, windows.TokenImpersonation, &impersonation); err != nil {
		t.Fatal(err)
	}
	defer impersonation.Close()
	user, err := process.GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		t.Fatal(err)
	}
	administrators, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		t.Fatal(err)
	}
	disabled := []windows.SIDAndAttributes{{Sid: user.User.Sid}, {Sid: system}, {Sid: administrators}}
	var restricted windows.Token
	create := windows.NewLazySystemDLL("advapi32.dll").NewProc("CreateRestrictedToken")
	result, _, callErr := create.Call(uintptr(impersonation), 1, /* DISABLE_MAX_PRIVILEGE */
		uintptr(len(disabled)), uintptr(unsafe.Pointer(&disabled[0])), 0, 0, 0, 0,
		uintptr(unsafe.Pointer(&restricted)))
	if result == 0 {
		t.Fatalf("CreateRestrictedToken: %v", callErr)
	}
	runtime.KeepAlive(disabled)
	t.Cleanup(func() { restricted.Close() })
	return restricted
}

func privateTestCheckAccess(t *testing.T, sd *windows.SECURITY_DESCRIPTOR, token windows.Token, want bool) {
	t.Helper()
	// 同じspecific bitが、fileのデータの読み取りとdirectoryの列挙を表します。
	mapping := [4]uint32{windows.FILE_GENERIC_READ, windows.FILE_GENERIC_WRITE, windows.FILE_GENERIC_EXECUTE, privateTestFullAccess}
	var privileges [1024]uint32
	privilegeBytes := uint32(unsafe.Sizeof(privileges))
	var granted, allowed uint32
	check := windows.NewLazySystemDLL("advapi32.dll").NewProc("AccessCheck")
	result, _, callErr := check.Call(uintptr(unsafe.Pointer(sd)), uintptr(token), windows.FILE_READ_DATA,
		uintptr(unsafe.Pointer(&mapping)), uintptr(unsafe.Pointer(&privileges)),
		uintptr(unsafe.Pointer(&privilegeBytes)), uintptr(unsafe.Pointer(&granted)), uintptr(unsafe.Pointer(&allowed)))
	if result == 0 {
		t.Fatalf("AccessCheck: %v", callErr)
	}
	if got := allowed != 0; got != want {
		t.Fatalf("untrusted read access = %t (mask %#x), want %t; security %s", got, granted, want, sd.String())
	}
	runtime.KeepAlive(sd)
}

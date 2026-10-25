//go:build windows

package fsstore

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var privateReOpenFile = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReOpenFile")

func isDirectoryReparsePoint(info os.FileInfo) bool {
	attributes, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return ok && attributes.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

// privateSecurityDescriptorは実際のeffective token user、SYSTEM、builtin
// Administratorsだけにfull controlを許可します。Unixの0750/0640にあるgroup-readを
// Windows UsersやEveryoneへ割り当てません。任意のreader groupは未設定です。
// protected DACLで共有parentの広いACEを継承させません。
func privateSecurityDescriptor(directory bool) (*windows.SECURITY_DESCRIPTOR, error) {
	user, err := windows.GetCurrentThreadEffectiveToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("query effective token user: %w", err)
	}
	flags := ""
	if directory {
		flags = "OICI"
	}
	return windows.SecurityDescriptorFromString("D:P" +
		"(A;" + flags + ";FA;;;" + user.User.Sid.String() + ")" +
		"(A;" + flags + ";FA;;;SY)" +
		"(A;" + flags + ";FA;;;BA)")
}

// CreatePrivateDirectoryはCreateDirectoryにDACLを渡し、作成時点から制限された
// app所有directoryを1つ作ります。modeはUnix用で、Windowsでは上記principalを使います。
// ACL対応のfilesystemを前提とし、作成後のACLの読み直しや再設定は行いません。
func CreatePrivateDirectory(path string, mode os.FileMode) error {
	// callerがWindows accountを偽装中でもtoken取得と作成を同じthreadで行います。
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	name, err := privateWindowsPath(path)
	if err != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: err}
	}
	sd, err := privateSecurityDescriptor(true)
	if err != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: err}
	}
	attributes := windows.SecurityAttributes{
		Length:             uint32(unsafe.Sizeof(windows.SecurityAttributes{})),
		SecurityDescriptor: sd,
	}
	if err := windows.CreateDirectory(name, &attributes); err != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: err}
	}
	return nil
}

// SetPrivateDirectoryPermissionsは指定したapp所有directoryだけを制限します。
// reparse pointを拒否し、共有parentや既存childの権限は変更しません。
// callerはDACLを継承するtemp fileを作る前に、このdirectoryを制限してください。
func SetPrivateDirectoryPermissions(path string, mode os.FileMode) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	name, err := privateWindowsPath(path)
	if err != nil {
		return &os.PathError{Op: "set private directory permissions", Path: path, Err: err}
	}
	// MAXIMUM_ALLOWEDでSetSecurityInfoによる既存childへのACE伝播を抑止します。
	// WRITE_DACだけを指定すると既存childのACLまで再帰的に変更します。
	// SetSecurityInfoのRemarks参照:
	// https://learn.microsoft.com/en-us/windows/win32/api/aclapi/nf-aclapi-setsecurityinfo
	handle, err := windows.CreateFile(name, windows.MAXIMUM_ALLOWED,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return &os.PathError{Op: "set private directory permissions", Path: path, Err: err}
	}
	defer windows.CloseHandle(handle)
	if err := setPrivateHandlePermissions(handle, true); err != nil {
		return &os.PathError{Op: "set private directory permissions", Path: path, Err: err}
	}
	return nil
}

// SetPrivateFilePermissionsはpayload書き込み前にopen済みregular fileを制限します。
// callerは既に制限したdirectory内にfileを作り、作成時もDACL継承で保護してください。
// os.CreateTempのread/write handleにはWRITE_DACがないため、ReOpenFileで同じobjectの
// 権限変更用handleを取得します。pathを再openして別fileへ差し替わることを防ぎます。
func SetPrivateFilePermissions(file *os.File, mode os.FileMode) error {
	if file == nil {
		return os.ErrInvalid
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	connection, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var reopened windows.Handle
	var reopenErr error
	controlErr := connection.Control(func(fd uintptr) {
		if reopenErr = validatePrivateHandle(windows.Handle(fd), false); reopenErr != nil {
			return
		}
		result, _, callErr := privateReOpenFile.Call(fd,
			uintptr(windows.WRITE_DAC|windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES),
			uintptr(windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE),
			uintptr(windows.FILE_FLAG_OPEN_REPARSE_POINT))
		if windows.Handle(result) == windows.InvalidHandle {
			reopenErr = callErr
			return
		}
		reopened = windows.Handle(result)
	})
	if controlErr != nil {
		return controlErr
	}
	if reopenErr != nil {
		return &os.PathError{Op: "reopen for private file permissions", Path: file.Name(), Err: reopenErr}
	}
	defer windows.CloseHandle(reopened)
	if err := setPrivateHandlePermissions(reopened, false); err != nil {
		return &os.PathError{Op: "set private file permissions", Path: file.Name(), Err: err}
	}
	return nil
}

// ReadPrivateFileは既存checkpointの権限を制限してから内容を読みます。
// Windowsでは通常directoryのtraverse検査を迂回できるため、parentだけ制限しても
// 広いDACLを持つ既存fileは保護できません。同じobjectをopen・検査・制限・読み取りします。
func ReadPrivateFile(path string, mode os.FileMode) ([]byte, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	name, err := privateWindowsPath(path)
	if err != nil {
		return nil, &os.PathError{Op: "read private file", Path: path, Err: err}
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.WRITE_DAC,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return nil, &os.PathError{Op: "read private file", Path: path, Err: err}
	}
	file := os.NewFile(uintptr(handle), path)
	defer file.Close()
	if err := setPrivateHandlePermissions(handle, false); err != nil {
		return nil, &os.PathError{Op: "read private file", Path: path, Err: err}
	}
	return io.ReadAll(file)
}

func setPrivateHandlePermissions(handle windows.Handle, directory bool) error {
	if err := validatePrivateHandle(handle, directory); err != nil {
		return err
	}
	if err := verifyPrivateOwner(handle); err != nil {
		return err
	}
	sd, err := privateSecurityDescriptor(directory)
	if err != nil {
		return err
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	if dacl == nil {
		return fmt.Errorf("private security descriptor has no DACL")
	}
	err = windows.SetSecurityInfo(handle, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil)
	runtime.KeepAlive(sd)
	if err != nil {
		return err
	}
	return verifyPrivateDACL(handle, sd)
}

func verifyPrivateOwner(handle windows.Handle) error {
	sd, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	user, err := windows.GetCurrentThreadEffectiveToken().GetTokenUser()
	if err != nil {
		return err
	}
	return validatePrivateOwner(sd, user.User.Sid)
}

func validatePrivateOwner(sd *windows.SECURITY_DESCRIPTOR, user *windows.SID) error {
	owner, _, err := sd.Owner()
	if err != nil {
		return err
	}
	if owner == nil {
		return fmt.Errorf("private permissions require an owner")
	}
	// DACLから削除してもownerは暗黙のWRITE_DACを持ちます。別account所有の
	// 既存objectは権限を変更せず拒否し、所有権を奪う操作も行いません。
	if !owner.Equals(user) && owner.String() != "S-1-5-18" && owner.String() != "S-1-5-32-544" {
		return fmt.Errorf("private permissions refuse untrusted owner %s", owner.String())
	}
	runtime.KeepAlive(sd)
	return nil
}

func verifyPrivateDACL(handle windows.Handle, expected *windows.SECURITY_DESCRIPTOR) error {
	actual, err := windows.GetSecurityInfo(handle, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	control, _, err := actual.Control()
	if err != nil {
		return err
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		return fmt.Errorf("filesystem did not protect the private DACL")
	}
	actualACL, _, err := actual.DACL()
	if err != nil {
		return err
	}
	expectedACL, _, err := expected.DACL()
	if err != nil {
		return err
	}
	if actualACL == nil || expectedACL == nil || actualACL.AceCount != expectedACL.AceCount {
		return fmt.Errorf("filesystem did not preserve the private DACL")
	}
	for index := uint32(0); index < uint32(expectedACL.AceCount); index++ {
		var got, want *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(actualACL, index, &got); err != nil {
			return err
		}
		if err := windows.GetAce(expectedACL, index, &want); err != nil {
			return err
		}
		gotSID := (*windows.SID)(unsafe.Pointer(&got.SidStart))
		wantSID := (*windows.SID)(unsafe.Pointer(&want.SidStart))
		if got.Header.AceType != want.Header.AceType || got.Header.AceFlags != want.Header.AceFlags ||
			got.Mask != want.Mask || !gotSID.Equals(wantSID) {
			return fmt.Errorf("filesystem did not preserve private DACL entry %d", index)
		}
	}
	runtime.KeepAlive(actual)
	runtime.KeepAlive(expected)
	return nil
}

func validatePrivateHandle(handle windows.Handle, directory bool) error {
	kind, err := windows.GetFileType(handle)
	if err != nil {
		return err
	}
	if kind != windows.FILE_TYPE_DISK {
		return fmt.Errorf("private permissions require a disk file or directory")
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return fmt.Errorf("private permissions refuse reparse points")
	}
	if isDirectory := info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0; isDirectory != directory {
		return fmt.Errorf("private permissions require directory=%t", directory)
	}
	return nil
}

// Win32直接呼び出しでもos同様に長いpathへ対応します。既存のdevice/extended prefixを
// 維持し、長い絶対DOS/UNC pathだけを呼び出し前に変換します。
func privateWindowsPath(path string) (*uint16, error) {
	if path == "" {
		return nil, os.ErrInvalid
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	absolute = pathidentity.AddWindowsLongPathPrefix(absolute)
	return windows.UTF16PtrFromString(absolute)
}

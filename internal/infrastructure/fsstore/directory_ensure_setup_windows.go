//go:build windows

package fsstore

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// Windowsの既存ディレクトリのACLは変更しません。FileModeはACLを表さないため、
// 管理範囲内でも権限を狭める対象として扱いません。
func directoryPermissionsExceed(os.FileInfo, os.FileMode) bool { return false }

func restrictDirectoryPermissions(string, os.FileInfo, os.FileMode) error { return nil }

// filesystem名ではなく、Windowsが返すACL保持能力を初期化時だけ確認します。
// 個々のディレクトリのACLを読み直したり、変更したりする検査ではありません。
func validateDirectorySetupFilesystem(directory string) error {
	return validateDirectorySetupFilesystemWithQuery(directory, inspectDirectorySetupFilesystem)
}

func validateDirectorySetupFilesystemWithQuery(directory string, query func(string) (string, uint32, error)) error {
	filesystem, flags, err := query(directory)
	if err != nil {
		return &DirectoryFilesystemError{Directory: directory, Err: err}
	}
	if flags&windows.FILE_PERSISTENT_ACLS == 0 {
		return &DirectoryFilesystemError{Directory: directory, Filesystem: filesystem, Unsupported: true, Err: errors.ErrUnsupported}
	}
	return nil
}

// DirectoryFilesystemErrorは、作成前に確認した保存先filesystemを調べられなかったか、
// ACLを保持できないことを表します。
type DirectoryFilesystemError struct {
	Directory string
	// Filesystemは照会できたfilesystem名です。照会に失敗した場合は空です。
	Filesystem string
	// Unsupportedは、照会したfilesystemがACLを保持できないことを表します。
	// この場合、Errはerrors.ErrUnsupportedです。
	Unsupported bool
	Err         error
}

func (e *DirectoryFilesystemError) Error() string {
	if e.Unsupported {
		return fmt.Sprintf("directory %q filesystem %q does not support persistent ACLs: %v", e.Directory, e.Filesystem, e.Err)
	}
	return fmt.Sprintf("inspect directory filesystem %q: %v", e.Directory, e.Err)
}

func (e *DirectoryFilesystemError) Unwrap() error {
	return e.Err
}

func inspectDirectorySetupFilesystem(directory string) (filesystem string, flags uint32, err error) {
	name, err := privateWindowsPath(directory)
	if err != nil {
		return "", 0, err
	}
	// ACLの読み取り権限は要求せず、volume情報を得るためだけにdirectoryを開きます。
	// 親のリンクを許す通常版ではリンク先のvolumeを調べます。
	handle, err := windows.CreateFile(name, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return "", 0, fmt.Errorf("open directory for filesystem inspection: %w", err)
	}
	defer func() {
		if closeErr := windows.CloseHandle(handle); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close directory filesystem inspection handle: %w", closeErr))
		}
	}()
	var filesystemName [windows.MAX_PATH + 1]uint16
	if err := windows.GetVolumeInformationByHandle(handle, nil, 0, nil, nil, &flags,
		&filesystemName[0], uint32(len(filesystemName))); err != nil {
		return "", 0, fmt.Errorf("query directory volume information: %w", err)
	}
	return windows.UTF16ToString(filesystemName[:]), flags, nil
}

func finishDirectorySetup(directory string, _ os.FileMode, _ bool) error {
	// Windowsの権限は作成時のACLで設定し、既存ACLは変更しません。
	// 通常版でも保存先自身のreparse pointは拒否します。ACLの読み戻しは行いません。
	info, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect directory %q: %w", directory, err)
	}
	if !info.IsDir() || isDirectoryReparsePoint(info) {
		return &DirectoryObstructionError{Path: directory, message: fmt.Sprintf("directory path %q is not a real directory", directory)}
	}
	return nil
}

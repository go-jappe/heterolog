//go:build windows

package pathidentity

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

func isDirectoryLink(info os.FileInfo) bool {
	if info.Mode()&os.ModeSymlink != 0 {
		return true
	}
	// 通常のsymlink以外にも、junctionなどのreparse pointを管理用directoryに使わせません。
	attributes, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return ok && attributes.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

func resolveExistingDirectory(directory string, _ os.FileInfo) (string, error) {
	// CreateFileを直接呼ぶため、Goのfile操作と同様に長いdrive/UNC pathへ
	// extended prefixを付けます。既存のdevice/extended表記は維持します。
	path := AddWindowsLongPathPrefix(directory)
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return "", fmt.Errorf("resolve storage directory %q: %w", directory, err)
	}
	// OPEN_REPARSE_POINTを指定せず、symlink/junctionの参照先を開きます。
	// directory属性の確認だけを要求し、書き込み・削除・ACLの変更は行いません。
	handle, err := windows.CreateFile(name, windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return "", fmt.Errorf("open storage directory target %q: %w", directory, err)
	}
	defer windows.CloseHandle(handle)
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return "", fmt.Errorf("inspect storage directory target %q: %w", directory, err)
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
		return "", fmt.Errorf("storage directory path %q is not a directory", directory)
	}
	buffer := make([]uint16, 256)
	for {
		// flags=0はFILE_NAME_NORMALIZED | VOLUME_NAME_DOSです。
		// 解決できない場合は開いた表記へfallbackせず、callerへ返します。
		n, err := windows.GetFinalPathNameByHandle(handle, &buffer[0], uint32(len(buffer)), 0)
		if err != nil {
			return "", fmt.Errorf("resolve storage directory target %q: %w", directory, err)
		}
		if n < uint32(len(buffer)) {
			return windowsDirectoryOperationPath(windows.UTF16ToString(buffer[:n])), nil
		}
		buffer = make([]uint16, n+1)
	}
}

// windowsDirectoryOperationPathはAPIの通常drive/UNC結果をfile操作の標準表記へ戻します。
// 大小文字を変えず、volume GUIDなど別のdevice namespaceはそのまま保持します。
func windowsDirectoryOperationPath(path string) string {
	if len(path) >= 8 && strings.EqualFold(path[:8], `\\?\UNC\`) {
		return `\\` + path[8:]
	}
	if len(path) >= 7 && strings.HasPrefix(path, `\\?\`) &&
		((path[4] >= 'a' && path[4] <= 'z') || (path[4] >= 'A' && path[4] <= 'Z')) &&
		path[5] == ':' && path[6] == '\\' {
		return path[4:]
	}
	return path
}

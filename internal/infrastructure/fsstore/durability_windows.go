//go:build windows

package fsstore

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func openDirectoryForSync(directoryPath string) (*os.File, error) {
	path, err := filepath.Abs(directoryPath)
	if err != nil {
		return nil, err
	}
	// CreateFileを直接呼ぶため、os.Openと同様に長いpathへ対応します。
	path = pathidentity.AddWindowsLongPathPrefix(path)
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	// os.OpenはGENERIC_READで開くため、WindowsのFile.Syncが使用する
	// FlushFileBuffersに必要なGENERIC_WRITEを持ちません。O_RDWRでもGoは
	// directoryのopenを拒否するため、directory用flagと共に直接取得します。
	handle, err := windows.CreateFile(name, windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, err
	}
	// 所有権をos.Fileへ渡し、同期とCloseは他OSと同じ共通経路で実行します。
	return os.NewFile(uintptr(handle), directoryPath), nil
}

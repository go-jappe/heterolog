//go:build !windows

package fsstore

import (
	"fmt"
	"os"
)

func validateDirectorySetupFilesystem(string) error { return nil }

// directoryPermissionsExceedは、既存ディレクトリにmodeを超える権限があるかを返します。
func directoryPermissionsExceed(info os.FileInfo, mode os.FileMode) bool {
	return info.Mode().Perm()&^mode.Perm() != 0
}

// restrictDirectoryPermissionsは、modeを超える権限だけを外します。所有者などmode内の
// 既存の制限と、setgidなどの特殊bitは維持します。
func restrictDirectoryPermissions(directory string, info os.FileInfo, mode os.FileMode) error {
	special := info.Mode() & (os.ModeSetuid | os.ModeSetgid | os.ModeSticky)
	return SetPrivateDirectoryPermissions(directory, special|info.Mode().Perm()&mode.Perm())
}

func finishDirectorySetup(directory string, mode os.FileMode, setDestinationPermissions bool) error {
	if !setDestinationPermissions {
		return nil
	}
	// 作成時のmode・umaskとは別に、指定された保存先自身にだけmodeを設定します。
	if err := SetPrivateDirectoryPermissions(directory, mode); err != nil {
		return fmt.Errorf("set directory %q permissions: %w", directory, err)
	}
	return nil
}

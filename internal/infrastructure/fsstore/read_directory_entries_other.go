//go:build !windows

package fsstore

import "os"

// ReadDirectoryEntriesはos.ReadDirと同じく、名前順のentry一覧を返します。
// Windowsのhandle共有modeへの対応は不要なので、Unixでは既存の読込処理を使います。
// macOSでは、同じ一覧に対象が含まれるAppleDoubleの付随file（「._」+対象の名前）を除きます。
func ReadDirectoryEntries(path string) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(path)
	return withoutAppleDoubleCompanions(entries), err
}

//go:build windows

package fsstore

import (
	"os"
	"slices"
	"strings"
)

// ReadDirectoryEntriesはos.ReadDirと同じく、名前順のentry一覧を返します。
// WindowsではOpenDirectoryForReadで権限設定用handleとの共有を許可します。
func ReadDirectoryEntries(path string) ([]os.DirEntry, error) {
	file, err := OpenDirectoryForRead(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries, err := file.ReadDir(-1)
	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})
	return entries, err
}

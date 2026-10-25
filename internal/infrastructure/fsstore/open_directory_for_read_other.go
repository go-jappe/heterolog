//go:build !windows

package fsstore

import "os"

// OpenDirectoryForReadはReadDir(n)による分割読込用にdirectoryを開きます。
// 戻り値のfileは呼出側でCloseする必要があります。
func OpenDirectoryForRead(path string) (*os.File, error) {
	return os.Open(path)
}

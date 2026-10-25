package fsstore

import (
	"errors"
	"os"
)

// ErrNotRegularFileは、指定されたパス自身がリンク・ディレクトリなどの通常ファイル以外であることを表します。
var ErrNotRegularFile = errors.New("path is not a regular file")

// InspectRegularFileは、Lstatでパス自身が通常ファイルかを確認し、その情報を返します。
// リンク先は検査せずErrNotRegularFileを返します。不在を含むLstatのエラーはそのまま返し、
// 不在を許容するかは呼び出し側が判断します。ファイルを開いたり内容を検証したりはしません。
func InspectRegularFile(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, newNotRegularFileError(path, info.Mode(), "")
	}
	return info, nil
}

package fsstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// EqualRegularFileContentsは、symlinkを辿らず二つの通常fileの内容をbyte単位で比較します。
// 読取失敗は不一致ではなくerrorとして返し、一致を返す前にsize・更新時刻も再確認します。
// 呼び出し元は比較から後続のrename・削除までの排他を保持し、対象の変更を防いでください。
// 読み取りだけのため、Windowsでは別の処理がrename中に保持するDELETE accessと共存して開きます。
func EqualRegularFileContents(ctx context.Context, firstPath, secondPath string) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	first, firstInfo, err := openRegularFile(firstPath, os.O_RDONLY, openFileSharingDelete)
	if err != nil {
		return false, err
	}
	defer first.Close()
	second, secondInfo, err := openRegularFile(secondPath, os.O_RDONLY, openFileSharingDelete)
	if err != nil {
		return false, err
	}
	defer second.Close()
	if firstInfo.Size() != secondInfo.Size() {
		return false, nil
	}

	firstBuffer := make([]byte, 128*1024)
	secondBuffer := make([]byte, len(firstBuffer))
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		firstRead, firstErr := io.ReadFull(first, firstBuffer)
		firstDone := errors.Is(firstErr, io.EOF) || errors.Is(firstErr, io.ErrUnexpectedEOF)
		if firstErr != nil && !firstDone {
			return false, firstErr
		}
		secondRead, secondErr := io.ReadFull(second, secondBuffer)
		secondDone := errors.Is(secondErr, io.EOF) || errors.Is(secondErr, io.ErrUnexpectedEOF)
		if secondErr != nil && !secondDone {
			return false, secondErr
		}
		if firstRead != secondRead || !bytes.Equal(firstBuffer[:firstRead], secondBuffer[:secondRead]) {
			return false, nil
		}
		if firstDone || secondDone {
			if !firstDone || !secondDone {
				return false, nil
			}
			break
		}
	}
	for _, opened := range []struct {
		file *os.File
		info os.FileInfo
	}{
		{file: first, info: firstInfo},
		{file: second, info: secondInfo},
	} {
		after, err := opened.file.Stat()
		if err != nil {
			return false, err
		}
		if after.Size() != opened.info.Size() || !after.ModTime().Equal(opened.info.ModTime()) {
			return false, errors.New("file changed while comparing")
		}
	}
	return true, nil
}

// OpenRegularFileはsymlink・directoryなどを拒否して既存の通常fileを開きます。
// flagは読取用ならos.O_RDONLY、metadataのSyncが必要ならos.O_RDWRを渡します。
// WindowsのSyncは書き込み権限を要求します。作成・切り詰めのflagは指定しません。
// LstatとOpenの間の差し替えをinodeで確認し、成功時のfileは呼び出し元がCloseします。
func OpenRegularFile(path string, flag int) (*os.File, os.FileInfo, error) {
	return openRegularFile(path, flag, os.OpenFile)
}

// openRegularFileは、OpenRegularFileの検査を指定したopen関数で行います。
func openRegularFile(path string, flag int, open func(string, int, os.FileMode) (*os.File, error)) (*os.File, os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, newNotRegularFileError(path, info.Mode(), fmt.Sprintf("path %q is not a regular file", path))
	}
	file, err := open(path, flag, 0)
	if err != nil {
		return nil, nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return nil, nil, fmt.Errorf("path %q changed while opening", path)
	}
	return file, openedInfo, nil
}

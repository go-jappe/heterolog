package fsstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// RemoveFileAndSyncDirectoryはファイルを削除し、親ディレクトリを同期します。
// すでに不存在でも、以前の削除後に同期だけ失敗した可能性があるため同期します。
// 不存在以外の削除エラーでは同期せず返します。同期失敗も成功として扱いません。
// pkgfsstore.ErrDurabilityUnconfirmedに一致する場合、ファイルは削除済み（または既に不存在）で、
// 親の同期だけが未確認です。呼び出し元では削除失敗と分けて扱えます。
// 削除可否・ファイル種別の確認、必要なlock、結果の記録・通知は呼び出し元の責務です。
func RemoveFileAndSyncDirectory(path string) error {
	removalErr, directorySyncErr := removeFileAndSyncDirectory(path, SyncDirectory)
	return errors.Join(removalErr, directorySyncErr)
}

// removeFileAndSyncDirectoryは、削除の結果を後続の同期・Closeのエラーと分けて返します。
// syncDirectoryは削除成功または不存在の確認後に一度だけ呼びます。
func removeFileAndSyncDirectory(path string, syncDirectory func(string) error) (removalErr, directorySyncErr error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &FileRemovalError{Step: FileRemovalStepRemove, Path: path, Err: err}, nil
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return nil, &FileRemovalError{Step: FileRemovalStepSyncAfterRemove, Path: path, Err: err}
	}
	return nil, nil
}

// RemoveRegularFileAndSyncDirectoryは、リンクでない通常ファイルを削除し、親ディレクトリを
// 同期します。削除対象かの判断と他の処理との排他は呼び出し元が行います。
// removal.Errは検査・削除の結果です。nilなら削除済み、または既に不存在です。
// 通常ファイルでないpathの拒否はErrNotRegularFileをwrapし、errors.Isで判別できます。
// directorySyncErrはその後の同期・Closeのエラーだけを返し、削除失敗とは区別します。
// 検査・削除に失敗した場合は同期せず、directorySyncErrはnilです。
func RemoveRegularFileAndSyncDirectory(path string) (removal FileRemovalResult, directorySyncErr error) {
	removal.File.Path = path

	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if syncErr := SyncDirectory(filepath.Dir(path)); syncErr != nil {
				directorySyncErr = &FileRemovalError{Step: FileRemovalStepSyncAfterAbsent, Path: path, Err: syncErr}
			}
			return removal, directorySyncErr
		}
		removal.Err = &FileRemovalError{Step: FileRemovalStepInspect, Path: path, Err: err}
		return removal, nil
	}
	if !info.Mode().IsRegular() {
		removal.Err = newNotRegularFileError(path, info.Mode(),
			fmt.Sprintf("file removal path %q is not a regular file: mode=%s", path, info.Mode()))
		return removal, nil
	}
	removal.File.Size = uint64(info.Size())
	removal.File.ModTime = info.ModTime()

	removal.Err, directorySyncErr = removeFileAndSyncDirectory(path, SyncDirectory)
	return removal, directorySyncErr
}

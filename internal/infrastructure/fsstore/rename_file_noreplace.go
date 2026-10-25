package fsstore

import "os"

// RenameFileNoReplaceは既存のdestinationPathを置換せずにfilesystemのエントリを移動します。
// 通常ファイル・directory・symlink等を受け付け、リンク先をたどらず名前そのものを移します。
// 成功するとsourcePathの名前が消えます。移動先にfile、directory、symlinkが存在する
// 場合はerrors.Is(err, fs.ErrExist)で判定できるerrorを返します。
// OS nativeの非置換移動を使い、異なる
// filesystemやvolumeへのcopyは行いません。未対応OSでは移動せずerrors.ErrUnsupportedを
// 返します。fileやdirectoryのsyncは行わず、durabilityの保証はcallerが担当します。
func RenameFileNoReplace(sourcePath, destinationPath string) error {
	if err := renameFileNoReplace(sourcePath, destinationPath); err != nil {
		return &os.LinkError{Op: "rename without replacement", Old: sourcePath, New: destinationPath, Err: err}
	}
	return nil
}

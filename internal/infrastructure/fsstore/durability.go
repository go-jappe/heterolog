package fsstore

import (
	"errors"
	"fmt"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// SyncDirectoryは、directoryPath自身の変更をfilesystemへ同期要求します。
// 呼び出し元で検証・確定済みの同期対象ディレクトリを渡してください。
// ファイルの作成・rename・削除を同期する場合は、その親ディレクトリを渡します。
// ファイル本体の同期は、この関数では行いません。
// Unix系ではfsync、Windowsでは書き込み権限付きdirectory handleに対する
// FlushFileBuffersを使用します。同期エラーは成功扱いにせず返します。
// 同期用handleの取得を含めて同期を完了できなければ、原因errorとともに
// pkgfsstore.ErrDurabilityUnconfirmedを返し、archive処理のreportでも判別可能にします。
// 永続化の保証範囲はfilesystemとstorageの同期実装に依存します。
func SyncDirectory(directoryPath string) error {
	directory, err := openDirectoryForSync(directoryPath)
	if err != nil {
		return fmt.Errorf("%w: open directory %q for sync: %w", pkgfsstore.ErrDurabilityUnconfirmed, directoryPath, err)
	}

	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return fmt.Errorf("%w: sync directory %q: %w", pkgfsstore.ErrDurabilityUnconfirmed, directoryPath, errors.Join(syncErr, closeErr))
	}
	if closeErr != nil {
		return fmt.Errorf("close directory %q after sync: %w", directoryPath, closeErr)
	}
	return nil
}

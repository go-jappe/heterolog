package objectstoragespool

import (
	"context"
	"fmt"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

// ConfirmSavedFileは、この宛先のspoolからsourcePathと同名・同内容の退避済みファイルを確認します。
// 呼び出し側がcheckpointに途中経過を持たない場合、重複送信を減らすため送信前に使います。
// 検索対象はactive・processing・done・dead・quarantineです。同内容の一件について、更新時刻を
// 現在時刻へ変更し、ファイルと親ディレクトリを同期してfound=trueを返します。更新時刻の変更は、
// 退避直後のpurge保護期間の起点を更新するためです。新しいコピーは作らず、見つからなければfound=falseです。
// 別内容の既存ファイルと、採用しなかった同内容の重複ファイルは、再送・purge対象外の
// .preservedへ移し、移動結果を戻り値のConflictRenamesで返します。
// 同名のdirectory・symlink等も、そのエントリ自体を保全してから検索を続けます。
// processing内の別内容ファイルは再送中の可能性があるため、移動せずエラーを返します。
// 呼び出し元は保存・再送の排他を取り、通知と送信の判断に戻り値を使ってください。
func (s *Store) ConfirmSavedFile(ctx context.Context, sourcePath string) (file CompressedLogFile, found bool, err error) {
	if s == nil {
		return CompressedLogFile{}, false, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if err := contextError(ctx); err != nil {
		return CompressedLogFile{}, false, err
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return CompressedLogFile{}, false, err
	}
	defer s.operationMu.Unlock()
	fileName := filepath.Base(sourcePath)
	if err := validateCompressedLogFileName(s.root.Path(), fileName); err != nil {
		return CompressedLogFile{}, false, err
	}
	if err := validateRegularSource(sourcePath); err != nil {
		return CompressedLogFile{}, false, err
	}
	if err := s.EnsureLayout(ctx); err != nil {
		return CompressedLogFile{}, false, err
	}
	var conflictRenames []infrafsstore.ConflictRename
	defer func() {
		file.ConflictRenames = append(file.ConflictRenames, conflictRenames...)
	}()
	return s.adoptOrPreserveExistingFiles(ctx, sourcePath, fileName, &conflictRenames)
}

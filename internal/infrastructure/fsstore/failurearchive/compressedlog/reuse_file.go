package compressedlogarchive

import (
	"context"
	"fmt"
)

// ReuseExistingFileはretryable・permanentの既存ファイルを探し、元ファイルと内容が
// 一致すれば同期して再利用します。別内容を確認した場合は旧ファイルを予約名へ
// 退避し、ConflictRenamesで返します。見つからなければfound=falseを返し、
// 新規保存はしません。戻った後もpurgeを防ぐ必要がある呼び出し元は、
// 保存時と同じ上位のlockを保持してください。
func (s *Store) ReuseExistingFile(ctx context.Context, sourcePath string) (file ArchivedFile, found bool, err error) {
	if s == nil {
		return ArchivedFile{}, false, fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return ArchivedFile{}, false, err
	}
	defer s.operationMu.Unlock()
	file, found, renamed, err := s.findOrPreserveExistingArchivedFiles(ctx, SaveInput{SourcePath: sourcePath})
	file.ConflictRenames = renamed
	if err != nil || !found {
		return file, false, err
	}
	if err := s.reuseEquivalentArchivedFile(ctx, file.Path); err != nil {
		return file, false, err
	}
	return file, true, nil
}

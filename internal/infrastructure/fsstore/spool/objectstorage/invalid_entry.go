package objectstoragespool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// preserveInvalidEntryLockedは、不正名や非通常ファイルのエントリを.preservedへ
// 隔離し、元パス・保全先・理由を警告として保持します。リンク先はたどらず、
// ディレクトリは中身ごと移します。呼び出し元はoperationMuを保持し、
// observedには走査時にLstatで取得した、そのエントリ自身の情報を渡してください。
func (s *Store) preserveInvalidEntryLocked(ctx context.Context, state pkgspool.State, fileName string, observed os.FileInfo, reason error) (infrafsstore.ConflictRename, error) {
	sourceDirectory, err := s.root.StateDir(state)
	if err != nil {
		return infrafsstore.ConflictRename{}, err
	}
	sourcePath := filepath.Join(sourceDirectory, fileName)
	current, err := os.Lstat(sourcePath)
	if err != nil {
		return infrafsstore.ConflictRename{}, err
	}
	if !os.SameFile(observed, current) {
		return infrafsstore.ConflictRename{}, fmt.Errorf("%w: invalid entry %q changed before preservation", ErrInvalidCompressedLogPath, sourcePath)
	}
	rename, err := s.preserveStateEntryLocked(ctx, state, fileName)
	if rename.RetainedPath != "" {
		// renameが成功した後のdirectory同期失敗でも、管理者へ移動先を通知します。
		s.preservationWarnings = append(s.preservationWarnings, &InvalidEntryPreservationWarning{
			SourcePath: rename.OriginalPath, PreservedPath: rename.RetainedPath,
			Destination: s.root.DestinationID(), Err: reason,
		})
	}
	return rename, err
}

// takePreservationWarningsは、保全時に記録した警告を一度だけ取り出します。
// 通知先は呼び出さず、呼び出し元がfilesystem操作の終了後にOnWarnへ渡します。
func (s *Store) takePreservationWarnings(ctx context.Context) ([]error, error) {
	if err := s.operationMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer s.operationMu.Unlock()
	warnings := s.preservationWarnings
	s.preservationWarnings = nil
	return warnings, nil
}

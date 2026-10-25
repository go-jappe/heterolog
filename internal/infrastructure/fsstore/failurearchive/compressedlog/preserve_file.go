package compressedlogarchive

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

// PreserveFileはsourceの内容を通常のarchive・purge対象外の.preservedへ保存します。
// sourceや既存の分類別archiveは変更せず、同じ名前・内容の退避済みfileは再利用します。
// 通常の分類directoryは初期化せず、退避先だけを必要時に作成します。
func (s *Store) PreserveFile(ctx context.Context, sourcePath string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return "", err
	}
	defer s.operationMu.Unlock()
	if sourcePath == "" {
		return "", fmt.Errorf("%w: source path is empty", ErrInvalidCompressedLogPath)
	}
	// 保全先は同じroot配下なので、rootの大小文字の規則で名前を検証します。
	if _, err := compressedLogFileSuffix(s.root.Path(), filepath.Base(sourcePath)); err != nil {
		return "", err
	}
	return fsstore.PreserveRegularFile(ctx, s.root.Path(), sourcePath)
}

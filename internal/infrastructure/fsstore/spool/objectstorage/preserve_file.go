package objectstoragespool

import (
	"context"
	"fmt"
	"path/filepath"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

// PreserveFileはsourceのコピーをroot直下の.preservedへ保全します。
// 通常stateのfileは移動せず、保全copyは再送・terminal purgeの対象外です。
// sourceは残し、同basename・同内容の再試行は同じ保全先を再利用します。
func (s *Store) PreserveFile(ctx context.Context, sourcePath string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("%w: store is nil", ErrInvalidRoot)
	}
	if err := s.operationMu.LockContext(ctx); err != nil {
		return "", err
	}
	defer s.operationMu.Unlock()
	// 保全先は同じroot配下なので、rootの大小文字の規則で名前を検証します。
	if err := validateCompressedLogFileName(s.root.Path(), filepath.Base(sourcePath)); err != nil {
		return "", err
	}
	return infrafsstore.PreserveRegularFile(ctx, s.root.Path(), sourcePath)
}

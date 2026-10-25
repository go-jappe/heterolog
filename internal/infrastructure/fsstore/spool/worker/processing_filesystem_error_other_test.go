//go:build !windows

package worker

import (
	"path/filepath"
	"testing"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/testutil"
)

// processingPathWithLstatFailureは、親directoryのsymlink loopでLstatが失敗するspool fileの
// pathを返します。loopは実行userの権限に依存せずELOOPになるため、rootでも失敗を再現できます。
func processingPathWithLstatFailure(t *testing.T, dir string) string {
	t.Helper()
	testutil.Symlink(t, "loop", filepath.Join(dir, "loop"))
	return filepath.Join(dir, "loop", "blocked"+spooldir.SpoolExt)
}

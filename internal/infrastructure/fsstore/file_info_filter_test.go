package fsstore

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestExcludeFileInfosByPathsUsesOperationPaths(t *testing.T) {
	dir := t.TempDir()
	protectedPath := filepath.Join(dir, "active.log")
	unprotectedPath := filepath.Join(dir, "inactive.log")
	files := []FileInfo{
		{Path: protectedPath},
		{Path: unprotectedPath},
	}
	protected := map[string]struct{}{
		pathidentity.AbsoluteOrCleanPath(protectedPath): {},
	}
	got := ExcludeFileInfosByPaths(files, protected)
	if len(got) != 1 || got[0].Path != unprotectedPath {
		t.Fatalf("ExcludeFileInfosByPaths() = %v, want only %q", got, unprotectedPath)
	}
}

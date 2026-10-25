package worker

import (
	"context"
	"path/filepath"
	"testing"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
)

// macOSのFAT32・ExFATでは、拡張属性を保存する「._」の付随fileがspool fileの隣に作られ、spool fileと
// 同じ拡張子を持ちます。APFSは付随fileを作らないため、同じ名前のfileを置いて再現します。
func TestProcessingCandidatesIgnoreAppleDoubleCompanions(t *testing.T) {
	t.Parallel()
	store := newCandidateTestStore(t)
	spoolPath := filepath.FromSlash("2026/01/01/00/00/a") + spooldir.SpoolExt
	writeCandidateTestFile(t, store, spoolPath)
	writeCandidateTestFile(t, store, filepath.Join(filepath.Dir(spoolPath), "._"+filepath.Base(spoolPath)))

	candidates, err := store.ListProcessingFileCandidatesAfter(context.Background(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].RelativePath != spoolPath {
		t.Fatalf("candidates = %v, want only %q", candidates, spoolPath)
	}
}

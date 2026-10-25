package spoolstorehandler

import (
	"path/filepath"
	"testing"
	"time"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
)

// macOSのFAT32・ExFATでは、拡張属性を保存する「._」の付随fileがfileやdirectoryの隣に作られます。
// APFSは付随fileを作らないため、同じ名前のfileを置いて再現します。付随fileは、対象より古く大きくても
// 件数・容量・最古更新時刻に含めません。
func TestScanDirStatsIgnoresAppleDoubleCompanions(t *testing.T) {
	t.Parallel()

	rootDir, err := spooldir.NewRootDir(t.TempDir(), "test-app", "test-backend")
	if err != nil {
		t.Fatalf("failed to create root dir: %v", err)
	}
	root := rootDir.ActiveDir()
	rootPath := root.Path()
	oldest := time.Date(2026, 6, 7, 10, 0, 0, 0, time.UTC)
	companionTime := oldest.Add(-time.Hour)
	companionBody := make([]byte, 4096)

	writeFileWithModTime(t, filepath.Join(rootPath, "old.log"), []byte("12345"), oldest)
	writeFileWithModTime(t, filepath.Join(rootPath, "._old.log"), companionBody, companionTime)
	writeFileWithModTime(t, filepath.Join(rootPath, "nested", "new.log"), []byte("123"), oldest.Add(time.Hour))
	writeFileWithModTime(t, filepath.Join(rootPath, "nested", "._new.log"), companionBody, companionTime)
	writeFileWithModTime(t, filepath.Join(rootPath, "._nested"), companionBody, companionTime)

	stats, err := scanDirStats(root)
	if err != nil {
		t.Fatalf("scanDirStats returned error: %v", err)
	}
	if stats.FileCount != 2 {
		t.Fatalf("FileCount = %d, want 2", stats.FileCount)
	}
	if stats.TotalSize != 8 {
		t.Fatalf("TotalSize = %d, want 8", stats.TotalSize)
	}
	if !stats.OldestModTime.Equal(oldest) {
		t.Fatalf("OldestModTime = %s, want %s", stats.OldestModTime, oldest)
	}
}

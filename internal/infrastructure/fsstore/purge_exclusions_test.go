package fsstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPurgeByDirectorySizeFromFileNameIfNeededCountsExcludedFilesButDoesNotDeleteThem(t *testing.T) {
	dir := t.TempDir()
	protectedPath := filepath.Join(dir, "active.log")
	inactivePath := filepath.Join(dir, "app_20260827_1.log")
	if err := os.WriteFile(protectedPath, make([]byte, 8), 0o600); err != nil {
		t.Fatalf("write protected file: %v", err)
	}
	if err := os.WriteFile(inactivePath, make([]byte, 5), 0o600); err != nil {
		t.Fatalf("write inactive file: %v", err)
	}

	now := time.Now()
	files := []FileInfo{
		{Path: protectedPath, Size: 8, ModTime: now.Add(-2 * time.Hour)},
		{Path: inactivePath, Size: 5, ModTime: now.Add(-time.Hour)},
	}
	protected := map[string]struct{}{filepath.Clean(protectedPath): {}}

	result := PurgeByDirectorySizeFromFileNameIfNeeded(
		files,
		10,
		protected,
	)
	if !result.Ran {
		t.Fatal("directory-size purge did not run")
	}
	if len(result.RemovalResults) != 1 || result.RemovalResults[0].File.Path != inactivePath ||
		result.RemovalResults[0].Err != nil {
		t.Fatalf("removal results = %+v, want successful removal of %s", result.RemovalResults, inactivePath)
	}
	if len(result.Remaining) != 1 || result.Remaining[0].Path != protectedPath {
		t.Fatalf("remaining files = %+v, want only %s", result.Remaining, protectedPath)
	}
	if _, err := os.Stat(protectedPath); err != nil {
		t.Fatalf("protected file was removed: %v", err)
	}
	if _, err := os.Stat(inactivePath); !os.IsNotExist(err) {
		t.Fatalf("inactive file still exists or stat failed unexpectedly: %v", err)
	}
}

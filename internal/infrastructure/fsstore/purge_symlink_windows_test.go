//go:build windows

package fsstore

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestPurgeExcludesWindowsJunction(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	entry := filepath.Join(root, "app_20260913_99.log")
	output, err := exec.Command("cmd", "/d", "/c", "mklink", "/J", entry, target).CombinedOutput()
	if err != nil {
		t.Fatalf("create test junction: %v\n%s", err, output)
	}
	t.Cleanup(func() {
		if err := os.Remove(entry); err != nil {
			t.Errorf("remove test junction: %v", err)
		}
	})
	if files := GetFilesInfo([]string{entry}); len(files) != 0 {
		t.Fatalf("junction included in purge observation: %+v", files)
	}
	result := removeObservedFile(FileInfo{Path: entry, Size: 1, ModTime: time.Now()})
	if result.Err == nil {
		t.Fatalf("junction deletion result = %+v, want rejection", result)
	}
	if _, err := os.Lstat(entry); err != nil {
		t.Fatalf("junction was removed by purge: %v", err)
	}
	if info, err := os.Lstat(target); err != nil || !info.IsDir() {
		t.Fatalf("junction target directory changed: %v", err)
	}
}

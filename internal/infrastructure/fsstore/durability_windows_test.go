package fsstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncDirectoryWindowsPaths(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	for _, name := range []string{
		"relative",
		"日本語 space",
		filepath.Join(strings.Repeat("a", 120), strings.Repeat("b", 120), "long"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.MkdirAll(name, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := SyncDirectory(name); err != nil {
				t.Fatalf("sync relative directory: %v", err)
			}
			absolute := filepath.Join(root, name)
			if err := SyncDirectory(absolute); err != nil {
				t.Fatalf("sync absolute directory: %v", err)
			}
			if err := SyncDirectory(`\\?\` + absolute); err != nil {
				t.Fatalf("sync extended-length directory: %v", err)
			}
		})
	}
	if err := SyncDirectory("invalid\x00directory"); err == nil {
		t.Fatal("sync path containing NUL succeeded")
	}
}

func TestSyncDirectoryWindowsReleasesHandle(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "synced")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for range 10 {
		if err := SyncDirectory(directory); err != nil {
			t.Fatal(err)
		}
	}
	// 未解放のhandleでdelete-pendingのまま残っていないことも、同名の
	// directoryを再作成できることによって確認します。
	if err := os.Remove(directory); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("recreate synced directory: %v", err)
	}
	if err := SyncDirectory(directory); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("stat recreated directory: %v", err)
	}
}

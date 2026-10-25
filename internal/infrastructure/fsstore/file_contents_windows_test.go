//go:build windows

package fsstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// MoveFileExでrename中のhandleと同じくDELETE accessを保持するhandleがあっても、
// 保全fileの内容比較と同期のための読取は共有違反にならず完了します。
func TestEqualRegularFileContentsWindowsSharesWithDeleteAccessHandle(t *testing.T) {
	directory := t.TempDir()
	first := filepath.Join(directory, "first")
	second := filepath.Join(directory, "second")
	for _, path := range []string{first, second} {
		if err := os.WriteFile(path, []byte("same preserved bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	name, err := privateWindowsPath(second)
	if err != nil {
		t.Fatal(err)
	}
	renaming, err := windows.CreateFile(name, windows.DELETE|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(renaming)
	if file, err := os.Open(second); !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		if file != nil {
			file.Close()
		}
		t.Fatalf("fixture must reproduce the ordinary open sharing violation, got %v", err)
	}
	equal, err := EqualRegularFileContents(context.Background(), first, second)
	if err != nil || !equal {
		t.Fatalf("EqualRegularFileContents with a DELETE-access handle = %t, %v", equal, err)
	}
	if err := syncExistingPreservedFile(context.Background(), second); err != nil {
		t.Fatalf("sync existing preserved file with a DELETE-access handle: %v", err)
	}
}

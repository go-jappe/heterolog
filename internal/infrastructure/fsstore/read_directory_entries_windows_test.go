//go:build windows

package fsstore

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestReadDirectoryEntriesWindowsSharesWithPermissionHandle(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "checkpoints")
	if err := CreatePrivateDirectory(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(directory, "checkpoint.json")
	if err := os.WriteFile(checkpoint, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	directoryBefore := privateTestSecurity(t, directory).String()
	fileBefore := privateTestSecurity(t, checkpoint).String()
	name, err := privateWindowsPath(directory)
	if err != nil {
		t.Fatal(err)
	}
	// SetPrivateDirectoryPermissionsと同じhandleを保持し、短い競合区間を
	// sleepやgoroutineの実行順へ依存せず再現します。
	permissionHandle, err := windows.CreateFile(name, windows.MAXIMUM_ALLOWED,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(permissionHandle)
	if _, err := os.ReadDir(directory); !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		t.Fatalf("fixture must reproduce ordinary ReadDir sharing violation, got %v", err)
	}
	entries, err := ReadDirectoryEntries(directory)
	if err != nil || len(entries) != 1 || entries[0].Name() != "checkpoint.json" {
		t.Fatalf("read while permissions handle is open = %v, %v", entries, err)
	}
	file, err := OpenDirectoryForRead(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	entries, err = file.ReadDir(1)
	if err != nil || len(entries) != 1 || entries[0].Name() != "checkpoint.json" {
		t.Fatalf("bounded read while permissions handle is open = %v, %v", entries, err)
	}
	if entries, err := file.ReadDir(1); len(entries) != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read after last entry = %v, %v; want EOF", entries, err)
	}
	if got := privateTestSecurity(t, directory).String(); got != directoryBefore {
		t.Fatalf("reader changed directory ACL: before %s, after %s", directoryBefore, got)
	}
	if got := privateTestSecurity(t, checkpoint).String(); got != fileBefore {
		t.Fatalf("reader changed checkpoint ACL: before %s, after %s", fileBefore, got)
	}
}

func TestReadDirectoryEntriesWindowsClosesHandle(t *testing.T) {
	directory := t.TempDir()
	if _, err := ReadDirectoryEntries(directory); err != nil {
		t.Fatal(err)
	}
	name, err := privateWindowsPath(directory)
	if err != nil {
		t.Fatal(err)
	}
	// 読込handleが残っていれば、共有なしで同じdirectoryを開く操作は失敗します。
	handle, err := windows.CreateFile(name, windows.GENERIC_READ, 0,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		t.Fatalf("reader left an open handle: %v", err)
	}
	windows.CloseHandle(handle)
}

// FILE_FLAG_BACKUP_SEMANTICSは通常fileも開けるため、directory以外は開いた時点で
// ERROR_DIRECTORYとして拒否し、未作成のdirectory（os.ErrNotExist）と区別します。
func TestOpenDirectoryForReadWindowsRejectsRegularFile(t *testing.T) {
	regular := filepath.Join(t.TempDir(), "regular")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := OpenDirectoryForRead(regular)
	if file != nil {
		file.Close()
	}
	if !errors.Is(err, windows.ERROR_DIRECTORY) || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("OpenDirectoryForRead(regular file) error = %v, want ERROR_DIRECTORY", err)
	}
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) || pathErr.Path != regular {
		t.Fatalf("OpenDirectoryForRead(regular file) error = %v, want PathError for %q", err, regular)
	}
}

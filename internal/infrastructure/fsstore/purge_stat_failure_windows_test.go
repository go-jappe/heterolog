package fsstore

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func makePurgeStatFailure(t *testing.T) (string, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app_20260824_1.log")
	const contents = "keep until stat can succeed"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(name, windows.DELETE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	closeHandle := func() {
		t.Helper()
		if !closed {
			if err := windows.CloseHandle(handle); err != nil {
				t.Fatalf("close stat failure fixture: %v", err)
			}
			closed = true
		}
	}
	t.Cleanup(closeHandle)
	restored := false
	restore := func() {
		t.Helper()
		if restored {
			return
		}
		var deleteFile byte
		if err := windows.SetFileInformationByHandle(handle, windows.FileDispositionInfo, &deleteFile, 1); err != nil {
			t.Fatalf("clear fixture's pending deletion: %v", err)
		}
		closeHandle()
		restored = true
		if got, err := os.ReadFile(path); err != nil || string(got) != contents {
			t.Fatalf("stat failure fixture contents = %q, error = %v", got, err)
		}
	}
	t.Cleanup(restore)

	// delete-pendingのfileは最後のhandleが閉じるまで存在しますが、pathで開くことは
	// できません。これにより、symlinkの特権を必要とせず、Unixのmode bitsにも頼らずに、
	// os.Lstatの実際の失敗経路を確かめます。FileDispositionInfoを使うと、fixtureは
	// close前に削除を取り消せます。
	deleteFile := byte(1)
	if err := windows.SetFileInformationByHandle(handle, windows.FileDispositionInfo, &deleteFile, 1); err != nil {
		t.Fatalf("mark fixture delete-pending: %v", err)
	}
	return path, restore
}

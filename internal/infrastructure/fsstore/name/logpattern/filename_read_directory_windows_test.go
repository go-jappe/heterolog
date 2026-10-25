//go:build windows

package logpattern

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

func TestFindDirectoryFileNamesWindowsSharesWithPermissionHandle(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "logs")
	if err := fsstore.CreatePrivateDirectory(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"b.log", "ignored.txt", "a.log"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	name, err := windows.UTF16PtrFromString(directory)
	if err != nil {
		t.Fatal(err)
	}
	// 権限設定用と同じhandleを保持し、通常のReadDirが共有違反になる状態を再現します。
	handle, err := windows.CreateFile(name, windows.MAXIMUM_ALLOWED,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(handle)
	if _, err := os.ReadDir(directory); !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		t.Fatalf("ordinary ReadDir error = %v, want sharing violation", err)
	}
	got, err := findDirectoryFileNames(directory, func(name string) bool {
		return strings.HasSuffix(name, ".log")
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{filepath.Join(directory, "a.log"), filepath.Join(directory, "b.log")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("matched file paths = %v, want %v", got, want)
	}
}

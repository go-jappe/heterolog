package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestPathExists(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "regular.log")
	if err := os.WriteFile(filePath, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	directoryPath := filepath.Join(dir, "directory")
	if err := os.Mkdir(directoryPath, 0o700); err != nil {
		t.Fatal(err)
	}
	missingPath := filepath.Join(dir, "missing.log")
	validLink := filepath.Join(dir, "valid-link")
	danglingLink := filepath.Join(dir, "dangling-link")
	loopLink := filepath.Join(dir, "loop")

	for _, tt := range []struct {
		name       string
		path       string
		linkPath   string
		linkTarget string
		wantExists bool
		wantError  bool
	}{
		{name: "regular-file", path: filePath, wantExists: true},
		{name: "directory", path: directoryPath, wantExists: true},
		{name: "missing", path: missingPath},
		{name: "valid-symlink", path: validLink, linkTarget: filepath.Base(filePath), wantExists: true},
		// リンク先がなくても、リンク自体が存在すればtrueです。
		{name: "dangling-symlink", path: danglingLink, linkTarget: filepath.Base(missingPath), wantExists: true},
		// 権限や実行ユーザーに依存せず、不存在以外のOS errorを再現します。
		{name: "invalid-path", path: filepath.Join(dir, "invalid\x00path"), wantError: true},
		// 親directoryのsymlinkが循環して名前を解決できないpathは、不存在ではありません。
		// Windowsのos.Lstatはこれを不存在として返すため、PathExistsが確かめます。
		{name: "symlink-loop-parent", path: filepath.Join(loopLink, "child.log"), linkPath: loopLink, linkTarget: filepath.Base(loopLink), wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.linkTarget != "" {
				linkPath := tt.linkPath
				if linkPath == "" {
					linkPath = tt.path
				}
				testutil.Symlink(t, tt.linkTarget, linkPath)
			}
			exists, err := PathExists(tt.path)
			if exists != tt.wantExists {
				t.Errorf("PathExists(%q) exists = %t, want %t", tt.path, exists, tt.wantExists)
			}
			if !tt.wantError {
				if err != nil {
					t.Fatalf("PathExists(%q) error = %v, want nil", tt.path, err)
				}
				return
			}
			var pathErr *os.PathError
			if !errors.As(err, &pathErr) {
				t.Fatalf("PathExists(%q) error = %v, want OS path error", tt.path, err)
			}
			if pathErr.Path != tt.path {
				t.Errorf("error path = %q, want %q", pathErr.Path, tt.path)
			}
			if errors.Is(err, os.ErrNotExist) {
				t.Fatalf("PathExists(%q) error = %v, want error other than not-exist", tt.path, err)
			}
		})
	}
}

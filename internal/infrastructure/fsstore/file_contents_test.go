package fsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestEqualRegularFileContents(t *testing.T) {
	t.Parallel()
	large := bytes.Repeat([]byte("0123456789abcdef"), 32*1024)
	changedLastByte := append([]byte(nil), large...)
	changedLastByte[len(changedLastByte)-1] = '!'
	for _, tt := range []struct {
		name   string
		first  []byte
		second []byte
		equal  bool
	}{
		{name: "empty", equal: true},
		{name: "small", first: []byte("same"), second: []byte("same"), equal: true},
		{name: "multiple-full-buffers", first: large, second: large, equal: true},
		{name: "partial-last-buffer", first: large[:len(large)-1], second: large[:len(large)-1], equal: true},
		{name: "same-size-different-content", first: []byte("left"), second: []byte("rght")},
		{name: "last-byte-differs", first: large, second: changedLastByte},
		{name: "different-size", first: []byte("short"), second: []byte("longer")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			firstPath, secondPath := filepath.Join(dir, "first"), filepath.Join(dir, "second")
			if err := os.WriteFile(firstPath, tt.first, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(secondPath, tt.second, 0o600); err != nil {
				t.Fatal(err)
			}
			equal, err := EqualRegularFileContents(context.Background(), firstPath, secondPath)
			if err != nil || equal != tt.equal {
				t.Fatalf("EqualRegularFileContents() = (%t, %v), want (%t, nil)", equal, err, tt.equal)
			}
		})
	}
}

func TestOpenRegularFile(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		flag int
	}{
		{name: "read-only", flag: os.O_RDONLY},
		{name: "read-write-for-sync", flag: os.O_RDWR},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source")
			want := []byte("regular file contents")
			if err := os.WriteFile(path, want, 0o600); err != nil {
				t.Fatal(err)
			}
			file, info, err := OpenRegularFile(path, tt.flag)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if !info.Mode().IsRegular() || info.Size() != int64(len(want)) {
				t.Fatalf("opened file info = %v, want regular file of size %d", info, len(want))
			}
			got, err := io.ReadAll(file)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("opened contents = (%q, %v), want (%q, nil)", got, err, want)
			}
			if tt.flag == os.O_RDWR {
				if err := file.Sync(); err != nil {
					t.Fatalf("Sync() with write access: %v", err)
				}
			}
			got, err = os.ReadFile(path)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("file contents after open/sync = (%q, %v), want (%q, nil)", got, err, want)
			}
		})
	}
}

func TestRegularFileContentOperationsRejectNonRegularPaths(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	regular := filepath.Join(dir, "regular")
	if err := os.WriteFile(regular, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing")
	validLink, danglingLink := filepath.Join(dir, "valid-link"), filepath.Join(dir, "dangling-link")
	for _, tt := range []struct {
		name       string
		path       string
		linkTarget string
		missing    bool
	}{
		{name: "directory", path: dir},
		{name: "missing", path: missing, missing: true},
		{name: "symlink", path: validLink, linkTarget: filepath.Base(regular)},
		{name: "dangling-symlink", path: danglingLink, linkTarget: filepath.Base(missing)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.linkTarget != "" {
				testutil.Symlink(t, tt.linkTarget, tt.path)
			}
			for _, flag := range []int{os.O_RDONLY, os.O_RDWR} {
				file, info, err := OpenRegularFile(tt.path, flag)
				if file != nil {
					_ = file.Close()
					t.Fatal("OpenRegularFile returned a file for an invalid path")
				}
				if info != nil || err == nil {
					t.Fatalf("OpenRegularFile() info/error = (%v, %v), want (nil, error)", info, err)
				}
				if errors.Is(err, os.ErrNotExist) != tt.missing {
					t.Fatalf("OpenRegularFile() error = %v, want not-exist=%t", err, tt.missing)
				}
			}
			// 不正pathが左右どちらでも、内容不一致ではなく比較失敗として扱います。
			for _, paths := range [][2]string{{tt.path, regular}, {regular, tt.path}} {
				equal, err := EqualRegularFileContents(context.Background(), paths[0], paths[1])
				if equal || err == nil {
					t.Fatalf("EqualRegularFileContents(%q, %q) = (%t, %v), want (false, error)", paths[0], paths[1], equal, err)
				}
				if errors.Is(err, os.ErrNotExist) != tt.missing {
					t.Fatalf("comparison error = %v, want not-exist=%t", err, tt.missing)
				}
			}
		})
	}
}

func TestEqualRegularFileContentsContext(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("same file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if equal, err := EqualRegularFileContents(nil, path, path); err != nil || !equal {
		t.Fatalf("EqualRegularFileContents(nil context) = (%t, %v), want (true, nil)", equal, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if equal, err := EqualRegularFileContents(ctx, path, path); equal || !errors.Is(err, context.Canceled) {
		t.Fatalf("EqualRegularFileContents(canceled) = (%t, %v), want (false, canceled)", equal, err)
	}
}

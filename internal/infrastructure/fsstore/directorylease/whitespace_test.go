package directorylease

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRegistryAcquirePreservesLiteralDirectoryWhitespace(t *testing.T) {
	// 相対pathを使うことで、すでに絶対pathになった値の途中だけでなく、引数全体の
	// 先頭にある空白も確かめます。
	t.Chdir(t.TempDir())
	for _, tt := range []struct {
		name, first, second string
		conflict            bool
		trailingSpace       bool
	}{
		{name: "leading space is distinct", first: " logs", second: "logs"},
		{name: "interior space is distinct", first: "log files", second: "logfiles"},
		{name: "leading space parent", first: " logs", second: " logs/child", conflict: true},
		{name: "leading space child", first: " logs/child", second: " logs", conflict: true},
		{name: "trailing space is distinct", first: "logs ", second: "logs", trailingSpace: true},
		{name: "trailing space parent", first: "logs ", second: "logs /child", conflict: true, trailingSpace: true},
		{name: "trailing space child", first: "logs /child", second: "logs ", conflict: true, trailingSpace: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.trailingSpace && runtime.GOOS == "windows" {
				t.Skip("Windows ordinary paths do not preserve trailing-space directory names")
			}
			for _, path := range []string{tt.first, tt.second} {
				if err := os.MkdirAll(filepath.FromSlash(path), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			registry := newRegistry()
			first, err := registry.acquire("first", filepath.FromSlash(tt.first))
			if err != nil {
				t.Fatal(err)
			}
			defer first.Release()
			second, err := registry.acquire("second", filepath.FromSlash(tt.second))
			if second != nil {
				defer second.Release()
			}
			if tt.conflict {
				if !errors.Is(err, ErrDirectoryLeaseConflict) {
					t.Fatalf("overlapping physical directories accepted: %v", err)
				}
			} else if err != nil {
				t.Fatalf("distinct physical directories rejected: %v", err)
			}
		})
	}
}

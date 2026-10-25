//go:build windows

package pathidentity

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveDirectoryWindowsJunctionAliases(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "real")
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "junction")
	createDirectoryJunction(t, alias, target)
	chain := filepath.Join(root, "chain")
	createDirectoryJunction(t, chain, alias)
	physical, err := ResolveDirectory(target)
	if err != nil {
		t.Fatal(err)
	}
	for _, base := range []string{target, alias, chain} {
		for _, suffix := range []string{"", filepath.Join("missing", "logs")} {
			got, err := ResolveDirectory(filepath.Join(base, suffix))
			want := filepath.Join(physical, suffix)
			if err != nil || ComparisonKey(got) != ComparisonKey(want) {
				t.Fatalf("ResolveDirectory(%q, %q) = %q, %v; want %q", base, suffix, got, err, want)
			}
		}
	}
	if _, err := os.Lstat(filepath.Join(target, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resolver created missing suffix: %v", err)
	}
}

func TestResolveDirectoryWindowsRejectsBrokenJunctions(t *testing.T) {
	for _, name := range []string{"dangling", "loop"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			link := filepath.Join(root, name)
			target := filepath.Join(root, "missing")
			if name == "loop" {
				target = link
			}
			createDirectoryJunction(t, link, target)
			for _, path := range []string{link, filepath.Join(link, "missing", "logs")} {
				if got, err := ResolveDirectory(path); err == nil || got != "" {
					t.Fatalf("ResolveDirectory(%q) = %q, %v; want error without fallback", path, got, err)
				}
			}
		})
	}
}

func TestResolveAndValidateManagedDirectoryWindowsJunctionBoundary(t *testing.T) {
	root, physical := t.TempDir(), t.TempDir()
	baseAlias := filepath.Join(root, "base")
	createDirectoryJunction(t, baseAlias, physical)
	wantBase, err := ResolveDirectory(physical)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ResolveAndValidateManagedDirectory(baseAlias, "archive-checkpoints")
	if err != nil || ComparisonKey(got) != ComparisonKey(filepath.Join(wantBase, "archive-checkpoints")) {
		t.Fatalf("base junction resolution = %q, %v", got, err)
	}
	managed := filepath.Join(physical, "archive-checkpoints")
	createDirectoryJunction(t, managed, t.TempDir())
	for _, suffix := range []string{"archive-checkpoints", filepath.Join("archive-checkpoints", "child")} {
		if got, err := ResolveAndValidateManagedDirectory(baseAlias, suffix); err == nil || got != "" {
			t.Fatalf("managed junction resolution = %q, %v; want rejection", got, err)
		}
	}
}

func TestResolveDirectoryWindowsLongPath(t *testing.T) {
	root, err := ResolveDirectory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := root
	for index := 0; index < 8; index++ {
		target = filepath.Join(target, strings.Repeat("directory", 8))
	}
	if err := os.MkdirAll(target, 0o750); err != nil {
		t.Fatal(err)
	}
	got, err := ResolveDirectory(target)
	if err != nil || ComparisonKey(got) != ComparisonKey(target) {
		t.Fatalf("ResolveDirectory long path = %q, %v; want %q", got, err, target)
	}
}

func TestWindowsDirectoryOperationPath(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{`\\?\C:\Logs`, `C:\Logs`},
		{`\\?\c:\Logs`, `c:\Logs`},
		{`\\?\UNC\Server\Share\Logs`, `\\Server\Share\Logs`},
		{`\\?\unc\Server\Share\Logs`, `\\Server\Share\Logs`},
		{`\\?\Volume{12345678-1234-1234-1234-123456789abc}\Logs`, `\\?\Volume{12345678-1234-1234-1234-123456789abc}\Logs`},
		{`\\.\C:\Logs`, `\\.\C:\Logs`},
	} {
		if got := windowsDirectoryOperationPath(tc.input); got != tc.want {
			t.Errorf("windowsDirectoryOperationPath(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func createDirectoryJunction(t *testing.T, link, target string) {
	// 呼び出し元が作るt.TempDir内のjunctionだけを作成・解除します。
	t.Helper()
	output, err := exec.Command("cmd", "/d", "/c", "mklink", "/J", link, target).CombinedOutput()
	if err != nil {
		t.Fatalf("create junction %q -> %q: %v\n%s", link, target, err, output)
	}
	t.Cleanup(func() {
		if err := os.Remove(link); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("remove junction %q: %v", link, err)
		}
	})
}

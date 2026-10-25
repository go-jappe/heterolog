package directorylease

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRegistryAcquireUsesFilesystemPathCaseRules(t *testing.T) {
	registry := newRegistry()
	root := t.TempDir()
	upper := filepath.Join(root, "Logs")
	lower := filepath.Join(root, "logs")
	if err := os.MkdirAll(upper, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(lower, 0o700); err != nil {
		t.Fatal(err)
	}
	a, err := os.Stat(upper)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(lower)
	if err != nil {
		t.Fatal(err)
	}
	shared := os.SameFile(a, b)
	lease, err := registry.acquire("first", upper, lower)
	if err != nil {
		t.Fatalf("acquire aliases in one request: %v", err)
	}
	defer lease.Release()
	wantCount := 2
	if shared {
		wantCount = 1
	}
	registered := onlyRegistration(t, registry)
	if len(registered.paths) != wantCount || registered.paths[0] != canonicalPath(t, upper) {
		t.Fatalf("registered paths = %v, want %d with original spelling %q", registered.paths, wantCount, upper)
	}
	lease.Release()
	first, err := registry.acquire("first", upper)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := registry.acquire("second", lower)
	if shared {
		if !errors.Is(err, ErrDirectoryLeaseConflict) {
			t.Fatalf("alias acquisition error = %v, want conflict", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("case-distinct acquisition: %v", err)
	}
	defer second.Release()
}

func TestRegistryAcquireWindowsExtendedAliases(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows path semantics")
	}
	path := filepath.Join(t.TempDir(), "Logs")
	alias := `\\?\` + strings.ToUpper(path)
	if strings.HasPrefix(path, `\\`) {
		alias = `\\?\UNC\` + strings.ToUpper(strings.TrimPrefix(path, `\\`))
	}
	registry := newRegistry()
	lease, err := registry.acquire("first", path, alias)
	if err != nil {
		t.Fatalf("acquire equivalent paths: %v", err)
	}
	defer lease.Release()
	registered := onlyRegistration(t, registry)
	if len(registered.paths) != 1 || registered.paths[0] != canonicalPath(t, path) {
		t.Fatalf("registered paths = %v, want original %q only", registered.paths, path)
	}
	for _, candidate := range []string{alias, filepath.Join(alias, "child")} {
		if _, err := registry.acquire("second", candidate); !errors.Is(err, ErrDirectoryLeaseConflict) {
			t.Fatalf("acquire %q: %v, want conflict", candidate, err)
		}
	}
}

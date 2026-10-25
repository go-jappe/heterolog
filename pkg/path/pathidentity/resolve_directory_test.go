package pathidentity

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestResolveDirectoryKeepsMissingSuffixWithoutCreatingIt(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "missing", "logs")
	got, err := ResolveDirectory(want)
	if err != nil || ComparisonKey(got) != ComparisonKey(want) {
		t.Fatalf("ResolveDirectory = %q, %v; want %q", got, err, want)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("resolver changed directory: entries=%v, err=%v", entries, err)
	}
}

func TestResolveDirectoryResolvesAliasesAndMissingDescendants(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(root, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	testutil.Symlink(t, real, alias)
	chain := filepath.Join(root, "chain")
	testutil.Symlink(t, "alias", chain)
	for _, base := range []string{real, alias, chain} {
		for _, suffix := range []string{"", filepath.Join("missing", "logs")} {
			got, err := ResolveDirectory(filepath.Join(base, suffix))
			want := filepath.Join(real, suffix)
			if err != nil || ComparisonKey(got) != ComparisonKey(want) {
				t.Fatalf("ResolveDirectory(%q) = %q, %v; want %q", base, got, err, want)
			}
		}
	}
	entries, err := os.ReadDir(real)
	if err != nil || len(entries) != 0 {
		t.Fatalf("resolver created missing descendants: entries=%v, err=%v", entries, err)
	}
}

func TestResolveDirectoryRejectsInvalidExistingPaths(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"", " \t\n", "\u2003", file, filepath.Join(file, "child"), string([]byte{0})} {
		if got, err := ResolveDirectory(path); err == nil || got != "" {
			t.Fatalf("ResolveDirectory(%q) = %q, %v; want error without fallback", path, got, err)
		}
	}
}

func TestResolveDirectoryRejectsBrokenLinksAndLinksToFiles(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, target string }{
		{"dangling", filepath.Join(root, "absent")},
		{"loop", filepath.Join(root, "loop")},
		{"to-file", file},
	} {
		t.Run(tc.name, func(t *testing.T) {
			link := filepath.Join(root, tc.name)
			testutil.Symlink(t, tc.target, link)
			for _, path := range []string{link, filepath.Join(link, "missing", "logs")} {
				if got, err := ResolveDirectory(path); err == nil || got != "" {
					t.Fatalf("ResolveDirectory(%q) = %q, %v; want error without fallback", path, got, err)
				}
			}
		})
	}
}

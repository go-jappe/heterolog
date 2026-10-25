package fsstore

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// macOSでは、同じdirectoryに対象があるAppleDoubleの付随fileだけを一覧から除き、対象のない「._」の
// 名前は通常のentryとして残すことを確かめます。macOS以外では「._」の名前も除きません。
func TestReadDirectoryEntriesOmitsAppleDoubleCompanionsOnlyOnDarwin(t *testing.T) {
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "logs"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"app.log", "._app.log", "._logs", "._orphan.log", "._"} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	entries, err := ReadDirectoryEntries(directory)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	want := []string{"._", "._app.log", "._logs", "._orphan.log", "app.log", "logs"}
	companions := map[string]bool{}
	if hidesAppleDoubleCompanions {
		want = []string{"._", "._orphan.log", "app.log", "logs"}
		companions = map[string]bool{"._app.log": true, "._logs": true}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("entries = %q, want %q", got, want)
	}
	for _, name := range []string{"._app.log", "._logs", "._orphan.log", "._", "app.log", "logs"} {
		if got := IsAppleDoubleCompanion(directory, name); got != companions[name] {
			t.Errorf("IsAppleDoubleCompanion(%q) = %t, want %t", name, got, companions[name])
		}
	}
}

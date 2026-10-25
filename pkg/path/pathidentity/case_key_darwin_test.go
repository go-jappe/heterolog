package pathidentity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// macOSの判定（pathconfの_PC_CASE_SENSITIVE）が、試験の一時directoryがあるfilesystemの
// 実際の振る舞いと一致することを、作成したfileを別表記で開けるかで確かめます。
// CIのmacOSは既定の大小文字を区別しないAPFSで実行します。testenv/macosの手順は、大小文字を
// 区別するAPFS・ExFAT・FAT32のdisk imageを一時directoryにして、この試験を実行します。
func TestDarwinDirectoryCaseMatchesTemporaryFilesystem(t *testing.T) {
	directory := t.TempDir()
	original := filepath.Join(directory, "Probe.log")
	alias := filepath.Join(directory, "PROBE.LOG")
	if err := os.WriteFile(original, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Stat(original)
	if err != nil {
		t.Fatal(err)
	}
	aliasInfo, err := os.Stat(alias)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat case alias %q: %v", alias, err)
	}
	insensitive := err == nil && os.SameFile(originalInfo, aliasInfo)
	t.Logf("temporary filesystem of %q is case-insensitive=%t", directory, insensitive)

	want := NameCaseSensitive
	if insensitive {
		want = NameCaseInsensitive
	}
	// 未作成のdirectoryは、最も近い既存の祖先の規則に従います。
	for _, current := range []string{directory, filepath.Join(directory, "Missing", "Logs")} {
		if got := DirectoryNameCase(current); got != want {
			t.Errorf("DirectoryNameCase(%q) = %d, want %d to match the filesystem", current, got, want)
		}
	}
	if folded := ComparisonKey(original) == ComparisonKey(alias); folded != insensitive {
		t.Errorf("ComparisonKey folds %q and %q = %t, want %t to match the filesystem", original, alias, folded, insensitive)
	}
	if got := Contains(map[string]struct{}{original: {}}, alias); got != insensitive {
		t.Errorf("Contains(case alias of an existing file) = %t, want %t", got, insensitive)
	}
}

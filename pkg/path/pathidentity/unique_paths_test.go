package pathidentity

import (
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

func TestAppendUniquePathsKeepsFirstOccurrenceAndOrder(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first.log")
	second := filepath.Join(root, "second.log")
	third := filepath.Join(root, "third.log")
	got := AppendUniquePaths([]string{"", first, first, second, ""}, "", second, third, third, first)
	if want := []string{first, second, third}; !slices.Equal(got, want) {
		t.Fatalf("AppendUniquePaths() = %q, want %q", got, want)
	}
	if got := AppendUniquePaths([]string{"", ""}, ""); len(got) != 0 {
		t.Fatalf("empty paths were retained: %q", got)
	}
	if got := AppendUniquePaths(nil); len(got) != 0 {
		t.Fatalf("empty input returned paths: %q", got)
	}
}

func TestAppendUniquePathsDoesNotMutateOrReuseInput(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first.log")
	second := filepath.Join(root, "second.log")
	third := filepath.Join(root, "third.log")
	for _, additional := range [][]string{nil, {third, third}} {
		// 入力スライスの余剰容量も比較し、追記による元の配列の書き換えを検出します。
		backing := []string{first, second, "unused capacity", "unused capacity"}
		original := slices.Clone(backing)
		originalAdditional := slices.Clone(additional)
		got := AppendUniquePaths(backing[:2], additional...)
		if !slices.Equal(backing, original) || !slices.Equal(additional, originalAdditional) {
			t.Fatalf("input changed: backing=%q, additional=%q", backing, additional)
		}
		if len(got) == 0 {
			t.Fatal("nonempty input returned no paths")
		}
		got[0] = third
		if !slices.Equal(backing, original) {
			t.Fatalf("result shares the input backing array: %q", backing)
		}
	}
}

func TestAppendUniquePathsUsesPlatformIdentityAndKeepsOriginalSpelling(t *testing.T) {
	if runtime.GOOS == "windows" {
		first := `C:\Logs\App.log`
		unc := `\\Server\Share\Logs\App.log`
		got := AppendUniquePaths([]string{first, `c:\logs\APP.LOG`, unc},
			`\\?\C:\LOGS\app.log`, `\\?\UNC\SERVER\SHARE\logs\APP.LOG`)
		if want := []string{first, unc}; !slices.Equal(got, want) {
			t.Fatalf("Windows aliases changed the first spelling or were retained: %q, want %q", got, want)
		}
		return
	}

	for _, rule := range directoryNameCaseRulesWithDirectoryRule() {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			if rule.nameCase != NameCaseUnspecified {
				useDirectoryNameCaseForTest(t, root, rule.nameCase)
			}
			upper := filepath.Join(root, "App.log")
			lower := filepath.Join(root, "app.log")
			got := AppendUniquePaths([]string{upper, lower}, upper, lower)
			want := []string{upper, lower}
			if DirectoryNameCase(root) == NameCaseInsensitive {
				want = []string{upper}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("case identity or original ordering changed: %q, want %q", got, want)
			}
		})
	}
}

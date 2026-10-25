package pathidentity

import (
	"os"
	"path/filepath"
	"testing"
)

// 問い合わせ結果を差し替え、大小文字を区別するvolume・区別しないvolumeと、
// 区別しないvolume配下の区別するmountを、どのOSでも同じ規則で確認します。
func TestDirectoryNameCaseFollowsCaseKeyRules(t *testing.T) {
	root := filepath.VolumeName(t.TempDir()) + string(filepath.Separator)
	mount := filepath.Join(root, "Mount")
	for _, rootSensitive := range []bool{true, false} {
		probe := func(directory string) (bool, error) {
			switch directory {
			case root, filepath.Join(root, "Logs"):
				return rootSensitive, nil
			case mount, filepath.Join(mount, "Logs"):
				return true, nil
			default:
				return false, os.ErrNotExist
			}
		}
		rootCase := NameCaseInsensitive
		if rootSensitive {
			rootCase = NameCaseSensitive
		}
		for _, test := range []struct {
			directory string
			want      NameCase
		}{
			{directory: root, want: rootCase},
			{directory: filepath.Join(root, "Logs"), want: rootCase},
			{directory: filepath.Join(root, "Missing", "Logs"), want: rootCase},
			{directory: mount, want: NameCaseSensitive},
			{directory: filepath.Join(mount, "Logs"), want: NameCaseSensitive},
			{directory: filepath.Join(mount, "Missing", "Logs"), want: NameCaseSensitive},
		} {
			got := directoryNameCase(test.directory, probe)
			if got != test.want {
				t.Fatalf("directoryNameCase(%q) with root sensitive=%t = %d, want %d",
					test.directory, rootSensitive, got, test.want)
			}
			// ComparisonKeyが同じdirectory直下のentryへ適用する規則と一致させます。
			entryKey := filepath.Base(caseKey(filepath.Join(test.directory, "Entry"), probe))
			if folded := entryKey == "entry"; folded != (got == NameCaseInsensitive) {
				t.Fatalf("caseKey entry %q disagrees with directory rule %d for %q", entryKey, got, test.directory)
			}
		}
	}
}

func TestDirectoryNameCaseQueriesExistingAncestorsInOrder(t *testing.T) {
	root := filepath.VolumeName(t.TempDir()) + string(filepath.Separator)
	directory := filepath.Join(root, "Existing", "Missing")
	var queries []string
	got := directoryNameCase(directory, func(path string) (bool, error) {
		queries = append(queries, path)
		if path == directory {
			return true, os.ErrNotExist
		}
		return false, nil
	})
	if got != NameCaseInsensitive {
		t.Fatalf("missing directory did not inherit its parent rule: %d", got)
	}
	// 問い合わせは操作用の表記を保ち、未作成の階層を作成しません。
	want := []string{root, filepath.Join(root, "Existing"), directory}
	if len(queries) != len(want) {
		t.Fatalf("queries = %q, want %q", queries, want)
	}
	for index := range want {
		if queries[index] != want[index] {
			t.Fatalf("queries = %q, want %q", queries, want)
		}
	}
	if _, err := os.Lstat(directory); !os.IsNotExist(err) {
		t.Fatalf("directory rule inspection created %q: %v", directory, err)
	}
}

func TestNameCaseKeyFoldsOnlyCaseInsensitiveNames(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, insensitive string
	}{
		{name: "", insensitive: ""},
		{name: "app_20260914_1.log.gz", insensitive: "app_20260914_1.log.gz"},
		{name: "Error_App_20260914_1.LOG.GZ", insensitive: "error_app_20260914_1.log.gz"},
		{name: ".BR", insensitive: ".br"},
		{name: "Kernel_ログ_ÄPP.LOG", insensitive: "kernel_ログ_äpp.log"},
	} {
		if got := NameCaseInsensitive.Key(test.name); got != test.insensitive {
			t.Errorf("NameCaseInsensitive.Key(%q) = %q, want %q", test.name, got, test.insensitive)
		}
		for _, nameCase := range []NameCase{NameCaseSensitive, NameCaseUnspecified} {
			if got := nameCase.Key(test.name); got != test.name {
				t.Errorf("NameCase(%d).Key(%q) = %q, want unchanged", nameCase, test.name, got)
			}
		}
	}
}

func TestNameCaseForDirectoryKeepsExplicitRules(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "Missing")
	for _, nameCase := range []NameCase{NameCaseSensitive, NameCaseInsensitive} {
		if got := nameCase.ForDirectory(directory); got != nameCase {
			t.Fatalf("explicit rule %d changed to %d", nameCase, got)
		}
		if got := EntryNameCases(nameCase)(filepath.Join(directory, "App.log")); got != nameCase {
			t.Fatalf("explicit entry rule %d changed to %d", nameCase, got)
		}
	}
	want := DirectoryNameCase(directory)
	if want != NameCaseSensitive && want != NameCaseInsensitive {
		t.Fatalf("DirectoryNameCase(%q) = %d, want a resolved rule", directory, want)
	}
	if got := NameCaseUnspecified.ForDirectory(directory); got != want {
		t.Fatalf("unspecified rule = %d, want directory rule %d", got, want)
	}
	if got := EntryNameCases(NameCaseUnspecified)(filepath.Join(directory, "App.log")); got != want {
		t.Fatalf("unspecified entry rule = %d, want parent directory rule %d", got, want)
	}
	if got, want := DirectoryNameCase(""), DirectoryNameCase("."); got != want {
		t.Fatalf("empty directory rule = %d, want current directory rule %d", got, want)
	}
}

func TestEntryComparisonKeyAppliesRuleOnlyToEntryName(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	original := filepath.Join(root, "Logs", "App_20260914_1.log")
	nameAlias := filepath.Join(root, "Logs", "app_20260914_1.LOG")
	directoryAlias := filepath.Join(root, "LOGS", "App_20260914_1.log")
	directoriesShareKey := ComparisonKey(filepath.Dir(original)) == ComparisonKey(filepath.Dir(directoryAlias))
	for _, test := range []struct {
		nameCase NameCase
		sameName bool
	}{
		{nameCase: NameCaseSensitive, sameName: false},
		{nameCase: NameCaseInsensitive, sameName: true},
	} {
		key := EntryComparisonKey(original, test.nameCase)
		if same := key == EntryComparisonKey(nameAlias, test.nameCase); same != test.sameName {
			t.Fatalf("rule %d: entry alias same key = %t, want %t", test.nameCase, same, test.sameName)
		}
		// directory部分はComparisonKeyの規則に従い、entry名の規則で上書きしません。
		if same := key == EntryComparisonKey(directoryAlias, test.nameCase); same != directoriesShareKey {
			t.Fatalf("rule %d: directory alias same key = %t, want ComparisonKey result %t",
				test.nameCase, same, directoriesShareKey)
		}
	}
	for _, path := range []string{original, nameAlias, root, filepath.Join("relative", "App.log")} {
		if got, want := EntryComparisonKey(path, NameCaseUnspecified), ComparisonKey(path); got != want {
			t.Fatalf("unspecified EntryComparisonKey(%q) = %q, want ComparisonKey %q", path, got, want)
		}
	}
	if got := EntryComparisonKey("", NameCaseInsensitive); got != "" {
		t.Fatalf("EntryComparisonKey(empty) = %q", got)
	}
}

// ホストのfilesystemでも、DirectoryNameCaseとComparisonKeyが同じ判定を共有することを確認します。
func TestDirectoryNameCaseSharesComparisonKeyRules(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "Existing")
	if err := os.Mkdir(existing, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{root, existing, filepath.Join(root, "Missing", "Logs")} {
		nameCase := DirectoryNameCase(directory)
		upper := ComparisonKey(filepath.Join(directory, "App.log"))
		lower := ComparisonKey(filepath.Join(directory, "app.LOG"))
		if same := upper == lower; same != (nameCase == NameCaseInsensitive) {
			t.Fatalf("DirectoryNameCase(%q) = %d, but ComparisonKey case equality = %t", directory, nameCase, same)
		}
		for _, path := range []string{filepath.Join(directory, "App.log"), filepath.Join(directory, "app.LOG")} {
			if got, want := EntryComparisonKey(path, nameCase), ComparisonKey(path); got != want {
				t.Fatalf("EntryComparisonKey(%q, directory rule) = %q, want ComparisonKey %q", path, got, want)
			}
		}
	}
}

package pathidentity

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// この表は、大小文字を区別しない祖先の下にある区別するmountも含め、どのCI hostでも
// 両方のfilesystemの挙動を扱います。platformごとの試験では、実際の試験用volumeで
// os.SameFileとの比較も行います。
func TestCaseKeyFollowsDirectoryRulesWithoutCreatingMissingPaths(t *testing.T) {
	root := filepath.VolumeName(t.TempDir()) + string(filepath.Separator)
	for _, tc := range []struct {
		name      string
		sensitive bool
		want      string
	}{
		{"sensitive", true, "Missing/Logs"},
		{"insensitive", false, "missing/logs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(root, "Missing", "Logs")
			queries := []string{}
			key := caseKey(path, func(parent string) (bool, error) {
				queries = append(queries, parent)
				if parent == root {
					return tc.sensitive, nil
				}
				return false, os.ErrNotExist
			})
			if want := filepath.Join(root, filepath.FromSlash(tc.want)); key != want {
				t.Fatalf("key = %q, want %q", key, want)
			}
			if len(queries) != 2 || queries[1] != filepath.Join(root, "Missing") {
				t.Fatalf("queries must preserve operation spelling: %q", queries)
			}
		})
	}
	mount := filepath.Join(root, "Mount")
	key := caseKey(filepath.Join(mount, "Sensitive", "Logs"), func(parent string) (bool, error) {
		switch parent {
		case root:
			return false, nil
		case mount:
			return true, nil
		default:
			return false, os.ErrNotExist
		}
	})
	want := filepath.Join(root, "mount", "Sensitive", "Logs")
	if key != want {
		t.Fatalf("mount boundary key = %q, want %q", key, want)
	}
}

func TestCaseIdentityKeepsExistingAndMissingNamesConsistent(t *testing.T) {
	// 規則を指定した両方の場合と、ComparisonKeyと判定を共用する保存先directoryの規則で確認します。
	for _, rule := range directoryNameCaseRulesWithDirectoryRule() {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			if rule.nameCase != NameCaseUnspecified {
				useDirectoryNameCaseForTest(t, root, rule.nameCase)
			}
			shared := DirectoryNameCase(root) == NameCaseInsensitive
			upper, lower := filepath.Join(root, "Pending", "Logs"), filepath.Join(root, "pending", "logs")
			before := ComparisonKey(upper)
			if equal := before == ComparisonKey(lower); equal != shared {
				t.Fatalf("missing identity equality = %v, want %v", equal, shared)
			}
			if err := os.MkdirAll(upper, 0o700); err != nil {
				t.Fatal(err)
			}
			if after := ComparisonKey(upper); after != before {
				t.Fatalf("creating directory changed identity: %q -> %q", before, after)
			}
			if equal := ComparisonKey(upper) == ComparisonKey(lower); equal != shared {
				t.Fatalf("existing identity equality = %v, want %v", equal, shared)
			}
			if !Contains(map[string]struct{}{upper: {}}, filepath.Join(root, "Pending", "Logs")) {
				t.Fatal("original path not found")
			}
			if got := Contains(map[string]struct{}{upper: {}}, filepath.Join(root, strings.ToLower("Pending"), "logs")); got != shared {
				t.Fatalf("case alias membership = %v, want %v", got, shared)
			}
		})
	}
}

// recordingCaseRuleは、ruleへの問い合わせをqueriesへ記録します。
func recordingCaseRule(queries *[]string, rule func(string) (bool, error)) func(string) (bool, error) {
	return func(directory string) (bool, error) {
		*queries = append(*queries, directory)
		return rule(directory)
	}
}

// 区切りが"/"のOSで整理済みの絶対pathに使う走査が、filepath.Joinで組み立てる走査と同じkeyを返し、
// 同じ親directoryを同じ順で問い合わせることを確かめます。directoryNameCaseも同じ順で問い合わせます。
func TestCleanSlashWalkMatchesJoinedWalk(t *testing.T) {
	if filepath.Separator != '/' {
		t.Skip("slash-separated path walk")
	}
	// 小文字化で長さが変わる名前、記号だけの名前、空白を含む名前も含めます。
	names := []string{"Logs", "logs", "App.log", "İstanbul", "ÄPP", "ログ", "a.b", "...", "with space", "UPPER", "x"}
	rules := map[string]func(string) (bool, error){
		"sensitive":   func(string) (bool, error) { return true, nil },
		"insensitive": func(string) (bool, error) { return false, nil },
		"alternating": func(directory string) (bool, error) { return strings.Count(directory, "/")%2 == 0, nil },
		// 失敗した階層では、親の規則を引き継ぎます。
		"errors": func(directory string) (bool, error) {
			if strings.Contains(directory, "UPPER") {
				return false, os.ErrNotExist
			}
			return len(directory)%3 != 0, nil
		},
	}
	paths := []string{"/"}
	for index, first := range names {
		paths = append(paths, "/"+first)
		for _, second := range names {
			paths = append(paths, "/"+first+"/"+second, "/"+first+"/"+second+"/"+names[(index+3)%len(names)])
		}
	}
	for ruleName, rule := range rules {
		for _, path := range paths {
			if !isCleanSlashAbsolute(path) {
				t.Fatalf("test path %q is not a clean absolute path", path)
			}
			var fastQueries, joinedQueries []string
			fast := cleanSlashCaseKey(path, recordingCaseRule(&fastQueries, rule))
			joined := joinedCaseKey(path, recordingCaseRule(&joinedQueries, rule))
			if fast != joined || !slices.Equal(fastQueries, joinedQueries) {
				t.Fatalf("%s rule, %q: key %q / queries %q, want joined walk %q / %q",
					ruleName, path, fast, fastQueries, joined, joinedQueries)
			}
			var directoryQueries, entryQueries []string
			nameCase := directoryNameCase(path, recordingCaseRule(&directoryQueries, rule))
			entryKey := caseKey(filepath.Join(path, "Entry"), recordingCaseRule(&entryQueries, rule))
			if !slices.Equal(directoryQueries, entryQueries) {
				t.Fatalf("%s rule, %q: directoryNameCase queries %q, want caseKey entry queries %q",
					ruleName, path, directoryQueries, entryQueries)
			}
			if folded := filepath.Base(entryKey) == "entry"; folded != (nameCase == NameCaseInsensitive) {
				t.Fatalf("%s rule, %q: directory rule %d disagrees with caseKey entry %q", ruleName, path, nameCase, entryKey)
			}
		}
	}
}

// 整理済みの絶対pathの判定は、filepath.Cleanで変わらない絶対pathと一致させます。
func TestIsCleanSlashAbsoluteMatchesFilepathClean(t *testing.T) {
	if filepath.Separator != '/' {
		t.Skip("slash-separated path walk")
	}
	for _, path := range []string{
		"", "/", "//", "///", ".", "..", "a", "a/b", "./a", "/a", "/a/", "/a//b", "/a/./b", "/a/../b",
		"/a/b/.", "/a/b/..", "/.", "/..", "/...", "/a/...", "/.a/b.", "/a/b/c", "/a/b/c/", "/ログ/İ",
		"/with space/ x ", "/a\\b",
	} {
		want := filepath.IsAbs(path) && filepath.Clean(path) == path
		if got := isCleanSlashAbsolute(path); got != want {
			t.Errorf("isCleanSlashAbsolute(%q) = %t, want %t", path, got, want)
		}
	}
}

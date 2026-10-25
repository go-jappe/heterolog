package appendlog

import (
	"github.com/go-jappe/heterolog/internal/testutil"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// directoryCaseRulesForTestは、保存先directoryが大小文字を区別する場合・しない場合を、
// 実行するOSやfilesystemに依存せず試験するための規則です。directory ruleでは規則を差し替えず、
// ComparisonKeyと共用する保存先directoryの判定を使います。
var directoryCaseRulesForTest = []struct {
	name     string
	nameCase pathidentity.NameCase
}{
	{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
	{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive},
	{name: "directory rule", nameCase: pathidentity.NameCaseUnspecified},
}

// useDirectoryCaseRuleForTestは、指定の規則をroot配下へ適用し、その規則で大小文字を区別しないかを返します。
// process全体の設定なので、t.Parallelを使う試験からは呼び出さないでください。
func useDirectoryCaseRuleForTest(t *testing.T, root string, nameCase pathidentity.NameCase) bool {
	t.Helper()
	if nameCase != pathidentity.NameCaseUnspecified {
		testutil.UseDirectoryCaseRuleForTest(t, root, nameCase == pathidentity.NameCaseSensitive)
	}
	return pathidentity.DirectoryNameCase(root) == pathidentity.NameCaseInsensitive
}

func TestDirectoryStateUsesFilesystemPathCaseRules(t *testing.T) {
	for _, rule := range directoryCaseRulesForTest {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			upper, lower := filepath.Join(root, "Logs"), filepath.Join(root, "logs")
			wantSame := useDirectoryCaseRuleForTest(t, root, rule.nameCase)
			if got := DirectoryGuardForPath(upper) == DirectoryGuardForPath(lower); got != wantSame {
				t.Fatalf("guard equality = %t, want %t", got, wantSame)
			}
			if got := DirectorySizeTrackerForPath(upper) == DirectorySizeTrackerForPath(lower); got != wantSame {
				t.Fatalf("size tracker equality = %t, want %t", got, wantSame)
			}
			path := filepath.Join(upper, "App.log")
			if got := pathidentity.AbsoluteOrCleanPath(path); got != path {
				t.Fatalf("file-operation path changed spelling: %q, want %q", got, path)
			}
		})
	}
}

func TestDirectoryGuardRetainedPathsUseFilesystemCaseRules(t *testing.T) {
	for _, rule := range directoryCaseRulesForTest {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			ignoresCase := useDirectoryCaseRuleForTest(t, root, rule.nameCase)
			guard := DirectoryGuardForPath(root)
			owner := NewDirectoryGuardToken()
			upper, lower := filepath.Join(root, "App.log"), filepath.Join(root, "app.log")
			guard.ProtectPaths(owner, upper, lower)
			defer guard.UnprotectPaths(owner, upper, lower)
			wantCount := 2
			if ignoresCase {
				wantCount = 1
			}
			guard.WithProtectedPaths(func(paths map[string]struct{}) {
				if len(paths) != wantCount {
					t.Fatalf("protected paths = %v, want %d", paths, wantCount)
				}
				if _, exists := paths[upper]; !exists {
					t.Fatalf("first registered spelling was not preserved: %v", paths)
				}
			})
			guard.UnprotectPaths(owner, lower)
			guard.WithProtectedPaths(func(paths map[string]struct{}) {
				if len(paths) != wantCount-1 {
					t.Fatalf("protected after alias release = %v, want %d", paths, wantCount-1)
				}
			})
		})
	}
}

func TestDirectoryGuardClaimsUseFilesystemPathCaseRules(t *testing.T) {
	for _, rule := range directoryCaseRulesForTest {
		for _, compressed := range []bool{false, true} {
			name := "paths"
			if compressed {
				name = "compressed with source key"
			}
			t.Run(rule.name+"/"+name, func(t *testing.T) {
				testDirectoryGuardClaimUsesPathCaseRule(t, rule.nameCase, compressed)
			})
		}
	}
}

func testDirectoryGuardClaimUsesPathCaseRule(t *testing.T, nameCase pathidentity.NameCase, compressed bool) {
	dir := t.TempDir()
	ignoresCase := useDirectoryCaseRuleForTest(t, dir, nameCase)
	guard := DirectoryGuardForPath(dir)
	owner, other := NewDirectoryGuardToken(), NewDirectoryGuardToken()
	upper := filepath.Join(dir, "App_20260914_7.log")
	lower := filepath.Join(dir, "app_20260914_7.log")
	guard.ProtectPaths(other, upper)
	paths := []string{lower}
	if compressed {
		paths = append(paths, lower+".gz")
	}
	claimed := guard.ProtectPathsIfNoOtherOwnerConflict(owner, nil, paths)
	if want := !ignoresCase; claimed != want {
		t.Fatalf("case-different claim = %t, want %t", claimed, want)
	}
	guard.UnprotectPaths(other, upper)
	guard.WithProtectedPaths(func(paths map[string]struct{}) {
		if !claimed {
			if len(paths) != 0 {
				t.Fatalf("rejected alias claim added protection: %v", paths)
			}
			return
		}
		wantCount := 1
		if compressed {
			wantCount = 2
		}
		if len(paths) != wantCount {
			t.Fatalf("successful case-distinct claim protection = %v", paths)
		}
		if _, exists := paths[lower]; !exists {
			t.Fatalf("successful claim lost its original path spelling: %v", paths)
		}
	})
}

func TestDirectoryGuardWindowsAliasesProtectActiveAndPurgePaths(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows path semantics")
	}
	root := filepath.Join(t.TempDir(), "Logs")
	extendedRoot := `\\?\` + root
	guard := DirectoryGuardForPath(root)
	if guard != DirectoryGuardForPath(extendedRoot) ||
		DirectorySizeTrackerForPath(root) != DirectorySizeTrackerForPath(extendedRoot) {
		t.Fatal("extended directory alias split shared state")
	}
	owner, task := NewDirectoryGuardToken(), NewDirectoryGuardToken()
	active := filepath.Join(root, "App.log")
	alias := filepath.Join(extendedRoot, "APP.LOG")
	if err := guard.TransitionActive(owner, func() (string, error) { return active, nil }); err != nil {
		t.Fatal(err)
	}
	defer guard.RemoveToken(owner)
	guard.ProtectPaths(task, alias)
	defer guard.UnprotectPaths(task, alias)
	guard.statesMu.RLock()
	protected, activePaths := guard.protectedAndActivePathsLocked()
	guard.statesMu.RUnlock()
	if len(protected) != 1 || len(activePaths) != 1 {
		t.Fatalf("alias snapshots are not deduplicated: %v / %v", protected, activePaths)
	}
	if _, exists := protected[active]; !exists {
		t.Fatalf("active spelling was not preserved: %v", protected)
	}
	if !pathidentity.Contains(protected, alias) || !pathidentity.Contains(activePaths, alias) {
		t.Fatalf("alias lookup failed: %v / %v", protected, activePaths)
	}
	files := []fsstore.FileInfo{{Path: alias}, {Path: active + ".other"}}
	if got := fsstore.ExcludeFileInfosByPaths(files, protected); len(got) != 1 || got[0].Path != active+".other" {
		t.Fatalf("protected alias was not excluded: %v", got)
	}
	claim := NewDirectoryGuardToken()
	if guard.ProtectPathsIfNoOtherOwnerConflict(claim, nil, []string{alias}) {
		t.Fatal("claim did not observe another owner's alias")
	}
	if err := guard.RunWithLifecycleLock(func() error {
		guard.RunPurgeWithProtection([]fsstore.FileInfo{{Path: alias}}, noAdditionalPurgeProtection, func(purge *Purge) {
			if !pathidentity.Contains(purge.ProtectedPaths(), alias) || !pathidentity.Contains(purge.ActivePaths(), alias) {
				t.Fatal("purge did not observe protected alias")
			}
			guard.statesMu.RLock()
			owners := len(guard.statesByToken)
			guard.statesMu.RUnlock()
			if owners != 2 {
				t.Fatalf("purge unnecessarily retained protected alias: owners=%d", owners)
			}
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	guard.UnprotectPaths(task, active)
	guard.WithProtectedPaths(func(paths map[string]struct{}) {
		if len(paths) != 1 || !pathidentity.Contains(paths, alias) {
			t.Fatalf("releasing one owner removed another owner's protection: %v", paths)
		}
	})
	guard.RemoveToken(owner)
	guard.WithProtectedPaths(func(paths map[string]struct{}) {
		if len(paths) != 0 {
			t.Fatalf("alias release left protection: %v", paths)
		}
	})
}

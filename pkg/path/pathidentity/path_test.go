package pathidentity

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestContainsKeepsCleanRelativePathExclusions(t *testing.T) {
	paths := map[string]struct{}{"App.log": {}}
	if !Contains(paths, "."+string(filepath.Separator)+"App.log") {
		t.Fatal("clean relative exclusion was ignored")
	}
	if Contains(nil, "App.log") {
		t.Fatal("empty set matched a path")
	}
}

func TestAbsoluteKeyNormalizesRelativePath(t *testing.T) {
	for _, rule := range directoryNameCaseRulesWithDirectoryRule() {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			t.Chdir(root)
			if rule.nameCase != NameCaseUnspecified {
				useDirectoryNameCaseForTest(t, root, rule.nameCase)
			}
			separator := string(filepath.Separator)
			path := "Parent" + separator + ".." + separator + "Logs" + separator + "App.log"
			leaf := filepath.Join("Logs", "App.log")
			if DirectoryNameCase(root) == NameCaseInsensitive {
				leaf = strings.ToLower(leaf)
			}
			want := filepath.Join(ComparisonKey(root), leaf)
			got, err := AbsoluteKey(path)
			if err != nil || got != want {
				t.Fatalf("AbsoluteKey(%q) = %q, %v; want %q", path, got, err, want)
			}
			if again, err := AbsoluteKey(got); err != nil || again != got {
				t.Fatalf("identity is not idempotent: %q, %v; want %q", again, err, got)
			}
		})
	}
}

func TestComparisonKeyKeepsEmptyPath(t *testing.T) {
	if got := ComparisonKey(""); got != "" {
		t.Fatalf("ComparisonKey(empty) = %q", got)
	}
}

func TestContainsPreservesOriginalSnapshotPaths(t *testing.T) {
	for _, rule := range directoryNameCaseRulesWithDirectoryRule() {
		t.Run(rule.name, func(t *testing.T) {
			testContainsPreservesOriginalSnapshotPaths(t, rule.nameCase)
		})
	}
}

func testContainsPreservesOriginalSnapshotPaths(t *testing.T, nameCase NameCase) {
	root := t.TempDir()
	t.Chdir(root)
	if nameCase != NameCaseUnspecified {
		useDirectoryNameCaseForTest(t, root, nameCase)
	}
	shared := DirectoryNameCase(root) == NameCaseInsensitive
	path, err := filepath.Abs(filepath.Join(root, "Logs", "App.log"))
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]struct{}{path: {}}
	for _, candidate := range []string{path, filepath.Join("Logs", "App.log")} {
		if !Contains(paths, candidate) {
			t.Fatalf("Contains(%v, %q) = false", paths, candidate)
		}
	}
	if got, want := Contains(paths, filepath.Join(root, "logs", "app.log")), shared; got != want {
		t.Fatalf("Contains(case alias) = %t, want %t", got, want)
	}
	if Contains(paths, path+".missing") || Contains(nil, path) {
		t.Fatal("missing path is reported as present")
	}
	if len(paths) != 1 {
		t.Fatalf("snapshot changed: %v", paths)
	}
	if _, exists := paths[path]; !exists {
		t.Fatalf("original path spelling changed: %v", paths)
	}
}

func TestComparisonKeyUsesFilesystemCaseRules(t *testing.T) {
	for _, rule := range directoryNameCaseRulesWithDirectoryRule() {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			if rule.nameCase != NameCaseUnspecified {
				useDirectoryNameCaseForTest(t, root, rule.nameCase)
			}
			upper := ComparisonKey(filepath.Join(root, "Logs", "App.log"))
			lower := ComparisonKey(filepath.Join(root, "logs", "app.log"))
			if got, want := upper == lower, DirectoryNameCase(root) == NameCaseInsensitive; got != want {
				t.Fatalf("case equality = %t, want %t: %q / %q", got, want, upper, lower)
			}
		})
	}
}

func TestWindowsPathKeyNormalizesDriveCaseAndExtendedAliases(t *testing.T) {
	for _, test := range []struct {
		path string
		want string
	}{
		{`C:\Logs\App.log`, `c:\logs\app.log`},
		{`c:\LOGS\APP.LOG`, `c:\logs\app.log`},
		{`\\?\C:\Logs\App.log`, `c:\logs\app.log`},
		{`\\Server\Share\Logs\App.log`, `\\server\share\logs\app.log`},
		{`\\?\UNC\Server\Share\Logs\App.log`, `\\server\share\logs\app.log`},
		{`\\?\Volume{ABC}\Logs\App.log`, `\\?\volume{abc}\logs\app.log`},
		{`\\.\PhysicalDrive0`, `\\.\physicaldrive0`},
	} {
		t.Run(test.path, func(t *testing.T) {
			if got := windowsPathKey(test.path); got != test.want {
				t.Fatalf("windowsPathKey(%q) = %q, want %q", test.path, got, test.want)
			}
		})
	}
}

func TestAbsoluteKeyWindowsExtendedAliases(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows path semantics")
	}
	for _, alias := range []string{`C:\Logs\App.log`, `c:\LOGS\APP.LOG`, `\\?\C:\Logs\App.log`} {
		got, err := AbsoluteKey(alias)
		if err != nil || got != `c:\logs\app.log` {
			t.Fatalf("AbsoluteKey(%q) = %q, %v", alias, got, err)
		}
		if !Contains(map[string]struct{}{`C:\Logs\App.log`: {}}, alias) {
			t.Fatalf("Contains() did not match alias %q", alias)
		}
	}
	for _, alias := range []string{`\\Server\Share\Logs`, `\\?\UNC\SERVER\SHARE\LOGS`} {
		got, err := AbsoluteKey(alias)
		if err != nil || got != `\\server\share\logs` {
			t.Fatalf("AbsoluteKey(%q) = %q, %v", alias, got, err)
		}
	}
}

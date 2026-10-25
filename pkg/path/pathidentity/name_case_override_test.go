package pathidentity

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/pathcaserule"
	"github.com/go-jappe/heterolog/internal/testutil"
)

// directoryNameCaseRulesは、directoryが大小文字を区別する場合・しない場合の両方を、
// OS名やfilesystemに依存せず試験するための規則です。
var directoryNameCaseRules = []struct {
	name     string
	nameCase NameCase
}{
	{name: "case-sensitive", nameCase: NameCaseSensitive},
	{name: "case-insensitive", nameCase: NameCaseInsensitive},
}

// directoryNameCaseRulesWithDirectoryRuleは、両方の規則に加えて、規則を差し替えず
// ComparisonKeyと判定を共用する保存先directoryの規則を使う場合を返します。
func directoryNameCaseRulesWithDirectoryRule() []struct {
	name     string
	nameCase NameCase
} {
	return append(append(directoryNameCaseRules[:0:0], directoryNameCaseRules...), struct {
		name     string
		nameCase NameCase
	}{name: "directory rule", nameCase: NameCaseUnspecified})
}

// useDirectoryNameCaseForTestは、root配下の規則をnameCaseへ差し替えます。
// process全体の設定なので、t.Parallelを使う試験からは呼び出さないでください。
func useDirectoryNameCaseForTest(t *testing.T, root string, nameCase NameCase) {
	t.Helper()
	testutil.UseDirectoryCaseRuleForTest(t, root, nameCase == NameCaseSensitive)
}

// 試験用の規則は指定root配下だけに適用し、rootの名前自体と他のdirectoryは通常の判定のままです。
func TestDirectoryNameCaseOverrideAppliesOnlyBelowRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "Root")
	outsideUpper, outsideLower := filepath.Join(parent, "Other", "App.log"), filepath.Join(parent, "other", "app.log")
	hostOutsideSame := ComparisonKey(outsideUpper) == ComparisonKey(outsideLower)
	for _, rule := range directoryNameCaseRules {
		t.Run(rule.name, func(t *testing.T) {
			useDirectoryNameCaseForTest(t, root, rule.nameCase)
			if got := DirectoryNameCase(root); got != rule.nameCase {
				t.Fatalf("DirectoryNameCase(root) = %d, want %d", got, rule.nameCase)
			}
			if got := DirectoryNameCase(filepath.Join(root, "Missing", "Logs")); got != rule.nameCase {
				t.Fatalf("DirectoryNameCase(missing child) = %d, want %d", got, rule.nameCase)
			}
			foldsCase := rule.nameCase == NameCaseInsensitive
			upper := ComparisonKey(filepath.Join(root, "Logs", "App.log"))
			lower := ComparisonKey(filepath.Join(root, "logs", "app.log"))
			if same := upper == lower; same != foldsCase {
				t.Fatalf("names below root share key = %t, want %t", same, foldsCase)
			}
			if got := Contains(map[string]struct{}{filepath.Join(root, "Logs", "App.log"): {}},
				filepath.Join(root, "LOGS", "APP.LOG")); got != foldsCase {
				t.Fatalf("Contains(case alias below root) = %t, want %t", got, foldsCase)
			}
			if same := ComparisonKey(outsideUpper) == ComparisonKey(outsideLower); same != hostOutsideSame {
				t.Fatalf("override changed a directory outside root: same = %t, want host rule %t", same, hostOutsideSame)
			}
		})
	}
	if pathcaserule.Active() {
		t.Fatal("directory case rule override was not restored")
	}
}

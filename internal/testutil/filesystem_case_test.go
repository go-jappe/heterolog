package testutil_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/pathcaserule"
	"github.com/go-jappe/heterolog/internal/testutil"
)

// 大小文字を区別しないfilesystemでは、本番の比較keyがrootの祖先の名前を小文字へ揃えるため、
// keyを再び比較すると、規則を差し替えたrootを登録時と異なる表記で問い合わせます。作成済みのrootは
// 実体で照合し、symlinkを解決した表記と解決していない表記のどちらで登録しても、両方の表記と
// symlinkを通した別名での問い合わせに同じ規則を適用することを確認します。
// rootの親から上と兄弟のdirectoryは、filesystemの判定のままです。
func TestUseDirectoryCaseRuleForTestMatchesExistingRootByIdentity(t *testing.T) {
	for _, sensitive := range []bool{true, false} {
		for _, registration := range []string{"unresolved", "resolved"} {
			t.Run(fmt.Sprintf("sensitive=%t/%s", sensitive, registration), func(t *testing.T) {
				fixture := newLinkedDirectoryCaseRuleFixtureForTest(t)
				linkedRoot, physicalRoot := filepath.Join(fixture.linkedParent, "Root"), filepath.Join(fixture.physicalParent, "Root")
				sibling := filepath.Join(fixture.physicalParent, "Other")
				for _, directory := range []string{physicalRoot, sibling} {
					if err := os.Mkdir(directory, 0o700); err != nil {
						t.Fatal(err)
					}
				}
				// macOSで大小文字を揃えた表記と同じく、表記は異なるが同じ実体を指すpathです。
				alias := filepath.Join(fixture.base, "Alias")
				testutil.Symlink(t, physicalRoot, alias)
				root := linkedRoot
				if registration == "resolved" {
					root = physicalRoot
				}

				testutil.UseDirectoryCaseRuleForTest(t, root, sensitive)
				for _, directory := range []string{
					linkedRoot, physicalRoot, alias,
					filepath.Join(linkedRoot, "Missing", "Logs"),
					filepath.Join(physicalRoot, "Logs"),
					filepath.Join(alias, "Missing", "Logs"),
				} {
					requireDirectoryCaseRuleForTest(t, directory, sensitive)
				}
				for _, directory := range []string{
					fixture.base, fixture.linkedParent, fixture.physicalParent, sibling, filepath.Join(sibling, "Logs"),
				} {
					requireFilesystemCaseRuleForTest(t, directory)
				}
			})
		}
	}
	if pathcaserule.Active() {
		t.Fatal("directory case rule override was not restored")
	}
}

// 大小文字だけ異なる兄弟は、filesystemが同じ実体へ解決する場合だけrootとして扱います。
// 大小文字を区別するfilesystemでは別のdirectoryを作れるため、その兄弟へ規則を適用しません。
func TestUseDirectoryCaseRuleForTestMatchesCaseVariantOnlyAsSameDirectory(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "Root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	testutil.UseDirectoryCaseRuleForTest(t, root, true)
	variant := filepath.Join(base, "ROOT")
	switch err := os.Mkdir(variant, 0o700); {
	case err == nil:
		for _, directory := range []string{variant, filepath.Join(variant, "Logs")} {
			requireFilesystemCaseRuleForTest(t, directory)
		}
	case errors.Is(err, fs.ErrExist):
		// 大小文字を区別しないfilesystemでは、大小文字を揃えた表記もrootそのものです。
		for _, directory := range []string{variant, filepath.Join(variant, "Missing", "Logs")} {
			requireDirectoryCaseRuleForTest(t, directory, true)
		}
	default:
		t.Fatalf("create case variant of root: %v", err)
	}
	requireDirectoryCaseRuleForTest(t, filepath.Join(root, "Logs"), true)
	requireFilesystemCaseRuleForTest(t, base)
}

// 未作成のrootは表記で照合し、既存祖先のsymlinkを解決した表記にも同じ規則を適用します。
// 照合のためにrootやその親を作成しません。
func TestUseDirectoryCaseRuleForTestMatchesMissingRootBySpelling(t *testing.T) {
	fixture := newLinkedDirectoryCaseRuleFixtureForTest(t)
	linkedRoot := filepath.Join(fixture.linkedParent, "Missing", "Root")
	physicalRoot := filepath.Join(fixture.physicalParent, "Missing", "Root")
	testutil.UseDirectoryCaseRuleForTest(t, linkedRoot, true)
	for _, directory := range []string{linkedRoot, filepath.Join(linkedRoot, "Logs"), physicalRoot, filepath.Join(physicalRoot, "Logs")} {
		requireDirectoryCaseRuleForTest(t, directory, true)
	}
	for _, directory := range []string{
		filepath.Dir(linkedRoot), filepath.Dir(physicalRoot), fixture.linkedParent, fixture.base,
		filepath.Join(fixture.physicalParent, "Other", "Root"),
	} {
		requireFilesystemCaseRuleForTest(t, directory)
	}
	if _, err := os.Lstat(filepath.Dir(physicalRoot)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("case rule lookup created the missing root parent: %v", err)
	}
}

// linkedDirectoryCaseRuleFixtureForTestは、macOSの/var -> /private/varと同じく、
// symlinkの親を通る表記と実体の表記の両方を持つ試験用directoryです。
type linkedDirectoryCaseRuleFixtureForTest struct {
	base           string
	physicalParent string
	linkedParent   string
}

func newLinkedDirectoryCaseRuleFixtureForTest(t *testing.T) linkedDirectoryCaseRuleFixtureForTest {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fixture := linkedDirectoryCaseRuleFixtureForTest{
		base:           base,
		physicalParent: filepath.Join(base, "private-var"),
		linkedParent:   filepath.Join(base, "var"),
	}
	if err := os.Mkdir(fixture.physicalParent, 0o700); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, fixture.physicalParent, fixture.linkedParent)
	return fixture
}

// requireDirectoryCaseRuleForTestは、directoryに差し替えた規則が適用されることを確認します。
func requireDirectoryCaseRuleForTest(t *testing.T, directory string, sensitive bool) {
	t.Helper()
	if got, ok := pathcaserule.Lookup(directory); !ok || got != sensitive {
		t.Fatalf("Lookup(%q) = %t, %t; want %t, true", directory, got, ok, sensitive)
	}
}

// requireFilesystemCaseRuleForTestは、directoryの規則を差し替えず、filesystemの判定へ任せることを確認します。
func requireFilesystemCaseRuleForTest(t *testing.T, directory string) {
	t.Helper()
	if got, ok := pathcaserule.Lookup(directory); ok {
		t.Fatalf("Lookup(%q) = %t, true; want the filesystem rule", directory, got)
	}
}

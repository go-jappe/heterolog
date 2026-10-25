package path

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func fixedDirectoryNameCase(nameCase pathidentity.NameCase) func(string) pathidentity.NameCase {
	return func(string) pathidentity.NameCase { return nameCase }
}

// OS名ではなく各階層の親ディレクトリの規則で照合し、両方の規則をどのOSでも確認します。
func TestBuildApplicationDataDirectoryPathFollowsDirectoryNameCase(t *testing.T) {
	for _, tt := range []struct {
		name, base, target string
		containsExact      bool
		containsCaseAlias  bool
	}{
		{"missing target", filepath.Join("Data", "Archives"), "service", false, false},
		{"exact final component", filepath.Join("Data", "service"), "service", true, true},
		{"case alias final component", filepath.Join("Data", "SERVICE"), "service", false, true},
		{"exact ancestor", filepath.Join("Data", "service", "Archives"), "service", true, true},
		{"case alias ancestor", filepath.Join("Data", "SERVICE", "Archives"), "service", false, true},
		{"case alias target", filepath.Join("Data", "service"), "SeRvIcE", false, true},
		{"Unicode case alias", filepath.Join("Data", "ÄPP"), "äpp", false, true},
		{"prefix is not component", filepath.Join("Data", "service-cache"), "service", false, false},
		{"suffix is not component", filepath.Join("Data", "otherservice"), "service", false, false},
		{"missing target preserves its case", "Data", "SeRvIcE", false, false},
	} {
		for _, rule := range []struct {
			name     string
			nameCase pathidentity.NameCase
		}{
			{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
			{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive},
		} {
			t.Run(rule.name+"/"+tt.name, func(t *testing.T) {
				directoryNameCase := fixedDirectoryNameCase(rule.nameCase)
				want := filepath.Join(tt.base, tt.target)
				// 大小文字を区別しないディレクトリでは、大小文字だけが異なる名前も同じディレクトリです。
				if tt.containsExact || (rule.nameCase == pathidentity.NameCaseInsensitive && tt.containsCaseAlias) {
					want = filepath.Clean(tt.base)
				}
				got := buildApplicationDataDirectoryPath(tt.base, tt.target, directoryNameCase)
				if got != want {
					t.Fatalf("buildApplicationDataDirectoryPath(%q, %q) = %q, want %q", tt.base, tt.target, got, want)
				}
				if again := buildApplicationDataDirectoryPath(got, tt.target, directoryNameCase); again != got {
					t.Fatalf("target duplicated on second resolution: %q -> %q", got, again)
				}
			})
		}
		t.Run("directory rule/"+tt.name, func(t *testing.T) {
			// 公開関数はComparisonKeyと共用する保存先ディレクトリの判定を使います。
			want := buildApplicationDataDirectoryPath(tt.base, tt.target, pathidentity.DirectoryNameCase)
			if got := BuildApplicationDataDirectoryPath(tt.base, tt.target); got != want {
				t.Fatalf("BuildApplicationDataDirectoryPath(%q, %q) = %q, want directory rule result %q", tt.base, tt.target, got, want)
			}
		})
	}
}

// 名前は、その名前を含む親ディレクトリの規則で照合します。
func TestBuildApplicationDataDirectoryPathUsesEachParentDirectoryRule(t *testing.T) {
	root := filepath.Join("Data", "Mount")
	base := filepath.Join(root, "SERVICE", "Archives")
	var queried []string
	directoryNameCase := func(directory string) pathidentity.NameCase {
		queried = append(queried, directory)
		if directory == root {
			return pathidentity.NameCaseInsensitive
		}
		return pathidentity.NameCaseSensitive
	}
	if got := buildApplicationDataDirectoryPath(base, "service", directoryNameCase); got != base {
		t.Fatalf("case alias inside an insensitive parent = %q, want %q", got, base)
	}
	if want := []string{filepath.Join(root, "SERVICE"), root}; len(queried) != len(want) || queried[0] != want[0] || queried[1] != want[1] {
		t.Fatalf("queried directories = %q, want %q", queried, want)
	}
	insensitiveElsewhere := func(directory string) pathidentity.NameCase {
		if directory == filepath.Join(root, "SERVICE") {
			return pathidentity.NameCaseInsensitive
		}
		return pathidentity.NameCaseSensitive
	}
	// 区別しないのが別の階層なら、区別するディレクトリ内の`SERVICE`は別名です。
	if got, want := buildApplicationDataDirectoryPath(base, "service", insensitiveElsewhere), filepath.Join(base, "service"); got != want {
		t.Fatalf("case alias inside a sensitive parent = %q, want %q", got, want)
	}
}

func TestBuildApplicationDataDirectoryPathPreservesCleaning(t *testing.T) {
	base := "Data" + string(filepath.Separator) + "." + string(filepath.Separator) + "service"
	if got, want := BuildApplicationDataDirectoryPath(base, "service"), filepath.Join("Data", "service"); got != want {
		t.Fatalf("cleaned path = %q, want %q", got, want)
	}
}

package logpattern

import (
	"github.com/go-jappe/heterolog/internal/testutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestCompressedFileMatchers(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "logs")
	pattern := FileNamePattern{
		Dir: directory, Prefix: "error_", Target: "app", Extension: "log",
	}
	matchPath := CompressedFilePathMatcher(pattern)
	matchName := CompressedFileNameMatcher(pattern)
	for _, test := range []struct {
		name     string
		path     string
		wantPath bool
		wantName bool
	}{
		{"compressed", filepath.Join(directory, "error_app_20260925_1.log.gz"), true, true},
		{"other codec", filepath.Join(directory, "error_app_20260925_2.log.zst"), true, true},
		{"custom codec", filepath.Join(directory, "error_app_20260925_3.log.custom-format"), true, true},
		{"different directory", filepath.Join(directory, "..", "spool", "error_app_20260925_1.log.gz"), false, true},
		{"nested directory", filepath.Join(directory, "child", "error_app_20260925_1.log.gz"), false, true},
		{"name only", "error_app_20260925_1.log.gz", false, true},
		{"raw", filepath.Join(directory, "error_app_20260925_1.log"), false, false},
		{"temporary compressed", filepath.Join(directory, "error_app_20260925_1.log.gz.tmp"), false, false},
		{"temporary raw", filepath.Join(directory, "error_app_20260925_1.log.tmp"), false, false},
		{"different prefix", filepath.Join(directory, "debug_app_20260925_1.log.gz"), false, false},
		{"different target", filepath.Join(directory, "error_other_20260925_1.log.gz"), false, false},
		{"different extension", filepath.Join(directory, "error_app_20260925_1.jsonl.gz"), false, false},
		{"invalid date", filepath.Join(directory, "error_app_20260230_1.log.gz"), false, false},
		{"zero index", filepath.Join(directory, "error_app_20260925_0.log.gz"), false, false},
		{"noncanonical index", filepath.Join(directory, "error_app_20260925_01.log.gz"), false, false},
		{"empty", "", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			// ファイル自体は作成せず、パスの命名規則だけで判定できることも確認します。
			if got := matchPath(test.path); got != test.wantPath {
				t.Errorf("path matcher(%q) = %v, want %v", test.path, got, test.wantPath)
			}
			if got := matchName(test.path); got != test.wantName {
				t.Errorf("name matcher(%q) = %v, want %v", test.path, got, test.wantName)
			}
		})
	}
}

func TestCompressedFileMatchersUseDirectoryPathAndNameRules(t *testing.T) {
	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		root := t.TempDir()
		// directoryの別表記も、OS名やfilesystemではなく同じ規則で比較します。
		testutil.UseDirectoryCaseRuleForTest(t, root, !foldsCase)
		directoryIgnoresCase := foldsCase
		pattern := FileNamePattern{
			Dir: filepath.Join(root, "Logs"), Prefix: "error_", Target: "app", Extension: ".log", NameCase: nameCase,
		}
		matchPath := CompressedFilePathMatcher(pattern)
		matchName := CompressedFileNameMatcher(pattern)
		name := "error_app_20260925_1.log.gz"
		for _, test := range []struct {
			name     string
			path     string
			wantPath bool
			wantName bool
		}{
			{"directory spelling", filepath.Join(root, "LOGS", name), directoryIgnoresCase, true},
			// file名は保存先directoryの規則で照合し、OS名では判定しません。
			{"filename spelling", filepath.Join(pattern.Dir, strings.ToUpper(name)), foldsCase, foldsCase},
			{"directory normalization", pattern.Dir + string(filepath.Separator) + "." + string(filepath.Separator) + name, true, true},
		} {
			if got := matchPath(test.path); got != test.wantPath {
				t.Errorf("%s: path matcher(%q) = %v, want %v", test.name, test.path, got, test.wantPath)
			}
			if got := matchName(test.path); got != test.wantName {
				t.Errorf("%s: name matcher(%q) = %v, want %v", test.name, test.path, got, test.wantName)
			}
		}
	})
	t.Run("directory rule", func(t *testing.T) {
		root := t.TempDir()
		directory := filepath.Join(root, "Logs")
		pattern := FileNamePattern{Dir: directory, Prefix: "error_", Target: "app", Extension: ".log"}
		resolved := pattern
		resolved.NameCase = pathidentity.DirectoryNameCase(directory)
		directoryAlias := filepath.Join(root, "LOGS", "error_app_20260925_1.log.gz")
		wantDirectoryAlias := pathidentity.DirectoryNameCase(root) == pathidentity.NameCaseInsensitive
		if got := CompressedFilePathMatcher(pattern)(directoryAlias); got != wantDirectoryAlias {
			t.Fatalf("path matcher(directory alias) = %t, want directory rule result %t", got, wantDirectoryAlias)
		}
		path := filepath.Join(directory, "ERROR_APP_20260925_1.LOG.GZ")
		if got, want := CompressedFilePathMatcher(pattern)(path), CompressedFilePathMatcher(resolved)(path); got != want {
			t.Fatalf("path matcher without explicit rule = %t, want directory rule result %t", got, want)
		}
		if got, want := CompressedFileNameMatcher(pattern)(path), CompressedFileNameMatcher(resolved)(path); got != want {
			t.Fatalf("name matcher without explicit rule = %t, want directory rule result %t", got, want)
		}
	})
}

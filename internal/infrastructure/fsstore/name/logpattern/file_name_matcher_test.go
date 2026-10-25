package logpattern

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestFileNameMatcherMatchesBasenameWithoutDirectoryOrGenerationRestrictions(t *testing.T) {
	t.Parallel()

	pattern := FileNamePattern{
		Dir: filepath.Join("not-created", "logs"), Prefix: "error_[x]_", Target: "app[1]",
		Date: "20991231", Extension: ".jsonl", Index: 999,
	}
	matchTarget := FileNameMatcher(pattern, GlobByTarget)
	matchAll := FileNameMatcher(pattern, GlobAllFiles)
	for _, test := range []struct {
		name       string
		path       string
		wantTarget bool
		wantAll    bool
	}{
		{name: "raw basename", path: "error_[x]_app[1]_20260925_1.jsonl", wantTarget: true, wantAll: true},
		{name: "custom compression in another directory", path: filepath.Join("not-created", "spool", "error_[x]_app[1]_20260925_1.jsonl.custom_format-v2"), wantTarget: true, wantAll: true},
		{name: "noncanonical notes", path: "error_[x]_app[1]_notes.jsonl", wantTarget: true, wantAll: true},
		{name: "prefix is literal", path: "error_x_app[1]_20260925_1.jsonl", wantAll: true},
		{name: "target is literal", path: "error_[x]_app1_20260925_1.jsonl.gz", wantAll: true},
		{name: "target separator required", path: "error_[x]_app[1]other_20260925_1.jsonl", wantAll: true},
		{name: "different base extension", path: "error_[x]_app[1]_20260925_1.log.gz"},
		{name: "reserved temporary suffix", path: "error_[x]_app[1]_20260925_1.jsonl.tmp"},
		{name: "unfinished compression", path: "error_[x]_app[1]_20260925_1.jsonl.gz.tmp"},
		{name: "multiple compression suffixes", path: "error_[x]_app[1]_20260925_1.jsonl.tar.gz"},
		{name: "directory spelling does not supply target", path: filepath.Join("error_[x]_app[1]_20260925_1.jsonl", "other.jsonl"), wantAll: true},
		{name: "empty path"},
	} {
		t.Run(test.name, func(t *testing.T) {
			// ディレクトリもファイルも作らず、basenameだけで判定できることを確認します。
			if got := matchTarget(test.path); got != test.wantTarget {
				t.Errorf("target matcher(%q) = %t, want %t", test.path, got, test.wantTarget)
			}
			if got := matchAll(test.path); got != test.wantAll {
				t.Errorf("all-files matcher(%q) = %t, want %t", test.path, got, test.wantAll)
			}
		})
	}
}

func TestFileNameMatcherPreservesLiteralWildcards(t *testing.T) {
	t.Parallel()

	match := FileNameMatcher(FileNamePattern{
		Prefix: "error_*?_", Target: "app[1]", Extension: "l[o]g",
	}, GlobByTarget)
	for _, test := range []struct {
		name string
		want bool
	}{
		{name: "error_*?_app[1]_notes.l[o]g", want: true},
		{name: "error_XY_app[1]_notes.l[o]g"},
		{name: "error_*?_app1_notes.l[o]g"},
		{name: "error_*?_app[1]_notes.log"},
	} {
		// glob記号は文字列として扱い、隣の系列や拡張子へ展開しません。
		if got := match(test.name); got != test.want {
			t.Errorf("matcher(%q) = %t, want %t", test.name, got, test.want)
		}
	}
}

func TestFileNameMatcherKeepsBroadDiscoverySeparateFromCanonicalParsing(t *testing.T) {
	t.Parallel()

	pattern := FileNamePattern{Target: "app", Extension: "log"}
	match := FileNameMatcher(pattern, GlobByTarget)
	for _, name := range []string{
		"app_notes.log",
		"app_notes.log.custom-format",
		"app_20260230_1.log.gz",
		"app_20260925_0.log",
		"app_20260925_01.log.gz",
	} {
		t.Run(name, func(t *testing.T) {
			// discoveryでは従来の広い一致を保ち、writer管理対象の厳密な検査とは分けます。
			if !match(name) {
				t.Fatalf("matcher(%q) = false, want broad discovery match", name)
			}
			if generation, ok := ParseCanonicalGenerationFilePath(name, pattern); ok {
				t.Fatalf("canonical parser(%q) = %+v, want rejected", name, generation)
			}
		})
	}
}

func TestFileNameMatcherUsesDirectoryNameComparisonRules(t *testing.T) {
	t.Parallel()

	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		pattern := FileNamePattern{Prefix: "error_", Target: "kernel", Extension: "jsonl", NameCase: nameCase}
		match := FileNameMatcher(pattern, GlobByTarget)
		for _, name := range []string{
			"Error_kernel_notes.jsonl",
			"error_Kernel_notes.jsonl.gz",
			"error_kernel_notes.JSONL",
		} {
			// 大小文字を区別しないディレクトリでは、大小文字とUnicodeの小文字化を反映します。
			if got := match(name); got != foldsCase {
				t.Errorf("matcher(%q) = %t, want %t", name, got, foldsCase)
			}
		}
		if !match("error_kernel_notes.jsonl") {
			t.Error("matcher rejected the configured spelling")
		}
	})
	t.Run("directory rule", func(t *testing.T) {
		directory := t.TempDir()
		pattern := FileNamePattern{Dir: directory, Prefix: "error_", Target: "kernel", Extension: "jsonl"}
		resolved := pattern
		resolved.NameCase = pathidentity.DirectoryNameCase(directory)
		for _, name := range []string{"Error_kernel_notes.jsonl", "error_kernel_notes.jsonl"} {
			if got, want := FileNameMatcher(pattern, GlobByTarget)(name), FileNameMatcher(resolved, GlobByTarget)(name); got != want {
				t.Errorf("matcher(%q) without explicit rule = %t, want directory rule result %t", name, got, want)
			}
		}
	})
}

func TestFileNameMatcherRejectsUnknownMode(t *testing.T) {
	t.Parallel()

	for _, mode := range []GlobMode{GlobMode(-1), GlobMode(99)} {
		match := FileNameMatcher(FileNamePattern{Target: "app", Extension: "log"}, mode)
		for _, name := range []string{"app_20260925_1.log", "app_notes.log.gz", "other.log", ""} {
			if match(name) {
				t.Errorf("matcher mode %d accepted %q", mode, name)
			}
		}
	}
}

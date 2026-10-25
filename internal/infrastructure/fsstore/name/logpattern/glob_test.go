package logpattern

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestGlobFileNamesByTargetIncludesUserDefinedCompressionSuffix(t *testing.T) {
	dir := t.TempDir()
	rawPath := filepath.Join(dir, "test-app_20260712_1.jsonl")
	compressedPaths := []string{
		rawPath + fsstore.ExtensionCompressGzip,
		rawPath + ".br",
		rawPath + ".custom_format-v2",
	}
	otherTargetPath := filepath.Join(dir, "other-app_20260712_1.jsonl.gz")
	incompletePaths := []string{
		rawPath + ".gz.tmp",
		rawPath + ".tmp",
		rawPath + ".invalid.suffix",
	}
	paths := append([]string{rawPath, otherTargetPath}, compressedPaths...)
	paths = append(paths, incompletePaths...)
	for _, path := range paths {
		if err := os.WriteFile(path, []byte("archive"), 0o600); err != nil {
			t.Fatalf("write %q: %v", path, err)
		}
	}

	matches, err := GlobFileNames(
		FileNamePattern{Dir: dir, Target: "test-app", Extension: "jsonl"},
		GlobByTarget,
	)
	if err != nil {
		t.Fatalf("GlobFileNames: %v", err)
	}
	if !slices.Contains(matches, rawPath) {
		t.Fatalf("matches = %v, want raw archive %q", matches, rawPath)
	}
	for _, compressedPath := range compressedPaths {
		if !slices.Contains(matches, compressedPath) {
			t.Fatalf("matches = %v, want compressed archive %q", matches, compressedPath)
		}
	}
	if slices.Contains(matches, otherTargetPath) {
		t.Fatalf("matches = %v, must not include other target %q", matches, otherTargetPath)
	}
	for _, incompletePath := range incompletePaths {
		if slices.Contains(matches, incompletePath) {
			t.Fatalf("matches = %v, must not include incomplete archive %q", matches, incompletePath)
		}
	}
}

func TestTrimBaseAndOptionalCompressionExtensions(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		wantStem string
		wantOK   bool
	}{
		{name: "uncompressed", fileName: "app_20260804_1.log", wantStem: "app_20260804_1", wantOK: true},
		{name: "built in", fileName: "app_20260804_1.log.gz", wantStem: "app_20260804_1", wantOK: true},
		{name: "user defined", fileName: "app_20260804_1.log.br", wantStem: "app_20260804_1", wantOK: true},
		{name: "user defined unicode", fileName: "app_20260804_1.log.独自", wantStem: "app_20260804_1", wantOK: true},
		{name: "compression temporary", fileName: "app_20260804_1.log.gz.tmp", wantOK: false},
		{name: "reserved temporary", fileName: "app_20260804_1.log.tmp", wantOK: false},
		{name: "multiple suffixes", fileName: "app_20260804_1.log.tar.gz", wantOK: false},
		{name: "path separator", fileName: "app_20260804_1.log.a/b", wantOK: false},
		{name: "missing base extension", fileName: "app_20260804_1.br", wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stem, ok := TrimBaseAndOptionalCompressionExtensions(test.fileName, "log")
			if stem != test.wantStem || ok != test.wantOK {
				t.Fatalf(
					"TrimBaseAndOptionalCompressionExtensions(%q, log) = (%q, %t), want (%q, %t)",
					test.fileName,
					stem,
					ok,
					test.wantStem,
					test.wantOK,
				)
			}
		})
	}
}

func TestIsCompressedGenerationFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "raw", path: "/logs/app_1.log", want: false},
		{name: "gzip", path: "/logs/app_1.log.gz", want: true},
		{name: "custom", path: "/logs/app_1.log.br", want: true},
		{name: "temporary", path: "/logs/app_1.log.gz.tmp", want: false},
		{name: "different base", path: "/logs/app_1.jsonl.gz", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := IsCompressedGenerationFile(test.path, "log"); got != test.want {
				t.Fatalf(
					"IsCompressedGenerationFile(%q, log) = %t, want %t",
					test.path,
					got,
					test.want,
				)
			}
		})
	}
}

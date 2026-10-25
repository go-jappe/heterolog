package logpattern

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestGlobLiteralNamesPreserveDiscoveryAndReservedGenerations(t *testing.T) {
	type literalNameCase struct {
		name, directory, neighborDirectory string
		prefix, neighborPrefix             string
		target, neighborTarget             string
		extension, neighborExtension       string
	}
	tests := []literalNameCase{
		{name: "directory brackets", directory: "logs[1]", neighborDirectory: "logs1", target: "app", neighborTarget: "app", extension: "log", neighborExtension: "log"},
		{name: "target brackets", target: "app[1]", neighborTarget: "app1", extension: "log", neighborExtension: "log"},
		{name: "unclosed bracket", target: "app[", neighborTarget: "app", extension: "log", neighborExtension: "log"},
		{name: "prefix brackets", prefix: "error_[x]_", neighborPrefix: "error_x_", target: "app", neighborTarget: "app", extension: "log", neighborExtension: "log"},
		{name: "extension brackets", target: "app", neighborTarget: "app", extension: "l[o]g", neighborExtension: "log"},
	}
	// Windowsで禁止されている文字も、Unix上では通常のfile名として照合します。
	if runtime.GOOS != "windows" {
		tests = append(tests,
			literalNameCase{name: "Unix wildcard target", target: "app*?", neighborTarget: "appXX", extension: "log", neighborExtension: "log"},
			literalNameCase{name: "Unix backslash target", target: `app\name`, neighborTarget: "appname", extension: "log", neighborExtension: "log"},
			literalNameCase{name: "Unix wildcard directory", directory: "logs*", neighborDirectory: "logsX", target: "app", neighborTarget: "app", extension: "log", neighborExtension: "log"},
		)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parent := t.TempDir()
			directory := filepath.Join(parent, test.directory)
			neighborDirectory := directory
			if test.neighborDirectory != "" {
				neighborDirectory = filepath.Join(parent, test.neighborDirectory)
			}
			for _, path := range []string{directory, neighborDirectory} {
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			base := test.prefix + test.target + "_20260911_"
			neighborBase := test.neighborPrefix + test.neighborTarget + "_20260911_"
			raw := filepath.Join(directory, base+"1."+test.extension)
			compressed := filepath.Join(directory, base+"4."+test.extension+".gz")
			custom := filepath.Join(directory, base+"5."+test.extension+".custom_format-v2")
			// discoveryの任意部分をcanonicalな日付・番号へ狭めないことも確認します。
			historical := filepath.Join(directory, test.prefix+test.target+"_historical."+test.extension)
			pending := filepath.Join(directory, base+"7."+test.extension+CompressionSourceDeletePendingSuffix)
			neighborRaw := filepath.Join(neighborDirectory, neighborBase+"90."+test.neighborExtension)
			neighborCompressed := neighborRaw + ".gz"
			neighborPending := neighborRaw + CompressionSourceDeletePendingSuffix
			paths := []string{raw, compressed, custom, historical, pending, neighborRaw, neighborCompressed, neighborPending,
				compressed + ".tmp", raw + ".tmp", raw + ".invalid.suffix",
				filepath.Join(directory, base+"99."+test.extension+".gz"+CompressionSourceDeletePendingSuffix)}
			for _, path := range paths {
				if err := os.WriteFile(path, []byte(path), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			pattern := FileNamePattern{Dir: directory, Prefix: test.prefix, Target: test.target, Date: "20260911", Extension: test.extension}
			for _, mode := range []GlobMode{GlobByTarget, GlobAllFiles} {
				want := []string{raw, compressed, custom, historical}
				wantPending := []string{pending}
				if mode == GlobAllFiles && directory == neighborDirectory && test.extension == test.neighborExtension {
					want = append(want, neighborRaw, neighborCompressed)
					wantPending = append(wantPending, neighborPending)
				}
				got, err := GlobFileNames(pattern, mode)
				if err != nil {
					t.Fatalf("GlobFileNames mode %d: %v", mode, err)
				}
				assertGlobLiteralPaths(t, got, want)
				gotPending, err := FindCompressionSourceDeletePendingPaths(pattern, mode)
				if err != nil {
					t.Fatalf("FindCompressionSourceDeletePendingPaths mode %d: %v", mode, err)
				}
				assertGlobLiteralPaths(t, gotPending, wantPending)
			}
			index, err := FindMaxIndex(pattern)
			if err != nil || index != 7 {
				t.Fatalf("FindMaxIndex = %d, %v; want 7", index, err)
			}
			if test.extension == "log" {
				cfg := rotationNameTestConfig{rotation: pkgfsstore.FileRotationDaily, fileDateFormat: pkgfsstore.FileDateFormat}
				got, err := ResolveFilePathAt(&pattern, cfg, time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC))
				want := filepath.Join(directory, base+"8.log")
				if err != nil || pattern.Index != 8 || got != want {
					t.Fatalf("ResolveFilePathAt = %q, index %d, %v; want %q, index 8", got, pattern.Index, err, want)
				}
			}
			for _, path := range paths {
				if body, err := os.ReadFile(path); err != nil || string(body) != path {
					t.Fatalf("existing file changed: %q: %q, %v", path, body, err)
				}
			}
		})
	}
}

func TestGlobLiteralCompressedPeerDoesNotReuseGenerationOrMatchNeighbor(t *testing.T) {
	for _, test := range []struct {
		name, existing     string
		wantMax, wantIndex int
	}{
		{name: "exact peer", existing: "app[1]_20260911_7.log.gz", wantMax: 7, wantIndex: 8},
		{name: "custom peer", existing: "app[1]_20260911_7.log.custom_format-v2", wantMax: 7, wantIndex: 8},
		{name: "neighbor", existing: "app1_20260911_7.log.gz", wantMax: 1, wantIndex: 7},
		{name: "longer raw name", existing: "app[1]_20260911_7.log.other.log.gz", wantMax: 1, wantIndex: 7},
		{name: "uncommitted peer", existing: "app[1]_20260911_7.log.gz.tmp", wantMax: 1, wantIndex: 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "logs[1]")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			existing := filepath.Join(directory, test.existing)
			if err := os.WriteFile(existing, []byte("retained archive"), 0o600); err != nil {
				t.Fatal(err)
			}
			pattern := FileNamePattern{Dir: directory, Target: "app[1]", Date: "20260911", Extension: "log", Index: 7}
			index, err := FindMaxIndex(pattern)
			if err != nil || index != test.wantMax {
				t.Fatalf("FindMaxIndex = %d, %v; want %d", index, err, test.wantMax)
			}
			cfg := rotationNameTestConfig{rotation: pkgfsstore.FileRotationDaily, fileDateFormat: pkgfsstore.FileDateFormat}
			raw := BuildFileFullPath(pattern.Date, pattern, cfg)
			// FindMaxIndexを経由しない経路でも、既存peerの世代を再openしません。
			got, err := selectWritableFilePathForDate(raw, &pattern, cfg, pattern.Date)
			wantPattern := pattern
			wantPattern.Index = test.wantIndex
			want := BuildFileFullPath(pattern.Date, wantPattern, cfg)
			if err != nil || pattern.Index != test.wantIndex || got != want {
				t.Fatalf("selectWritableFilePathForDate = %q, index %d, %v; want %q, index %d", got, pattern.Index, err, want, test.wantIndex)
			}
			if body, err := os.ReadFile(existing); err != nil || string(body) != "retained archive" {
				t.Fatalf("existing peer changed: %q, %v", body, err)
			}
		})
	}
}

func assertGlobLiteralPaths(t *testing.T, got, want []string) {
	t.Helper()
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("paths = %q, want %q", got, want)
	}
}

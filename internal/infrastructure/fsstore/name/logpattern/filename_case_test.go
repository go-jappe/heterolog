package logpattern

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// runForNameCasesは、保存先ディレクトリが大小文字を区別する場合・しない場合の両方で試験します。
// 規則はFileNamePattern.NameCaseなどで明示し、実行するOSやfilesystemには依存しません。
func runForNameCases(t *testing.T, test func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool)) {
	t.Helper()
	for _, nameCase := range []pathidentity.NameCase{pathidentity.NameCaseSensitive, pathidentity.NameCaseInsensitive} {
		name := "case-sensitive"
		if nameCase == pathidentity.NameCaseInsensitive {
			name = "case-insensitive"
		}
		t.Run(name, func(t *testing.T) {
			test(t, nameCase, nameCase == pathidentity.NameCaseInsensitive)
		})
	}
}

func TestFileNamePatternEffectiveNameCaseUsesDirectoryUnlessSpecified(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "Logs")
	for _, nameCase := range []pathidentity.NameCase{pathidentity.NameCaseSensitive, pathidentity.NameCaseInsensitive} {
		if got := (FileNamePattern{Dir: directory, NameCase: nameCase}).EffectiveNameCase(); got != nameCase {
			t.Fatalf("explicit name case %d changed to %d", nameCase, got)
		}
	}
	// 未指定ならComparisonKeyと共用するディレクトリの判定を使います。
	if got, want := (FileNamePattern{Dir: directory}).EffectiveNameCase(), pathidentity.DirectoryNameCase(directory); got != want {
		t.Fatalf("unspecified name case = %d, want directory rule %d", got, want)
	}
	if got, want := (FileNamePattern{}).EffectiveNameCase(), pathidentity.DirectoryNameCase("."); got != want {
		t.Fatalf("empty directory name case = %d, want current directory rule %d", got, want)
	}
}

func TestCanonicalGenerationFileNameCaseFollowsDirectoryRule(t *testing.T) {
	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		for _, test := range []struct {
			name   string
			prefix string
			target string
			path   string
			kind   GenerationFileKind
			exact  bool
		}{
			{name: "exact", target: "app", path: "app_20260911_7.log.gz", kind: GenerationFileKindCompressed, exact: true},
			{name: "target", target: "app", path: "App_20260911_7.log.gz", kind: GenerationFileKindCompressed},
			{name: "base extension", target: "app", path: "app_20260911_7.LOG", kind: GenerationFileKindUncompressed},
			{name: "prefix", prefix: "error_", target: "app", path: "ERROR_app_20260911_7.log.gz", kind: GenerationFileKindCompressed},
			{name: "unicode target", target: "kernel", path: "Kernel_20260911_7.log.gz", kind: GenerationFileKindCompressed},
			{name: "unicode letter", target: "äpp", path: "ÄPP_20260911_7.log.gz", kind: GenerationFileKindCompressed},
		} {
			pattern := FileNamePattern{Prefix: test.prefix, Target: test.target, Extension: "log", NameCase: nameCase}
			got, ok := ParseCanonicalGenerationFilePath(test.path, pattern)
			if wantOK := test.exact || foldsCase; ok != wantOK {
				t.Fatalf("%s: ParseCanonicalGenerationFilePath(%q) = %+v, %t, want matched=%t", test.name, test.path, got, ok, wantOK)
			}
			if ok && got != (CanonicalGenerationFile{Period: "20260911", Index: 7, Kind: test.kind}) {
				t.Fatalf("%s: case alias changed generation: %+v", test.name, got)
			}
		}
	})
}

func TestSplitGenerationExtensionCasePreservesOriginalStemAndCompressionSuffix(t *testing.T) {
	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		for _, test := range []struct {
			name      string
			base      string
			stem      string
			extension string
		}{
			{name: "Mixed_App_20260911_7.LOG", base: "log", stem: "Mixed_App_20260911_7", extension: ".log"},
			{name: "Mixed_App_20260911_7.LOG.GZ", base: "log", stem: "Mixed_App_20260911_7", extension: ".log.GZ"},
			{name: "Mixed_App_20260911_7.K.GZ", base: "k", stem: "Mixed_App_20260911_7", extension: ".k.GZ"},
		} {
			stem, extension, ok := fsstore.SplitBaseAndOptionalCompressionExtensionsForNameCase(test.name, test.base, nameCase)
			if ok != foldsCase {
				t.Fatalf("SplitBaseAndOptionalCompressionExtensionsForNameCase(%q) = %q, %q, %t; want matched=%t",
					test.name, stem, extension, ok, foldsCase)
			}
			if ok && (stem != test.stem || extension != test.extension) {
				t.Fatalf("split altered original filename spelling: stem=%q extension=%q", stem, extension)
			}
		}
	})
}

func TestGlobFileNamesCaseFollowsDirectoryRuleAndPreservesPaths(t *testing.T) {
	directory := t.TempDir()
	// 大小文字を区別しないfilesystemでも共存できる名前だけを作成します。
	names := []string{
		"Error_App_20260911_1.LOG",
		"Error_App_20260911_1.LOG.GZ",
		"Error_App_20260911_8.LOG.GZ.TMP",
		"Error_App_20260911_9.LOG.TMP",
		"Other_20260911_1.LOG.GZ",
		"error_app_20260911_2.log",
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		for _, mode := range []GlobMode{GlobByTarget, GlobAllFiles} {
			pattern := FileNamePattern{Dir: directory, Prefix: "error_", Target: "app", Extension: "log", NameCase: nameCase}
			got, err := GlobFileNames(pattern, mode)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{filepath.Join(directory, names[5])}
			if foldsCase {
				want = append(want, filepath.Join(directory, names[0]), filepath.Join(directory, names[1]))
				if mode == GlobAllFiles {
					want = append(want, filepath.Join(directory, names[4]))
				}
			}
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Fatalf("mode %d matched paths = %v, want original paths %v", mode, got, want)
			}
		}
	})
}

func TestFindMaxIndexCaseIncludesCompressedAndDeletePendingGenerations(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{
		"Error_App_20260911_3.LOG",
		"Error_App_20260911_7.LOG.GZ",
		"Error_App_20260911_9.LOG" + strings.ToUpper(CompressionSourceDeletePendingSuffix),
		"Error_App_20260911_20.LOG.GZ.TMP",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		got, err := FindMaxIndex(FileNamePattern{
			Dir: directory, Prefix: "error_", Target: "app", Date: "20260911", Extension: "log", NameCase: nameCase,
		})
		if err != nil {
			t.Fatal(err)
		}
		want := DefaultFileIndex
		if foldsCase {
			want = 9
		}
		if got != want {
			t.Fatalf("FindMaxIndex = %d, want %d", got, want)
		}
	})
}

func TestResolveFilePathAfterTargetCaseChangeKeepsCompletedGenerationReserved(t *testing.T) {
	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		for _, test := range []struct {
			name     string
			existing string
			want     int
		}{
			{name: "compressed", existing: "App_20260911_1.LOG.GZ", want: 2},
			{name: "delete pending", existing: "App_20260911_5.LOG" + strings.ToUpper(CompressionSourceDeletePendingSuffix), want: 6},
		} {
			directory := t.TempDir()
			existing := filepath.Join(directory, test.existing)
			if err := os.WriteFile(existing, []byte("retained generation"), 0o600); err != nil {
				t.Fatal(err)
			}
			pattern := FileNamePattern{Dir: directory, Target: "app", Date: "20260911", Extension: "log", NameCase: nameCase}
			cfg := rotationNameTestConfig{rotation: pkgfsstore.FileRotationDaily, fileDateFormat: pkgfsstore.FileDateFormat}
			at := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
			got, err := ResolveFilePathAt(&pattern, cfg, at)
			if err != nil {
				t.Fatal(err)
			}
			// 大小文字を区別しないディレクトリでは、設定のtargetの大小文字だけを変えても
			// 圧縮済み・削除待ちの世代を上書きしないよう、次のindexを使います。
			wantIndex := DefaultFileIndex
			if foldsCase {
				wantIndex = test.want
			}
			wantPattern := pattern
			wantPattern.Index = wantIndex
			wantPath := BuildFileFullPath(pattern.Date, wantPattern, cfg)
			if pattern.Index != wantIndex || got != wantPath {
				t.Fatalf("%s: resolved path=%q index=%d, want %q index=%d", test.name, got, pattern.Index, wantPath, wantIndex)
			}
			if pattern.NameCase != nameCase {
				t.Fatalf("%s: resolution changed the configured name case: %d", test.name, pattern.NameCase)
			}
			if body, err := os.ReadFile(existing); err != nil || string(body) != "retained generation" {
				t.Fatalf("%s: existing generation changed: %q, %v", test.name, body, err)
			}
		}
	})
}

func TestCompressionSourceDeletePendingDiscoveryCaseFollowsDirectoryRule(t *testing.T) {
	directory := t.TempDir()
	rawName := "App_20260911_3.LOG"
	pendingName := rawName + strings.ToUpper(CompressionSourceDeletePendingSuffix)
	pendingPath := filepath.Join(directory, pendingName)
	if err := os.WriteFile(pendingPath, []byte("retired"), 0o600); err != nil {
		t.Fatal(err)
	}
	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		pattern := FileNamePattern{Dir: directory, Target: "app", Extension: "log", NameCase: nameCase}
		for _, mode := range []GlobMode{GlobByTarget, GlobAllFiles} {
			got, err := FindCompressionSourceDeletePendingPaths(pattern, mode)
			if err != nil {
				t.Fatal(err)
			}
			var want []string
			if foldsCase {
				want = []string{pendingPath}
			}
			if !slices.Equal(got, want) {
				t.Fatalf("pending discovery mode %d = %v, want %v", mode, got, want)
			}
		}
		if _, err := FindCompressionSourceDeletePendingPaths(pattern, GlobMode(99)); err == nil {
			t.Fatal("invalid pending discovery mode accepted")
		}
		source, ok := compressionSourcePathFromDeletePendingPath(pendingPath, nameCase)
		if ok != foldsCase || isCompressionSourceDeletePendingPath(pendingPath, nameCase) != foldsCase {
			t.Fatalf("pending suffix recognition = %q, %t, want matched=%t", source, ok, foldsCase)
		}
		if ok && source != filepath.Join(directory, rawName) {
			t.Fatalf("source path spelling changed: %q", source)
		}
	})
	// patternを受け取らない判定は、pathの親ディレクトリの規則を使います。
	nameCase := pathidentity.DirectoryNameCase(directory)
	wantSource, wantOK := compressionSourcePathFromDeletePendingPath(pendingPath, nameCase)
	if source, ok := CompressionSourcePathFromDeletePendingPath(pendingPath); source != wantSource || ok != wantOK {
		t.Fatalf("CompressionSourcePathFromDeletePendingPath = %q, %t, want directory rule result %q, %t", source, ok, wantSource, wantOK)
	}
	if got := IsCompressionSourceDeletePendingPath(pendingPath); got != wantOK {
		t.Fatalf("IsCompressionSourceDeletePendingPath = %t, want directory rule result %t", got, wantOK)
	}
}

func TestGenerationCountCaseFollowsDirectoryRule(t *testing.T) {
	directory := t.TempDir()
	files := []fsstore.FileInfo{
		{Path: filepath.Join(directory, "app_20260911_1.log")},
		{Path: filepath.Join(directory, "App_20260911_1.LOG.GZ")},
		{Path: filepath.Join(directory, "app_20260911_2.log.gz")},
	}
	runForNameCases(t, func(t *testing.T, nameCase pathidentity.NameCase, foldsCase bool) {
		// 大小文字を区別しないディレクトリでは、大小文字だけが異なる名前も同じ世代です。
		// 区別するディレクトリでは`.LOG`はbase extensionに一致せず、世代に数えません。
		want := uint64(2)
		if got := countGenerations(files, "log", nameCase); got != want {
			t.Fatalf("generation count = %d, want %d", got, want)
		}
		result := purgeGenerations(append([]fsstore.FileInfo(nil), files...), "log", 1, nameCase)
		wantRemoved := []string{files[0].Path}
		if foldsCase {
			wantRemoved = append(wantRemoved, files[1].Path)
		}
		if got := generationRemovalPaths(result.RemovalResults); !slices.Equal(got, wantRemoved) {
			t.Fatalf("removed paths = %q, want %q", got, wantRemoved)
		}
	})
	want := countGenerations(files, "log", pathidentity.DirectoryNameCase(directory))
	if got := CountGenerations(files, "log"); got != want {
		t.Fatalf("CountGenerations = %d, want directory rule result %d", got, want)
	}
}

package checkpoint

import (
	"context"
	"errors"
	"github.com/go-jappe/heterolog/internal/testutil"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestArchiveCheckpointSourceIDPreservesCaseSensitiveIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix source ID compatibility")
	}
	pattern := logpattern.FileNamePattern{
		Dir: "/var/log/App", Prefix: "error_", Target: "App", Extension: ".log",
	}
	if pathidentity.ComparisonKey(pattern.Dir) != pattern.Dir {
		t.Skip("legacy fingerprint applies to case-sensitive directory identities")
	}
	const want = "logger_file_error-27e9badba2c077f575c30f13aaba7f6d864052db8db301645c4e2cf3586171c2"
	if got := ArchiveCheckpointSourceID("logger_file", "error", pattern); got != want {
		t.Fatalf("source ID = %q, want unchanged Unix identity %q", got, want)
	}
	pattern.Dir = "/var/log/app"
	if got := ArchiveCheckpointSourceID("logger_file", "error", pattern); got == want {
		t.Fatal("Unix case-distinct directories share a source ID")
	}
}

// directoryの規則を差し替えるため、process全体の設定を共有するt.Parallelは使いません。
func TestArchiveCheckpointSourceIDUsesDirectoryCaseRules(t *testing.T) {
	for _, rule := range []struct {
		name     string
		nameCase pathidentity.NameCase
	}{
		{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
		{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive},
	} {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			// directoryの別表記も、OS名やfilesystemではなく同じ規則で比較します。
			testutil.UseDirectoryCaseRuleForTest(t, root, rule.nameCase == pathidentity.NameCaseSensitive)
			directoryIgnoresCase := rule.nameCase == pathidentity.NameCaseInsensitive
			pattern := logpattern.FileNamePattern{
				Dir: filepath.Join(root, "Logs"), Prefix: "Error_", Target: "App", Extension: ".Log",
				NameCase: rule.nameCase,
			}
			want := ArchiveCheckpointSourceID("logger_file", "error", pattern)
			for _, field := range []string{"directory", "prefix", "target", "extension"} {
				alias := pattern
				// file名の構成要素は、OS名ではなく保存先directoryの規則で比較します。
				wantSame := rule.nameCase == pathidentity.NameCaseInsensitive
				switch field {
				case "directory":
					alias.Dir = filepath.Join(root, "logs")
					wantSame = directoryIgnoresCase
				case "prefix":
					alias.Prefix = strings.ToLower(pattern.Prefix)
				case "target":
					alias.Target = strings.ToLower(pattern.Target)
				case "extension":
					alias.Extension = strings.ToLower(pattern.Extension)
				}
				got := ArchiveCheckpointSourceID("logger_file", "error", alias)
				if same := got == want; same != wantSame {
					t.Fatalf("case-only %s change: same source ID = %t, want %t", field, same, wantSame)
				}
			}
			if got := ArchiveCheckpointSourceID("LOGGER_FILE", "error", pattern); got == want {
				t.Fatal("source prefix is a logical namespace, not a case-insensitive path")
			}
			if got := ArchiveCheckpointSourceID("logger_file", "ERROR", pattern); got == want {
				t.Fatal("source class is a logical namespace, not a case-insensitive path")
			}
		})
	}
}

func TestArchiveCheckpointSourceIDUsesDirectoryCaseRulesForUnicodeNames(t *testing.T) {
	t.Parallel()
	for _, rule := range []struct {
		nameCase pathidentity.NameCase
		wantSame bool
	}{
		{nameCase: pathidentity.NameCaseSensitive, wantSame: false},
		{nameCase: pathidentity.NameCaseInsensitive, wantSame: true},
	} {
		pattern := logpattern.FileNamePattern{
			Dir: t.TempDir(), Prefix: "Kernel_", Target: "Äpp", Extension: ".Log", NameCase: rule.nameCase,
		}
		alias := pattern
		alias.Prefix = "kernel_"
		alias.Target = "äpp"
		alias.Extension = ".log"
		first := ArchiveCheckpointSourceID("logger_file", "error", pattern)
		second := ArchiveCheckpointSourceID("logger_file", "error", alias)
		if same := first == second; same != rule.wantSame {
			t.Fatalf("Unicode case-only name change with rule %d: same source ID = %t, want %t", rule.nameCase, same, rule.wantSame)
		}
	}
}

func TestArchiveCheckpointSourceIDUsesDirectoryRuleByDefault(t *testing.T) {
	t.Parallel()
	pattern := logpattern.FileNamePattern{
		Dir: filepath.Join(t.TempDir(), "Logs"), Prefix: "Error_", Target: "App", Extension: ".Log",
	}
	resolved := pattern
	resolved.NameCase = pathidentity.DirectoryNameCase(pattern.Dir)
	directoryAlias := pattern
	directoryAlias.Dir = filepath.Join(filepath.Dir(pattern.Dir), "logs")
	sameDirectory := pathidentity.DirectoryNameCase(filepath.Dir(pattern.Dir)) == pathidentity.NameCaseInsensitive
	if same := ArchiveCheckpointSourceID("logger_file", "error", directoryAlias) ==
		ArchiveCheckpointSourceID("logger_file", "error", pattern); same != sameDirectory {
		t.Fatalf("case-only directory change: same source ID = %t, want directory rule result %t", same, sameDirectory)
	}
	// 規則を指定しない場合は、ComparisonKeyと共用する保存先directoryの判定を使います。
	if got, want := ArchiveCheckpointSourceID("logger_file", "error", pattern),
		ArchiveCheckpointSourceID("logger_file", "error", resolved); got != want {
		t.Fatalf("source ID without explicit rule = %q, want directory rule ID %q", got, want)
	}
	if resolved.NameCase == pathidentity.NameCaseSensitive {
		// 大小文字を区別するdirectoryでは、file名の構成要素を変換しない従来のIDです。
		legacy := resolved
		legacy.Prefix, legacy.Target = "error_", "app"
		if ArchiveCheckpointSourceID("logger_file", "error", legacy) == ArchiveCheckpointSourceID("logger_file", "error", pattern) {
			t.Fatal("case-sensitive directory merged case-distinct source identities")
		}
	}
}

func TestArchiveCheckpointGenerationKeysUseDirectoryCaseRules(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	for _, rule := range []struct {
		nameCase pathidentity.NameCase
		want     []string
	}{
		{nameCase: pathidentity.NameCaseSensitive, want: []string{"20260911-2"}},
		{nameCase: pathidentity.NameCaseInsensitive, want: []string{"20260911-1", "20260911-2", "20260911-3"}},
	} {
		pattern := logpattern.FileNamePattern{Dir: directory, Target: "app", Extension: "log", NameCase: rule.nameCase}
		got := ArchiveCheckpointGenerationKeys([]string{
			filepath.Join(directory, "App_20260911_1.log.gz"),
			filepath.Join(directory, "app_20260911_2.log"),
			filepath.Join(directory, "APP_20260911_3.LOG.ZST"),
			filepath.Join(directory, "other", "app_20260911_4.log"),
		}, pattern)
		if !slices.Equal(got, rule.want) {
			t.Fatalf("generation keys with rule %d = %q, want %q", rule.nameCase, got, rule.want)
		}
	}
}

func TestArchiveCheckpointGenerationKeyIsSharedByRawCompressedAndRunnerCursor(t *testing.T) {
	t.Parallel()

	fileName := logpattern.FileNamePattern{
		Dir:       t.TempDir(),
		Prefix:    "internal_",
		Target:    "error",
		Extension: "log",
	}
	rawPath := filepath.Join(fileName.Dir, "internal_error_20260903_12.log")
	compressedPath := rawPath + ".gz"

	rawKey, err := ArchiveCheckpointGenerationKeyForPath(rawPath, fileName)
	if err != nil {
		t.Fatalf("ArchiveCheckpointGenerationKeyForPath(raw) error = %v", err)
	}
	compressedKey, err := ArchiveCheckpointGenerationKeyForPath(compressedPath, fileName)
	if err != nil {
		t.Fatalf("ArchiveCheckpointGenerationKeyForPath(compressed) error = %v", err)
	}
	runnerKey := ArchiveCheckpointGenerationKey("20260903", 12)
	if rawKey != runnerKey || compressedKey != runnerKey {
		t.Fatalf(
			"generation keys = raw %q, compressed %q, runner %q",
			rawKey,
			compressedKey,
			runnerKey,
		)
	}
}

func TestArchiveCheckpointGenerationKeyForPathRejectsNonCanonicalPath(t *testing.T) {
	t.Parallel()

	fileName := logpattern.FileNamePattern{
		Dir:       t.TempDir(),
		Target:    "app",
		Extension: "log",
	}
	path := filepath.Join(fileName.Dir, "app_latest.log.gz")
	key, err := ArchiveCheckpointGenerationKeyForPath(path, fileName)
	if err == nil {
		t.Fatal("ArchiveCheckpointGenerationKeyForPath() accepted a non-canonical file name")
	}
	if key != "" {
		t.Fatalf("key = %q, want empty key for a non-canonical file name", key)
	}
	var pathErr *NonCanonicalGenerationPathError
	if !errors.As(err, &pathErr) || !errors.Is(err, ErrNonCanonicalGenerationPath) || pathErr.Path != path {
		t.Fatalf("error = %v, want rejection of %q", err, path)
	}
}

func TestArchiveCheckpointSuccessIsSharedAcrossCompressionFormats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pattern := logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: "log"}
	sourceID := ArchiveCheckpointSourceID("logger_file", "default", pattern)
	directory := t.TempDir()
	first, err := NewFilesystemStore(directory, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	gzipKey, err := ArchiveCheckpointGenerationKeyForPath(filepath.Join(pattern.Dir, "app_20260915_1.log.gz"), pattern)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: gzipKey, Compressed: true}, []objectstorage.DestinationID{"primary"}); err != nil {
		t.Fatal(err)
	}
	zstandardKey, err := ArchiveCheckpointGenerationKeyForPath(filepath.Join(pattern.Dir, "app_20260915_1.log.zst"), pattern)
	if err != nil {
		t.Fatal(err)
	}
	if zstandardKey != gzipKey {
		t.Fatalf("compression formats use different generation keys: %q, %q", gzipKey, zstandardKey)
	}
	restarted, err := NewFilesystemStore(directory, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := restarted.Load(ctx, CheckpointKey{Generation: zstandardKey, Compressed: true})
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Complete([]objectstorage.DestinationID{"primary"}) {
		t.Fatalf("compression format change lost generation success: %+v", checkpoint)
	}
	if pending := checkpoint.PendingDestinationIDs([]objectstorage.DestinationID{"primary", "secondary"}); len(pending) != 1 || pending[0] != "secondary" {
		t.Fatalf("pending destinations after compression format change = %v, want [secondary]", pending)
	}
}

func TestArchiveCheckpointUploadSuccessSurvivesLifecycleModeSwitch(t *testing.T) {
	t.Parallel()

	fileName := logpattern.FileNamePattern{
		Dir:       t.TempDir(),
		Target:    "app",
		Extension: "log",
	}
	path := filepath.Join(fileName.Dir, "app_20260903_13.log.gz")
	inlineKey, err := ArchiveCheckpointGenerationKeyForPath(path, fileName)
	if err != nil {
		t.Fatalf("ArchiveCheckpointGenerationKeyForPath() error = %v", err)
	}
	runnerKey := ArchiveCheckpointGenerationKey("20260903", 13)
	ctx := context.Background()

	tests := []struct {
		name              string
		firstKey          string
		secondKey         string
		firstDestinations []objectstorage.DestinationID
		secondDestination []objectstorage.DestinationID
	}{
		{
			name:              "inline to runner",
			firstKey:          inlineKey,
			secondKey:         runnerKey,
			firstDestinations: []objectstorage.DestinationID{"inline-original"},
			secondDestination: []objectstorage.DestinationID{"runner-current"},
		},
		{
			name:              "runner to inline",
			firstKey:          runnerKey,
			secondKey:         inlineKey,
			firstDestinations: []objectstorage.DestinationID{"runner-original"},
			secondDestination: []objectstorage.DestinationID{"inline-current"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			const sourceID = "logger_file_default-mode-switch"
			first, err := NewFilesystemStore(directory, sourceID)
			if err != nil {
				t.Fatalf("NewFilesystemStore(first mode) error = %v", err)
			}
			if _, err := first.AdvanceCompletionCursor(
				ctx,
				CheckpointKey{Generation: test.firstKey, Compressed: true},
				test.firstDestinations); err != nil {
				t.Fatalf("AdvanceCompletionCursor(first mode) error = %v", err)
			}

			restarted, err := NewFilesystemStore(directory, sourceID)
			if err != nil {
				t.Fatalf("NewFilesystemStore(second mode) error = %v", err)
			}
			checkpoint, err := restarted.Load(
				ctx,
				CheckpointKey{Generation: test.secondKey, Compressed: true})
			if err != nil {
				t.Fatalf("Load(second mode) error = %v", err)
			}
			if uploaded := checkpoint.CompletedDestinationIDs; len(uploaded) != 1 ||
				uploaded[0] != test.firstDestinations[0] {
				t.Fatalf(
					"uploaded destinations after mode switch = %v, want %v",
					uploaded,
					test.firstDestinations,
				)
			}
			if pending := checkpoint.PendingDestinationIDs(test.secondDestination); len(pending) != 1 || pending[0] != test.secondDestination[0] {
				t.Fatalf("pending destinations after mode switch = %v, want %v", pending, test.secondDestination)
			}
			if checkpoint.Complete(test.secondDestination) {
				t.Fatal("new active destination was treated as already uploaded")
			}
			checkpoint, err = restarted.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: test.secondKey, Compressed: true}, test.secondDestination)
			if err != nil {
				t.Fatalf("AdvanceCompletionCursor(second mode) error = %v", err)
			}
			if !checkpoint.Complete(test.secondDestination) || !checkpoint.Complete(test.firstDestinations) {
				t.Fatal("mode switch lost a recorded upload success")
			}
		})
	}
}

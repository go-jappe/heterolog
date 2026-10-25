package fsarchivehandler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// OS名ではなく保存先directoryの規則を指定し、大小文字を区別する場合・しない場合の両方を
// どのOSでも確認します。規則を指定しない場合は、保存先directoryの判定を使います。
func TestFailureArchiveTargetCaseMatchesRawRecoveryAndBroadScan(t *testing.T) {
	for _, rule := range []struct {
		name     string
		nameCase pathidentity.NameCase
	}{
		{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
		{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive},
		{name: "directory rule", nameCase: pathidentity.NameCaseUnspecified},
	} {
		for _, prefix := range []string{"retryable_", "permanent_"} {
			t.Run(rule.name+"/"+prefix, func(t *testing.T) {
				directory := t.TempDir()
				pattern := logpattern.FileNamePattern{
					Dir: directory, Prefix: prefix, Target: "app", Extension: "jsonl", NameCase: rule.nameCase,
				}
				// targetと同じ小文字の名前は、どの規則でも同じ系列です。大小文字を区別しない
				// filesystemでも別fileになるよう、大小文字だけが異なる名前とは日付・名前を変えます。
				canonicalRaw := filepath.Join(directory, prefix+"app_20260102_1.jsonl")
				canonicalCompressed := canonicalRaw + ".gz"
				canonicalLegacy := filepath.Join(directory, prefix+"app_legacy.jsonl")
				raw := filepath.Join(directory, prefix+"App_20260101_1.jsonl")
				compressed := raw + ".gz"
				legacy := filepath.Join(directory, prefix+"App_old.jsonl")
				other := filepath.Join(directory, prefix+"Another_20260101_1.jsonl")
				for _, path := range []string{canonicalRaw, canonicalCompressed, canonicalLegacy, raw, compressed, legacy, other} {
					require.NoError(t, os.WriteFile(path, []byte("unchanged existing payload"), 0o600))
				}
				paths, err := logpattern.GlobFileNames(pattern, logpattern.GlobAllFiles)
				require.NoError(t, err)
				inspection := newArchiveDirectoryInspection(fsstore.InspectFilePaths(paths), pattern)
				var selectedPaths []string
				for _, file := range inspection.seriesSources.Files.RegularFiles {
					selectedPaths = append(selectedPaths, file.Path)
				}
				// 大小文字を区別しない保存先では、targetの大小文字だけが異なる既存fileも同じ系列です。
				foldsCase := pattern.EffectiveNameCase() == pathidentity.NameCaseInsensitive
				// purge用の広いprefix一致をcanonical日付だけへ狭めません。
				want := []string{canonicalRaw, canonicalCompressed, canonicalLegacy}
				if foldsCase {
					want = append(want, raw, compressed, legacy)
				}
				require.ElementsMatch(t, want, selectedPaths)
				require.True(t, isCanonicalUncompressedArchivePath(canonicalRaw, pattern))
				require.False(t, isCanonicalUncompressedArchivePath(canonicalCompressed, pattern))
				require.False(t, isCanonicalUncompressedArchivePath(canonicalLegacy, pattern))
				require.Equal(t, foldsCase, isCanonicalUncompressedArchivePath(raw, pattern))
				require.False(t, isCanonicalUncompressedArchivePath(compressed, pattern))
				require.False(t, isCanonicalUncompressedArchivePath(legacy, pattern))
				require.False(t, isCanonicalUncompressedArchivePath(filepath.Join(directory, prefix+"App_20260101_18446744073709551615.jsonl"), pattern),
					"writer uses int indices; uint64-only values are not canonical generations")
			})
		}
	}
}

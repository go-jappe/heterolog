package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

// storageDirectoryCaseRulesは、保存先directoryが大小文字を区別する場合・しない場合を、
// 実行するOSやfilesystemに依存せず試験するための規則です。
var storageDirectoryCaseRules = []struct {
	name          string
	caseSensitive bool
}{
	{name: "case-sensitive", caseSensitive: true},
	{name: "case-insensitive", caseSensitive: false},
}

func TestStorageDirectoryPathIdentityFollowsDirectoryCaseRules(t *testing.T) {
	for _, rule := range storageDirectoryCaseRules {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			// 規則を差し替える試験では別名のdirectoryを作成せず、比較keyの規則だけを確認します。
			testutil.UseDirectoryCaseRuleForTest(t, root, rule.caseSensitive)
			assertStorageDirectoryCaseIdentity(t, root, !rule.caseSensitive)
		})
	}
	t.Run("directory rule", func(t *testing.T) {
		root := t.TempDir()
		for _, directory := range []string{root, filepath.Join(root, "app")} {
			for _, name := range []string{"Logs", "LOGS"} {
				require.NoError(t, os.MkdirAll(filepath.Join(directory, name), 0o700))
			}
		}
		// 規則を差し替えない場合は、作成済みのdirectoryでもComparisonKeyと共用する判定に従います。
		shared := pathidentity.DirectoryNameCase(root) == pathidentity.NameCaseInsensitive
		assertStorageDirectoryCaseIdentity(t, root, shared)
	})
}

func assertStorageDirectoryCaseIdentity(t *testing.T, root string, wantShared bool) {
	t.Helper()
	left := filepath.Join(root, "Logs")
	right := filepath.Join(root, "LOGS")
	require.Equal(t, wantShared, storagePathsOverlap(left, right))
	leftKey, err := canonicalStoragePath(left)
	require.NoError(t, err)
	rightKey, err := canonicalStoragePath(right)
	require.NoError(t, err)
	require.Equal(t, wantShared, leftKey == rightKey)

	claims, err := newFileLogDirectoryClaims("logger.file", root, "app", "Logs", "LOGS", "Logs", true, false)
	require.NoError(t, err)
	if wantShared {
		require.Len(t, claims, 1)
		require.Equal(t, "logger.file.default+error", claims[0].source)
	} else {
		require.Len(t, claims, 2)
	}
	// 別ownerの共有は、表記だけが異なる場合も許可しません。
	err = validateStorageDirectoryClaims([]storageDirectoryClaim{
		{source: "logger.file", path: leftKey},
		{source: "async_logger.file", path: rightKey},
	})
	require.Equal(t, wantShared, err != nil)
}

func TestStorageDirectoryPathIdentityRejectsErrorDebugOnlySharing(t *testing.T) {
	for _, rule := range storageDirectoryCaseRules {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.UseDirectoryCaseRuleForTest(t, root, rule.caseSensitive)
			_, err := newFileLogDirectoryClaims("logger.file", root, "app", "normal", "Logs", "LOGS", true, true)
			// 大小文字を区別するdirectoryでは別の保存先なので受理し、区別しないdirectoryでは
			// defaultを含まないerror・debugだけの共有として拒否します。
			if rule.caseSensitive {
				require.NoError(t, err)
				return
			}
			var sharedErr *SharedRoleLogDirectoryError
			require.ErrorAs(t, err, &sharedErr)
			require.Equal(t, "logger.file", sharedErr.Source)
			shared, err := canonicalStoragePath(filepath.Join(root, "app", "Logs"))
			require.NoError(t, err)
			require.Equal(t, shared, sharedErr.Directory)
			normal, err := canonicalStoragePath(filepath.Join(root, "app", "normal"))
			require.NoError(t, err)
			require.Equal(t, normal, sharedErr.DefaultDirectory)
		})
	}
}

func TestInternalLogPathIdentityCaseOnlyOverride(t *testing.T) {
	for _, rule := range storageDirectoryCaseRules {
		t.Run(rule.name, func(t *testing.T) {
			base := pkgfsstore.FsstoreConfig{
				LogPath: t.TempDir(), Target: "app", LogDir: "Logs", ErrorLogDir: "Errors",
				IsSeparateErrorLogsEnabled: true,
			}
			testutil.UseDirectoryCaseRuleForTest(t, base.LogPath, rule.caseSensitive)
			for _, tt := range []struct {
				name     string
				override logconfig.InternalLogDestinationConfig
			}{
				{name: "normal", override: logconfig.InternalLogDestinationConfig{FileLogDir: "LOGS"}},
				{name: "error", override: logconfig.InternalLogDestinationConfig{FileErrorLogDir: "ERRORS"}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					originalBase, originalOverride := base, tt.override
					// 大小文字を区別しないdirectoryでは、大小文字だけの変更は保存先の変更ではありません。
					require.Equal(t, rule.caseSensitive, internalLogFileDestinationOverridden(tt.override, base))
					// identityのための変更を入力設定へ書き戻さないことを確認します。
					require.Equal(t, originalBase, base)
					require.Equal(t, originalOverride, tt.override)
				})
			}
		})
	}
}

func TestNormalizeLogFileLayoutForComparisonPreservesSourceAndOtherFields(t *testing.T) {
	for _, tt := range []struct {
		name          string
		separateError bool
		separateDebug bool
	}{
		{name: "neither directory separated"},
		{name: "error directory separated", separateError: true},
		{name: "debug directory separated", separateDebug: true},
		{name: "both directories separated", separateError: true, separateDebug: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			layout := pkgfsstore.FsstoreConfig{
				Target:                       "App_CASE",
				LogPath:                      filepath.Join(root, "unused") + string(filepath.Separator) + "..",
				LogDir:                       "./Logs",
				ErrorLogDir:                  "./Errors",
				DebugLogDir:                  "./Debug",
				IsSeparateErrorLogsEnabled:   tt.separateError,
				IsSeparateDebugLogDirEnabled: tt.separateDebug,
				// debugのfile分離flagはdirectory分離とは独立して保持します。
				IsSeparateDebugLogsEnabled: !tt.separateDebug,
				SeparateErrorLogsPrefix:    "Error_",
				SeparateDebugLogsPrefix:    "Debug_",
				FileNameFormat:             "%s_%s_%d.%s",
				FileDateFormat:             "20060102",
			}
			original := layout
			want := original
			want.LogPath = pathidentity.ComparisonKey(root)
			want.LogDir = pathidentity.ComparisonKey(filepath.Join(root, layout.Target, "Logs"))
			want.ErrorLogDir = ""
			if tt.separateError {
				want.ErrorLogDir = pathidentity.ComparisonKey(filepath.Join(root, layout.Target, "Errors"))
			}
			want.DebugLogDir = ""
			if tt.separateDebug {
				want.DebugLogDir = pathidentity.ComparisonKey(filepath.Join(root, layout.Target, "Debug"))
			}

			got := normalizeLogFileLayoutForComparison(layout)
			require.Equal(t, want, got, "directory以外のTarget・prefix・flag・formatも保持する")
			require.Equal(t, original, layout, "比較用の正規化を元設定へ書き戻さない")
		})
	}
}

func TestStorageDirectoryPathIdentityWindowsExtendedAlias(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows extended path identity")
	}
	root := t.TempDir()
	require.True(t, storagePathsOverlap(root, `\\?\`+root))
	base := pkgfsstore.FsstoreConfig{LogPath: root, Target: "app", LogDir: "logs"}
	override := logconfig.InternalLogDestinationConfig{FileLogPath: `\\?\` + root}
	require.False(t, internalLogFileDestinationOverridden(override, base))
}

func TestStorageDirectoryPathIdentityForUncreatedChildren(t *testing.T) {
	for _, rule := range append(append(storageDirectoryCaseRules[:0:0], storageDirectoryCaseRules...), struct {
		name          string
		caseSensitive bool
	}{name: "directory rule"}) {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			shared := !rule.caseSensitive
			if rule.name == "directory rule" {
				// 規則を差し替えない場合は、ComparisonKeyと共用する保存先directoryの判定を使います。
				shared = pathidentity.DirectoryNameCase(root) == pathidentity.NameCaseInsensitive
			} else {
				testutil.UseDirectoryCaseRuleForTest(t, root, rule.caseSensitive)
			}
			left, right := filepath.Join(root, "Missing", "Logs"), filepath.Join(root, "MISSING", "LOGS")
			a, err := canonicalStoragePath(left)
			require.NoError(t, err)
			b, err := canonicalStoragePath(right)
			require.NoError(t, err)
			require.Equal(t, shared, a == b)
			require.Equal(t, shared, storagePathsOverlap(a, filepath.Join(b, "child")))
			_, err = os.Stat(filepath.Join(root, "Missing"))
			require.ErrorIs(t, err, os.ErrNotExist, "identity must not create storage directories")
		})
	}
}

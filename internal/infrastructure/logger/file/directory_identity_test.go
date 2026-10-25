package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

func TestLoggerSetPathIdentityCaseOnlyRoleDirectories(t *testing.T) {
	root := t.TempDir()
	cfg := newTestFileLoggerConfig(root, "Logs", "app")
	cfg.FsstoreConfig.Layout.ErrorLogDir = "LOGS"
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	require.NoError(t, err)
	closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	normal, err := set.NewFileLogger(context.Background())
	require.NoError(t, err)
	errorLogger, err := set.NewErrorFileLogger(context.Background())
	require.NoError(t, err)
	// OS名ではなく、loggerが作成したLogsとLOGSが同じ実体のdirectoryかを確認します。
	parent := filepath.Join(root, "app")
	sameDirectory := sameExistingDirectory(t, filepath.Join(parent, "Logs"), filepath.Join(parent, "LOGS"))
	skipUnsupportedCaseConfiguration(t, parent, sameDirectory)
	// 期待値は本番の規則から求めます。実体の確認と一致する構成なので、
	// 同じ実体のdirectoryだけがlifecycleGuardを共有することも確認できます。
	wantShared := pathidentity.DirectoryNameCase(parent) == pathidentity.NameCaseInsensitive
	require.Equal(t, wantShared, normal.lifecycleGuard == errorLogger.lifecycleGuard,
		"同じ実体のdirectoryだけがlifecycleGuardを共有する")
}

func TestCompressedArchiveGroupsPathIdentityPreservesUploadPath(t *testing.T) {
	root := t.TempDir()
	original := filepath.Join(root, "Logs", "App_20260911_1.log.gz")
	alias := filepath.Join(root, "LOGS", "App_20260911_1.log.gz")
	sameDirectory := caseAliasDirectoriesShareIdentity(t, filepath.Dir(original), filepath.Dir(alias))
	skipUnsupportedCaseConfiguration(t, root, sameDirectory)
	if !sameDirectory {
		t.Skip("the test filesystem distinguishes case-only directory names")
	}
	fileName := logpattern.FileNamePattern{
		Dir: filepath.Dir(original), Target: "App", Extension: "log",
	}
	logger := newInlineUsageTestLogger(t, time.Now(), "path-identity")
	logger.ArchiveUploader = newSingleArchiveUploader(t, &recordingArchiveUploader{})
	inspection := fsstore.FileInspection{
		Paths:        []string{original, alias},
		RegularFiles: []fsstore.FileInfo{{Path: original}, {Path: alias}},
	}
	var groups []archiveUploadGroup
	err := logger.lifecycleGuard.RunWithLifecycleLock(func() error {
		var err error
		groups, err = logger.selectRemainingLogFileProcessingCandidates(context.Background(),
			fsstore.GroupFilesByCompressionSource(inspection, fileName.Extension), fileName, nil)
		return err
	})
	require.NoError(t, err)
	require.Len(t, groups, 1)
	// 比較keyではなく元pathを保持し、S3のcase-sensitiveなobject keyを変えません。
	require.Equal(t, original, groups[0].path)
}

func TestLoggerSetPathIdentityRejectsCaseAliasedFilePrefixes(t *testing.T) {
	root := t.TempDir()
	sameName := caseAliasDirectoriesShareIdentity(t, filepath.Join(root, "CaseProbe"), filepath.Join(root, "CASEPROBE"))
	skipUnsupportedCaseConfiguration(t, root, sameName)
	if !sameName {
		t.Skip("the test filesystem distinguishes case-only file names")
	}
	cfg := newTestFileLoggerConfig(root, "logs", "app")
	cfg.FsstoreConfig.Layout.ErrorLogDir = "logs"
	cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "Err_"
	cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "err_"
	set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
	if set != nil {
		closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
	}
	require.ErrorIs(t, err, ErrLoggerRolesShareFiles)
	require.Nil(t, set)
}

// 同じdirectoryのroleで大小文字だけが異なるprefixは、OS名ではなくdirectoryの規則で
// 同じfileとして扱います。規則を指定し、どのOSでも両方の場合を確認します。
// 規則はprocess全体に適用されるため、t.Parallelは使いません。
func TestLoggerSetDirectoryLeaseRejectsCaseAliasedPrefixesByDirectoryRule(t *testing.T) {
	for _, rule := range []struct {
		name          string
		caseSensitive bool
	}{
		{name: "case-sensitive", caseSensitive: true},
		{name: "case-insensitive", caseSensitive: false},
	} {
		t.Run(rule.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.UseDirectoryCaseRuleForTest(t, root, rule.caseSensitive)
			cfg := newTestFileLoggerConfig(root, "logs", "app")
			cfg.FsstoreConfig.Layout.ErrorLogDir = "logs"
			cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "Err_"
			cfg.FsstoreConfig.Layout.SeparateDebugLogsPrefix = "err_"
			set, err := NewLoggerSet(NewConfigAdapter(cfg), nil, nil)
			if set != nil {
				closeOnCleanupWithinForTest(t, boundedwait.HangTimeout, "set Close", func() closerForTest { return set }, true)
			}
			if rule.caseSensitive {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrLoggerRolesShareFiles)
			require.Nil(t, set)
		})
	}
}

// skipUnsupportedCaseConfigurationは、作成したdirectoryで確かめた実体と、本番の
// pathidentity.DirectoryNameCaseの判定が食い違う場合にskipします。大小文字を区別しないが
// 外から確かめられないLinuxのmount（SMB/CIFS、NFS、NTFS、FUSEなど）は、pathidentityが区別すると
// 判定するため非対応の構成です。ext4・F2FS・tmpfsのcasefold directoryとFAT・exFATは判定できます。
// 対応する構成では、同じ実体のdirectoryだけを同じ保存先として扱います。
func skipUnsupportedCaseConfiguration(t *testing.T, parent string, sameEntity bool) {
	t.Helper()
	insensitive := pathidentity.DirectoryNameCase(parent) == pathidentity.NameCaseInsensitive
	if sameEntity != insensitive {
		t.Skipf("unsupported configuration: case-only names under %q are the same entity=%t, but pathidentity reports case-insensitive=%t",
			parent, sameEntity, insensitive)
	}
}

// caseAliasDirectoriesShareIdentityは、大小文字だけが異なる二つのdirectoryを作成し、
// filesystem上で同じ実体になるかを返します。OS名では判定しません。
func caseAliasDirectoriesShareIdentity(t *testing.T, first, second string) bool {
	t.Helper()
	for _, directory := range []string{first, second} {
		require.NoError(t, os.MkdirAll(directory, 0o700))
	}
	return sameExistingDirectory(t, first, second)
}

// sameExistingDirectoryは、作成済みの二つのdirectoryをos.Statで調べ、同じ実体かを返します。
func sameExistingDirectory(t *testing.T, first, second string) bool {
	t.Helper()
	firstInfo, err := os.Stat(first)
	require.NoError(t, err)
	secondInfo, err := os.Stat(second)
	require.NoError(t, err)
	require.True(t, firstInfo.IsDir() && secondInfo.IsDir(), "%q and %q must be directories", first, second)
	return os.SameFile(firstInfo, secondInfo)
}

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/config/failure"
	"github.com/go-jappe/heterolog/internal/config/logconfig"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/path/appstatepath"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestArchiveCheckpointClaimAllowsApplicationDirectoryAlias(t *testing.T) {
	base, physical := t.TempDir(), t.TempDir()
	testutil.Symlink(t, physical, filepath.Join(base, "app"))
	cfg := AppConfig{}
	cfg.FsstoreConfig.AppStatePath = base
	cfg.RuntimeConfig.Target = "app"
	claim, err := cfg.newArchiveCheckpointDirectoryClaim()
	require.NoError(t, err)
	want, err := canonicalStoragePath(filepath.Join(physical, appstatepath.ArchiveCheckpointDirectoryName))
	require.NoError(t, err)
	require.Equal(t, want, claim.path)
	entries, err := os.ReadDir(physical)
	require.NoError(t, err)
	require.Empty(t, entries, "configuration validation must not create directories")
}

func TestArchiveCheckpointClaimRejectsManagedDirectoryLink(t *testing.T) {
	for _, dangling := range []bool{false, true} {
		base, external := t.TempDir(), t.TempDir()
		link := filepath.Join(base, "app", appstatepath.ArchiveCheckpointDirectoryName)
		require.NoError(t, os.MkdirAll(filepath.Dir(link), 0o700))
		target := external
		if dangling {
			target = filepath.Join(external, "missing")
		}
		testutil.Symlink(t, target, link)
		cfg := AppConfig{}
		cfg.FsstoreConfig.AppStatePath = base
		cfg.RuntimeConfig.Target = "app"
		// 利用者が指定する保存先までのリンクは解決するため、拒否するpathは解決後の保存先配下です。
		resolvedApp, err := pathidentity.ResolveDirectory(filepath.Dir(link))
		require.NoError(t, err)
		resolvedLink := filepath.Join(resolvedApp, appstatepath.ArchiveCheckpointDirectoryName)
		_, err = cfg.newArchiveCheckpointDirectoryClaim()
		requireManagedDirectoryLinkRejected(t, err, "archive checkpoint", resolvedLink)
		_, err = cfg.collectStorageDirectoryClaims()
		requireManagedDirectoryLinkRejected(t, err, "archive checkpoint", resolvedLink)
		entries, err := os.ReadDir(external)
		require.NoError(t, err)
		require.Empty(t, entries)
	}
}

func TestArchiveDirectoryClaimAllowsRootAliasButRejectsClassAliases(t *testing.T) {
	base, physical := t.TempDir(), t.TempDir()
	alias := filepath.Join(base, "app", "archives")
	require.NoError(t, os.MkdirAll(filepath.Dir(alias), 0o700))
	testutil.Symlink(t, physical, alias)
	cfg := AppConfig{}
	cfg.FsstoreConfig.AppStatePath = base
	cfg.RuntimeConfig.Target = "app"
	archive := failure.FileArchiveConfig{Dir: "archives", DirCaseRetryable: "retryable", DirCasePermanent: "permanent"}
	claim, err := cfg.newArchiveDirectoryClaim("logger", archive)
	require.NoError(t, err)
	resolved, err := pathidentity.ResolveDirectory(physical)
	require.NoError(t, err)
	require.Equal(t, pathidentity.ComparisonKey(resolved), claim.path)
	// 同じarchive root内へのリンクでも、固定の分類directoryには使用しません。
	otherClass := filepath.Join(physical, "other")
	require.NoError(t, os.Mkdir(otherClass, 0o700))
	testutil.Symlink(t, otherClass, filepath.Join(physical, "retryable"))
	_, err = cfg.newArchiveDirectoryClaim("logger", archive)
	requireManagedDirectoryLinkRejected(t, err, "logger failure archive retryable", filepath.Join(resolved, "retryable"))
	entries, err := os.ReadDir(otherClass)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestArchiveDirectoryClaimPreservesClassPathNormalization(t *testing.T) {
	cfg := AppConfig{}
	cfg.FsstoreConfig.AppStatePath = t.TempDir()
	cfg.RuntimeConfig.Target = "app"
	archive := failure.FileArchiveConfig{Dir: "archives", DirCaseRetryable: "retryable", DirCasePermanent: "permanent"}
	want, err := cfg.newArchiveDirectoryClaim("logger", archive)
	require.NoError(t, err)
	for _, name := range []string{"/retryable", "temporary/../retryable"} {
		archive.DirCaseRetryable = name
		got, err := cfg.newArchiveDirectoryClaim("logger", archive)
		require.NoError(t, err, "class name %q", name)
		require.Equal(t, want, got)
	}
}

func TestStorageDirectoryClaimsResolveSymlinkAliases(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "app", "real")
	require.NoError(t, os.MkdirAll(target, 0o750))
	testutil.Symlink(t, target, filepath.Join(root, "app", "alias"))

	physical, err := canonicalStoragePath(filepath.Join(target, "missing", "logs"))
	require.NoError(t, err)
	alias, err := canonicalStoragePath(filepath.Join(root, "app", "alias", "missing", "logs"))
	require.NoError(t, err)
	require.Equal(t, physical, alias)
	require.NoError(t, validateStorageDirectoryClaims([]storageDirectoryClaim{{source: "single", path: alias}}))
	require.Error(t, validateStorageDirectoryClaims([]storageDirectoryClaim{
		{source: "logger.file", path: physical}, {source: "async_logger.file", path: alias},
	}))
	_, err = os.Lstat(filepath.Join(target, "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)

	claims, err := newFileLogDirectoryClaims("logger.file", root, "app", "real", "alias", "", true, false)
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.Equal(t, "logger.file.default+error", claims[0].source)
	_, err = newFileLogDirectoryClaims("logger.file", root, "app", "normal", "real", "alias", true, true)
	var sharedErr *SharedRoleLogDirectoryError
	require.ErrorAs(t, err, &sharedErr)
	require.Equal(t, "logger.file", sharedErr.Source)
	shared, err := canonicalStoragePath(target)
	require.NoError(t, err)
	require.Equal(t, shared, sharedErr.Directory)
	normal, err := canonicalStoragePath(filepath.Join(root, "app", "normal"))
	require.NoError(t, err)
	require.Equal(t, normal, sharedErr.DefaultDirectory)
}

func TestStorageDirectoryClaimsRejectDanglingSymlink(t *testing.T) {
	root := t.TempDir()
	testutil.Symlink(t, filepath.Join(root, "missing"), filepath.Join(root, "alias"))
	_, err := newFileLogDirectoryClaims("logger.file", root, "", "alias", "", "", false, false)
	var resolutionErr *StorageDirectoryResolutionError
	require.ErrorAs(t, err, &resolutionErr)
	require.Equal(t, "logger.file.default", resolutionErr.Directory)
	_, err = os.Lstat(filepath.Join(root, "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestInternalLogMappingSharesResolvedSymlinkDestination(t *testing.T) {
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
	} {
		t.Run(contract.String(), func(t *testing.T) {
			root := t.TempDir()
			physical := filepath.Join(root, "app", "real")
			require.NoError(t, os.MkdirAll(physical, 0o750))
			testutil.Symlink(t, physical, filepath.Join(root, "app", "alias"))
			cfg := mapperValidationTestConfig()
			cfg.RuntimeConfig.Target = "app"
			cfg.FsstoreConfig.LogPath = root
			cfg.FsstoreConfig.AppStatePath = t.TempDir()
			configureDebugDirectoryModeContract(&cfg, contract, logconfig.FileSystemStoreConfig{
				LogDir: "real", LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
			}, logconfig.InternalLogDestinationConfig{FileLogDir: "alias"}, true)
			switch contract {
			case pkglogger.LoggerContractSync:
				mapped, err := MapLoggerConfig(cfg)
				require.NoError(t, err)
				require.NotNil(t, mapped.File)
				if mapped.InternalLogDestinations != nil {
					require.Nil(t, mapped.InternalLogDestinations.File)
				}
			case pkglogger.LoggerContractAsyncBestEffort:
				mapped, err := MapAsyncLoggerConfig(cfg)
				require.NoError(t, err)
				require.NotNil(t, mapped.File)
				if mapped.InternalLogDestinations != nil {
					require.Nil(t, mapped.InternalLogDestinations.File)
				}
			default:
				mapped, err := MapDeliveryLoggerConfig(cfg)
				require.NoError(t, err)
				require.NotNil(t, mapped.File)
				if mapped.InternalLogDestinations != nil {
					require.Nil(t, mapped.InternalLogDestinations.File)
				}
			}
			claims, err := cfg.collectStorageDirectoryClaims()
			require.NoError(t, err)
			require.NoError(t, validateStorageDirectoryClaims(claims))
		})
	}
}

func TestInternalLogMappingReturnsSymlinkResolutionFailure(t *testing.T) {
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
	} {
		t.Run(contract.String(), func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(root, "app"), 0o750))
			testutil.Symlink(t, filepath.Join(root, "missing"), filepath.Join(root, "app", "alias"))
			cfg := mapperValidationTestConfig()
			cfg.RuntimeConfig.Target = "app"
			cfg.FsstoreConfig.LogPath = root
			cfg.FsstoreConfig.AppStatePath = t.TempDir()
			configureDebugDirectoryModeContract(&cfg, contract, logconfig.FileSystemStoreConfig{
				LogDir: "normal", LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
			}, logconfig.InternalLogDestinationConfig{FileLogDir: "alias"}, true)
			requireInternalLogFileLayoutResolutionError(t, mapLogDirectoryTestConfig(cfg, contract))
			_, err := cfg.collectStorageDirectoryClaims()
			requireInternalLogFileLayoutResolutionError(t, err)
			_, err = os.Lstat(filepath.Join(root, "app", "normal"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestInternalLogMappingIgnoresUnusedBaseSymlink(t *testing.T) {
	for _, contract := range []pkglogger.LoggerContractKind{
		pkglogger.LoggerContractSync, pkglogger.LoggerContractAsyncBestEffort, pkglogger.LoggerContractDelivery,
	} {
		t.Run(contract.String(), func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(root, "app"), 0o750))
			testutil.Symlink(t, filepath.Join(root, "missing"), filepath.Join(root, "app", "unused-base"))
			cfg := mapperValidationTestConfig()
			cfg.RuntimeConfig.Target = "app"
			cfg.FsstoreConfig.LogPath = root
			cfg.FsstoreConfig.AppStatePath = t.TempDir()
			configureDebugDirectoryModeContract(&cfg, contract, logconfig.FileSystemStoreConfig{
				LogDir: "unused-base", LifecycleMode: pkgfsstore.FileLifecycleModeDisable,
			}, logconfig.InternalLogDestinationConfig{FileLogDir: "internal"}, false)
			require.NoError(t, mapLogDirectoryTestConfig(cfg, contract))
			claims, err := cfg.collectStorageDirectoryClaims()
			require.NoError(t, err)
			require.NoError(t, validateStorageDirectoryClaims(claims))
			_, err = os.Lstat(filepath.Join(root, "app", "internal"))
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

// requireManagedDirectoryLinkRejectedは、errがdirectoryの保存先の解決で、アプリが管理する
// path linkをリンクとして拒否したerrorを含むことを確認します。
func requireManagedDirectoryLinkRejected(t *testing.T, err error, directory, link string) {
	t.Helper()
	var resolutionErr *StorageDirectoryResolutionError
	require.ErrorAs(t, err, &resolutionErr)
	require.Equal(t, directory, resolutionErr.Directory)
	var obstruction *pathidentity.ManagedDirectoryObstructionError
	require.ErrorAs(t, resolutionErr, &obstruction)
	require.Equal(t, link, obstruction.Path)
}

// requireInternalLogFileLayoutResolutionErrorは、errが内部ログ専用fileのlayoutを解決できない
// errorを含むことを確認します。
func requireInternalLogFileLayoutResolutionError(t *testing.T, err error) {
	t.Helper()
	var layoutErr *LogFileLayoutResolutionError
	require.ErrorAs(t, err, &layoutErr)
	require.Equal(t, "internal log", layoutErr.Layout)
}

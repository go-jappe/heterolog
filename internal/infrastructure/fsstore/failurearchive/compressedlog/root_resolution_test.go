package compressedlogarchive

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestNewRootResolvesConfiguredRootAliasWithoutCreatingLayout(t *testing.T) {
	base, physical := t.TempDir(), resolvedTempDirForTest(t)
	configuredRoot := filepath.Join(base, "configured-root")
	testutil.Symlink(t, physical, configuredRoot)
	root, err := NewRoot(configuredRoot, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(physical, "s3", "logger"), root.Path())
	entries, err := os.ReadDir(physical)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestNewRootRejectsManagedServiceAndContractSymlinks(t *testing.T) {
	for _, relative := range []string{"s3", filepath.Join("s3", "logger")} {
		t.Run(relative, func(t *testing.T) {
			base, physical := t.TempDir(), t.TempDir()
			link := filepath.Join(base, relative)
			require.NoError(t, os.MkdirAll(filepath.Dir(link), 0o750))
			testutil.Symlink(t, physical, link)
			root, err := NewRoot(base, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.ErrorIs(t, err, ErrInvalidRoot)
			require.Empty(t, root.Path())
			entries, err := os.ReadDir(physical)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestStoreRejectsManagedFailureCategorySymlinks(t *testing.T) {
	for _, relative := range []string{"retryable", "permanent"} {
		t.Run(relative, func(t *testing.T) {
			base, physical := t.TempDir(), t.TempDir()
			root, err := NewRoot(base, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			root, err = root.ForDestination("destination-a")
			require.NoError(t, err)
			require.NoError(t, os.MkdirAll(root.Path(), 0o750))
			testutil.Symlink(t, physical, filepath.Join(root.Path(), relative))
			store, err := NewStore(root)
			require.NoError(t, err)
			require.Error(t, store.EnsureDirectories(context.Background()))
			entries, err := os.ReadDir(physical)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestNewRootRejectsExistingNonDirectoryBeforeCreatingLayout(t *testing.T) {
	base := t.TempDir()
	configuredRoot := filepath.Join(base, "s3", "logger")
	require.NoError(t, os.MkdirAll(filepath.Dir(configuredRoot), 0o750))
	require.NoError(t, os.WriteFile(configuredRoot, []byte("preserve"), 0o600))
	root, err := NewRoot(base, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.ErrorIs(t, err, ErrInvalidRoot)
	require.Empty(t, root.Path())
	body, err := os.ReadFile(configuredRoot)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(body))
}

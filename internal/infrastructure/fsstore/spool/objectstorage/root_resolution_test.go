package objectstoragespool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestNewRootResolvesUserBaseAliasWithoutCreatingLayout(t *testing.T) {
	base, physical := t.TempDir(), resolvedTempDirForTest(t)
	configuredRoot := filepath.Join(base, "target")
	testutil.Symlink(t, physical, configuredRoot)
	root, err := NewRoot(base, "target", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(physical, "spool", "objectstorage", "s3", "logger"), root.Path())
	entries, err := os.ReadDir(physical)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestNewRootRejectsFixedLayoutAliasesWithoutCreatingDirectories(t *testing.T) {
	for _, relative := range []string{
		"spool",
		filepath.Join("spool", "objectstorage"),
		filepath.Join("spool", "objectstorage", "s3"),
		filepath.Join("spool", "objectstorage", "s3", "logger"),
	} {
		t.Run(relative, func(t *testing.T) {
			base, outside := t.TempDir(), t.TempDir()
			path := filepath.Join(base, "target", relative)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
			testutil.Symlink(t, outside, path)
			root, err := NewRoot(base, "target", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.ErrorIs(t, err, ErrInvalidRoot)
			require.Empty(t, root.Path())
			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestNewRootRejectsExistingNonDirectoryBeforeCreatingLayout(t *testing.T) {
	base := t.TempDir()
	configuredRoot := filepath.Join(base, "target", "spool", "objectstorage", "s3", "logger")
	require.NoError(t, os.MkdirAll(filepath.Dir(configuredRoot), 0o750))
	require.NoError(t, os.WriteFile(configuredRoot, []byte("preserve"), 0o600))
	root, err := NewRoot(base, "target", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.ErrorIs(t, err, ErrInvalidRoot)
	require.Empty(t, root.Path())
	body, err := os.ReadFile(configuredRoot)
	require.NoError(t, err)
	require.Equal(t, "preserve", string(body))
}

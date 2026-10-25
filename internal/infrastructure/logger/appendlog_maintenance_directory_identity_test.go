package logger

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestLifecycleDirectoryOwnerResolvesSymlinkAliases(t *testing.T) {
	root := t.TempDir()
	physical := filepath.Join(root, "app", "real")
	require.NoError(t, os.MkdirAll(physical, 0o750))
	testutil.Symlink(t, physical, filepath.Join(root, "app", "alias"))
	layout := pkgfsstore.FsstoreConfig{LogPath: root, Target: "app", LogDir: filepath.Join("alias", "missing")}
	aliasOwner, err := newLifecycleDirectoryOwner("alias", layout)
	require.NoError(t, err)
	want, err := pathidentity.ResolveDirectory(filepath.Join(physical, "missing"))
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{pathidentity.ComparisonKey(want): {}}, aliasOwner.paths)
	owners := []lifecycleDirectoryOwner{aliasOwner}
	layout.LogDir = filepath.Join("real", "missing")
	physicalOwner, err := newLifecycleDirectoryOwner("real", layout)
	require.NoError(t, err)
	requireLifecycleDirectoryOverlap(t, registerLifecycleDirectoryOwner(&owners, physicalOwner), "alias", "real")
	_, err = os.Lstat(filepath.Join(physical, "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestLifecycleDirectoryOwnerReturnsSymlinkResolutionFailure(t *testing.T) {
	root := t.TempDir()
	testutil.Symlink(t, filepath.Join(root, "missing"), filepath.Join(root, "alias"))
	_, err := newLifecycleDirectoryOwner("base file", pkgfsstore.FsstoreConfig{LogPath: root, LogDir: "alias"})
	var directoryErr *LifecycleDirectoryError
	require.ErrorAs(t, err, &directoryErr)
	require.Equal(t, "base file", directoryErr.Source)
	require.Equal(t, filepath.Join(root, "alias"), directoryErr.Directory)
	// 参照先のないlinkは、解決できない原因をstdlibのsentinelで保持します。
	require.ErrorIs(t, err, fs.ErrNotExist)
}

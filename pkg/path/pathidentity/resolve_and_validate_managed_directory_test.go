package pathidentity

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"

	"github.com/stretchr/testify/require"
)

func TestResolveAndValidateManagedDirectoryAllowsBaseAliasWithoutCreatingChildren(t *testing.T) {
	base := t.TempDir()
	physical, err := ResolveDirectory(t.TempDir())
	require.NoError(t, err)
	alias := filepath.Join(base, "alias")
	testutil.Symlink(t, physical, alias)
	for _, suffix := range []string{"archive-checkpoints", filepath.Join("missing", "archive-checkpoints")} {
		resolved, err := ResolveAndValidateManagedDirectory(alias, suffix)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(physical, suffix), resolved)
	}
	entries, err := os.ReadDir(physical)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestResolveAndValidateManagedDirectoryRejectsLinksInEachManagedComponent(t *testing.T) {
	for _, depth := range []string{"first", "last"} {
		for _, kind := range []string{"directory-link", "dangling-link", "loop", "file"} {
			t.Run(depth+"/"+kind, func(t *testing.T) {
				base, external := t.TempDir(), t.TempDir()
				link := filepath.Join(base, "spool")
				if depth == "last" {
					require.NoError(t, os.Mkdir(link, 0o700))
					link = filepath.Join(link, "active")
				}
				switch kind {
				case "directory-link":
					testutil.Symlink(t, external, link)
				case "dangling-link":
					testutil.Symlink(t, filepath.Join(external, "missing"), link)
				case "loop":
					testutil.Symlink(t, link, link)
				case "file":
					require.NoError(t, os.WriteFile(link, []byte("keep"), 0o600))
				}
				resolved, err := ResolveAndValidateManagedDirectory(base, filepath.Join("spool", "active"))
				// 塞いでいる管理パスとその情報を、型付きerrorから確認します。
				var obstruction *ManagedDirectoryObstructionError
				require.ErrorAs(t, err, &obstruction)
				require.Empty(t, resolved)
				linkInfo, err := os.Lstat(link)
				require.NoError(t, err)
				obstructionInfo, err := os.Lstat(obstruction.Path)
				require.NoError(t, err)
				require.True(t, os.SameFile(linkInfo, obstructionInfo), "obstruction path %q, want %q", obstruction.Path, link)
				require.NotNil(t, obstruction.Info)
				require.Equal(t, linkInfo.Mode().Type(), obstruction.Info.Mode().Type(), "Info must describe the entry itself")
				entries, err := os.ReadDir(external)
				require.NoError(t, err)
				require.Empty(t, entries)
			})
		}
	}
}

func TestResolveAndValidateManagedDirectoryAcceptsExistingRealChildren(t *testing.T) {
	base, err := ResolveDirectory(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(base, "spool", "active"), 0o700))
	for _, suffix := range []string{filepath.Join("spool", "active"), filepath.Join("spool", "active", "2026", "09")} {
		resolved, err := ResolveAndValidateManagedDirectory(base, suffix)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(base, suffix), resolved)
	}
}

func TestResolveAndValidateManagedDirectoryRejectsNonChildPaths(t *testing.T) {
	base := t.TempDir()
	for _, suffix := range []string{"", ".", "..", filepath.Join("..", "outside"), base} {
		resolved, err := ResolveAndValidateManagedDirectory(base, suffix)
		require.ErrorIs(t, err, ErrManagedDirectoryNotChild, "relative path %q", suffix)
		require.Empty(t, resolved)
	}
	for _, empty := range []string{"", " \t"} {
		resolved, err := ResolveAndValidateManagedDirectory(empty, "checkpoints")
		require.ErrorIs(t, err, ErrEmptyStorageDirectory, "base %q", empty)
		require.Empty(t, resolved)
	}
}

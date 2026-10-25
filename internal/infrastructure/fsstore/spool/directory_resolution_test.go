package spool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestNewRootDirResolvesConfiguredAliasesWithoutCreatingDirectories(t *testing.T) {
	for _, placement := range []string{"ancestor", "target"} {
		t.Run(placement, func(t *testing.T) {
			// 解決結果と比べる実体側は、一時ディレクトリの上位のリンクも解決しておきます。
			base, physical := t.TempDir(), resolvedTempDirForTest(t)
			alias := filepath.Join(base, "alias")
			configuredBase := alias
			want := filepath.Join(physical, "target", "spool", "record")
			if placement == "target" {
				configuredBase = base
				alias = filepath.Join(base, "target")
				want = filepath.Join(physical, "spool", "record")
			}
			testutil.Symlink(t, physical, alias)
			root, err := NewRootDir(configuredBase, "target", "record")
			require.NoError(t, err)
			require.Equal(t, want, root.String())
			entries, err := os.ReadDir(physical)
			require.NoError(t, err)
			require.Empty(t, entries, "root resolution must not initialize the layout")
			replacement := t.TempDir()
			require.NoError(t, os.Remove(alias))
			testutil.Symlink(t, replacement, alias)
			require.Equal(t, want, root.String())
			require.Equal(t, filepath.Join(want, "active"), root.ActiveDir().Path())
		})
	}
}

func TestNewRootDirRejectsFixedLayoutAliases(t *testing.T) {
	for _, relative := range []string{"spool", filepath.Join("spool", "record")} {
		t.Run(relative, func(t *testing.T) {
			base, outside := t.TempDir(), t.TempDir()
			path := filepath.Join(base, "target", relative)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
			testutil.Symlink(t, outside, path)
			root, err := NewRootDir(base, "target", "record")
			require.Error(t, err)
			require.Empty(t, root.String())
			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestNewRootDirRejectsManagedStateSymlinksAndNonDirectories(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateActive, pkgspool.StateProcessing, pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		for _, kind := range []string{"symlink", "file"} {
			t.Run(string(state)+"/"+kind, func(t *testing.T) {
				// エラーが示すpathを比較するため、一時ディレクトリの上位のリンクを解決します。
				base, outside := resolvedTempDirForTest(t), t.TempDir()
				root := filepath.Join(base, "target", "spool", "record")
				require.NoError(t, os.MkdirAll(root, 0o750))
				path := filepath.Join(root, string(state))
				if kind == "symlink" {
					testutil.Symlink(t, outside, path)
				} else {
					require.NoError(t, os.WriteFile(path, []byte("not a directory"), 0o600))
				}
				dir, err := NewRootDir(base, "target", "record")
				var stateErr *StateDirectoryError
				require.ErrorAs(t, err, &stateErr)
				require.Equal(t, state, stateErr.State)
				require.Equal(t, path, stateErr.Path)
				require.Empty(t, dir.String())
				entries, err := os.ReadDir(outside)
				require.NoError(t, err)
				require.Empty(t, entries)
			})
		}
	}
}

package spool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	"github.com/stretchr/testify/require"
)

func TestDirectoryPreservationRejectsUnsafePaths(t *testing.T) {
	for _, kind := range []string{"root link", "ancestor link", "root file", "outside path", "preserved link", "preserved file"} {
		t.Run(kind, func(t *testing.T) {
			// 試験が作るリンク・ファイルだけが拒否の原因になるよう、一時ディレクトリの上位のリンクを解決します。
			base, outside := resolvedTempDirForTest(t), resolvedTempDirForTest(t)
			root := filepath.Join(base, "root")
			require.NoError(t, os.Mkdir(root, 0o755))
			path := filepath.Join(root, "active", "2026")
			var obstruction string
			switch kind {
			case "root link":
				require.NoError(t, os.Remove(root))
				testutil.Symlink(t, outside, root)
			case "ancestor link":
				alias := filepath.Join(base, "alias")
				testutil.Symlink(t, outside, alias)
				root = filepath.Join(alias, "root")
				path = filepath.Join(root, "active", "2026")
			case "root file":
				require.NoError(t, os.Remove(root))
				require.NoError(t, os.WriteFile(root, []byte("keep root"), 0o644))
			case "outside path":
				path = filepath.Join(outside, "active", "2026")
			case "preserved link", "preserved file":
				require.NoError(t, os.Mkdir(filepath.Join(root, "active"), 0o755))
				obstruction = path
				require.NoError(t, os.WriteFile(obstruction, []byte("keep obstruction"), 0o644))
				if kind == "preserved link" {
					testutil.Symlink(t, outside, filepath.Join(root, ".preserved"))
				} else {
					require.NoError(t, os.WriteFile(filepath.Join(root, ".preserved"), []byte("keep preservation entry"), 0o644))
				}
			}
			preserved, err := EnsureDirectoryWithPreservation(context.Background(), root, path)
			// 試験が作ったリンク・ファイル・pathが、拒否の原因として示されることを確認します。
			switch kind {
			case "root link", "ancestor link", "root file":
				var rootErr *RootError
				require.ErrorAs(t, err, &rootErr)
				require.Equal(t, root, rootErr.Root)
				require.Equal(t, kind != "root file", rootErr.Link)
			case "outside path":
				var rangeErr *DirectoryRangeError
				require.ErrorAs(t, err, &rangeErr)
				require.Equal(t, path, rangeErr.Directory)
				require.True(t, rangeErr.OutsideRoot)
			default:
				var managed *pathidentity.ManagedDirectoryObstructionError
				require.ErrorAs(t, err, &managed)
				require.Equal(t, filepath.Join(root, ".preserved"), managed.Path)
			}
			require.Empty(t, preserved)
			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			require.Empty(t, entries, "unsafe ancestors and preservation links must never be followed")
			if obstruction != "" {
				body, err := os.ReadFile(obstruction)
				require.NoError(t, err)
				require.Equal(t, "keep obstruction", string(body))
			}
		})
	}
}

func TestDirectoryPreservationDoesNotMoveHealthyDirectories(t *testing.T) {
	// 隔離処理は解決済みのrootを要求するため、一時ディレクトリの上位のリンクを解決します。
	root := resolvedTempDirForTest(t)
	directory := filepath.Join(root, "active", "2026", "10")
	require.NoError(t, os.MkdirAll(directory, 0o755))
	file := filepath.Join(directory, "keep.hlogspool")
	require.NoError(t, os.WriteFile(file, []byte("keep"), 0o644))
	preserved, err := EnsureDirectoryWithPreservation(context.Background(), root, directory)
	require.NoError(t, err)
	require.Empty(t, preserved)
	body, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "keep", string(body))
	_, err = os.Lstat(filepath.Join(root, ".preserved"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDirectoryPreservationCancelsBeforeModificationAndWhileWaiting(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "active"), []byte("keep"), 0o644))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	preserved, err := EnsureDirectoryWithPreservation(ctx, root, filepath.Join(root, "active", "2026"))
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, preserved)
	release, err := lockDirectoryPreparation(context.Background(), root)
	require.NoError(t, err)
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := EnsureDirectoryWithPreservation(ctx, root, filepath.Join(root, "active", "2026"))
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		release()
		require.True(t, errors.Is(err, context.Canceled))
	case <-time.After(3 * time.Second):
		release()
		t.Fatal("canceled directory lock wait did not return")
	}
	body, err := os.ReadFile(filepath.Join(root, "active"))
	require.NoError(t, err)
	require.Equal(t, "keep", string(body))
	_, err = os.Lstat(filepath.Join(root, ".preserved"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

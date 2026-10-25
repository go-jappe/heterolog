package spool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestSpoolDirectoryPreparationRetriesPendingSyncAcrossCallers(t *testing.T) {
	t.Parallel()
	// 同期失敗は解決済みのパスで返るため、macOSの/varなど上位のリンクを先に解決します。
	root := resolvedTempDirForTest(t)
	directory := filepath.Join(root, "active", "2026", "10")
	restore := testutil.DenyDirectorySync(t, root)
	err := EnsureDirectoryNoSymlinks(root, directory)
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	var failure *infrafsstore.DirectorySyncError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, root, failure.Directory)
	// 作成済みの書込先も、親の同期を確認できるまでは成功扱いにしません。
	err = EnsureDirectoryNoSymlinks(root, directory)
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	require.ErrorAs(t, err, &failure)
	require.Equal(t, root, failure.Directory)

	const callers = 8
	results := make(chan error, callers)
	next := filepath.Join(root, "next")
	for range callers {
		go func() { results <- EnsureDirectoryNoSymlinks(root, next) }()
	}
	for range callers {
		require.ErrorIs(t, <-results, pkgfsstore.ErrDurabilityUnconfirmed)
	}
	_, err = os.Lstat(next)
	require.ErrorIs(t, err, os.ErrNotExist, "do not grow new partitions while their parent sync is pending")
	// 同期失敗の記録はinfrafsstoreが保持します。rootのlockは呼び出しの終了後に解放します。
	key, err := pathidentity.AbsoluteKey(root)
	require.NoError(t, err)
	directoryPreparationRegistry.mu.Lock()
	_, retained := directoryPreparationRegistry.byRoot[key]
	directoryPreparationRegistry.mu.Unlock()
	require.False(t, retained, "release finished callers instead of retaining a growing registry")

	// A pending root must not block an unrelated spool root.
	otherRoot := t.TempDir()
	require.NoError(t, EnsureDirectoryNoSymlinks(otherRoot, filepath.Join(otherRoot, "active")))
	restore()
	require.NoError(t, EnsureDirectoryNoSymlinks(root, directory))
	directoryPreparationRegistry.mu.Lock()
	_, retained = directoryPreparationRegistry.byRoot[key]
	directoryPreparationRegistry.mu.Unlock()
	require.False(t, retained, "release recovered roots instead of retaining a growing registry")
	require.NoError(t, EnsureDirectoryNoSymlinks(root, next))
}

func TestSpoolDirectoryPreparationRetriesPendingSyncAcrossStoreEntryPoints(t *testing.T) {
	t.Parallel()
	root := resolvedTempDirForTest(t)
	directory := filepath.Join(root, "active", "2026")
	restore := testutil.DenyDirectorySync(t, root)
	_, err := EnsureDirectoryWithPreservation(t.Context(), root, directory)
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	// 初回退避と再送で入口が異なっても、同じ親の記録を再試行してから成功します。
	err = EnsureDirectoryNoSymlinks(root, directory)
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	var failure *infrafsstore.DirectorySyncError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, root, failure.Directory)
	restore()
	preserved, err := EnsureDirectoryWithPreservation(t.Context(), root, directory)
	require.NoError(t, err)
	require.Empty(t, preserved)
	require.NoError(t, EnsureDirectoryNoSymlinks(root, directory))
}

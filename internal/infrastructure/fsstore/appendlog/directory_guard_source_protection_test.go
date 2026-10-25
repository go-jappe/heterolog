package appendlog

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectoryGuardSourceClaimKeepsOneOwnerAcrossCompressionFormats(t *testing.T) {
	dir := t.TempDir()
	guard := DirectoryGuardForPath(dir)
	raw := filepath.Join(dir, "app_20260914_7.log")
	owners := []*DirectoryGuardToken{NewDirectoryGuardToken(), NewDirectoryGuardToken()}
	paths := make([][]string, len(owners))
	for index, extension := range []string{".gz", ".zst"} {
		compressed := raw + extension
		source, ok := fsstore.CompressionSourcePath(compressed, "log")
		require.True(t, ok)
		paths[index] = []string{source, compressed}
	}
	// fileが存在しなくても、元ログのpathを共通キーにして同時claimを防ぎます。
	require.NoFileExists(t, raw)
	start := make(chan struct{})
	type claimResult struct {
		index   int
		claimed bool
	}
	done := make(chan claimResult, len(owners))
	for index, owner := range owners {
		go func() {
			<-start
			done <- claimResult{index, guard.ProtectPathsIfNoOtherOwnerConflict(owner, nil, paths[index])}
		}()
	}
	close(start)
	first, second := <-done, <-done
	require.NotEqual(t, first.claimed, second.claimed)
	winner, loser := first.index, second.index
	if !first.claimed {
		winner, loser = loser, winner
	}
	guard.statesMu.RLock()
	_, loserRegistered := guard.statesByToken[owners[loser]]
	guard.statesMu.RUnlock()
	require.False(t, loserRegistered)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Equal(t, map[string]struct{}{raw: {}, paths[winner][1]: {}}, protected)
	})
	guard.UnprotectPaths(owners[winner], paths[winner]...)
	require.True(t, guard.ProtectPathsIfNoOtherOwnerConflict(owners[loser], nil, paths[loser]))
	guard.UnprotectPaths(owners[loser], paths[loser]...)
	guard.WithProtectedPaths(func(protected map[string]struct{}) { require.Empty(t, protected) })
}

func TestDirectoryGuardRejectedSourceClaimPreservesExistingProtection(t *testing.T) {
	dir := t.TempDir()
	guard := DirectoryGuardForPath(dir)
	owner, other := NewDirectoryGuardToken(), NewDirectoryGuardToken()
	raw := filepath.Join(dir, "app_20260914_7.log")
	next := filepath.Join(dir, "app_20260914_8.log")
	guard.ProtectPaths(owner, next)
	guard.ProtectPaths(other, raw, raw+".gz")
	require.False(t, guard.ProtectPathsIfNoOtherOwnerConflict(owner, nil, []string{raw, raw + ".zst"}))
	guard.UnprotectPaths(other, raw, raw+".gz")
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Equal(t, map[string]struct{}{next: {}}, protected)
	})
	guard.UnprotectPaths(owner, next)
}

func TestDirectoryGuardClaimsRejectNilTokens(t *testing.T) {
	dir := t.TempDir()
	guard := DirectoryGuardForPath(dir)
	owner := NewDirectoryGuardToken()
	raw := filepath.Join(dir, "app_20260914_7.log")
	guard.ProtectPaths(owner, raw)
	require.False(t, guard.ProtectPathsIfNoOtherOwnerConflict(nil, owner, []string{raw, raw + ".gz"}))
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Equal(t, map[string]struct{}{raw: {}}, protected)
	})
	guard.statesMu.RLock()
	_, nilRegistered := guard.statesByToken[nil]
	guard.statesMu.RUnlock()
	require.False(t, nilRegistered)
	guard.UnprotectPaths(owner, raw)
}

package appendlog

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

func TestDirectoryGuardSnapshotsDeduplicateOwnersAndIsolateReturnedMaps(t *testing.T) {
	dir := t.TempDir()
	guard := DirectoryGuardForPath(dir)
	first, second := filepath.Join(dir, "first.log"), filepath.Join(dir, "second.log")
	retained := filepath.Join(dir, "retained.log")
	callerOnly := filepath.Join(dir, "caller-only.log")
	for _, active := range []string{first, first, second} {
		owner := NewDirectoryGuardToken()
		require.NoError(t, guard.TransitionActive(owner, func() (string, error) { return active, nil }))
		guard.ProtectPaths(owner, first, retained)
	}
	wantProtected := map[string]struct{}{first: {}, second: {}, retained: {}}
	wantActive := map[string]struct{}{first: {}, second: {}}

	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Equal(t, wantProtected, protected)
		delete(protected, first)
		protected[callerOnly] = struct{}{}
	})
	require.NoError(t, guard.RunWithLifecycleLock(func() error {
		guard.RunPurgeWithProtection(nil, noAdditionalPurgeProtection, func(purge *Purge) {
			protected, active := purge.ProtectedPaths(), purge.ActivePaths()
			require.Equal(t, wantProtected, protected)
			require.Equal(t, wantActive, active)
			for path := range active {
				require.Contains(t, protected, path)
			}
			delete(protected, first)
			protected[callerOnly] = struct{}{}
			require.Equal(t, wantActive, active)
			delete(active, second)
			active[callerOnly] = struct{}{}
			require.Equal(t, wantProtected, purge.ProtectedPaths())
			require.Equal(t, wantActive, purge.ActivePaths())
		})
		return nil
	}))
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		require.Equal(t, wantProtected, protected)
	})
}

func TestDirectoryGuardSnapshotsChooseStableActiveSpellingUsingFilesystemCaseRules(t *testing.T) {
	for _, rule := range directoryCaseRulesForTest {
		for _, reverse := range []bool{false, true} {
			name := "retained-first"
			if reverse {
				name = "active-first"
			}
			t.Run(rule.name+"/"+name, func(t *testing.T) {
				testDirectoryGuardSnapshotsChooseStableActiveSpelling(t, rule.nameCase, reverse)
			})
		}
	}
}

func testDirectoryGuardSnapshotsChooseStableActiveSpelling(t *testing.T, nameCase pathidentity.NameCase, reverse bool) {
	dir := t.TempDir()
	ignoresCase := useDirectoryCaseRuleForTest(t, dir, nameCase)
	guard := DirectoryGuardForPath(dir)
	retained := filepath.Join(dir, "APP.LOG")
	activeFirst, activeSecond := filepath.Join(dir, "App.log"), filepath.Join(dir, "app.log")
	paths := []string{retained, activeSecond, activeFirst}
	for i := range paths {
		if reverse {
			i = len(paths) - 1 - i
		}
		owner, path := NewDirectoryGuardToken(), paths[i]
		if path == retained {
			guard.ProtectPaths(owner, path)
		} else {
			require.NoError(t, guard.TransitionActive(owner, func() (string, error) { return path, nil }))
		}
	}
	wantProtected := map[string]struct{}{retained: {}, activeFirst: {}, activeSecond: {}}
	wantActive := map[string]struct{}{activeFirst: {}, activeSecond: {}}
	if ignoresCase {
		// Active spelling wins even though the retained spelling sorts first.
		// Multiple active spellings use the lexical minimum, independent of owner order.
		wantProtected = map[string]struct{}{activeFirst: {}}
		wantActive = map[string]struct{}{activeFirst: {}}
	}
	for range 32 {
		guard.WithProtectedPaths(func(protected map[string]struct{}) {
			require.Equal(t, wantProtected, protected)
		})
		require.NoError(t, guard.RunWithLifecycleLock(func() error {
			guard.RunPurgeWithProtection(nil, noAdditionalPurgeProtection, func(purge *Purge) {
				require.Equal(t, wantProtected, purge.ProtectedPaths())
				require.Equal(t, wantActive, purge.ActivePaths())
			})
			return nil
		}))
	}
}

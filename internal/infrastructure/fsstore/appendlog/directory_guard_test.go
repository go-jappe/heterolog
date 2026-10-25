package appendlog

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// directoryGuardHangTimeoutは、期待した通知が届かない回帰でtestが止まり続けないための
// 上限です。処理順序の判定には使わず、負荷の高い環境でも誤検出しない長さにします。
const directoryGuardHangTimeout = 30 * time.Second

func TestDirectoryGuardLifecycleWaitCanBeCanceled(t *testing.T) {
	t.Parallel()
	guard := &DirectoryGuard{}
	guard.lifecycleMu.Lock()
	defer guard.lifecycleMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := guard.RunWithLifecycleLockContext(ctx, func() error { called = true; return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called)
}

func TestDirectoryGuardRunWithLifecycleLockSerializesOperations(t *testing.T) {
	// lifecycle lockの待機はbubble内のchannelで止まるため、synctest.Waitで二件目が
	// 待機に入ったことを時間に依存せず確認できます。
	synctest.Test(t, func(t *testing.T) {
		guard := DirectoryGuardForPath(t.TempDir())
		firstEntered := make(chan struct{})
		releaseFirst := make(chan struct{})
		unblockFirst := sync.OnceFunc(func() { close(releaseFirst) })
		defer unblockFirst()
		go func() {
			_ = guard.RunWithLifecycleLock(func() error {
				close(firstEntered)
				<-releaseFirst
				return nil
			})
		}()
		<-firstEntered

		secondEntered := make(chan struct{})
		secondDone := make(chan struct{})
		go func() {
			defer close(secondDone)
			_ = guard.RunWithLifecycleLock(func() error {
				close(secondEntered)
				return nil
			})
		}()
		synctest.Wait()
		select {
		case <-secondEntered:
			t.Fatal("second lifecycle operation entered before the first finished")
		default:
		}
		unblockFirst()
		select {
		case <-secondEntered:
		case <-time.After(time.Minute):
			// bubble内の時刻は全goroutineが停止した場合だけ進むため、二件目の取りこぼしを表します。
			t.Fatal("second lifecycle operation was dropped instead of queued")
		}
		<-secondDone
	})
}

func TestDirectoryGuardBlocksActiveTransitionDuringDeletion(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	token := NewDirectoryGuardToken()
	oldPath := filepath.Join(t.TempDir(), "old.log")
	newPath := filepath.Join(filepath.Dir(oldPath), "new.log")
	if err := guard.TransitionActive(token, func() (string, error) {
		return oldPath, nil
	}); err != nil {
		t.Fatalf("initial TransitionActive() error = %v", err)
	}

	deletionEntered := make(chan struct{})
	releaseDeletion := make(chan struct{})
	go guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if _, ok := protected[filepath.Clean(oldPath)]; !ok {
			t.Errorf("old active path %q is not protected", oldPath)
		}
		close(deletionEntered)
		<-releaseDeletion
	})
	<-deletionEntered

	transitionEntered := make(chan struct{})
	transitionDone := make(chan error, 1)
	go func() {
		transitionDone <- guard.TransitionActive(token, func() (string, error) {
			close(transitionEntered)
			return newPath, nil
		})
	}()
	waitForQueuedStatesWriter(t, guard, transitionEntered)
	select {
	case <-transitionEntered:
		t.Fatal("active transition entered while deletion held the protected-path view")
	default:
	}
	close(releaseDeletion)
	select {
	case err := <-transitionDone:
		if err != nil {
			t.Fatalf("TransitionActive() error = %v", err)
		}
	case <-time.After(directoryGuardHangTimeout):
		t.Fatal("active transition did not resume")
	}
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if _, ok := protected[pathidentity.AbsoluteOrCleanPath(newPath)]; !ok {
			t.Fatalf("protected paths = %v, want new active path %q", protected, newPath)
		}
		if _, ok := protected[pathidentity.AbsoluteOrCleanPath(oldPath)]; ok {
			t.Fatalf("protected paths = %v, old active path %q remains", protected, oldPath)
		}
	})
}

// waitForQueuedStatesWriterは、statesMuの書き込みlockを待つgoroutineが登録されるまで待ちます。
// RWMutexは書き込み待ちがある間に新しい読み取りlockを許可しないため、TryRLockの失敗で
// active切替が保護一覧の排他待ちに入ったことを時間に依存せず確認できます。待機中に
// enteredが閉じた場合は、保護一覧の参照中に切替が実行されたことを表します。
func waitForQueuedStatesWriter(t *testing.T, guard *DirectoryGuard, entered <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(directoryGuardHangTimeout)
	for guard.statesMu.TryRLock() {
		guard.statesMu.RUnlock()
		select {
		case <-entered:
			t.Fatal("active transition entered while deletion held the protected-path view")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("active transition did not wait for the protected-path view")
		}
		runtime.Gosched()
	}
}

func TestDirectoryGuardKeepsActivePathWhenTransitionFails(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	token := NewDirectoryGuardToken()
	oldPath := filepath.Join(t.TempDir(), "old.log")
	if err := guard.TransitionActive(token, func() (string, error) {
		return oldPath, nil
	}); err != nil {
		t.Fatalf("initial TransitionActive() error = %v", err)
	}
	wantErr := errors.New("open failed")
	newPath := filepath.Join(filepath.Dir(oldPath), "new.log")

	err := guard.TransitionActive(token, func() (string, error) {
		return newPath, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("TransitionActive() error = %v, want %v", err, wantErr)
	}
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if _, ok := protected[pathidentity.AbsoluteOrCleanPath(oldPath)]; !ok {
			t.Fatalf("protected paths after failed transition = %v, want old path %q", protected, oldPath)
		}
		if _, ok := protected[pathidentity.AbsoluteOrCleanPath(newPath)]; ok {
			t.Fatalf("protected paths after failed transition = %v, unexpected new path %q", protected, newPath)
		}
	})
}

func TestDirectoryGuardRetainsArchivePathsAfterWriterClose(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	token := NewDirectoryGuardToken()
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old.log")
	compressedPath := oldPath + ".gz"
	if err := guard.TransitionActive(token, func() (string, error) {
		return oldPath, nil
	}); err != nil {
		t.Fatalf("initial TransitionActive() error = %v", err)
	}
	guard.ProtectPaths(token, oldPath, compressedPath)
	guard.RemoveToken(token)

	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		for _, path := range []string{oldPath, compressedPath} {
			if _, ok := protected[filepath.Clean(path)]; !ok {
				t.Errorf("archive path %q lost protection when writer closed", path)
			}
		}
	})
	guard.UnprotectPaths(token, oldPath, compressedPath)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if len(protected) != 0 {
			t.Fatalf("protected paths after archive completion = %v, want none", protected)
		}
	})
}

func TestDirectoryGuardDistinguishesActivePathsFromRetainedPaths(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	activeToken := NewDirectoryGuardToken()
	retainedToken := NewDirectoryGuardToken()
	directory := t.TempDir()
	activePath := filepath.Join(directory, "active.log")
	retainedPath := filepath.Join(directory, "retained.log")
	if err := guard.TransitionActive(activeToken, func() (string, error) {
		return activePath, nil
	}); err != nil {
		t.Fatalf("TransitionActive() error = %v", err)
	}
	guard.ProtectPaths(retainedToken, retainedPath)

	guard.statesMu.RLock()
	protectedPaths, activePaths := guard.protectedAndActivePathsLocked()
	guard.statesMu.RUnlock()
	if _, ok := protectedPaths[pathidentity.AbsoluteOrCleanPath(activePath)]; !ok {
		t.Errorf("protected paths = %v, want active path %q", protectedPaths, activePath)
	}
	if _, ok := protectedPaths[pathidentity.AbsoluteOrCleanPath(retainedPath)]; !ok {
		t.Errorf("protected paths = %v, want retained path %q", protectedPaths, retainedPath)
	}
	if _, ok := activePaths[pathidentity.AbsoluteOrCleanPath(activePath)]; !ok {
		t.Errorf("active paths = %v, want active path %q", activePaths, activePath)
	}
	if _, ok := activePaths[pathidentity.AbsoluteOrCleanPath(retainedPath)]; ok {
		t.Errorf("active paths = %v, retained path %q must not be active", activePaths, retainedPath)
	}
}

func TestDirectoryGuardTreatsDuplicateProtectionAsIdempotent(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	token := NewDirectoryGuardToken()
	path := filepath.Join(t.TempDir(), "retained.log")

	guard.ProtectPaths(token, path)
	guard.ProtectPaths(token, path)
	guard.UnprotectPaths(token, path)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if _, ok := protected[pathidentity.AbsoluteOrCleanPath(path)]; ok {
			t.Fatal("path remained protected after duplicate protection was released once")
		}
	})
}

func TestDirectoryGuardKeepsPathProtectedUntilEveryIndependentOwnerReleases(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	firstOwner := NewDirectoryGuardToken()
	secondOwner := NewDirectoryGuardToken()
	path := filepath.Join(t.TempDir(), "shared-retained.log")
	normalizedPath := pathidentity.AbsoluteOrCleanPath(path)

	guard.ProtectPaths(firstOwner, path)
	guard.ProtectPaths(secondOwner, path)
	guard.UnprotectPaths(firstOwner, path)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if _, ok := protected[normalizedPath]; !ok {
			t.Fatal("path lost protection while the second owner still retained it")
		}
	})

	guard.UnprotectPaths(secondOwner, path)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if _, ok := protected[normalizedPath]; ok {
			t.Fatal("path remained protected after both independent owners released it")
		}
	})
}

func TestDirectoryGuardClaimsPathsAtomicallyWhileExcludingTransitionOwner(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	writerToken := NewDirectoryGuardToken()
	archiveToken := NewDirectoryGuardToken()
	retryToken := NewDirectoryGuardToken()
	rawPath := filepath.Join(t.TempDir(), "series_1.log")
	compressedPath := rawPath + ".gz"
	if err := guard.TransitionActive(writerToken, func() (string, error) {
		return rawPath, nil
	}); err != nil {
		t.Fatalf("TransitionActive() error = %v", err)
	}
	guard.ProtectPaths(archiveToken, rawPath)
	guard.ProtectPaths(retryToken, compressedPath)
	paths := []string{rawPath, compressedPath}

	if guard.ProtectPathsIfNoOtherOwnerConflict(
		archiveToken,
		writerToken,
		paths,
	) {
		t.Fatal("claim succeeded while an independent retry owner protected the generation")
	}
	// 失敗したclaimはarchiveTokenの先行raw保護を残し、compressed pathを追加しません。
	guard.UnprotectPaths(retryToken, compressedPath)
	if !guard.ProtectPathsIfNoOtherOwnerConflict(
		archiveToken,
		writerToken,
		paths,
	) {
		t.Fatal("claim failed after the independent owner released the generation")
	}

	guard.UnprotectPaths(archiveToken, paths...)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if _, exists := protected[pathidentity.AbsoluteOrCleanPath(rawPath)]; !exists {
			t.Fatal("writer active path was removed with the archive task protection")
		}
		if _, exists := protected[pathidentity.AbsoluteOrCleanPath(compressedPath)]; exists {
			t.Fatal("compressed path remained protected after the archive task released it")
		}
	})
}

func TestDirectoryGuardConcurrentClaimsHaveOneOwner(t *testing.T) {
	dir := t.TempDir()
	guard := DirectoryGuardForPath(dir)
	paths := []string{filepath.Join(dir, "series_1.log"), filepath.Join(dir, "series_1.log.gz")}
	owners := []*DirectoryGuardToken{NewDirectoryGuardToken(), NewDirectoryGuardToken()}
	type claimResult struct {
		owner   *DirectoryGuardToken
		claimed bool
	}
	ready := make(chan struct{}, len(owners))
	start := make(chan struct{})
	done := make(chan claimResult, len(owners))
	for _, owner := range owners {
		go func() {
			ready <- struct{}{}
			<-start
			done <- claimResult{owner: owner, claimed: guard.ProtectPathsIfNoOtherOwnerConflict(owner, nil, paths)}
		}()
	}
	for range owners {
		<-ready
	}
	close(start)
	first, second := <-done, <-done
	if first.claimed == second.claimed {
		t.Fatalf("concurrent claims = %t, %t; want exactly one successful claim", first.claimed, second.claimed)
	}
	winner, loser := first.owner, second.owner
	if !first.claimed {
		winner, loser = loser, winner
	}
	guard.statesMu.RLock()
	_, loserRegistered := guard.statesByToken[loser]
	guard.statesMu.RUnlock()
	if loserRegistered {
		t.Fatal("losing claim registered an owner")
	}
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if len(protected) != len(paths) {
			t.Fatalf("protected paths = %v, want all winning paths", protected)
		}
		for _, path := range paths {
			if _, exists := protected[pathidentity.AbsoluteOrCleanPath(path)]; !exists {
				t.Errorf("winning path %q was not registered", path)
			}
		}
	})
	guard.UnprotectPaths(winner, paths...)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if len(protected) != 0 {
			t.Fatalf("protected paths after winning owner released = %v, losing claim must not retain paths", protected)
		}
	})
}

func TestDirectoryGuardRejectedClaimPreservesExistingProtection(t *testing.T) {
	dir := t.TempDir()
	guard := DirectoryGuardForPath(dir)
	owner := NewDirectoryGuardToken()
	otherOwner := NewDirectoryGuardToken()
	activePath := filepath.Join(dir, "active.log")
	rawPath := filepath.Join(dir, "series_1.log")
	compressedPath := rawPath + ".gz"
	otherCompressedPath := rawPath + ".zst"
	if err := guard.TransitionActive(owner, func() (string, error) {
		return activePath, nil
	}); err != nil {
		t.Fatalf("TransitionActive() error = %v", err)
	}
	guard.ProtectPaths(owner, rawPath)
	guard.ProtectPaths(otherOwner, compressedPath)
	if guard.ProtectPathsIfNoOtherOwnerConflict(
		owner,
		nil,
		[]string{rawPath, otherCompressedPath, compressedPath},
	) {
		t.Fatal("claim succeeded while another owner protected a requested path")
	}

	// 他ownerを解除してから確認し、失敗したclaimによる同じpathへの誤登録も検出します。
	guard.UnprotectPaths(otherOwner, compressedPath)
	guard.WithProtectedPaths(func(protected map[string]struct{}) {
		if len(protected) != 2 {
			t.Errorf("protected paths after rejected claim = %v, want only the original active and retained paths", protected)
		}
		for _, path := range []string{activePath, rawPath} {
			if _, exists := protected[pathidentity.AbsoluteOrCleanPath(path)]; !exists {
				t.Errorf("rejected claim removed preexisting protection for %q", path)
			}
		}
		for _, path := range []string{compressedPath, otherCompressedPath} {
			if _, exists := protected[pathidentity.AbsoluteOrCleanPath(path)]; exists {
				t.Errorf("rejected claim added protection for %q", path)
			}
		}
	})
}

func TestDirectoryGuardTokensAreDistinct(t *testing.T) {
	first := NewDirectoryGuardToken()
	second := NewDirectoryGuardToken()
	if first == second {
		t.Fatal("different directory guard tokens have the same address")
	}
}

func TestDirectoryGuardRejectsNilTokenWithoutRunningTransition(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	called := false

	err := guard.TransitionActive(nil, func() (string, error) {
		called = true
		return "unused.log", nil
	})

	if !errors.Is(err, ErrNilDirectoryGuardToken) {
		t.Fatalf("TransitionActive() error = %v, want %v", err, ErrNilDirectoryGuardToken)
	}
	if called {
		t.Fatal("TransitionActive() ran transition with a nil token")
	}
}

func TestDirectoryGuardNormalizesRelativeAndAbsolutePaths(t *testing.T) {
	// Windowsではpackageの作業directoryと一時directoryが別のdriveにある場合があるため、
	// 同じvolumeの一時directoryを作業directoryにして相対pathを作ります。
	// 8.3形式などの別表記を混ぜないよう、どちらもlinkを解決したpathを使います。
	absoluteDir := resolvedTempDirForTest(t)
	workingDir := resolvedTempDirForTest(t)
	t.Chdir(workingDir)
	relativeDir, err := filepath.Rel(workingDir, absoluteDir)
	if err != nil {
		t.Fatalf("filepath.Rel() error = %v", err)
	}

	fromRelative := DirectoryGuardForPath(relativeDir)
	fromAbsolute := DirectoryGuardForPath(absoluteDir)
	if fromRelative != fromAbsolute {
		t.Fatal("relative and absolute directory paths returned different guards")
	}

	token := NewDirectoryGuardToken()
	if err := fromRelative.TransitionActive(token, func() (string, error) {
		return filepath.Join(relativeDir, "active.log"), nil
	}); err != nil {
		t.Fatalf("TransitionActive() error = %v", err)
	}
	want := pathidentity.AbsoluteOrCleanPath(filepath.Join(absoluteDir, "active.log"))

	fromAbsolute.WithProtectedPaths(func(protected map[string]struct{}) {
		if _, ok := protected[want]; !ok {
			t.Fatalf("protected paths = %v, want normalized path %q", protected, want)
		}
	})
}

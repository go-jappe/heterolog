package appendlog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
)

func TestDirectoryGuardRetainedPurgeAllowsRotationAndReleasesOnlyItsOwnPaths(t *testing.T) {
	for _, panicOnReturn := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panicOnReturn), func(t *testing.T) {
			dir := t.TempDir()
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			guard := DirectoryGuardForPath(dir)
			activePath := filepath.Join(dir, "active.log")
			nextActivePath := filepath.Join(dir, "next.log")
			pendingPath := filepath.Join(dir, "pending.log.gz")
			candidate := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_1.log", 1, now.AddDate(0, 0, -3))
			sharedCandidatePath := filepath.Join(dir, "shared-old.log")
			writer := NewDirectoryGuardToken()
			pendingOwner := NewDirectoryGuardToken()
			laterOwner := NewDirectoryGuardToken()
			require.NoError(t, guard.TransitionActive(writer, func() (string, error) {
				return activePath, nil
			}))
			guard.ProtectPaths(pendingOwner, pendingPath)
			files := []fsstore.FileInfo{
				{Path: activePath}, {Path: pendingPath},
				candidate, {Path: sharedCandidatePath},
			}

			views := make(chan [2]map[string]struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			done := make(chan struct{})
			var panicValue any
			go func() {
				defer close(done)
				defer func() { panicValue = recover() }()
				_ = guard.RunWithLifecycleLock(func() error {
					guard.RunPurgeWithProtection(files, noAdditionalPurgeProtection, func(purge *Purge) {
						views <- [2]map[string]struct{}{purge.ProtectedPaths(), purge.ActivePaths()}
						<-release
						if panicOnReturn {
							panic("purge operation failed")
						}
					})
					return nil
				})
			}()
			t.Cleanup(func() {
				unblock()
				<-done
			})

			var snapshots [2]map[string]struct{}
			select {
			case snapshots = <-views:
			case <-time.After(directoryGuardHangTimeout):
				t.Fatal("purge did not enter its operation")
			}
			require.Equal(t, map[string]struct{}{activePath: {}, pendingPath: {}}, snapshots[0])
			require.Equal(t, map[string]struct{}{activePath: {}}, snapshots[1])

			rotationDone := make(chan error, 1)
			go func() {
				var checkErr error
				guard.WithProtectedPaths(func(protected map[string]struct{}) {
					for _, file := range files {
						if _, ok := protected[file.Path]; !ok {
							checkErr = fmt.Errorf("purge candidate or existing protection missing: %s", file.Path)
						}
					}
				})
				if checkErr != nil {
					rotationDone <- checkErr
					return
				}
				// 別ownerが後から同じpathを保護しても、purge側の解除で失われてはいけません。
				guard.ProtectPaths(laterOwner, sharedCandidatePath)
				rotationDone <- guard.TransitionActive(writer, func() (string, error) {
					return nextActivePath, nil
				})
			}()
			select {
			case err := <-rotationDone:
				require.NoError(t, err)
			case <-time.After(directoryGuardHangTimeout):
				unblock()
				<-rotationDone
				t.Fatal("rotation waited for the purge operation to finish")
			}
			// active診断は開始時のsnapshotであり、並行rotationによって書き換わりません。
			require.Equal(t, map[string]struct{}{activePath: {}}, snapshots[1])
			unblock()
			<-done
			if panicOnReturn {
				require.Equal(t, "purge operation failed", panicValue)
			} else {
				require.Nil(t, panicValue)
			}
			guard.WithProtectedPaths(func(protected map[string]struct{}) {
				require.Equal(t, map[string]struct{}{
					nextActivePath: {}, pendingPath: {}, sharedCandidatePath: {},
				}, protected)
			})
			require.FileExists(t, candidate.Path)
			// 正常終了・panicのどちらでも一時保護が残らず、次のpurgeで同じファイルを削除できます。
			require.NoError(t, guard.RunWithLifecycleLock(func() error {
				candidateFiles := []fsstore.FileInfo{candidate}
				guard.RunPurgeWithProtection(candidateFiles, noAdditionalPurgeProtection, func(purge *Purge) {
					result := purge.PurgeByAge(candidateFiles, 1, now, time.Time{})
					require.True(t, result.Ran)
					require.Equal(t, []fsstore.FileRemovalResult{{File: candidate}}, result.RemovalResults)
					require.Empty(t, result.Remaining)
				})
				return nil
			}))
			require.NoFileExists(t, candidate.Path)
			guard.WithProtectedPaths(func(protected map[string]struct{}) {
				require.Equal(t, map[string]struct{}{
					nextActivePath: {}, pendingPath: {}, sharedCandidatePath: {},
				}, protected)
			})
		})
	}
}

func TestDirectoryGuardRetainedPurgeRunsWithNoUnprotectedCandidates(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	owner := NewDirectoryGuardToken()
	path := filepath.Join(t.TempDir(), "active.log")
	require.NoError(t, guard.TransitionActive(owner, func() (string, error) { return path, nil }))
	for _, files := range [][]fsstore.FileInfo{nil, {{Path: path}}} {
		var calls []string
		guard.RunPurgeWithProtection(files, func(protected map[string]struct{}) map[string]struct{} {
			calls = append(calls, "prepare")
			require.Contains(t, protected, path)
			return nil
		}, func(purge *Purge) {
			calls = append(calls, "purge")
			require.Contains(t, purge.ProtectedPaths(), path)
			require.Contains(t, purge.ActivePaths(), path)
		})
		require.Equal(t, []string{"prepare", "purge"}, calls,
			"protection preparation and diagnostics must each run once even when no file can be removed")
	}
}

func TestDirectoryGuardPurgeRequiresProtectionPreparation(t *testing.T) {
	guard := DirectoryGuardForPath(t.TempDir())
	ran := false
	require.Panics(t, func() {
		guard.RunPurgeWithProtection(nil, nil, func(*Purge) { ran = true })
	})
	require.False(t, ran)
}

func TestDirectoryGuardPurgePreparationRetainsCandidatesAndAllowsRotation(t *testing.T) {
	for _, panicDuringPreparation := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panicDuringPreparation), func(t *testing.T) {
			dir := t.TempDir()
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			active := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_1.log", 1, now)
			peer := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_1.log.gz", 1, now)
			candidate := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_2.log", 1, now)
			files := []fsstore.FileInfo{active, peer, candidate}
			pendingPath := filepath.Join(dir, "pending.log.gz")
			nextActivePath := filepath.Join(dir, "app_20260914_1.log")
			guard := DirectoryGuardForPath(dir)
			writer, pendingOwner, laterOwner := NewDirectoryGuardToken(), NewDirectoryGuardToken(), NewDirectoryGuardToken()
			require.NoError(t, guard.TransitionActive(writer, func() (string, error) { return active.Path, nil }))
			guard.ProtectPaths(pendingOwner, pendingPath)

			prepared := make(chan map[string]struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			done := make(chan struct{})
			var (
				panicValue       any
				runCalled        bool
				protectedAtPurge map[string]struct{}
				activeAtPurge    map[string]struct{}
				result           fsstore.PurgeResult
			)
			go func() {
				defer close(done)
				defer func() { panicValue = recover() }()
				_ = guard.RunWithLifecycleLock(func() error {
					guard.RunPurgeWithProtection(files, func(protected map[string]struct{}) map[string]struct{} {
						prepared <- protected
						<-release
						if panicDuringPreparation {
							panic("protection preparation failed")
						}
						// 開始時のactiveから同世代の圧縮peerを保護します。
						// 現在のGuard一覧で再計算すると、rotationで旧activeが失われます。
						extra := make(map[string]struct{})
						if _, protectedSource := protected[active.Path]; protectedSource {
							extra[peer.Path] = struct{}{}
						}
						return extra
					}, func(purge *Purge) {
						runCalled = true
						protectedAtPurge = purge.ProtectedPaths()
						activeAtPurge = purge.ActivePaths()
						result = purge.PurgeByAge(files, 1, now, time.Time{})
					})
					return nil
				})
			}()
			t.Cleanup(func() {
				unblock()
				<-done
			})
			var snapshot map[string]struct{}
			select {
			case snapshot = <-prepared:
			case <-time.After(directoryGuardHangTimeout):
				t.Fatal("purge did not enter protection preparation")
			}
			wantBaseline := map[string]struct{}{active.Path: {}, pendingPath: {}}
			require.Equal(t, wantBaseline, snapshot, "preparation must not include this purge's candidate retention")

			rotationDone := make(chan error, 1)
			go func() {
				var checkErr error
				guard.WithProtectedPaths(func(protected map[string]struct{}) {
					for _, file := range files {
						if _, exists := protected[file.Path]; !exists {
							checkErr = fmt.Errorf("candidate is not retained during preparation: %s", file.Path)
						}
					}
				})
				if checkErr != nil {
					rotationDone <- checkErr
					return
				}
				guard.ProtectPaths(laterOwner, peer.Path)
				rotationDone <- guard.TransitionActive(writer, func() (string, error) { return nextActivePath, nil })
			}()
			select {
			case err := <-rotationDone:
				require.NoError(t, err)
			case <-time.After(directoryGuardHangTimeout):
				unblock()
				<-rotationDone
				t.Fatal("rotation waited for protection preparation to finish")
			}
			require.Equal(t, wantBaseline, snapshot, "rotation must not change the preparation snapshot")
			unblock()
			<-done
			if panicDuringPreparation {
				require.Equal(t, "protection preparation failed", panicValue)
				require.False(t, runCalled, "failed preparation must prevent deletion")
				require.FileExists(t, candidate.Path)
			} else {
				require.Nil(t, panicValue)
				require.True(t, runCalled)
				require.Equal(t, map[string]struct{}{
					active.Path: {}, pendingPath: {}, peer.Path: {},
				}, protectedAtPurge)
				require.Equal(t, map[string]struct{}{active.Path: {}}, activeAtPurge)
				require.Equal(t, []fsstore.FileInfo{active, peer}, result.Remaining)
				require.Equal(t, []fsstore.FileRemovalResult{{File: candidate}}, result.RemovalResults)
				require.NoFileExists(t, candidate.Path)
			}
			require.FileExists(t, active.Path)
			require.FileExists(t, peer.Path)
			guard.WithProtectedPaths(func(protected map[string]struct{}) {
				require.Equal(t, map[string]struct{}{
					nextActivePath: {}, pendingPath: {}, peer.Path: {},
				}, protected, "normal return and preparation panic must release only this purge's retention")
			})
		})
	}
}

func TestDirectoryGuardPurgeAlwaysKeepsProtectedPaths(t *testing.T) {
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	for _, method := range []string{"age", "generation", "directory-size"} {
		t.Run(method, func(t *testing.T) {
			dir := t.TempDir()
			guard := DirectoryGuardForPath(dir)
			files := make([]fsstore.FileInfo, 0, 5)
			for _, name := range []string{
				"active_20260911_1.log", "pending_20260911_1.log", "extra_20260911_1.log",
				"candidate_20260911_1.log", "unknown_20260911_1.log",
			} {
				files = append(files, writeDirectoryGuardPurgeFile(t, dir, name, 1, now.AddDate(0, 0, -3)))
			}
			writer, pending := NewDirectoryGuardToken(), NewDirectoryGuardToken()
			require.NoError(t, guard.TransitionActive(writer, func() (string, error) { return files[0].Path, nil }))
			guard.ProtectPaths(pending, files[1].Path)

			var preparationInput map[string]struct{}
			extra := map[string]struct{}{files[2].Path: {}}
			guard.RunPurgeWithProtection(files[:4], func(protected map[string]struct{}) map[string]struct{} {
				require.Equal(t, map[string]struct{}{files[0].Path: {}, files[1].Path: {}}, protected)
				preparationInput = protected
				// 準備用コピーから基本保護を消しても、返す追加分との合成時に保護を維持します。
				clear(protected)
				protected[files[3].Path] = struct{}{}
				return extra
			}, func(purge *Purge) {
				// 準備時の引数と返却mapを後から変更しても、確定した保護は変わりません。
				clear(preparationInput)
				clear(extra)
				extra[files[3].Path] = struct{}{}
				require.Equal(t, map[string]struct{}{
					files[0].Path: {}, files[1].Path: {}, files[2].Path: {},
				}, purge.ProtectedPaths())
				candidates := purge.DeletionCandidates(files)
				require.Equal(t, []fsstore.FileInfo{files[3]}, candidates)
				candidates[0] = files[0]
				require.Equal(t, []fsstore.FileInfo{files[3]}, purge.DeletionCandidates(files))

				var result fsstore.PurgeResult
				switch method {
				case "age":
					result = purge.PurgeByAge(files, 1, now, time.Time{})
				case "generation":
					result = purge.PurgeGenerationSeries(files, "log", 0)
				case "directory-size":
					result = purge.PurgeByDirectorySize(0)
				}
				require.True(t, result.Ran)
				require.Len(t, result.RemovalResults, 1)
				require.Equal(t, files[3], result.RemovalResults[0].File)
				require.NoError(t, result.RemovalResults[0].Err)
				remaining := append([]fsstore.FileInfo(nil), files[:3]...)
				if method != "directory-size" {
					remaining = append(remaining, files[4])
				}
				require.Equal(t, remaining, result.Remaining)
			})
			for index, file := range files {
				if index == 3 {
					require.NoFileExists(t, file.Path)
				} else {
					require.FileExists(t, file.Path)
				}
			}
		})
	}
}

func TestDirectoryGuardPurgeUsesObservationCopies(t *testing.T) {
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	for _, method := range []string{"age", "generation", "directory-size"} {
		t.Run(method, func(t *testing.T) {
			dir := t.TempDir()
			old := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_1.log", 7, now.AddDate(0, 0, -3))
			newer := writeDirectoryGuardPurgeFile(t, dir, "app_20260914_1.log", 3, now)
			files := []fsstore.FileInfo{old, newer}
			DirectoryGuardForPath(dir).RunPurgeWithProtection(files, noAdditionalPurgeProtection, func(purge *Purge) {
				// 観測元sliceとメソッド入力の日時・sizeを逆転しても開始時の情報を使用します。
				files[0].ModTime, files[1].ModTime = newer.ModTime, old.ModTime
				files[0].Size, files[1].Size = 0, 100
				require.Equal(t, []fsstore.FileInfo{old, newer}, purge.DeletionCandidates(files))
				var result fsstore.PurgeResult
				switch method {
				case "age":
					result = purge.PurgeByAge(files, 1, now, time.Time{})
				case "generation":
					result = purge.PurgeGenerationSeries(files, "log", 1)
				case "directory-size":
					result = purge.PurgeByDirectorySize(3)
				}
				require.True(t, result.Ran)
				require.Equal(t, []fsstore.FileInfo{newer}, result.Remaining)
				require.Len(t, result.RemovalResults, 1)
				require.Equal(t, old, result.RemovalResults[0].File)
				require.NoError(t, result.RemovalResults[0].Err)
			})
			require.NoFileExists(t, old.Path)
			require.FileExists(t, newer.Path)
		})
	}
}

func TestDirectoryGuardPurgeGenerationDoesNotCountProtectedFiles(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	active := writeDirectoryGuardPurgeFile(t, dir, "app_20260914_3.log", 1, now)
	old := writeDirectoryGuardPurgeFile(t, dir, "app_20260914_1.log", 1, now.Add(-time.Hour))
	newer := writeDirectoryGuardPurgeFile(t, dir, "app_20260914_2.log", 1, now)
	files := []fsstore.FileInfo{active, old, newer}
	guard := DirectoryGuardForPath(dir)
	guard.ProtectPaths(NewDirectoryGuardToken(), active.Path)
	guard.RunPurgeWithProtection(files, noAdditionalPurgeProtection, func(purge *Purge) {
		result := purge.PurgeGenerationSeries(files, "log", 2)
		require.False(t, result.Ran)
		require.Empty(t, result.RemovalResults)
		require.Equal(t, files, result.Remaining)
		result = purge.PurgeGenerationSeries(files, "log", 1)
		require.True(t, result.Ran)
		require.Equal(t, []fsstore.FileInfo{active, newer}, result.Remaining)
		require.Len(t, result.RemovalResults, 1)
		require.Equal(t, old, result.RemovalResults[0].File)
	})
}

func TestDirectoryGuardPurgeCapacityCountsProtectedButNotAlreadyRemovedFiles(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	active := writeDirectoryGuardPurgeFile(t, dir, "app_20260914_2.log", 5, now)
	old := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_1.log", 4, now.AddDate(0, 0, -3))
	newer := writeDirectoryGuardPurgeFile(t, dir, "app_20260914_1.log", 3, now)
	files := []fsstore.FileInfo{active, old, newer}
	guard := DirectoryGuardForPath(dir)
	guard.ProtectPaths(NewDirectoryGuardToken(), active.Path)
	guard.RunPurgeWithProtection(files, noAdditionalPurgeProtection, func(purge *Purge) {
		ageResult := purge.PurgeByAge(files, 1, now, time.Time{})
		require.Equal(t, []fsstore.FileInfo{active, newer}, ageResult.Remaining)
		result := purge.PurgeByDirectorySize(8)
		require.False(t, result.Ran, "removed file must no longer contribute to capacity")
		require.Equal(t, ageResult.Remaining, result.Remaining)
		result = purge.PurgeByDirectorySize(7)
		require.True(t, result.Ran, "protected file must still contribute to capacity")
		require.Equal(t, []fsstore.FileInfo{active}, result.Remaining)
		require.Len(t, result.RemovalResults, 1)
		require.Equal(t, newer, result.RemovalResults[0].File)
		result = purge.PurgeByDirectorySize(0)
		require.True(t, result.Ran)
		require.Empty(t, result.RemovalResults, "protected-only excess must not delete protected files")
		require.Equal(t, []fsstore.FileInfo{active}, result.Remaining)
	})
	require.FileExists(t, active.Path)
}

func TestDirectoryGuardPurgeDoesNotRemoveSuccessfulPathAgain(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	file := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_1.log", 1, now.AddDate(0, 0, -3))
	files := []fsstore.FileInfo{file, file}
	DirectoryGuardForPath(dir).RunPurgeWithProtection(files, noAdditionalPurgeProtection, func(purge *Purge) {
		result := purge.PurgeByAge(files, 1, now, time.Time{})
		require.Len(t, result.RemovalResults, 1, "duplicate observations must not cause repeated removal")
		require.Empty(t, result.Remaining)
		require.NoError(t, os.WriteFile(file.Path, []byte("recreated"), 0o600))
		require.Empty(t, purge.DeletionCandidates(files))
		for _, next := range []fsstore.PurgeResult{
			purge.PurgeByAge(files, 1, now, time.Time{}),
			purge.PurgeGenerationSeries(files, "log", 0),
			purge.PurgeByDirectorySize(0),
		} {
			require.Empty(t, next.RemovalResults)
			require.Empty(t, next.Remaining)
		}
		require.FileExists(t, file.Path)
	})
}

func TestDirectoryGuardPurgeKeepsRemovalFailures(t *testing.T) {
	for _, method := range []string{"age", "generation", "directory-size"} {
		t.Run(method, func(t *testing.T) {
			dir := t.TempDir()
			now := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
			bad := fsstore.FileInfo{Path: filepath.Join(dir, "app_20260911_1.log"), Size: 1, ModTime: now.AddDate(0, 0, -3)}
			require.NoError(t, os.Mkdir(bad.Path, 0o700))
			good := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_2.log", 1, bad.ModTime.Add(time.Hour))
			files := []fsstore.FileInfo{bad, good}
			DirectoryGuardForPath(dir).RunPurgeWithProtection(files, noAdditionalPurgeProtection, func(purge *Purge) {
				var result fsstore.PurgeResult
				switch method {
				case "age":
					result = purge.PurgeByAge(files, 1, now, time.Time{})
				case "generation":
					result = purge.PurgeGenerationSeries(files, "log", 0)
				case "directory-size":
					result = purge.PurgeByDirectorySize(0)
				}
				require.True(t, result.Ran)
				require.Len(t, result.RemovalResults, 2)
				require.Equal(t, bad, result.RemovalResults[0].File)
				var notRegular *fsstore.NotRegularFileError
				require.ErrorAs(t, result.RemovalResults[0].Err, &notRegular)
				require.Equal(t, bad.Path, notRegular.Path)
				require.Equal(t, good, result.RemovalResults[1].File)
				require.NoError(t, result.RemovalResults[1].Err)
				require.Error(t, fsstore.JoinFileRemovalFailures(result.RemovalResults))
				require.Equal(t, []fsstore.FileInfo{bad}, result.Remaining)
			})
			require.DirExists(t, bad.Path)
			require.NoFileExists(t, good.Path)
		})
	}
}

func TestDirectoryGuardPurgeZeroValueCannotDeleteUnregisteredFiles(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	file := writeDirectoryGuardPurgeFile(t, dir, "app_20260911_1.log", 1, now.AddDate(0, 0, -3))
	files := []fsstore.FileInfo{file}
	var purge Purge
	require.Empty(t, purge.DeletionCandidates(files))
	for _, result := range []fsstore.PurgeResult{
		purge.PurgeByAge(files, 1, now, time.Time{}),
		purge.PurgeGenerationSeries(files, "log", 0),
	} {
		require.Empty(t, result.RemovalResults)
		require.Equal(t, files, result.Remaining)
	}
	require.Empty(t, purge.PurgeByDirectorySize(0).RemovalResults)
	require.FileExists(t, file.Path)
}

func writeDirectoryGuardPurgeFile(t *testing.T, dir, name string, size int, modified time.Time) fsstore.FileInfo {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o600))
	return fsstore.FileInfo{Path: path, Size: uint64(size), ModTime: modified}
}

func noAdditionalPurgeProtection(map[string]struct{}) map[string]struct{} {
	return nil
}

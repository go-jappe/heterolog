//go:build !windows

package fsstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestEnsureDirectorySyncsCreatedParentsBeforeReturningPartialMkdirFailure(t *testing.T) {
	root := cleanedTempDirForTest(t)
	createdParent := filepath.Join(root, "new")
	directory := filepath.Join(createdParent, "checkpoints")
	t.Cleanup(func() {
		for _, path := range []string{createdParent, directory} {
			if err := os.Chmod(path, 0o700); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Error(err)
			}
		}
		if err := EnsureDirectory(directory, 0o700); err != nil {
			t.Error(err)
		}
	})
	// 最初のdirectoryは作れるが、mode 0000のため次のcomponentを作れません。
	var initialCalls []string
	err := ensureDirectoryWithPolicy(context.Background(), directory, 0, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		initialCalls = append(initialCalls, path)
		return nil
	})
	skipIfChildCreatedDespiteDirectoryPermissions(t, createdParent, directory)
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("partial mkdir error = %v, want permission error", err)
	}
	// 作成を試みた各componentの親は、mkdirが失敗した場合も同じ呼出し内で同期します。
	if want := []string{root, createdParent}; !reflect.DeepEqual(initialCalls, want) {
		t.Fatalf("partial creation parent syncs = %v, want %v", initialCalls, want)
	}
	var calls []string
	if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		calls = append(calls, path)
		return SyncDirectory(path)
	}); err != nil {
		t.Fatal(err)
	}
	// 前回のrootの同期は成功して記録がないため再同期せず、今回作ったchildの親だけを同期します。
	if want := []string{createdParent}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("retry syncs = %v, want %v", calls, want)
	}
}

func TestEnsureDirectoryDoesNotSyncParentWhenMkdirCreatedNothing(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "new")
	t.Cleanup(func() {
		if err := os.Chmod(root, 0o700); err != nil {
			t.Error(err)
		}
	})
	if err := os.Chmod(root, 0o500); err != nil {
		t.Fatal(err)
	}
	var calls []string
	err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		calls = append(calls, path)
		return errors.New("parent sync must not run")
	})
	if _, statErr := os.Stat(directory); statErr == nil {
		t.Skip("current user or filesystem bypasses directory permissions")
	} else if !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("inspect failed mkdir target: %v", statErr)
	}
	// mkdirが何も作成していなければ親は変わっていないため、同期せず、永続化を確認できない
	// とも報告しません。mkdirのエラーだけを返します。
	var syncErr *DirectorySyncError
	if !errors.Is(err, os.ErrPermission) || errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || errors.As(err, &syncErr) {
		t.Fatalf("creation error = %v, want only the mkdir permission error", err)
	}
	if len(calls) != 0 {
		t.Fatalf("parent synced although nothing was created: %v", calls)
	}
	// 同期失敗の記録も残さず、既存の親の準備を妨げません。
	if err := ensureDirectoryWithPolicy(context.Background(), root, 0o700, directorySetupPolicy{}, func(path string) error {
		t.Errorf("parent without a created child was synced: %q", path)
		return nil
	}); err != nil {
		t.Fatalf("existing parent after failed mkdir: %v", err)
	}
}

func TestEnsureDirectoryJoinsMkdirAndParentSyncErrorsAfterCreation(t *testing.T) {
	root := cleanedTempDirForTest(t)
	createdParent := filepath.Join(root, "new")
	directory := filepath.Join(createdParent, "checkpoints")
	t.Cleanup(func() {
		if err := os.Chmod(createdParent, 0o700); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Error(err)
		}
	})
	// 最初のdirectoryは作れるが、mode 0000のため次のcomponentを作れません。
	syncFailure := errors.New("created directory parent sync failed")
	var calls []string
	err := ensureDirectoryWithPolicy(context.Background(), directory, 0, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		calls = append(calls, path)
		return syncFailure
	})
	skipIfChildCreatedDespiteDirectoryPermissions(t, createdParent, directory)
	// 実際に作成したdirectoryの親は同期し、先行したmkdirのエラーを上書きせずに
	// 同期失敗とともに返します。
	var syncErr *DirectorySyncError
	if !errors.Is(err, os.ErrPermission) || !errors.Is(err, syncFailure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || !errors.As(err, &syncErr) {
		t.Fatalf("creation error = %v, want mkdir permission error and unconfirmed parent sync", err)
	}
	if syncErr.Directory != root {
		t.Fatalf("first unconfirmed parent = %q, want %q", syncErr.Directory, root)
	}
	if len(calls) == 0 || calls[0] != root {
		t.Fatalf("parent syncs = %v, want the created directory's parent %q first", calls, root)
	}
}

// skipIfChildCreatedDespiteDirectoryPermissionsは、mode 0000で作られたcreatedParentの権限を戻してから、
// その中にchildが作成されたかを確かめます。作成されていれば、rootなど実行ユーザーやfilesystemが
// directoryの権限を無視しているため、mkdirの失敗を前提とする試験をskipします。
// 判定は戻り値のerrorに依存しないため、errorを失う不具合をskipとして隠しません。
func skipIfChildCreatedDespiteDirectoryPermissions(t *testing.T, createdParent, child string) {
	t.Helper()
	if err := os.Chmod(createdParent, 0o700); err != nil {
		t.Fatalf("restore permissions of the created parent %q: %v", createdParent, err)
	}
	if _, err := os.Lstat(child); err == nil {
		t.Skip("current user or filesystem bypasses directory permissions")
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("inspect child %q after the failed mkdir: %v", child, err)
	}
}

func TestStrictDirectorySetupPreservesExistingUnixModes(t *testing.T) {
	root := resolvedTempDirForTest(t)
	directory := filepath.Join(root, "existing")
	if err := os.Mkdir(directory, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o711); err != nil {
		t.Fatal(err)
	}
	if err := EnsureDirectoryNoSymlinks(directory, 0o750); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(directory)
	if err != nil || info.Mode().Perm() != 0o711 {
		t.Fatalf("strict helper changed existing mode: info=%v, error=%v", info, err)
	}
	if err := EnsureDirectory(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(directory)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("checkpoint helper did not retain its chmod policy: info=%v, error=%v", info, err)
	}
}

func TestDirectorySetupPermissionPolicyIsIndependentOfSymlinkPolicy(t *testing.T) {
	for _, rejectSymlinks := range []bool{false, true} {
		for _, setPermissions := range []bool{false, true} {
			for _, recordedFailure := range []bool{false, true} {
				t.Run(fmt.Sprintf("reject_links=%t/set_permissions=%t/recorded_failure=%t", rejectSymlinks, setPermissions, recordedFailure), func(t *testing.T) {
					directory := resolvedTempDirForTest(t)
					policy := directorySetupPolicy{
						rejectSymlinks:            rejectSymlinks,
						setDestinationPermissions: setPermissions,
					}
					var wantSyncs []string
					if recordedFailure {
						// 子の作成で保存先自身の同期に失敗し、その記録を残します。
						failure := errors.New("destination sync failed")
						child := filepath.Join(directory, "child")
						if err := ensureDirectoryWithPolicy(context.Background(), child, 0o700, policy, func(string) error { return failure }); !errors.Is(err, failure) {
							t.Fatalf("record destination sync failure: %v", err)
						}
						wantSyncs = []string{directory}
					}
					if err := os.Chmod(directory, 0o711); err != nil {
						t.Fatal(err)
					}
					if recordedFailure {
						// 記録した同期が失敗し続ける間は、既存の保存先でも成功扱いにせず権限も変更しません。
						failure := errors.New("destination sync still fails")
						var calls []string
						err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, policy, func(path string) error {
							calls = append(calls, path)
							return failure
						})
						if !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
							t.Fatalf("existing destination with recorded failure error = %v", err)
						}
						if !reflect.DeepEqual(calls, wantSyncs) {
							t.Fatalf("recorded syncs retried = %v, want %v", calls, wantSyncs)
						}
						if info, err := os.Stat(directory); err != nil || info.Mode().Perm() != 0o711 {
							t.Fatalf("failed preparation changed mode: info=%v, error=%v", info, err)
						}
					}
					// 記録のない既存の親は同期しません。記録がある場合は同期できてから成功します。
					var calls []string
					if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, policy, func(path string) error {
						calls = append(calls, path)
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(calls, wantSyncs) {
						t.Fatalf("existing directory syncs = %v, want %v", calls, wantSyncs)
					}
					wantMode := os.FileMode(0o711)
					if setPermissions {
						wantMode = 0o700
					}
					info, err := os.Stat(directory)
					if err != nil {
						t.Fatal(err)
					}
					if info.Mode().Perm() != wantMode {
						t.Fatalf("mode = %o, want %o", info.Mode().Perm(), wantMode)
					}
				})
			}
		}
	}
}

func TestDirectorySetupSetsPermissionsBeforeParentSync(t *testing.T) {
	for _, syncFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync_fails=%t", syncFails), func(t *testing.T) {
			root := cleanedTempDirForTest(t)
			directory := filepath.Join(root, "new")
			syncFailure := errors.New("parent sync failed")
			syncCalls := 0
			err := ensureDirectoryWithPolicy(context.Background(), directory, 0o750, directorySetupPolicy{
				setDestinationPermissions: true,
			}, func(path string) error {
				syncCalls++
				if path != root {
					t.Errorf("synced path = %q, want %q", path, root)
				}
				info, err := os.Stat(directory)
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm() != 0o750 {
					t.Errorf("mode before parent sync = %o, want 750", info.Mode().Perm())
				}
				// 同期後にchmodを実行する形へ変わっていないことも確認します。
				if err := os.Chmod(directory, 0o711); err != nil {
					t.Fatal(err)
				}
				if syncFails {
					return syncFailure
				}
				return nil
			})
			if syncFails {
				if !errors.Is(err, syncFailure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
					t.Fatalf("error = %v, want sync failure with unconfirmed durability", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if syncCalls != 1 {
				t.Fatalf("parent sync calls = %d, want 1", syncCalls)
			}
			info, err := os.Stat(directory)
			if err != nil {
				t.Fatal(err)
			}
			if info.Mode().Perm() != 0o711 {
				t.Fatalf("mode changed after parent sync: %o, want 711", info.Mode().Perm())
			}
		})
	}
}

func TestDirectorySetupPoliciesKeepExistingSymlinkBehavior(t *testing.T) {
	// 試験が作るリンクだけがpath上のリンクになるよう、一時ディレクトリの上位のリンクを解決します。
	root := resolvedTempDirForTest(t)
	realDirectory := filepath.Join(root, "real")
	linkedDirectory := filepath.Join(root, "linked")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realDirectory, linkedDirectory); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{linkedDirectory, filepath.Join(linkedDirectory, "new")} {
		var obstruction *DirectoryObstructionError
		if err := EnsureDirectoryNoSymlinks(directory, 0o700); !errors.As(err, &obstruction) || obstruction.Path != linkedDirectory {
			t.Fatalf("strict helper error for %q = %v, want rejection of symlink %q", directory, err, linkedDirectory)
		}
	}
	if _, err := os.Stat(filepath.Join(realDirectory, "new")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("strict helper mutated symlink target before validation: %v", err)
	}
	for _, directory := range []string{linkedDirectory, filepath.Join(linkedDirectory, "new")} {
		if err := EnsureDirectory(directory, 0o700); err != nil {
			t.Fatalf("checkpoint helper changed existing symlink policy: %v", err)
		}
	}
}

func TestManagedDirectorySetupRestrictsOnlyManagedExistingDirectories(t *testing.T) {
	base := resolvedTempDirForTest(t)
	root := filepath.Join(base, "managed")
	state := filepath.Join(root, "state")
	existing := filepath.Join(state, "existing")
	if err := os.MkdirAll(existing, 0o700); err != nil {
		t.Fatal(err)
	}
	// 以前の版が広い権限で作った管理下のディレクトリと、管理外の上位を用意します。
	fixtures := []struct {
		path string
		mode os.FileMode
	}{
		{path: base, mode: 0o755},
		{path: root, mode: 0o755},
		{path: state, mode: 0o751},
		{path: existing, mode: 0o750 | os.ModeSetgid},
	}
	for _, fixture := range fixtures {
		if err := os.Chmod(fixture.path, fixture.mode); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(existing)
	if err != nil {
		t.Fatal(err)
	}
	setgid := info.Mode()&os.ModeSetgid != 0
	directory := filepath.Join(existing, "new")
	if err := EnsureManagedDirectoryNoSymlinks(root, directory, 0o700, 0o755); err != nil {
		t.Fatal(err)
	}
	// 管理範囲内の既存ディレクトリはgroup・otherの権限だけを外し、管理外の上位は変更しません。
	for _, want := range []struct {
		path string
		mode os.FileMode
	}{
		{path: base, mode: 0o755},
		{path: root, mode: 0o700},
		{path: state, mode: 0o700},
		{path: existing, mode: 0o700},
	} {
		info, err := os.Stat(want.path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != want.mode {
			t.Fatalf("mode of %q = %o, want %o", want.path, info.Mode().Perm(), want.mode)
		}
	}
	if info, err := os.Stat(existing); err != nil || (info.Mode()&os.ModeSetgid != 0) != setgid {
		t.Fatalf("restriction changed special mode bits: info=%v, error=%v", info, err)
	}
	// 新規作成はmodeとprocess umaskに従い、modeを超える権限を持ちません。
	if info, err := os.Stat(directory); err != nil || info.Mode().Perm()&^0o700 != 0 {
		t.Fatalf("created directory mode: info=%v, error=%v", info, err)
	}
}

func TestManagedDirectorySetupDoesNotLoosenExistingRestrictions(t *testing.T) {
	root := resolvedTempDirForTest(t)
	directory := filepath.Join(root, "read-only")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Error(err)
		}
	})
	if err := os.Chmod(directory, 0o555); err != nil {
		t.Fatal(err)
	}
	if err := EnsureManagedDirectoryNoSymlinks(root, directory, 0o700, 0o755); err != nil {
		t.Fatal(err)
	}
	// 所有者の書込禁止など、mode内の既存の制限は維持します。
	if info, err := os.Stat(directory); err != nil || info.Mode().Perm() != 0o500 {
		t.Fatalf("restricted mode: info=%v, error=%v, want 500", info, err)
	}
}

func TestManagedDirectorySetupCreatesAncestorsWithAncestorMode(t *testing.T) {
	base := resolvedTempDirForTest(t)
	ancestor := filepath.Join(base, "shared")
	root := filepath.Join(ancestor, "managed")
	directory := filepath.Join(root, "child")
	if err := EnsureManagedDirectoryNoSymlinks(root, directory, 0o700, 0o755); err != nil {
		t.Fatal(err)
	}
	// 管理範囲より上位は他のStoreと共有し得るためancestorMode、管理下はmodeで作成し、
	// どちらもprocess umaskを適用します。
	for _, want := range []struct {
		path string
		mode os.FileMode
	}{
		{path: ancestor, mode: createdDirectoryModeForTest(t, 0o755)},
		{path: root, mode: createdDirectoryModeForTest(t, 0o700)},
		{path: directory, mode: createdDirectoryModeForTest(t, 0o700)},
	} {
		info, err := os.Stat(want.path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != want.mode {
			t.Fatalf("mode of %q = %o, want %o", want.path, info.Mode().Perm(), want.mode)
		}
	}
}

// createdDirectoryModeForTestは、process umaskを変更せずに、modeで新規作成した
// directoryの実際の権限を観測します。並行する試験のumaskにも影響しません。
func createdDirectoryModeForTest(t *testing.T, mode os.FileMode) os.FileMode {
	t.Helper()
	control := filepath.Join(t.TempDir(), "creation-mode-control")
	if err := os.Mkdir(control, mode); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(control)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}

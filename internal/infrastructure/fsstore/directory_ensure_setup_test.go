package fsstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestEnsureDirectorySyncsOnlyCreatedParents(t *testing.T) {
	for _, depth := range []int{0, 1, 3} {
		t.Run(string(rune('0'+depth)), func(t *testing.T) {
			root := cleanedTempDirForTest(t)
			directory := root
			var want []string
			for range depth {
				want = append(want, directory)
				directory = filepath.Join(directory, "new")
			}
			var calls []string
			if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
				calls = append(calls, path)
				return SyncDirectory(path)
			}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("synced parents = %v, want %v", calls, want)
			}
			// 同期に成功した親は記録を残さず、既存の保存先の準備では同期しません。
			if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
				t.Errorf("completed directory without a recorded failure resynced parent %q", path)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEnsureDirectoryRetainsFailedOrPanickedSyncUntilRetrySucceeds(t *testing.T) {
	for _, panicSync := range []bool{false, true} {
		name := "error"
		if panicSync {
			name = "panic"
		}
		t.Run(name, func(t *testing.T) {
			root := cleanedTempDirForTest(t)
			directory := filepath.Join(root, "state", "target", "checkpoints")
			failure := errors.New("parent sync failed")
			var firstCalls []string
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						if !panicSync || recovered != failure {
							t.Fatalf("unexpected panic: %v", recovered)
						}
					} else if panicSync {
						t.Error("sync panic did not reach caller")
					}
				}()
				err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
					firstCalls = append(firstCalls, path)
					if len(firstCalls) == 2 {
						if panicSync {
							panic(failure)
						}
						return failure
					}
					return SyncDirectory(path)
				})
				if !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
					t.Fatalf("initial ensure error = %v, want unconfirmed durability and cause", err)
				}
			}()
			wantFirst := []string{root, filepath.Join(root, "state")}
			// 同期できなかった親を記録します。panicでは、同期結果を反映できなかった親も含めます。
			wantRetry := []string{filepath.Join(root, "state")}
			if !panicSync {
				// エラー時も同じ呼出し内の後続parentを同期します。panicはそのまま伝播します。
				wantFirst = append(wantFirst, filepath.Join(root, "state", "target"))
			} else {
				wantRetry = append(wantRetry, filepath.Join(root, "state", "target"))
			}
			if !reflect.DeepEqual(firstCalls, wantFirst) {
				t.Fatalf("initial syncs = %v, want %v", firstCalls, wantFirst)
			}
			// 保存先は作成済みでも、記録した親を同期できるまでは成功を返しません。
			// 別Storeからの初期化と同じく、同じprocessの次の準備で先に再試行します。
			for attempt := 0; attempt < 2; attempt++ {
				var retryCalls []string
				err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
					retryCalls = append(retryCalls, path)
					return failure
				})
				var syncErr *DirectorySyncError
				if !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || !errors.As(err, &syncErr) {
					t.Fatalf("retry %d error = %v, want unconfirmed parent sync", attempt, err)
				}
				if !reflect.DeepEqual(retryCalls, wantRetry) {
					t.Fatalf("retry %d syncs = %v, want %v", attempt, retryCalls, wantRetry)
				}
			}
			var retryCalls []string
			if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
				retryCalls = append(retryCalls, path)
				return SyncDirectory(path)
			}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(retryCalls, wantRetry) {
				t.Fatalf("successful retry syncs = %v, want %v", retryCalls, wantRetry)
			}
			// 同期できた記録は解除し、以後の準備では既存の親を同期しません。
			if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
				t.Errorf("recovered parent sync was retried again: %q", path)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEnsureDirectoryReturnsAllParentSyncFailures(t *testing.T) {
	root := cleanedTempDirForTest(t)
	directory := filepath.Join(root, "state", "target")
	rootFailure := errors.New("root sync failed")
	stateFailure := errors.New("state sync failed")
	var calls []string
	err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		calls = append(calls, path)
		if path == root {
			return rootFailure
		}
		return stateFailure
	})
	if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || !errors.Is(err, rootFailure) || !errors.Is(err, stateFailure) {
		t.Fatalf("creation error = %v, want unconfirmed durability and both sync failures", err)
	}
	if want := []string{root, filepath.Join(root, "state")}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("parent syncs after errors = %v, want %v", calls, want)
	}
	// 失敗した親はすべて記録し、既存の保存先でも上位から順に再試行します。
	// 一部だけ同期できた場合は、その記録だけを解除します。
	var retried []string
	err = ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		retried = append(retried, path)
		if path == root {
			return nil
		}
		return stateFailure
	})
	if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) || errors.Is(err, rootFailure) || !errors.Is(err, stateFailure) {
		t.Fatalf("partial retry error = %v, want only the remaining unconfirmed parent", err)
	}
	if want := []string{root, filepath.Join(root, "state")}; !reflect.DeepEqual(retried, want) {
		t.Fatalf("parent syncs retried = %v, want %v", retried, want)
	}
	retried = nil
	if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		retried = append(retried, path)
		return SyncDirectory(path)
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{filepath.Join(root, "state")}; !reflect.DeepEqual(retried, want) {
		t.Fatalf("remaining parent syncs retried = %v, want %v", retried, want)
	}
	if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		t.Errorf("existing directory retried recovered parent sync: %q", path)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureDirectoryDoesNotWaitForUnrelatedParentSync(t *testing.T) {
	firstDirectory := filepath.Join(t.TempDir(), "first")
	secondDirectory := filepath.Join(t.TempDir(), "second")
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	var secondDone chan error
	secondFinished := false
	failure := errors.New("first caller sync failed")
	go func() {
		firstDone <- ensureDirectoryWithPolicy(context.Background(), firstDirectory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(string) error {
			close(entered)
			<-release
			return failure
		})
	}()
	defer func() {
		close(release)
		if err := <-firstDone; !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
			t.Errorf("first error = %v, want unconfirmed durability and cause", err)
		}
		if secondDone != nil && !secondFinished {
			<-secondDone
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first caller did not reach directory sync")
	}
	secondDone = make(chan error, 1)
	var secondCalls []string
	go func() {
		secondDone <- ensureDirectoryWithPolicy(context.Background(), secondDirectory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
			secondCalls = append(secondCalls, path)
			return SyncDirectory(path)
		})
	}()
	select {
	case err := <-secondDone:
		secondFinished = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated directory waited for the first caller's parent sync")
	}
	if want := []string{filepath.Dir(secondDirectory)}; !reflect.DeepEqual(secondCalls, want) {
		t.Fatalf("unrelated caller syncs = %v, want %v", secondCalls, want)
	}
}

func TestEnsureDirectoryDoesNotInheritUnrelatedFailure(t *testing.T) {
	firstDirectory := filepath.Join(t.TempDir(), "first")
	secondDirectory := filepath.Join(t.TempDir(), "second")
	failure := errors.New("unrelated parent sync failed")
	if err := ensureDirectoryWithPolicy(context.Background(), firstDirectory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(string) error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("first ensure error = %v", err)
	}
	var calls []string
	if err := ensureDirectoryWithPolicy(context.Background(), secondDirectory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
		calls = append(calls, path)
		return SyncDirectory(path)
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{filepath.Dir(secondDirectory)}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("unrelated caller syncs = %v, want %v", calls, want)
	}
}

func TestManagedDirectorySetupRejectsDirectoryOutsideManagedRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "managed")
	directory := filepath.Join(base, "outside", "child")
	if err := EnsureManagedDirectoryNoSymlinks(root, directory, 0o700, 0o755); err == nil {
		t.Fatal("managed setup accepted a directory outside its root")
	}
	if err := EnsureManagedDirectoryNoSymlinks(" ", directory, 0o700, 0o755); err == nil {
		t.Fatal("managed setup accepted an empty root")
	}
	// 範囲外の指定は検査・作成の前に拒否します。
	for _, path := range []string{root, filepath.Dir(directory)} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rejected managed setup created %q: %v", path, err)
		}
	}
}

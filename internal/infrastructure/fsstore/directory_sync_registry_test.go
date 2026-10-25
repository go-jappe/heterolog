package fsstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// directorySyncRecordsForTestは、root自身と配下にある同期記録の写しをkeyごとに返します。
func directorySyncRecordsForTest(t *testing.T, root string) map[string]directorySyncEntry {
	t.Helper()
	rootKey := pathidentity.ComparisonKey(root)
	directorySyncs.mu.Lock()
	defer directorySyncs.mu.Unlock()
	records := make(map[string]directorySyncEntry)
	for key, entry := range directorySyncs.entries {
		if key == rootKey || strings.HasPrefix(key, rootKey+string(filepath.Separator)) {
			records[key] = *entry
		}
	}
	return records
}

func newDirectorySyncRegistryForTest() *directorySyncRegistry {
	return &directorySyncRegistry{entries: make(map[string]*directorySyncEntry)}
}

// resolvedTempDirForTestは、リンクを拒否する準備で使えるよう、macOSの/varなど
// 一時ディレクトリの上位のリンクを解決したパスを返します。
func resolvedTempDirForTest(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

// cleanedTempDirForTestは、保存先のパスを正規化して扱う処理と比べられるよう、TMPDIRの
// 「//」などを残すt.TempDirを正規化したパスを返します。リンクは解決しません。
func cleanedTempDirForTest(t *testing.T) string {
	t.Helper()
	return filepath.Clean(t.TempDir())
}

func TestEnsureDirectoryKeepsOneFailureRecordPerParentAcrossConcurrentCallers(t *testing.T) {
	root := resolvedTempDirForTest(t)
	failure := errors.New("parent sync failed")
	failing := func(string) error { return failure }
	policy := directorySetupPolicy{rejectSymlinks: true}
	if err := ensureDirectoryWithPolicy(context.Background(), filepath.Join(root, "active"), 0o700, policy, failing); !errors.Is(err, failure) {
		t.Fatalf("initial ensure error = %v", err)
	}
	const callers = 8
	results := make(chan error, callers)
	next := filepath.Join(root, "next")
	for range callers {
		go func() { results <- ensureDirectoryWithPolicy(context.Background(), next, 0o700, policy, failing) }()
	}
	for range callers {
		if err := <-results; !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
			t.Fatalf("dependent ensure error = %v, want recorded parent sync failure", err)
		}
	}
	// 同期できない親の下へ新しいdirectoryを作らず、同じ親の失敗は一件の記録にまとめます。
	if _, err := os.Lstat(next); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dependent directory was created while its parent sync failed: %v", err)
	}
	records := directorySyncRecordsForTest(t, root)
	record, found := records[pathidentity.ComparisonKey(root)]
	if len(records) != 1 || !found || !record.failed || record.creators != 0 || record.path != root {
		t.Fatalf("sync records = %+v, want one failed record for %q", records, root)
	}
	if err := ensureDirectoryWithPolicy(context.Background(), next, 0o700, policy, SyncDirectory); err != nil {
		t.Fatal(err)
	}
	// 同期できた記録は解除し、registryを大きくしません。
	if records := directorySyncRecordsForTest(t, root); len(records) != 0 {
		t.Fatalf("records retained after recovery: %+v", records)
	}
}

func TestEnsureDirectoryReleasesRecordForRemovedParent(t *testing.T) {
	root := resolvedTempDirForTest(t)
	parent := filepath.Join(root, "partition")
	directory := filepath.Join(parent, "child")
	failure := errors.New("partition sync failed")
	if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{rejectSymlinks: true}, func(path string) error {
		if path == parent {
			return failure
		}
		return SyncDirectory(path)
	}); !errors.Is(err, failure) {
		t.Fatalf("initial ensure error = %v", err)
	}
	if err := os.RemoveAll(parent); err != nil {
		t.Fatal(err)
	}
	// 親が既に存在しなければ、確認すべき作成結果も残っていません。記録を解除して作り直し、
	// 作り直したdirectoryの親を同期します。
	var calls []string
	if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{rejectSymlinks: true}, func(path string) error {
		calls = append(calls, path)
		return SyncDirectory(path)
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{parent, root, parent}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("syncs after parent removal = %v, want %v", calls, want)
	}
	if records := directorySyncRecordsForTest(t, root); len(records) != 0 {
		t.Fatalf("records retained after re-creation: %+v", records)
	}
}

func TestEnsureDirectorySyncsParentCreatedByConcurrentCall(t *testing.T) {
	root := cleanedTempDirForTest(t)
	first := filepath.Join(root, "first")
	second := filepath.Join(first, "second")
	policy := directorySetupPolicy{setDestinationPermissions: true}
	entered := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	failure := errors.New("first caller sync failed")
	go func() {
		firstDone <- ensureDirectoryWithPolicy(context.Background(), first, 0o700, policy, func(string) error {
			close(entered)
			<-release
			return failure
		})
	}()
	released := false
	defer func() {
		if !released {
			close(release)
			<-firstDone
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first caller did not reach directory sync")
	}
	// firstを作った呼び出しのroot同期は未完了です。作成済みのfirstを使う別の呼び出しは、
	// 先行する呼び出しを待たず、自身でもrootを同期してから成功を返します。
	var calls []string
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- ensureDirectoryWithPolicy(context.Background(), second, 0o700, policy, func(path string) error {
			calls = append(calls, path)
			return SyncDirectory(path)
		})
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dependent caller waited for the first caller's parent sync")
	}
	if want := []string{root, first}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("dependent caller syncs = %v, want %v", calls, want)
	}
	released = true
	close(release)
	if err := <-firstDone; !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("first error = %v, want unconfirmed durability and cause", err)
	}
	// 後から記録された失敗は、それより前に始まった同期の成功では解除しません。
	var retried []string
	if err := ensureDirectoryWithPolicy(context.Background(), second, 0o700, policy, func(path string) error {
		retried = append(retried, path)
		return SyncDirectory(path)
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{root}; !reflect.DeepEqual(retried, want) {
		t.Fatalf("syncs after concurrent failure = %v, want %v", retried, want)
	}
	if records := directorySyncRecordsForTest(t, root); len(records) != 0 {
		t.Fatalf("records retained after recovery: %+v", records)
	}
}

func TestDirectorySyncRegistryDoesNotClearFailureRecordedDuringOlderSync(t *testing.T) {
	registry := newDirectorySyncRegistryForTest()
	parent := directoryPathKeys(filepath.Join(t.TempDir(), "parent"))[0]
	failure := errors.New("parent sync failed")
	failing := func(string) error { return failure }
	if err := registry.beginCreation(parent); err != nil {
		t.Fatal(err)
	}
	if err := registry.syncCreated([]directoryPathKey{parent}, []directoryPathKey{parent}, true, failing); !errors.Is(err, failure) {
		t.Fatalf("initial sync error = %v", err)
	}
	// 再試行の同期を始める前の記録を読み、その同期中に別の作成の失敗が記録されます。
	older, _ := registry.failedTargets([]directoryPathKey{parent})
	if len(older) != 1 {
		t.Fatalf("failed targets = %+v, want one", older)
	}
	if err := registry.beginCreation(parent); err != nil {
		t.Fatal(err)
	}
	if err := registry.syncCreated([]directoryPathKey{parent}, []directoryPathKey{parent}, true, failing); !errors.Is(err, failure) {
		t.Fatalf("concurrent sync error = %v", err)
	}
	registry.settle(older[0], nil)
	if entry := registry.entries[parent.key]; entry == nil || !entry.failed || registry.failed != 1 {
		t.Fatalf("older sync success cleared a newer failure: entry=%+v failed=%d", entry, registry.failed)
	}
	if err := registry.retryFailed([]directoryPathKey{parent}, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(registry.entries) != 0 || registry.failed != 0 {
		t.Fatalf("records after successful retry = %+v, failed=%d", registry.entries, registry.failed)
	}
}

func TestDirectorySyncRegistryRefusesNewParentsAtCapacity(t *testing.T) {
	registry := newDirectorySyncRegistryForTest()
	root := t.TempDir()
	failure := errors.New("parent sync failed")
	failing := func(string) error { return failure }
	for index := range maxFailedDirectorySyncs {
		parent := directoryPathKeys(filepath.Join(root, "failed-"+strconv.Itoa(index)))[0]
		if err := registry.beginCreation(parent); err != nil {
			t.Fatal(err)
		}
		if err := registry.syncCreated([]directoryPathKey{parent}, []directoryPathKey{parent}, true, failing); !errors.Is(err, failure) {
			t.Fatalf("sync %d error = %v", index, err)
		}
	}
	chain := directoryPathKeys(filepath.Join(root, "next", "child"))
	next := chain[1]
	// 上限に達すると、無関係な記録も再試行します。同期できないままなら記録を捨てず、
	// 記録のない親への新しい作成を拒否します。
	attempts := 0
	if err := registry.retryFailed(chain, func(string) error {
		attempts++
		return failure
	}); err != nil {
		t.Fatalf("unrelated failures were returned to the caller: %v", err)
	}
	if attempts != maxFailedDirectorySyncs || registry.failed != maxFailedDirectorySyncs {
		t.Fatalf("retried %d records, %d remain failed; want %d", attempts, registry.failed, maxFailedDirectorySyncs)
	}
	if err := registry.beginCreation(next); !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("creation at capacity error = %v, want unconfirmed durability", err)
	}
	// 再試行で同期できた記録を解除すると、作成を再開します。
	attempts = 0
	if err := registry.retryFailed(chain, func(string) error {
		attempts++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if attempts != maxFailedDirectorySyncs || registry.failed != 0 || len(registry.entries) != 0 {
		t.Fatalf("recovery retried %d records, failed=%d entries=%d", attempts, registry.failed, len(registry.entries))
	}
	if err := registry.beginCreation(next); err != nil {
		t.Fatal(err)
	}
	if err := registry.syncCreated(chain, []directoryPathKey{next}, true, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(registry.entries) != 0 {
		t.Fatalf("records after confirmed creation = %+v", registry.entries)
	}
}

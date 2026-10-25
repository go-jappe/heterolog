package retryafter

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// resolvedTempDirForTestは、symlinkを解決したtemporary directoryを返します。
// productionはspool rootを実体のpathへ解決してから渡すため、試験も同じ表記を使います。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	return dir
}

// testNowは試験の基準時刻です。markerは保存時にmonotonic clockを除いたUTCへ揃えます。
var testNow = time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)

func newTestLogSpoolStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewLogSpoolStore(resolvedTempDirForTest(t), "opensearch")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func requireMarkerError(t *testing.T, err error, operation MarkerOperation, target error) *MarkerError {
	t.Helper()
	var marker *MarkerError
	if !errors.As(err, &marker) || marker.Operation != operation {
		t.Fatalf("error = %v, want %s MarkerError", err, operation)
	}
	if target != nil && !errors.Is(err, target) {
		t.Fatalf("error = %v, want cause %v", err, target)
	}
	return marker
}

func directoryNames(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestStoreSavesAndLoadsStateAtDestinationPath(t *testing.T) {
	t.Parallel()
	root := resolvedTempDirForTest(t)
	store, err := NewLogSpoolStore(root, "opensearch")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, DirectoryName, "opensearch.json"); store.Path() != want {
		t.Fatalf("Path() = %q, want %q", store.Path(), want)
	}
	state := State{NotBefore: testNow.Add(time.Minute), AppliedThrough: testNow.Add(2 * time.Minute)}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	if names := directoryNames(t, filepath.Join(root, DirectoryName)); len(names) != 1 || names[0] != "opensearch.json" {
		t.Fatalf("marker directory entries = %v, want only the marker", names)
	}
	body, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(body, &document); err != nil {
		t.Fatalf("marker is not JSON: %v: %s", err, body)
	}
	if document["version"] != float64(1) || document["destination_id"] != "opensearch" {
		t.Fatalf("marker document = %s, want version 1 for opensearch", body)
	}

	// 別のStoreは再起動後のrunnerと同じく、保存済みの内容だけを読みます。
	restarted, err := NewLogSpoolStore(root, "opensearch")
	if err != nil {
		t.Fatal(err)
	}
	loaded, clamp, err := restarted.Load(testNow, time.Hour)
	if err != nil || clamp != nil {
		t.Fatalf("Load() clamp/error = %+v/%v, want neither", clamp, err)
	}
	if !loaded.NotBefore.Equal(state.NotBefore) || !loaded.AppliedThrough.Equal(state.AppliedThrough) {
		t.Fatalf("Load() = %+v, want %+v", loaded, state)
	}
}

func TestStoreLoadWithoutMarkerReturnsZeroState(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	state, clamp, err := store.Load(testNow, time.Hour)
	if err != nil || clamp != nil || state != (State{}) {
		t.Fatalf("Load() = %+v/%+v/%v, want zero state", state, clamp, err)
	}
	if _, err := os.Lstat(filepath.Dir(store.Path())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load() created the marker directory: %v", err)
	}
}

// 期限と反映済みの保存期限をすべて過ぎたmarkerは、読み込み時に削除して送信を制限しない。
func TestStoreLoadRemovesExpiredMarker(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	if err := store.Save(State{NotBefore: testNow.Add(-time.Minute), AppliedThrough: testNow}); err != nil {
		t.Fatal(err)
	}
	state, clamp, err := store.Load(testNow, time.Hour)
	if err != nil || clamp != nil || state != (State{}) {
		t.Fatalf("Load() = %+v/%+v/%v, want zero state for an expired marker", state, clamp, err)
	}
	if _, err := os.Lstat(store.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired marker still exists: %v", err)
	}
	if err := store.Remove(); err != nil {
		t.Fatalf("Remove() without a marker = %v, want nil", err)
	}
}

// 期限を過ぎても、上限まで短縮した保存期限がまだ先なら、その記録を残して待機を延ばさない。
func TestStoreLoadKeepsMarkerWhileAppliedThroughIsPending(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	saved := State{NotBefore: testNow.Add(-time.Minute), AppliedThrough: testNow.Add(24 * time.Hour)}
	if err := store.Save(saved); err != nil {
		t.Fatal(err)
	}
	state, clamp, err := store.Load(testNow, time.Hour)
	if err != nil || clamp != nil || !state.NotBefore.Equal(saved.NotBefore) || !state.AppliedThrough.Equal(saved.AppliedThrough) {
		t.Fatalf("Load() = %+v/%+v/%v, want %+v", state, clamp, err, saved)
	}
	if _, err := os.Lstat(store.Path()); err != nil {
		t.Fatalf("marker with a pending applied deadline was removed: %v", err)
	}
}

// 上限より先の期限は読み込んだ時点から上限までに短縮して保存し直すため、次の読み込みでは
// 短縮後の期限をそのまま使い、待機を延ばさない。
func TestStoreLoadPersistsClampedDeadline(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	far := testNow.Add(10 * time.Hour)
	if err := store.Save(State{NotBefore: far}); err != nil {
		t.Fatal(err)
	}
	state, clamp, err := store.Load(testNow, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	want := testNow.Add(time.Hour)
	if !state.NotBefore.Equal(want) || !state.AppliedThrough.Equal(far) {
		t.Fatalf("clamped state = %+v, want NotBefore %s and AppliedThrough %s", state, want, far)
	}
	if clamp == nil || clamp.Requested != 10*time.Hour || clamp.MaxRetryAfter != time.Hour || !clamp.NotBefore.Equal(want) || !clamp.Stored {
		t.Fatalf("clamp = %+v, want stored 10h deadline capped at 1h", clamp)
	}

	restarted, err := NewLogSpoolStore(filepath.Dir(filepath.Dir(store.Path())), "opensearch")
	if err != nil {
		t.Fatal(err)
	}
	later := testNow.Add(30 * time.Minute)
	again, clamp, err := restarted.Load(later, time.Hour)
	if err != nil || clamp != nil {
		t.Fatalf("second Load() clamp/error = %+v/%v, want neither", clamp, err)
	}
	if !again.NotBefore.Equal(want) {
		t.Fatalf("second Load() NotBefore = %s, want the persisted clamp %s, not %s", again.NotBefore, want, later.Add(time.Hour))
	}
}

// 読めない・壊れたmarkerは、markerがないものとして扱い、fileを残して次の保存で置き換える。
func TestStoreLoadTreatsCorruptMarkerAsAbsent(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name string
		body string
	}{
		{name: "malformed json", body: `{"version":1,`},
		{name: "unsupported version", body: `{"version":2,"destination_id":"opensearch","not_before":"2026-10-22T13:00:00Z"}`},
		{name: "other destination", body: `{"version":1,"destination_id":"loki","not_before":"2026-10-22T13:00:00Z"}`},
		{name: "missing deadline", body: `{"version":1,"destination_id":"opensearch"}`},
		{name: "oversized", body: `{"version":1,"destination_id":"opensearch","pad":"` + strings.Repeat("x", maxMarkerFileSize) + `"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := newTestLogSpoolStore(t)
			if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(store.Path(), []byte(tt.body), 0o600); err != nil {
				t.Fatal(err)
			}
			state, clamp, err := store.Load(testNow, time.Hour)
			requireMarkerError(t, err, MarkerOperationLoad, ErrInvalidMarker)
			if state != (State{}) || clamp != nil {
				t.Fatalf("Load() = %+v/%+v, want zero state for a corrupt marker", state, clamp)
			}
			if body, readErr := os.ReadFile(store.Path()); readErr != nil || string(body) != tt.body {
				t.Fatalf("corrupt marker changed before replacement: %q/%v", body, readErr)
			}

			replacement := State{NotBefore: testNow.Add(time.Minute)}
			if err := store.Save(replacement); err != nil {
				t.Fatal(err)
			}
			state, _, err = store.Load(testNow, time.Hour)
			if err != nil || !state.NotBefore.Equal(replacement.NotBefore) {
				t.Fatalf("Load() after replacement = %+v/%v, want %+v", state, err, replacement)
			}
		})
	}
}

// markerの位置にあるdirectoryは辿らず、壊れたmarkerとして扱う。
func TestStoreLoadRejectsNonRegularMarker(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	if err := os.MkdirAll(store.Path(), 0o700); err != nil {
		t.Fatal(err)
	}
	state, _, err := store.Load(testNow, time.Hour)
	requireMarkerError(t, err, MarkerOperationLoad, ErrInvalidMarker)
	if state != (State{}) {
		t.Fatalf("Load() = %+v, want zero state", state)
	}
	requireMarkerError(t, store.Remove(), MarkerOperationRemove, ErrInvalidMarker)
	if info, err := os.Lstat(store.Path()); err != nil || !info.IsDir() {
		t.Fatalf("Remove() changed a non-regular marker entry: %v", err)
	}
}

// 一時fileの同期に失敗した保存は既存のmarkerを置き換えず、一時fileも残さない。
func TestStoreSaveFileSyncFailureKeepsPreviousMarker(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	previous := State{NotBefore: testNow.Add(time.Minute)}
	if err := store.Save(previous); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected file sync failure")
	store.syncFile = func(*os.File) error { return injected }

	err := store.Save(State{NotBefore: testNow.Add(time.Hour)})
	requireMarkerError(t, err, MarkerOperationSave, injected)
	if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("Save() error = %v, want ErrDurabilityUnconfirmed", err)
	}
	if names := directoryNames(t, filepath.Dir(store.Path())); len(names) != 1 {
		t.Fatalf("marker directory entries = %v, want no temporary file", names)
	}
	state, _, err := store.Load(testNow, time.Hour)
	if err != nil || !state.NotBefore.Equal(previous.NotBefore) {
		t.Fatalf("Load() = %+v/%v, want previous marker %+v", state, err, previous)
	}
}

// 置き換え後のdirectory同期に失敗した場合は、公開済みのmarkerを残して永続化未確認を返す。
func TestStoreSaveDirectorySyncFailureReportsPublishedMarker(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	injected := errors.New("injected directory sync failure")
	store.syncDirectory = func(string) error { return injected }

	saved := State{NotBefore: testNow.Add(time.Minute)}
	err := store.Save(saved)
	requireMarkerError(t, err, MarkerOperationSave, injected)
	if !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
		t.Fatalf("Save() error = %v, want ErrDurabilityUnconfirmed", err)
	}
	state, _, err := store.Load(testNow, time.Hour)
	if err != nil || !state.NotBefore.Equal(saved.NotBefore) {
		t.Fatalf("Load() = %+v/%v, want published marker %+v", state, err, saved)
	}
}

// markerのdirectoryを作成した親の同期を確認できない間は、既存のspool directoryと同じく
// 保存を失敗にし、同期できた後の保存で記録を解除する。
func TestStoreSaveRespectsUnconfirmedParentDirectorySync(t *testing.T) {
	t.Parallel()
	root := resolvedTempDirForTest(t)
	store, err := NewLogSpoolStore(root, "loki")
	if err != nil {
		t.Fatal(err)
	}
	restore := testutil.DenyDirectorySync(t, root)
	err = store.Save(State{NotBefore: testNow.Add(time.Minute)})
	restore()
	requireMarkerError(t, err, MarkerOperationSave, pkgfsstore.ErrDurabilityUnconfirmed)
	if _, statErr := os.Lstat(store.Path()); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("marker was published while its directory entry was unconfirmed: %v", statErr)
	}

	if err := store.Save(State{NotBefore: testNow.Add(time.Minute)}); err != nil {
		t.Fatalf("Save() after the parent sync recovered = %v", err)
	}
}

func TestNewObjectStorageStoreRequiresExistingDestinationDirectory(t *testing.T) {
	t.Parallel()
	destination := filepath.Join(resolvedTempDirForTest(t), "s3-a")
	store, err := NewObjectStorageStore(destination, "s3-a")
	if err != nil {
		t.Fatal(err)
	}
	err = store.Save(State{NotBefore: testNow.Add(time.Minute)})
	requireMarkerError(t, err, MarkerOperationSave, ErrMarkerOwnerDirectory)
	if _, statErr := os.Lstat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Save() created the destination directory: %v", statErr)
	}

	if err := os.Mkdir(destination, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(State{NotBefore: testNow.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(destination, DirectoryName, "s3-a.json"); store.Path() != want {
		t.Fatalf("Path() = %q, want %q", store.Path(), want)
	}
}

func TestValidateDestinationIDAcceptsOnlyPortableLowercaseNames(t *testing.T) {
	t.Parallel()
	for _, id := range []string{
		"opensearch", "loki", "clickhouse", "cloudwatchlogs", "s3", "0",
		"s3-" + strings.Repeat("ab", 32), "old_bucket", strings.Repeat("a", maxDestinationIDLength),
	} {
		if err := ValidateDestinationID(id); err != nil {
			t.Errorf("ValidateDestinationID(%q) = %v, want nil", id, err)
		}
	}
	for _, id := range []string{
		"", "OpenSearch", "s3-A", "-a", "_a", ".a", "a.b", "a b", "a/b", `a\b`, "..", "a:b", "日本",
		"con", "prn", "aux", "nul", "com1", "lpt9", strings.Repeat("a", maxDestinationIDLength+1),
	} {
		if err := ValidateDestinationID(id); !errors.Is(err, ErrInvalidDestinationID) {
			t.Errorf("ValidateDestinationID(%q) = %v, want ErrInvalidDestinationID", id, err)
		}
		if _, err := NewLogSpoolStore(t.TempDir(), id); !errors.Is(err, ErrInvalidDestinationID) {
			t.Errorf("NewLogSpoolStore(%q) = %v, want ErrInvalidDestinationID", id, err)
		}
	}
}

// 読み込み時は、保存の途中で停止したprocessが残したこの送信先の一時fileだけを削除し、
// markerや他の送信先のfile、形式の異なるfile、directoryには触れない。
func TestStoreLoadRemovesOnlyStaleTemporaryMarkers(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	saved := State{NotBefore: testNow.Add(time.Minute)}
	if err := store.Save(saved); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Dir(store.Path())
	stale := []string{".opensearch.json.123456.tmp", ".opensearch.json.0.tmp"}
	kept := []string{
		".opensearch.json.abc.tmp", ".opensearch.json..tmp", ".loki.json.123.tmp", "opensearch.json.123.tmp",
		".opensearch.json.123.tmp.bak", "notes.txt", "loki.json",
	}
	for _, name := range append(append([]string{}, stale...), kept...) {
		if err := os.WriteFile(filepath.Join(directory, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	keptDirectory := ".opensearch.json.789.tmp"
	if err := os.Mkdir(filepath.Join(directory, keptDirectory), 0o700); err != nil {
		t.Fatal(err)
	}

	state, _, err := store.Load(testNow, time.Hour)
	if err != nil || !state.NotBefore.Equal(saved.NotBefore) {
		t.Fatalf("Load() = %+v/%v, want the saved marker", state, err)
	}
	for _, name := range stale {
		if _, err := os.Lstat(filepath.Join(directory, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale temporary marker %q still exists: %v", name, err)
		}
	}
	for _, name := range append(append([]string{}, kept...), keptDirectory, "opensearch.json") {
		if _, err := os.Lstat(filepath.Join(directory, name)); err != nil {
			t.Fatalf("Load() touched %q: %v", name, err)
		}
	}
}

// 一時fileの削除後にdirectoryを同期できなければ、警告としてcleanupの失敗を返し、markerは読み込む。
func TestStoreLoadReportsTemporaryMarkerCleanupSyncFailure(t *testing.T) {
	t.Parallel()
	store := newTestLogSpoolStore(t)
	saved := State{NotBefore: testNow.Add(time.Minute)}
	if err := store.Save(saved); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(filepath.Dir(store.Path()), ".opensearch.json.42.tmp")
	if err := os.WriteFile(stale, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected directory sync failure")
	store.syncDirectory = func(string) error { return injected }

	state, _, err := store.Load(testNow, time.Hour)
	requireMarkerError(t, err, MarkerOperationCleanup, injected)
	if !state.NotBefore.Equal(saved.NotBefore) {
		t.Fatalf("Load() state = %+v, want the saved marker despite the cleanup warning", state)
	}
	if _, statErr := os.Lstat(stale); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("stale temporary marker still exists: %v", statErr)
	}
}

package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	spoolstorecfg "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

type testSpoolStoreConfig struct {
	spoolPath string
	target    string
	dir       string
}

func (c testSpoolStoreConfig) SpoolPath() string { return c.spoolPath }
func (c testSpoolStoreConfig) Target() string    { return c.target }
func (c testSpoolStoreConfig) Dir() string       { return c.dir }
func (c testSpoolStoreConfig) OverloadPolicyConfig() spoolstorecfg.SpoolStoreOverloadPolicyConfigProvider {
	return nil
}

// resolvedTempDirForTestは、macOSの/varなど一時ディレクトリの上位のリンクを解決したパスを返します。
// rootは解決済みのパスを返し、管理下の作成ではリンクを拒否するため、比べる側も解決しておきます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func TestStoreByStateWritesSpoolFileToTimePartitionDir(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 10, 23, 45, 6, 789_000_000, time.UTC)
	cfg := testSpoolStoreConfig{
		spoolPath: resolvedTempDirForTest(t),
		target:    "test-app",
		dir:       "opensearch",
	}
	store := newTestSpoolStore(t, fixedClock{now: now}, cfg)
	records := []pkgspool.SpoolRecord{
		{
			OccurredAt: now.Add(-time.Minute),
			RequestID:  "request-1",
			Payload:    json.RawMessage(`{"id":"doc-1","message":"first"}`),
		},
		{
			OccurredAt: now,
			RequestID:  "request-2",
			Payload:    json.RawMessage(`{"id":"doc-2","message":"second"}`),
		},
	}

	if err := store.StoreByState(context.Background(), pkgspool.StateActive, records); err != nil {
		t.Fatalf("StoreByState returned error: %v", err)
	}

	timePartitionDir := filepath.Join(
		cfg.spoolPath,
		cfg.target,
		"spool",
		cfg.dir,
		string(pkgspool.StateActive),
		"2026",
		"06",
		"10",
		"23",
		"45",
	)
	assertDirExists(t, timePartitionDir)
	// spool rootから日時別保存先までのdirectoryも、spool fileと同じく所有者以外へ公開しません。
	// rootは解決済みのパスなので、同じ表記の保存先からたどります。
	for path := filepath.Join(store.RootDir.ActiveDir().Path(), "2026", "06", "10", "23", "45"); ; path = filepath.Dir(path) {
		assertPrivateSpoolDirectory(t, path)
		if path == store.RootDir.String() {
			break
		}
		if filepath.Dir(path) == path {
			t.Fatalf("time partition directory is not below spool root %q", store.RootDir.String())
		}
	}

	spoolFiles := listSpoolFiles(t, store.RootDir.ActiveDir().Path())
	if len(spoolFiles) != 1 {
		t.Fatalf("spool file count = %d, want 1: %v", len(spoolFiles), spoolFiles)
	}

	spoolFile := spoolFiles[0]
	if filepath.Dir(spoolFile) != timePartitionDir {
		t.Fatalf("spool file dir = %q, want %q", filepath.Dir(spoolFile), timePartitionDir)
	}
	if gotName := filepath.Base(spoolFile); !strings.HasPrefix(gotName, "06789_") || !strings.HasSuffix(gotName, spooldir.SpoolExt) {
		t.Fatalf("spool file name = %q, want prefix %q and suffix %q", gotName, "06789_", spooldir.SpoolExt)
	}

	assertPrivateSpoolFile(t, spoolFile)
	assertNoTmpFiles(t, store.RootDir.String())

	gotRecords := readSpoolRecords(t, spoolFile)
	assertSpoolRecords(t, gotRecords, records)
}

// assertPrivateSpoolFileは、spool fileへ所有者以外がアクセスできないことを確認します。
// Unixでは所有者だけが読み書きでき、group・otherに権限がない0600であることを確認します。
// WindowsのFileModeはACLを表現できないため、実際のDACLが実行ユーザー・SYSTEM・
// Administratorsだけを許可し、親directoryの広いACEを継承していないことを確認します。
func assertPrivateSpoolFile(t testing.TB, path string) {
	t.Helper()
	testutil.RequirePrivatePermissions(t, path, 0o600)
}

// assertPrivateSpoolDirectoryは、spoolのdirectoryへ所有者以外がアクセスできないことを確認します。
// Unixでは0700を超える権限がないことを確かめ、umaskが追加で外したbitは許容します。
// Windowsでは、実行ユーザー・SYSTEM・Administratorsだけを許可する保護されたDACLを確認します。
func assertPrivateSpoolDirectory(t testing.TB, path string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		testutil.RequirePrivatePermissions(t, path, 0o700)
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm()&^0o700 != 0 {
		t.Fatalf("spool directory %q mode = %v, want a directory without bits beyond 0700", path, info.Mode())
	}
}

func TestStoreByStateUsesClockTimezoneForTimePartitionDir(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("JST", 9*60*60)
	// Clockが返すJSTの6月11日を日時別保存ディレクトリにもそのまま使います。
	now := time.Date(2026, 6, 11, 0, 5, 6, 789_000_000, location)
	cfg := testSpoolStoreConfig{
		spoolPath: resolvedTempDirForTest(t),
		target:    "test-app",
		dir:       "opensearch",
	}
	store, err := NewSpoolStore(fixedClock{now: now}, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewSpoolStore returned error: %v", err)
	}
	record := pkgspool.SpoolRecord{
		OccurredAt: now,
		RequestID:  "request-calendar-location",
		Payload:    json.RawMessage(`{"id":"calendar-location"}`),
	}

	if err := store.StoreByState(
		context.Background(),
		pkgspool.StateActive,
		[]pkgspool.SpoolRecord{record},
	); err != nil {
		t.Fatalf("StoreByState returned error: %v", err)
	}

	wantDir := filepath.Join(
		cfg.spoolPath,
		cfg.target,
		"spool",
		cfg.dir,
		string(pkgspool.StateActive),
		"2026",
		"06",
		"11",
		"00",
		"05",
	)
	assertDirExists(t, wantDir)
	files := listSpoolFiles(t, store.RootDir.ActiveDir().Path())
	if len(files) != 1 || filepath.Dir(files[0]) != wantDir {
		t.Fatalf("spool files = %v, want one file under %q", files, wantDir)
	}
}

func TestStoreByStateWritesDifferentStatesUnderEachStateDir(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 6, 10, 1, 2, 3, 4_000_000, time.UTC)
	cfg := testSpoolStoreConfig{
		spoolPath: t.TempDir(),
		target:    "test-app",
		dir:       "opensearch",
	}
	store := newTestSpoolStore(t, fixedClock{now: now}, cfg)

	tests := []struct {
		name      string
		state     pkgspool.State
		statePath string
	}{
		{name: "processing", state: pkgspool.StateProcessing, statePath: store.RootDir.ProcessingDir().Path()},
		{name: "done", state: pkgspool.StateDone, statePath: store.RootDir.DoneDir().Path()},
		{name: "dead", state: pkgspool.StateDead, statePath: store.RootDir.DeadDir().Path()},
		{name: "quarantine", state: pkgspool.StateQuarantine, statePath: store.RootDir.QuarantineDir().Path()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := pkgspool.SpoolRecord{
				OccurredAt: now,
				RequestID:  "request-" + tt.name,
				Payload:    json.RawMessage(`{"state":"` + tt.name + `"}`),
			}

			if err := store.StoreByState(context.Background(), tt.state, []pkgspool.SpoolRecord{record}); err != nil {
				t.Fatalf("StoreByState returned error: %v", err)
			}

			files := listSpoolFiles(t, tt.statePath)
			if len(files) != 1 {
				t.Fatalf("%s spool file count = %d, want 1: %v", tt.name, len(files), files)
			}
			gotRecords := readSpoolRecords(t, files[0])
			assertSpoolRecords(t, gotRecords, []pkgspool.SpoolRecord{record})
		})
	}
}

func TestStoreByStateWithEmptyRecordsDoesNotCreateFiles(t *testing.T) {
	t.Parallel()

	cfg := testSpoolStoreConfig{
		spoolPath: t.TempDir(),
		target:    "test-app",
		dir:       "opensearch",
	}
	store := newTestSpoolStore(t, fixedClock{now: time.Now()}, cfg)

	if err := store.StoreByState(context.Background(), pkgspool.StateActive, nil); err != nil {
		t.Fatalf("StoreByState returned error: %v", err)
	}

	if _, err := os.Stat(store.RootDir.String()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root dir stat error = %v, want os.ErrNotExist", err)
	}
}

func TestStoreByStateRejectsInvalidState(t *testing.T) {
	t.Parallel()

	cfg := testSpoolStoreConfig{
		spoolPath: t.TempDir(),
		target:    "test-app",
		dir:       "opensearch",
	}
	store := newTestSpoolStore(t, fixedClock{now: time.Now()}, cfg)
	record := pkgspool.SpoolRecord{
		OccurredAt: time.Now(),
		RequestID:  "request-1",
		Payload:    json.RawMessage(`{"ok":true}`),
	}

	err := store.StoreByState(context.Background(), pkgspool.State("unknown"), []pkgspool.SpoolRecord{record})
	if err == nil {
		t.Fatal("StoreByState returned nil error, want error")
	}
	if !errors.Is(err, ErrInvalidState) {
		t.Fatalf("error = %v, want invalid state", err)
	}
}

func TestFileNameTimePrefix(t *testing.T) {
	t.Parallel()

	got := fileNameTimePrefix(time.Date(2026, 6, 10, 1, 2, 3, 4_000_000, time.UTC))
	if got != "03004" {
		t.Fatalf("fileNameTimePrefix = %q, want %q", got, "03004")
	}
}

func newTestSpoolStore(t *testing.T, clk fixedClock, cfg testSpoolStoreConfig) *SpoolStore {
	t.Helper()

	store, err := NewSpoolStore(clk, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewSpoolStore returned error: %v", err)
	}
	return store
}

func listSpoolFiles(t *testing.T, root string) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("failed to list spool files under %q: %v", root, err)
	}
	return files
}

func readSpoolRecords(t *testing.T, path string) []pkgspool.SpoolRecord {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open spool file %q: %v", path, err)
	}
	defer file.Close()

	var records []pkgspool.SpoolRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record pkgspool.SpoolRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("failed to unmarshal spool record: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to read spool file: %v", err)
	}
	return records
}

func assertSpoolRecords(t *testing.T, got, want []pkgspool.SpoolRecord) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("record count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !got[i].OccurredAt.Equal(want[i].OccurredAt) {
			t.Fatalf("record[%d].OccurredAt = %s, want %s", i, got[i].OccurredAt, want[i].OccurredAt)
		}
		if got[i].RequestID != want[i].RequestID {
			t.Fatalf("record[%d].RequestID = %q, want %q", i, got[i].RequestID, want[i].RequestID)
		}
		if string(got[i].Payload) != string(want[i].Payload) {
			t.Fatalf("record[%d].Payload = %s, want %s", i, got[i].Payload, want[i].Payload)
		}
	}
}

func assertDirExists(t *testing.T, path string) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat dir %q: %v", path, err)
	}
	if !info.IsDir() {
		t.Fatalf("%q is not a directory", path)
	}
}

func assertNoTmpFiles(t *testing.T, root string) {
	t.Helper()

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, tmpFileSuffix) {
			t.Fatalf("temporary spool file remained: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to inspect tmp files under %q: %v", root, err)
	}
}

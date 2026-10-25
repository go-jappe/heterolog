package objectstoragespool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestTerminalPurgerRejectsInvalidEnabledStateConfig(t *testing.T) {
	stores, _ := newTestDestinationStores(t)
	_, err := NewTerminalPurger(stores, NewPurgeConfigAdapter(TerminalPurgeConfig{
		Done: pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig{Enabled: true},
	}))
	if err == nil {
		t.Fatal("NewTerminalPurger() error = nil")
	}
	if _, err := NewTerminalPurger(stores, NewPurgeConfigAdapter(TerminalPurgeConfig{
		CapacityPurgeMinimumFileAge: -time.Second,
	})); err == nil {
		t.Fatal("NewTerminalPurger(negative minimum age) error = nil")
	}
}

func TestTerminalPurgerPurgesOnlyTerminalFilesByAge(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	done := saveTerminalPurgeTestFile(t, store, "app_20260906_21.jsonl.gz", "done", pkgspool.StateDone)
	dead := saveTerminalPurgeTestFile(t, store, "app_20260906_22.log.gz", "dead", pkgspool.StateDead)
	quarantine := saveTerminalPurgeTestFile(
		t,
		store,
		"app_20260906_23.jsonl.custom_format-v2",
		"quarantine",
		pkgspool.StateQuarantine,
	)
	active, err := store.SaveRetryableFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), "app_20260906_20.log.gz", "active"),
	)
	if err != nil {
		t.Fatalf("publish active: %v", err)
	}
	old := time.Now().AddDate(0, 0, -10)
	for _, file := range []CompressedLogFile{active, done, dead, quarantine} {
		if err := os.Chtimes(file.Path, old, old); err != nil {
			t.Fatalf("Chtimes(%s): %v", file.State, err)
		}
	}

	purger := newTestTerminalPurger(t, stores, enabledPurgeConfig(1, 1<<20))
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if _, err := os.Stat(active.Path); err != nil {
		t.Fatalf("active file was deleted: %v", err)
	}
	for _, file := range []CompressedLogFile{done, dead, quarantine} {
		if _, err := os.Stat(file.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("terminal %s file remains: %v", file.State, err)
		}
		stateReport := report.ByState[file.State]
		if stateReport.ScannedFileCount != 1 || stateReport.DeletedFileCount != 1 {
			t.Fatalf("%s report = %+v, want one scanned/deleted", file.State, stateReport)
		}
	}
}

func TestTerminalPurgerAgeUsesModTimeAndIncludesExactCutoff(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			stores, store := newTestDestinationStores(t)
			now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.FixedZone("UTC+09", 9*60*60))
			cutoff := now.AddDate(0, 0, -1)
			// 名前の日付・indexとmtimeを逆順にし、spoolが名前基準にならないことを確認します。
			entries := []struct {
				name    string
				mtime   time.Time
				removed bool
			}{
				{"app_20260922_10.log.gz", cutoff.Add(-time.Second), true},
				{"app_20260922_2.log.gz", cutoff, true},
				{"app_20200101_10.log.gz", cutoff.Add(time.Second), false},
				{"app_20200101_2.log.gz", now, false},
			}
			files := make([]CompressedLogFile, 0, len(entries))
			for _, entry := range entries {
				file := saveTerminalPurgeTestFile(t, store, entry.name, "log", state)
				if err := os.Chtimes(file.Path, entry.mtime, entry.mtime); err != nil {
					t.Fatal(err)
				}
				files = append(files, file)
			}
			config := enabledPurgeConfig(1, 1<<20)
			purger := newTestTerminalPurger(t, stores, config)
			report, err := purger.purgeByState(context.Background(), state, NewStatePurgeConfigAdapter(config), now)
			if err != nil {
				t.Fatal(err)
			}
			if report.ScannedFileCount != 4 || report.DeletedFileCount != 2 || report.RemainingFileCount != 2 {
				t.Fatalf("report = %+v", report)
			}
			for index, file := range files {
				_, err := os.Stat(file.Path)
				if entries[index].removed {
					if !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("expired file %q remains: %v", file.FileName, err)
					}
				} else if err != nil {
					t.Fatalf("retained file %q: %v", file.FileName, err)
				}
			}
		})
	}
}

func TestTerminalPurgerPurgesFlatFilesOldestFirstBySize(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	oldest := saveTerminalPurgeTestFile(
		t,
		store,
		"app_20260907_30.log.gz",
		strings.Repeat("a", 8),
		pkgspool.StateDone,
	)
	newest := saveTerminalPurgeTestFile(
		t,
		store,
		"app_20260906_31.log.gz",
		strings.Repeat("b", 8),
		pkgspool.StateDone,
	)
	now := time.Now()
	if err := os.Chtimes(oldest.Path, now.Add(-time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatalf("set oldest time: %v", err)
	}
	if err := os.Chtimes(newest.Path, now, now); err != nil {
		t.Fatalf("set newest time: %v", err)
	}

	disabled := pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig{}
	purger, err := NewTerminalPurger(stores, NewPurgeConfigAdapter(TerminalPurgeConfig{
		Done:       enabledPurgeConfig(365, 8),
		Dead:       disabled,
		Quarantine: disabled,
	}))
	if err != nil {
		t.Fatalf("NewTerminalPurger() error = %v", err)
	}
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if _, err := os.Stat(oldest.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest file remains: %v", err)
	}
	if _, err := os.Stat(newest.Path); err != nil {
		t.Fatalf("newest file was deleted: %v", err)
	}
	doneReport := report.ByState[pkgspool.StateDone]
	if doneReport.ScannedFileCount != 2 ||
		doneReport.ScannedBytes != 16 ||
		doneReport.DeletedFileCount != 1 ||
		doneReport.DeletedBytes != 8 ||
		doneReport.RemainingFileCount != 1 ||
		doneReport.RemainingBytes != 8 {
		t.Fatalf("done report = %+v", doneReport)
	}
}

func TestTerminalPurgerCapacityPurgeRetainsProtectedSuffixByModTime(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		for _, minimumAge := range []time.Duration{time.Hour, 0} {
			t.Run(string(state)+"/"+minimumAge.String(), func(t *testing.T) {
				stores, store := newTestDestinationStores(t)
				now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
				// 名前とは逆のmtime順で、保護期間の境界ちょうどのファイルも用意します。
				entries := []struct {
					name string
					age  time.Duration
				}{
					{"app_20260922_10.log.gz", 2 * time.Hour},
					{"app_20260922_2.log.gz", time.Hour},
					{"app_20200101_10.log.gz", time.Hour - time.Second},
					{"app_20200101_2.log.gz", 0},
				}
				files := make([]CompressedLogFile, 0, len(entries))
				for _, entry := range entries {
					file := saveTerminalPurgeTestFile(t, store, entry.name, "log", state)
					mtime := now.Add(-entry.age)
					if err := os.Chtimes(file.Path, mtime, mtime); err != nil {
						t.Fatal(err)
					}
					files = append(files, file)
				}
				config := enabledPurgeConfig(365, 1)
				purger, err := NewTerminalPurger(stores, NewPurgeConfigAdapter(TerminalPurgeConfig{
					Done: config, Dead: config, Quarantine: config,
					CapacityPurgeMinimumFileAge: minimumAge,
				}))
				if err != nil {
					t.Fatal(err)
				}
				report, err := purger.purgeByState(context.Background(), state, NewStatePurgeConfigAdapter(config), now)
				if err != nil {
					t.Fatal(err)
				}
				wantDeleted := 2
				if minimumAge == 0 {
					wantDeleted = 4
				}
				if report.ScannedFileCount != 4 || report.ScannedBytes != 12 ||
					report.DeletedFileCount != wantDeleted || report.DeletedBytes != uint64(wantDeleted*3) ||
					report.RemainingFileCount != 4-wantDeleted || report.RemainingBytes != uint64((4-wantDeleted)*3) {
					t.Fatalf("report = %+v, want %d deleted", report, wantDeleted)
				}
				for index, file := range files {
					_, err := os.Stat(file.Path)
					if index < wantDeleted {
						if !errors.Is(err, os.ErrNotExist) {
							t.Fatalf("eligible file %q remains: %v", file.FileName, err)
						}
					} else if err != nil {
						t.Fatalf("protected file %q: %v", file.FileName, err)
					}
				}
			})
		}
	}
}

func TestTerminalPurgerCapacityPurgeDefersFreshDeadHandoff(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	dead, err := store.SavePermanentFailure(
		context.Background(),
		writeTestSource(
			t,
			t.TempDir(),
			"app_20260906_33.log.gz",
			"larger than the configured capacity",
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(dead.Path)
	if err != nil {
		t.Fatal(err)
	}
	minimumAge := time.Hour
	now := info.ModTime()
	purger, err := NewTerminalPurger(stores, NewPurgeConfigAdapter(TerminalPurgeConfig{
		Dead:                        enabledPurgeConfig(365, 1),
		CapacityPurgeMinimumFileAge: minimumAge,
	}))
	if err != nil {
		t.Fatal(err)
	}
	purger.now = func() time.Time { return now }

	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run(fresh) error = %v", err)
	}
	deadReport := report.ByState[pkgspool.StateDead]
	if deadReport.DeletedFileCount != 0 || deadReport.RemainingBytes <= 1 {
		t.Fatalf("fresh dead report = %+v, want retained over capacity", deadReport)
	}
	if _, err := os.Lstat(dead.Path); err != nil {
		t.Fatalf("fresh dead handoff was deleted: %v", err)
	}

	now = info.ModTime().Add(minimumAge)
	report, err = purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run(after minimum age) error = %v", err)
	}
	deadReport = report.ByState[pkgspool.StateDead]
	if deadReport.DeletedFileCount != 1 || deadReport.RemainingBytes != 0 {
		t.Fatalf("aged dead report = %+v, want capacity deletion", deadReport)
	}
	if _, err := os.Lstat(dead.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("aged over-capacity dead file remains: %v", err)
	}
}

func TestTerminalPurgerCapacityPurgeProtectsRefreshedEquivalentDeadHandoff(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	fileName := "app_20260906_35.log.gz"
	dead, err := store.SavePermanentFailure(
		context.Background(),
		writeTestSource(t, t.TempDir(), fileName, "equivalent durable bytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-48 * time.Hour)
	if err := os.Chtimes(dead.Path, old, old); err != nil {
		t.Fatal(err)
	}
	callerSource := writeTestSource(t, t.TempDir(), fileName, "equivalent durable bytes")
	adopted, err := store.SavePermanentFailure(context.Background(), callerSource)
	if err != nil {
		t.Fatalf("SavePermanentFailure(equivalent) error = %v", err)
	}
	info, err := os.Lstat(adopted.Path)
	if err != nil {
		t.Fatal(err)
	}
	minimumAge := time.Hour
	now := info.ModTime()
	purger, err := NewTerminalPurger(stores, NewPurgeConfigAdapter(TerminalPurgeConfig{
		Dead:                        enabledPurgeConfig(365, 1),
		CapacityPurgeMinimumFileAge: minimumAge,
	}))
	if err != nil {
		t.Fatal(err)
	}
	purger.now = func() time.Time { return now }
	if err := os.Remove(callerSource); err != nil {
		t.Fatal(err)
	}

	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run(refreshed handoff) error = %v", err)
	}
	if deadReport := report.ByState[pkgspool.StateDead]; deadReport.DeletedFileCount != 0 || deadReport.RemainingBytes <= 1 {
		t.Fatalf("refreshed handoff report = %+v, want retained over capacity", deadReport)
	}
	if _, err := os.Lstat(adopted.Path); err != nil {
		t.Fatalf("refreshed equivalent handoff was deleted: %v", err)
	}

	now = info.ModTime().Add(minimumAge)
	report, err = purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run(after minimum age) error = %v", err)
	}
	if deadReport := report.ByState[pkgspool.StateDead]; deadReport.DeletedFileCount != 1 {
		t.Fatalf("aged refreshed handoff report = %+v, want one deletion", deadReport)
	}
	if _, err := os.Lstat(adopted.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("aged refreshed equivalent handoff remains: %v", err)
	}
}

func TestTerminalPurgerAgeThenCapacityDoesNotCountRemovedFileTwice(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			stores, store := newTestDestinationStores(t)
			now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
			files := []CompressedLogFile{
				saveTerminalPurgeTestFile(t, store, "app_20260922_10.log.gz", "old", state),
				saveTerminalPurgeTestFile(t, store, "app_20260922_2.log.gz", "mid", state),
				saveTerminalPurgeTestFile(t, store, "app_20200101_1.log.gz", "new", state),
			}
			for index, age := range []time.Duration{48 * time.Hour, 2 * time.Hour, 0} {
				mtime := now.Add(-age)
				if err := os.Chtimes(files[index].Path, mtime, mtime); err != nil {
					t.Fatal(err)
				}
			}
			config := enabledPurgeConfig(1, 3)
			purger := newTestTerminalPurger(t, stores, config)
			report, err := purger.purgeByState(context.Background(), state, NewStatePurgeConfigAdapter(config), now)
			if err != nil {
				t.Fatal(err)
			}
			if report.ScannedFileCount != 3 || report.ScannedBytes != 9 ||
				report.DeletedFileCount != 2 || report.DeletedBytes != 6 ||
				report.RemainingFileCount != 1 || report.RemainingBytes != 3 {
				t.Fatalf("report = %+v, want age and capacity to delete one file each", report)
			}
			for _, file := range files[:2] {
				if _, err := os.Stat(file.Path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("purged file %q remains: %v", file.FileName, err)
				}
			}
			if _, err := os.Stat(files[2].Path); err != nil {
				t.Fatalf("newest file was not retained: %v", err)
			}
		})
	}
}

func TestInspectTerminalFileAcceptsEmptyRegularFileAndRejectsInvalidPaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app_20260921_1.log.gz")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	file := CompressedLogFile{FileName: filepath.Base(path), Path: path, State: pkgspool.StateDone}
	candidate, err := inspectTerminalFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(candidate.file, file) || candidate.sizeBytes != 0 ||
		!candidate.modifiedAt.Equal(info.ModTime()) || candidate.removed {
		t.Fatalf("empty regular file observation = %+v", candidate)
	}
	for _, invalidPath := range []string{dir, filepath.Join(dir, "missing.log.gz")} {
		if _, err := inspectTerminalFile(CompressedLogFile{Path: invalidPath}); err == nil {
			t.Fatalf("inspection of %q must fail", invalidPath)
		}
	}
}

func TestTerminalPurgePreservesStateDebrisAndDeletesExpiredFile(t *testing.T) {
	stores, store := newTestDestinationStores(t)
	done := saveTerminalPurgeTestFile(
		t,
		store,
		"app_20260906_32.log.gz",
		"expired",
		pkgspool.StateDone,
	)
	old := time.Now().AddDate(0, 0, -10)
	if err := os.Chtimes(done.Path, old, old); err != nil {
		t.Fatal(err)
	}
	legacyBundle := filepath.Join(store.Root().DoneDir(), "legacy-bundle")
	if err := os.Mkdir(legacyBundle, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyBundle, "keep.txt"), []byte("keep nested data"), 0o600); err != nil {
		t.Fatal(err)
	}

	purger := newTestTerminalPurger(t, stores, enabledPurgeConfig(1, 1))
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if _, err := os.Lstat(legacyBundle); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("legacy bundle was not moved: %v", err)
	}
	if _, err := os.Lstat(done.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired terminal file was not deleted: %v", err)
	}
	doneReport := report.ByState[pkgspool.StateDone]
	if doneReport.DeletedFileCount != 1 || len(doneReport.Errors) != 0 {
		t.Fatalf("done report = %+v, want one deletion and no errors", doneReport)
	}
	entries, err := os.ReadDir(store.root.preservedDir())
	if err != nil || len(entries) != 1 {
		t.Fatalf("preserved entries = %v, %v", entries, err)
	}
	if body, err := os.ReadFile(filepath.Join(store.root.preservedDir(), entries[0].Name(), "keep.txt")); err != nil || string(body) != "keep nested data" {
		t.Fatalf("preserved contents = %q, %v", body, err)
	}
}

func saveTerminalPurgeTestFile(
	t *testing.T,
	store *Store,
	fileName string,
	body string,
	state pkgspool.State,
) CompressedLogFile {
	t.Helper()
	source := writeTestSource(t, t.TempDir(), fileName, body)
	if state == pkgspool.StateDead {
		file, err := store.SavePermanentFailure(context.Background(), source)
		if err != nil {
			t.Fatalf("SavePermanentFailure(%q): %v", fileName, err)
		}
		return file
	}
	file, err := store.SaveRetryableFailure(context.Background(), source)
	if err != nil {
		t.Fatalf("SaveRetryableFailure(%q): %v", fileName, err)
	}
	claimed, ok, err := store.ClaimOldestActive(context.Background())
	if err != nil || !ok || claimed.FileName != file.FileName {
		t.Fatalf("claim %q = %+v, %t, %v", fileName, claimed, ok, err)
	}
	transitioned, err := store.TransitionProcessing(context.Background(), claimed, state)
	if err != nil {
		t.Fatalf("TransitionProcessing(%s): %v", state, err)
	}
	return transitioned
}

func enabledPurgeConfig(maxAgeDays int, maxBytes uint64) pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig {
	return pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig{
		Enabled:        true,
		MaxFileAgeDays: maxAgeDays,
		MaxDirSize:     maxBytes,
	}
}

func newTestTerminalPurger(
	t *testing.T,
	stores *DestinationStores,
	config pkgobjectstorage.CompressedLogFileUploadSpoolPurgeConfig,
) *TerminalPurger {
	t.Helper()
	purger, err := NewTerminalPurger(stores, NewPurgeConfigAdapter(TerminalPurgeConfig{
		Done:       config,
		Dead:       config,
		Quarantine: config,
	}))
	if err != nil {
		t.Fatalf("NewTerminalPurger() error = %v", err)
	}
	return purger
}

// newTestDestinationStoresは本番と同じ共通ルートと、一つの宛先の保存用Storeを用意します。
func newTestDestinationStores(t testing.TB) (*DestinationStores, *Store) {
	t.Helper()
	stores, err := NewDestinationStores(newTestStore(t).Root())
	if err != nil {
		t.Fatal(err)
	}
	store, err := stores.ForDestination("primary")
	if err != nil {
		t.Fatal(err)
	}
	return stores, store
}

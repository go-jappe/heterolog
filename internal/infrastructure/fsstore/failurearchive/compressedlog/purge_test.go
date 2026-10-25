package compressedlogarchive

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/clock"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	pkgobjectstorage "github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestNewPurgerRejectsInvalidEnabledFailureCategoryConfig(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	if _, err := NewPurger(store, NewPurgeConfigAdapter(PurgeConfig{
		Retryable: pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{Enabled: true},
	}), clock.NewUTCClock()); err == nil {
		t.Fatal("NewPurger() error = nil")
	}
	if _, err := NewPurger(nil, NewPurgeConfigAdapter(PurgeConfig{}), clock.NewUTCClock()); err == nil {
		t.Fatal("NewPurger(nil) error = nil")
	}
}

func TestPurgerPurgesRetryableAndPermanentFilesByAge(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	now := time.Now().UTC()
	oldDate := now.AddDate(0, 0, -3).Format("20060102")
	newDate := now.Format("20060102")
	oldRetryable := publishPurgeTestCompressedLog(t, store, "old-retryable_"+oldDate+"_1", uploader.FailureCategoryRetryable, "old retryable")
	newRetryable := publishPurgeTestCompressedLog(t, store, "new-retryable_"+newDate+"_1", uploader.FailureCategoryRetryable, "new retryable")
	// 通常ログと同じ日付規則で、圧縮済みのbackend送信失敗archiveも保持整理します。
	oldPermanent, err := store.Save(context.Background(), testSaveInput(
		writeTestCompressedLog(t, "old-permanent_"+oldDate+"_1.jsonl.gz", []byte("old permanent")),
		uploader.FailureCategoryPermanent,
	))
	if err != nil {
		t.Fatal(err)
	}
	newPermanent, err := store.Save(context.Background(), testSaveInput(
		writeTestCompressedLog(t, "new-permanent_"+newDate+"_1.jsonl.gz", []byte("new permanent")),
		uploader.FailureCategoryPermanent,
	))
	if err != nil {
		t.Fatal(err)
	}
	// 更新日時が逆転していても、削除するのは名前の日付が古いファイルです。
	setFileTime(t, oldRetryable.Path, now)
	setFileTime(t, oldPermanent.Path, now)
	setFileTime(t, newRetryable.Path, now.Add(-72*time.Hour))
	setFileTime(t, newPermanent.Path, now.Add(-72*time.Hour))

	purger := newTestPurger(t, store, PurgeConfig{
		Retryable: enabledArchivePurgeConfig(1, math.MaxUint64),
		Permanent: enabledArchivePurgeConfig(1, math.MaxUint64),
	})
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for _, failureCategory := range archiveFailureCategories {
		failureCategoryReport := report.ByFailureCategory[failureCategory]
		if failureCategoryReport.ScannedEntryCount != 2 ||
			failureCategoryReport.DeletedFileCount != 1 ||
			failureCategoryReport.RemainingEntryCount != 1 ||
			failureCategoryReport.RetainedUnsafeEntryCount != 0 {
			t.Fatalf("%s report = %#v", failureCategory, failureCategoryReport)
		}
	}
	for _, archived := range []ArchivedFile{oldRetryable, oldPermanent} {
		if _, err := os.Lstat(archived.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("old file %q remains: %v", archived.Path, err)
		}
	}
	for _, archived := range []ArchivedFile{newRetryable, newPermanent} {
		if _, err := os.Lstat(archived.Path); err != nil {
			t.Fatalf("new file %q was removed: %v", archived.Path, err)
		}
	}
}

func TestPurgerPurgesOldestFileFirstByCapacity(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractDelivery)
	now := time.Now().UTC()
	oldest := publishPurgeTestCompressedLog(t, store, "capacity-oldest_"+now.AddDate(0, 0, -2).Format("20060102")+"_1", uploader.FailureCategoryPermanent, "oldest payload is intentionally larger")
	middle := publishPurgeTestCompressedLog(t, store, "capacity-middle_"+now.AddDate(0, 0, -1).Format("20060102")+"_1", uploader.FailureCategoryPermanent, "middle payload")
	newest := publishPurgeTestCompressedLog(t, store, "capacity-newest_"+now.Format("20060102")+"_1", uploader.FailureCategoryPermanent, "newest payload")
	setFileTime(t, oldest.Path, now)
	setFileTime(t, middle.Path, now.Add(-2*time.Hour))
	setFileTime(t, newest.Path, now.Add(-3*time.Hour))
	middleBytes := fileSize(t, middle.Path)
	newestBytes := fileSize(t, newest.Path)
	maxBytes := middleBytes + newestBytes

	purger := newTestPurger(t, store, PurgeConfig{
		Permanent: enabledArchivePurgeConfig(365, maxBytes),
	})
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	permanent := report.ByFailureCategory[uploader.FailureCategoryPermanent]
	if permanent.DeletedFileCount != 1 || permanent.RemainingEntryCount != 2 ||
		permanent.RemainingBytes != maxBytes || permanent.CapacityLimitExceeded {
		t.Fatalf("permanent report = %#v", permanent)
	}
	if _, err := os.Lstat(oldest.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oldest file remains: %v", err)
	}
	for _, archived := range []ArchivedFile{middle, newest} {
		if _, err := os.Lstat(archived.Path); err != nil {
			t.Fatalf("newer file %q was removed: %v", archived.Path, err)
		}
	}
}

func TestPurgerCapacityPurgeRemovesFreshOverCapacityFile(t *testing.T) {
	t.Parallel()

	for _, failureCategory := range archiveFailureCategories {
		t.Run(string(failureCategory), func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			archived := publishPurgeTestCompressedLog(
				t,
				store,
				"fresh-handoff_"+time.Now().UTC().Format("20060102")+"_1",
				failureCategory,
				"larger than the configured capacity",
			)
			archivedBytes := fileSize(t, archived.Path)
			purger := newTestPurger(t, store, PurgeConfig{
				Retryable: enabledArchivePurgeConfig(365, 1),
				Permanent: enabledArchivePurgeConfig(365, 1),
			})
			report, err := purger.Run(context.Background())
			if err != nil {
				t.Fatalf("Run(fresh) error = %v", err)
			}
			categoryReport := report.ByFailureCategory[failureCategory]
			if categoryReport.DeletedFileCount != 1 || categoryReport.DeletedBytes != archivedBytes ||
				categoryReport.RemainingEntryCount != 0 || categoryReport.RemainingBytes != 0 ||
				categoryReport.CapacityLimitExceeded {
				t.Fatalf("fresh report = %#v", categoryReport)
			}
			if _, err := os.Lstat(archived.Path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("fresh over-capacity file remains: %v", err)
			}
		})
	}
}

func TestPurgerCapacityPurgeDoesNotDeferEquivalentHandoff(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	fileName := "equivalent-handoff_" + time.Now().UTC().Format("20060102") + "_1.log.gz"
	body := []byte("larger than the configured capacity")
	firstSource := writeTestCompressedLogIn(t, t.TempDir(), fileName, body)
	archived, err := store.Save(
		context.Background(),
		testSaveInput(firstSource, uploader.FailureCategoryRetryable),
	)
	if err != nil {
		t.Fatalf("Save(first) error = %v", err)
	}
	old := time.Now().UTC().Add(-2 * time.Hour)
	setFileTime(t, archived.Path, old)

	secondSource := writeTestCompressedLogIn(t, t.TempDir(), fileName, body)
	reused, err := store.Save(
		context.Background(),
		testSaveInput(secondSource, uploader.FailureCategoryPermanent),
	)
	if err != nil {
		t.Fatalf("Save(equivalent) error = %v", err)
	}
	if reused.Path != archived.Path || reused.FailureCategory != archived.FailureCategory {
		t.Fatalf("equivalent handoff = %#v, want existing %#v", reused, archived)
	}
	refreshedInfo, err := os.Stat(archived.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !refreshedInfo.ModTime().After(old.Add(time.Hour)) {
		t.Fatalf("equivalent handoff mtime = %v, want refreshed from %v", refreshedInfo.ModTime(), old)
	}

	purger := newTestPurger(t, store, PurgeConfig{
		Retryable: enabledArchivePurgeConfig(365, 1),
	})
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run(refreshed) error = %v", err)
	}
	retryable := report.ByFailureCategory[uploader.FailureCategoryRetryable]
	if retryable.DeletedFileCount != 1 || retryable.CapacityLimitExceeded {
		t.Fatalf("refreshed report = %#v", retryable)
	}
	if _, err := os.Lstat(archived.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("over-capacity archive remains after equivalent handoff: %v", err)
	}
}

func TestPurgerRetainsUnsafeEntriesButPurgesSafeFile(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		withSymlink bool
	}{
		{name: "unknown file and directory"},
		{name: "with symlink", withSymlink: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t, pkglogger.LoggerContractSync)
			if err := store.EnsureDirectories(context.Background()); err != nil {
				t.Fatal(err)
			}
			old := time.Now().UTC().Add(-72 * time.Hour)
			safe := publishPurgeTestCompressedLog(t, store, "safe-old_"+old.Format("20060102")+"_1", uploader.FailureCategoryRetryable, "safe")
			setFileTime(t, safe.Path, old)

			unknownFile := filepath.Join(store.Root().RetryableDir(), "unknown.txt")
			if err := os.WriteFile(unknownFile, []byte("unknown"), archivedFileMode); err != nil {
				t.Fatal(err)
			}
			setFileTime(t, unknownFile, old)
			legacyDirectory := filepath.Join(store.Root().RetryableDir(), "legacy-bundle")
			if err := os.Mkdir(legacyDirectory, directoryMode); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(legacyDirectory, "valuable.log.gz"), []byte("legacy"), archivedFileMode); err != nil {
				t.Fatal(err)
			}
			externalFile := writeTestCompressedLog(t, "external.log.gz", []byte("external-must-remain"))
			retained := []string{unknownFile, legacyDirectory}
			if test.withSymlink {
				symlinkPath := filepath.Join(store.Root().RetryableDir(), "unsafe.log.gz")
				testutil.Symlink(t, externalFile, symlinkPath)
				retained = append(retained, symlinkPath)
			}

			purger := newTestPurger(t, store, PurgeConfig{
				Retryable: enabledArchivePurgeConfig(1, math.MaxUint64),
			})
			report, err := purger.Run(context.Background())
			if err == nil {
				t.Fatal("Run() error = nil, want unsafe entry errors")
			}
			retryable := report.ByFailureCategory[uploader.FailureCategoryRetryable]
			if retryable.ScannedEntryCount != len(retained)+1 || retryable.DeletedFileCount != 1 ||
				retryable.RetainedUnsafeEntryCount != len(retained) || retryable.RemainingEntryCount != len(retained) {
				t.Fatalf("retryable report = %#v", retryable)
			}
			if _, err := os.Lstat(safe.Path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("safe old file remains: %v", err)
			}
			for _, path := range append(retained, externalFile) {
				if _, err := os.Lstat(path); err != nil {
					t.Fatalf("unsafe or external path %q was removed: %v", path, err)
				}
			}
			assertFileBytes(t, externalFile, []byte("external-must-remain"))
		})
	}
}

func TestPurgerDisabledFailureCategoryDoesNotScanOrDelete(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	archived := publishPurgeTestCompressedLog(t, store, "disabled_20000101_1", uploader.FailureCategoryRetryable, "preserve")
	setFileTime(t, archived.Path, time.Now().UTC().Add(-365*24*time.Hour))
	purger := newTestPurger(t, store, PurgeConfig{})
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	retryable := report.ByFailureCategory[uploader.FailureCategoryRetryable]
	if retryable.Enabled || retryable.ScannedEntryCount != 0 || retryable.DeletedFileCount != 0 {
		t.Fatalf("retryable report = %#v", retryable)
	}
	if _, err := os.Lstat(archived.Path); err != nil {
		t.Fatalf("disabled purger changed file: %v", err)
	}
}

func TestPurgerHonorsCanceledContextWithoutDeleting(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	archived := publishPurgeTestCompressedLog(t, store, "canceled_20000101_1", uploader.FailureCategoryRetryable, "preserve")
	setFileTime(t, archived.Path, time.Now().UTC().Add(-72*time.Hour))
	purger := newTestPurger(t, store, PurgeConfig{
		Retryable: enabledArchivePurgeConfig(1, 1),
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := purger.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := os.Lstat(archived.Path); err != nil {
		t.Fatalf("canceled purger removed file: %v", err)
	}
}

func TestPurgerRemovesOnlyRecognizedTemporaryFiles(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	if err := store.EnsureDirectories(context.Background()); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(store.Root().RetryableDir(), ".compressed-log-crashed.tmp")
	if err := os.WriteFile(stale, []byte("source still owns these bytes"), archivedFileMode); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(store.Root().RetryableDir(), "operator-file.tmp")
	if err := os.WriteFile(unknown, []byte("do not infer ownership"), archivedFileMode); err != nil {
		t.Fatal(err)
	}
	// 一時ファイルの名前でも、通常ファイルでなければ削除しません。
	directory := filepath.Join(store.Root().PermanentDir(), ".compressed-log-directory.tmp")
	if err := os.Mkdir(directory, directoryMode); err != nil {
		t.Fatal(err)
	}
	purger := newTestPurger(t, store, PurgeConfig{})
	if _, err := purger.Run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want non-regular temporary entry error")
	}
	if _, err := os.Lstat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned temporary file remains: %v", err)
	}
	assertFileBytes(t, unknown, []byte("do not infer ownership"))
	if info, err := os.Lstat(directory); err != nil || !info.IsDir() {
		t.Fatalf("temporary-named directory was not retained: info=%v err=%v", info, err)
	}
}

func TestInspectArchiveEntryRetainsObservedInfoOnlyForRegularFiles(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	for name, body := range map[string]string{
		"empty_20260920_1.log.gz":   "",
		"empty.log.gz":              "",
		"invalid_20260230_1.log.gz": "invalid",
		"unknown.txt":               "unknown",
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(body), archivedFileMode); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(directory, "directory.log.gz"), directoryMode); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name          string
		wantInfo      bool
		wantSize      int64
		wantRemovable bool
		wantError     bool
	}{
		{name: "empty_20260920_1.log.gz", wantInfo: true, wantRemovable: true},
		{name: "empty.log.gz", wantInfo: true, wantError: true},
		{name: "invalid_20260230_1.log.gz", wantInfo: true, wantSize: 7, wantError: true},
		{name: "unknown.txt", wantInfo: true, wantSize: 7, wantError: true},
		{name: "directory.log.gz", wantError: true},
		{name: "missing.log.gz", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate, errs := inspectArchiveEntry(uploader.FailureCategoryRetryable, directory, test.name)
			if candidate.removable != test.wantRemovable || candidate.removeTried {
				t.Fatalf("candidate flags = %+v", candidate)
			}
			if (len(errs) > 0) != test.wantError {
				t.Fatalf("inspection errors = %v, want error: %t", errs, test.wantError)
			}
			if (candidate.observedInfo != nil) != test.wantInfo {
				t.Fatalf("observedInfo = %v, want information: %t", candidate.observedInfo, test.wantInfo)
			}
			if !test.wantInfo {
				return
			}
			info, err := os.Lstat(filepath.Join(directory, test.name))
			if err != nil {
				t.Fatal(err)
			}
			if candidate.observedInfo.Size() != test.wantSize ||
				!candidate.observedInfo.ModTime().Equal(info.ModTime()) ||
				!os.SameFile(candidate.observedInfo, info) {
				t.Fatalf("observedInfo does not preserve size, modification time and file identity: %+v", candidate.observedInfo)
			}
		})
	}
}

func TestPurgerCountsUnknownFileBytesWhilePurgingEmptyArchive(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	empty := publishPurgeTestCompressedLog(t, store, "empty_20000101_1", uploader.FailureCategoryRetryable, "")
	setFileTime(t, empty.Path, time.Now().UTC().Add(-72*time.Hour))
	unknownPath := filepath.Join(store.Root().RetryableDir(), "unknown.txt")
	if err := os.WriteFile(unknownPath, []byte("unknown"), archivedFileMode); err != nil {
		t.Fatal(err)
	}
	directoryPath := filepath.Join(store.Root().RetryableDir(), "directory.log.gz")
	if err := os.Mkdir(directoryPath, directoryMode); err != nil {
		t.Fatal(err)
	}
	purger := newTestPurger(t, store, PurgeConfig{
		Retryable: enabledArchivePurgeConfig(1, math.MaxUint64),
	})
	report, err := purger.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want errors for the unknown file and directory")
	}
	result := report.ByFailureCategory[uploader.FailureCategoryRetryable]
	if result.ScannedEntryCount != 3 || result.ScannedBytes != 7 ||
		result.DeletedFileCount != 1 || result.DeletedBytes != 0 ||
		result.RemainingEntryCount != 2 || result.RemainingBytes != 7 ||
		result.RetainedUnsafeEntryCount != 2 {
		t.Fatalf("purge report = %+v", result)
	}
	if _, err := os.Lstat(empty.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty expired archive was not removed: %v", err)
	}
	for _, path := range []string{unknownPath, directoryPath} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("excluded entry %q was removed: %v", path, err)
		}
	}
}

func TestPurgerDoesNotRetryAgeDeletionDuringCapacityPurge(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, pkglogger.LoggerContractSync)
	now := time.Now().UTC()
	old := publishPurgeTestCompressedLog(t, store, "old_"+now.AddDate(0, 0, -3).Format("20060102")+"_1", uploader.FailureCategoryRetryable, "old archive")
	recent := publishPurgeTestCompressedLog(t, store, "recent_"+now.Format("20060102")+"_1", uploader.FailureCategoryRetryable, "recent archive")
	setFileTime(t, old.Path, now.Add(-72*time.Hour))
	setFileTime(t, recent.Path, now.Add(-time.Hour))
	totalBytes := fileSize(t, old.Path) + fileSize(t, recent.Path)
	purger := newTestPurger(t, store, PurgeConfig{
		Retryable: enabledArchivePurgeConfig(1, 1),
	})
	report, err := purger.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() retried an already deleted file: %v", err)
	}
	result := report.ByFailureCategory[uploader.FailureCategoryRetryable]
	if result.DeletedFileCount != 2 || result.DeletedBytes != totalBytes ||
		result.RemainingEntryCount != 0 || result.RemainingBytes != 0 || result.CapacityLimitExceeded {
		t.Fatalf("purge report = %+v", result)
	}
	for _, archived := range []ArchivedFile{old, recent} {
		if _, err := os.Lstat(archived.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("archive %q remains: %v", archived.Path, err)
		}
	}
}

func publishPurgeTestCompressedLog(
	t *testing.T,
	store *Store,
	fileStem string,
	failureCategory uploader.FailureCategory,
	body string,
) ArchivedFile {
	t.Helper()
	sourcePath := writeTestCompressedLog(t, fileStem+".log.gz", []byte(body))
	archived, err := store.Save(
		context.Background(),
		testSaveInput(sourcePath, failureCategory),
	)
	if err != nil {
		t.Fatalf("Save(%q) error = %v", fileStem, err)
	}
	return archived
}

func enabledArchivePurgeConfig(
	maxFileAgeDays int,
	maxDirSize uint64,
) pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig {
	return pkgobjectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
		Enabled:        true,
		MaxFileAgeDays: maxFileAgeDays,
		MaxDirSize:     maxDirSize,
	}
}

func newTestPurger(t *testing.T, store *Store, config PurgeConfig) *Purger {
	t.Helper()
	purger, err := NewPurger(store, NewPurgeConfigAdapter(config), clock.NewUTCClock())
	if err != nil {
		t.Fatalf("NewPurger() error = %v", err)
	}
	return purger
}

func setFileTime(t *testing.T, path string, modifiedAt time.Time) {
	t.Helper()
	if err := os.Chtimes(path, modifiedAt, modifiedAt); err != nil {
		t.Fatalf("Chtimes(%q) error = %v", path, err)
	}
}

func fileSize(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", path, err)
	}
	return uint64(info.Size())
}

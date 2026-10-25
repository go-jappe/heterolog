package compressedlogarchive

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
)

func TestPurgerCapacityUsesFileNameDateThenNumericIndex(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 21, 12, 0, 0, 0, time.UTC)
	for _, failureCategory := range archiveFailureCategories {
		for _, test := range []struct {
			name       string
			oldestStem string
			newestStem string
		}{
			{
				name:       "numeric index before lexical path",
				oldestStem: "z_20260920_2",
				newestStem: "a_20260920_10",
			},
			{
				name:       "date before index",
				oldestStem: "z_20260919_99",
				newestStem: "a_20260920_1",
			},
		} {
			t.Run(string(failureCategory)+"/"+test.name, func(t *testing.T) {
				store := newTestStore(t, pkglogger.LoggerContractSync)
				oldest := publishPurgeTestCompressedLog(t, store, test.oldestStem, failureCategory, "payload")
				newest := publishPurgeTestCompressedLog(t, store, test.newestStem, failureCategory, "payload")
				// mtimeも名前の文字列順も、意図する削除順とは逆にします。
				setFileTime(t, oldest.Path, now)
				setFileTime(t, newest.Path, now.AddDate(0, 0, -7))
				remainingBytes := fileSize(t, newest.Path)
				config := enabledArchivePurgeConfig(365, remainingBytes)
				purger := newTestPurger(t, store, PurgeConfig{Retryable: config, Permanent: config})

				report, err := purger.purgeByFailureCategory(context.Background(), failureCategory, NewFailureCategoryPurgeConfigAdapter(config), now)
				if err != nil {
					t.Fatalf("purgeByFailureCategory() error = %v", err)
				}
				if report.ScannedEntryCount != 2 || report.DeletedFileCount != 1 ||
					report.RemainingEntryCount != 1 || report.RemainingBytes != remainingBytes ||
					report.RetainedUnsafeEntryCount != 0 || report.CapacityLimitExceeded {
					t.Fatalf("purge report = %+v", report)
				}
				if _, err := os.Lstat(oldest.Path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("older filename %q remains: %v", oldest.Path, err)
				}
				assertFileBytes(t, newest.Path, []byte("payload"))
			})
		}
	}
}

func TestPurgerAgeRetainsCutoffDayRegardlessOfTimeAndIndex(t *testing.T) {
	t.Parallel()

	location := time.FixedZone("JST", 9*60*60)
	for _, failureCategory := range archiveFailureCategories {
		for _, test := range []struct {
			name string
			now  time.Time
		}{
			{name: "start of day", now: time.Date(2026, time.September, 21, 0, 1, 0, 0, location)},
			{name: "end of day", now: time.Date(2026, time.September, 21, 23, 59, 0, 0, location)},
			{name: "year boundary", now: time.Date(2026, time.January, 2, 0, 1, 0, 0, location)},
			{name: "leap day", now: time.Date(2024, time.March, 1, 23, 59, 0, 0, location)},
		} {
			t.Run(string(failureCategory)+"/"+test.name, func(t *testing.T) {
				store := newTestStore(t, pkglogger.LoggerContractSync)
				expired := publishPurgeTestCompressedLog(t, store,
					"z_"+test.now.AddDate(0, 0, -2).Format("20060102")+"_999", failureCategory, "expired")
				cutoffDate := test.now.AddDate(0, 0, -1).Format("20060102")
				retained := []ArchivedFile{
					publishPurgeTestCompressedLog(t, store, "a_"+cutoffDate+"_1", failureCategory, "cutoff first"),
					publishPurgeTestCompressedLog(t, store, "a_"+cutoffDate+"_999", failureCategory, "cutoff last"),
					publishPurgeTestCompressedLog(t, store, "a_"+test.now.Format("20060102")+"_1", failureCategory, "today"),
				}
				// 新しく保存し直した古い世代も期限を延ばさず、mtimeが古い期限当日は保持します。
				setFileTime(t, expired.Path, test.now)
				var remainingBytes uint64
				for _, archived := range retained {
					setFileTime(t, archived.Path, test.now.AddDate(-1, 0, 0))
					remainingBytes += fileSize(t, archived.Path)
				}
				config := enabledArchivePurgeConfig(1, math.MaxUint64)
				purger := newTestPurger(t, store, PurgeConfig{Retryable: config, Permanent: config})

				report, err := purger.purgeByFailureCategory(context.Background(), failureCategory, NewFailureCategoryPurgeConfigAdapter(config), test.now)
				if err != nil {
					t.Fatalf("purgeByFailureCategory() error = %v", err)
				}
				if report.ScannedEntryCount != 4 || report.DeletedFileCount != 1 ||
					report.RemainingEntryCount != len(retained) || report.RemainingBytes != remainingBytes ||
					report.RetainedUnsafeEntryCount != 0 || report.CapacityLimitExceeded {
					t.Fatalf("purge report = %+v", report)
				}
				if _, err := os.Lstat(expired.Path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("expired filename %q remains: %v", expired.Path, err)
				}
				for _, archived := range retained {
					if _, err := os.Lstat(archived.Path); err != nil {
						t.Fatalf("nonexpired filename %q was removed: %v", archived.Path, err)
					}
				}
			})
		}
	}
}

func TestPurgerRetainsInvalidFileNameDatesAndIndexesWhilePurgingValidFiles(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 21, 12, 0, 0, 0, time.UTC)
	invalidNames := []string{
		"missing-date.log.gz",
		"invalid-day_20260230_1.log.gz",
		"zero-index_20260919_0.log.gz",
		"padded-index_20260919_01.log.gz",
		"overflow-index_20260919_999999999999999999999999999.log.gz",
	}
	for _, failureCategory := range archiveFailureCategories {
		for _, mode := range []string{"age", "capacity"} {
			t.Run(string(failureCategory)+"/"+mode, func(t *testing.T) {
				store := newTestStore(t, pkglogger.LoggerContractSync)
				valid := publishPurgeTestCompressedLog(t, store, "valid_20260919_1", failureCategory, "valid")
				validBytes := fileSize(t, valid.Path)
				failureCategoryDir, err := store.Root().FailureCategoryDir(failureCategory)
				if err != nil {
					t.Fatal(err)
				}
				var retainedBytes uint64
				for _, name := range invalidNames {
					path := filepath.Join(failureCategoryDir, name)
					if err := os.WriteFile(path, []byte("unsafe"), archivedFileMode); err != nil {
						t.Fatal(err)
					}
					setFileTime(t, path, now.AddDate(-1, 0, 0))
					retainedBytes += fileSize(t, path)
				}
				config := enabledArchivePurgeConfig(1, math.MaxUint64)
				if mode == "capacity" {
					config = enabledArchivePurgeConfig(365, retainedBytes)
				}
				purger := newTestPurger(t, store, PurgeConfig{Retryable: config, Permanent: config})

				report, err := purger.purgeByFailureCategory(context.Background(), failureCategory, NewFailureCategoryPurgeConfigAdapter(config), now)
				if err == nil || len(report.Errors) != len(invalidNames) {
					t.Fatalf("purgeByFailureCategory() error = %v, report errors = %v; want one per invalid name", err, report.Errors)
				}
				if report.ScannedEntryCount != len(invalidNames)+1 ||
					report.ScannedBytes != retainedBytes+validBytes ||
					report.DeletedFileCount != 1 || report.DeletedBytes != validBytes ||
					report.RemainingEntryCount != len(invalidNames) || report.RemainingBytes != retainedBytes ||
					report.RetainedUnsafeEntryCount != len(invalidNames) || report.CapacityLimitExceeded {
					t.Fatalf("purge report = %+v", report)
				}
				if _, err := os.Lstat(valid.Path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("valid purge candidate %q remains: %v", valid.Path, err)
				}
				for _, name := range invalidNames {
					assertFileBytes(t, filepath.Join(failureCategoryDir, name), []byte("unsafe"))
				}
			})
		}
	}
}

func TestPurgerAgeThenCapacityUsesSortedCandidates(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.September, 21, 0, 30, 0, 0, time.FixedZone("JST", 9*60*60))
	for _, failureCategory := range archiveFailureCategories {
		for _, capacityExceeded := range []bool{false, true} {
			name := string(failureCategory) + "/age-only"
			if capacityExceeded {
				name = string(failureCategory) + "/age-then-capacity"
			}
			t.Run(name, func(t *testing.T) {
				store := newTestStore(t, pkglogger.LoggerContractSync)
				// 一覧の文字列順では期限内のaが先に来ます。期限判定は日付順に
				// 並べてから行い、zの期限超過分をすべて削除する必要があります。
				expired := []ArchivedFile{
					publishPurgeTestCompressedLog(t, store, "z_20260918_99", failureCategory, "payload"),
					publishPurgeTestCompressedLog(t, store, "z_20260919_1", failureCategory, "payload"),
				}
				cutoffFirst := publishPurgeTestCompressedLog(t, store, "a_20260920_2", failureCategory, "payload")
				cutoffLast := publishPurgeTestCompressedLog(t, store, "a_20260920_10", failureCategory, "payload")
				today := publishPurgeTestCompressedLog(t, store, "a_20260921_1", failureCategory, "payload")
				for _, file := range expired {
					setFileTime(t, file.Path, now)
				}
				for _, file := range []ArchivedFile{cutoffFirst, cutoffLast, today} {
					setFileTime(t, file.Path, now.AddDate(-1, 0, 0))
				}
				config := enabledArchivePurgeConfig(1, math.MaxUint64)
				retained := []ArchivedFile{cutoffFirst, cutoffLast, today}
				removed := append([]ArchivedFile(nil), expired...)
				if capacityExceeded {
					// ageのbreak対象だった期限当日のindex 2を、容量超過で削除します。
					config.MaxDirSize = fileSize(t, cutoffLast.Path) + fileSize(t, today.Path)
					retained = retained[1:]
					removed = append(removed, cutoffFirst)
				}
				purger := newTestPurger(t, store, PurgeConfig{Retryable: config, Permanent: config})
				report, err := purger.purgeByFailureCategory(context.Background(), failureCategory, NewFailureCategoryPurgeConfigAdapter(config), now)
				if err != nil {
					t.Fatal(err)
				}
				var remainingBytes uint64
				for _, file := range retained {
					assertFileBytes(t, file.Path, []byte("payload"))
					remainingBytes += fileSize(t, file.Path)
				}
				for _, file := range removed {
					if _, err := os.Lstat(file.Path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("file %q remains: %v", file.Path, err)
					}
				}
				if report.ScannedEntryCount != 5 || report.DeletedFileCount != len(removed) ||
					report.RemainingEntryCount != len(retained) || report.RemainingBytes != remainingBytes ||
					report.RetainedUnsafeEntryCount != 0 || report.CapacityLimitExceeded {
					t.Fatalf("purge report = %+v", report)
				}
			})
		}
	}
}

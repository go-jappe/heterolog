package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestManagedUploaderArchivePurgeUsesClockTimeAndTimezone(t *testing.T) {
	for _, test := range []struct {
		name        string
		location    *time.Location
		wantDeleted int
	}{
		{name: "UTC retains cutoff day", location: time.UTC, wantDeleted: 0},
		{name: "JST has advanced to next day", location: time.FixedZone("JST", 9*60*60), wantDeleted: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// UTCの18時はJSTでは翌日です。周期goroutineを作る前に仮想時刻だけを進めます。
				now := time.Now().UTC()
				year, month, day := now.Date()
				target := time.Date(year, month, day, 18, 0, 0, 0, time.UTC)
				if target.Before(now) {
					target = target.AddDate(0, 0, 1)
				}
				time.Sleep(target.Sub(now))
				now = time.Now().UTC()

				base := t.TempDir()
				storeConfig := testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{
					failurestore.DurableTypeFileArchive,
				})
				archiveConfig := &storeConfig.DurableStoreConfig.FileArchiveConfig
				archiveConfig.LifecycleInterval = 24 * time.Hour
				archiveConfig.Retryable = objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
					Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 1 << 20,
				}
				archiveConfig.Permanent = archiveConfig.Retryable
				type purgeResult struct {
					report compressedlogarchive.PurgeReport
					err    error
				}
				results := make(chan purgeResult, 2)
				managed := newTestManagedUploader(t, ManagedUploaderConfig{
					Clock:                               clock.NewFixedClock(now.In(test.location)),
					CompressedLogFileUploadFailureStore: NewConfigAdapter(storeConfig),
					FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
					ArchivePurgeObserverForRoot: func(compressedlogarchive.Root) ArchivePurgeObserver {
						return func(_ context.Context, report compressedlogarchive.PurgeReport, err error) {
							results <- purgeResult{report: report, err: err}
						}
					},
				}, &testDestinationUploader{ids: []objectstorage.DestinationID{"destination-a"}})
				receiveResult := func() purgeResult {
					t.Helper()
					synctest.Wait()
					select {
					case result := <-results:
						if result.err != nil {
							t.Fatalf("archive purge error = %v", result.err)
						}
						return result
					default:
						t.Fatal("archive purge did not report its result")
						return purgeResult{}
					}
				}
				// 初回purgeの終了を確認してから保存し、手動で次の一周期を実行します。
				receiveResult()
				categories := []uploader.FailureCategory{uploader.FailureCategoryRetryable, uploader.FailureCategoryPermanent}
				archivedFiles := make(map[uploader.FailureCategory]compressedlogarchive.ArchivedFile, len(categories))
				// UTCでは期限当日、JSTでは期限より一日前になる日付です。
				fileDate := now.AddDate(0, 0, -1).Format("20060102")
				for _, category := range categories {
					source, _ := writeCompressedLog(t, base, string(category)+"_"+fileDate+"_1.log.gz")
					archived, err := archiveRouteForTest(t, managed, "destination-a").store.Save(context.Background(), compressedlogarchive.SaveInput{
						SourcePath: source, FailureCategory: category,
					})
					if err != nil {
						t.Fatal(err)
					}
					if err := os.Chtimes(archived.Path, now, now); err != nil {
						t.Fatal(err)
					}
					archivedFiles[category] = archived
				}
				archiveRouteForTest(t, managed, "destination-a").owner.coordinator.runOnce(context.Background())
				result := receiveResult()
				for _, category := range categories {
					report := result.report.ByFailureCategory[category]
					if report.ScannedEntryCount != 1 || report.DeletedFileCount != test.wantDeleted ||
						report.RemainingEntryCount != 1-test.wantDeleted || report.RetainedUnsafeEntryCount != 0 {
						t.Fatalf("%s report = %+v, want %d deletion", category, report, test.wantDeleted)
					}
					_, err := os.Lstat(archivedFiles[category].Path)
					if test.wantDeleted == 1 && !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("%s archive remains after its filename date expired: %v", category, err)
					}
					if test.wantDeleted == 0 && err != nil {
						t.Fatalf("%s cutoff-day archive was removed: %v", category, err)
					}
				}
			})
		})
	}
}

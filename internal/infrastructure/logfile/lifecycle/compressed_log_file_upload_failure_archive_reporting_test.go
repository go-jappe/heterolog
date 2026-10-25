package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/clock"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/stretchr/testify/require"
)

func TestLifecycleCompressedLogArchivePurgeReportsThroughManagedUploader(t *testing.T) {
	for _, withUnsafeEntry := range []bool{false, true} {
		name := "success"
		if withUnsafeEntry {
			name = "failure category error"
		}
		t.Run(name, func(t *testing.T) {
			archiveConfig := objectstorage.CompressedLogFileUploadFailureArchiveConfig{
				RootPath:       resolvedTempDirForTest(t),
				LoggerContract: pkglogger.LoggerContractSync.String(), LifecycleInterval: 10 * time.Millisecond,
				Retryable: objectstorage.CompressedLogFileUploadFailureArchivePurgeConfig{
					Enabled: true, MaxFileAgeDays: 1, MaxDirSize: 1 << 20,
				},
			}
			root, err := compressedlogarchive.NewRoot(archiveConfig.RootPath, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			destinationRoot, err := root.ForDestination("s3-current")
			require.NoError(t, err)
			store, err := compressedlogarchive.NewStore(destinationRoot)
			require.NoError(t, err)
			require.NoError(t, store.EnsureDirectories(context.Background()))
			now := time.Now().UTC()
			oldPath := filepath.Join(destinationRoot.RetryableDir(), "app_"+now.AddDate(0, 0, -3).Format("20060102")+"_1.log.gz")
			require.NoError(t, os.WriteFile(oldPath, []byte("old"), 0o640))
			oldTime := time.Now().Add(-72 * time.Hour)
			require.NoError(t, os.Chtimes(oldPath, oldTime, oldTime))
			retainedPath := filepath.Join(destinationRoot.RetryableDir(), "app_"+now.Format("20060102")+"_1.log.gz")
			require.NoError(t, os.WriteFile(retainedPath, []byte("keep"), 0o640))
			if withUnsafeEntry {
				require.NoError(t, os.WriteFile(filepath.Join(destinationRoot.RetryableDir(), "unexpected.txt"), []byte("unsafe"), 0o640))
			}

			type notification struct {
				contextErr error
				report     RunReport
				err        error
			}
			reports := make(chan notification, 8)
			_, owner, err := WrapObjectStorageUploader(
				objectstorage.UploadTargetConfig{CompressedLogFileUploadFailureStore: objectstorage.CompressedLogFileUploadFailureStoreConfig{
					DurableType: []failurestore.DurableType{failurestore.DurableTypeFileArchive},
					DurableStoreConfig: objectstorage.CompressedLogFileUploadFailureDurableStoreConfig{
						FileArchiveConfig: archiveConfig,
					},
				}},
				nil,
				newDestinationUploaderRecorder("s3-current"),
				logpattern.FileNamePattern{Dir: resolvedTempDirForTest(t), Target: "app", Extension: "log"},
				clock.NewUTCClock(),
				runReporterFunc(func(ctx context.Context, report RunReport, err error) {
					select {
					case reports <- notification{contextErr: ctx.Err(), report: report, err: err}:
					default:
					}
				}),
				nil,
				false,
			)
			require.NoError(t, err)
			require.NotNil(t, owner)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "owner.Close()", owner.Close, boundedwait.ForceCloserOf(owner), boundedwait.NoError)

			for run := 0; run < 2; run++ {
				var got notification
				select {
				case got = <-reports:
				case <-time.After(boundedwait.HangTimeout):
					t.Fatal("managed uploader did not forward archive purge result")
				}
				require.NoError(t, got.contextErr, "accepted reports stay usable after their producer cycle returns")
				require.Equal(t, destinationRoot.Path(), got.report.DirectoryPath)
				require.NotNil(t, got.report.UploadFailureArchivePurge)
				require.Equal(t, PurgeReport{}, got.report.Purge)
				require.Len(t, got.report.UploadFailureArchivePurge.ByFailureCategory, 2)
				require.Equal(t, compressedlogarchive.FailureCategoryPurgeReport{}, got.report.UploadFailureArchivePurge.ByFailureCategory[uploader.FailureCategoryPermanent])
				retryable := got.report.UploadFailureArchivePurge.ByFailureCategory[uploader.FailureCategoryRetryable]
				want := compressedlogarchive.FailureCategoryPurgeReport{
					Enabled: true, ScannedEntryCount: 1, ScannedBytes: 4,
					RemainingEntryCount: 1, RemainingBytes: 4,
				}
				if run == 0 {
					want.ScannedEntryCount++
					want.ScannedBytes += 3
					want.DeletedFileCount, want.DeletedBytes = 1, 3
				}
				if withUnsafeEntry {
					want.ScannedEntryCount++
					want.ScannedBytes += 6
					want.RemainingEntryCount++
					want.RemainingBytes += 6
					want.RetainedUnsafeEntryCount = 1
					require.Len(t, retryable.Errors, 1)
					require.ErrorIs(t, got.err, retryable.Errors[0])
					var entryErr *compressedlogarchive.ArchiveEntryError
					require.ErrorAs(t, got.err, &entryErr)
					require.Equal(t, "unexpected.txt", entryErr.Name)
					want.Errors = retryable.Errors
				} else {
					require.NoError(t, got.err)
				}
				require.Equal(t, want, retryable)
			}
			require.NoFileExists(t, oldPath)
			require.FileExists(t, retainedPath)
		})
	}
}

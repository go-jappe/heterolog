package lifecycle

import (
	"context"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestCheckpointFactoriesConfigureRetryNotificationsBeforeMaintenance(t *testing.T) {
	for _, factory := range []string{"normal", "delivery", "failure archive"} {
		for _, enabled := range []bool{true, false} {
			name := factory + "/reporter"
			if !enabled {
				name = factory + "/nil reporter"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					type notification struct {
						report RunReport
						err    error
					}
					reports := make(chan notification, 4)
					var reporter RunReporter
					if enabled {
						reporter = runReporterFunc(func(_ context.Context, report RunReport, err error) {
							boundedwait.SendOrReport(t, reports, notification{report: report, err: err}, "notification")
						})
					}
					lifecycles, cfg, classes := newCheckpointNotificationFactoryLifecycles(t, factory, reporter)
					t.Cleanup(func() {
						for _, lifecycle := range lifecycles {
							_ = boundedwait.CloseInBubble(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose)
						}
					})
					require.Len(t, lifecycles, len(classes))
					type expectedNotification struct {
						sourceID string
						path     string
					}
					expected := make(map[string]expectedNotification, len(lifecycles))
					var restoreCheckpoints []func()
					for index, lifecycle := range lifecycles {
						require.Len(t, lifecycle.fileSeriesManagers, 1)
						manager := lifecycle.fileSeriesManagers[0]
						store := manager.objectStorageUpload.checkpointStore
						pattern := manager.FileNamePattern()
						sourceID := archiveCheckpointSourceID(cfg.SourcePrefix, classes[index], pattern)
						path := filepath.Join(cfg.Directory, sourceID+".log-file-uploads.json")
						expected[pattern.Dir] = expectedNotification{sourceID: sourceID, path: path}
						restoreCheckpoints = append(restoreCheckpoints, blockRunnerCheckpointDocument(t, path))
						// 構築後、RunOnceもdestinationのreconciliationもまだ実行されていません。
						_, err := store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{
							Generation: "20260927-1", Compressed: true,
						}, []objectstorage.DestinationID{"primary"})
						var pending *checkpointstore.PendingSaveError
						require.ErrorAs(t, err, &pending)
					}
					synctest.Wait()
					require.Empty(t, reports, "the initial operation returns its save error without duplicate notification")
					time.Sleep(checkpointstore.SaveRecoveryInterval)
					synctest.Wait()
					if enabled {
						require.Len(t, reports, len(expected), "every constructed source must report its first failed retry")
						for range len(expected) {
							got := boundedwait.ReceiveInBubble(t, reports, "receive from reports")
							want, ok := expected[got.report.DirectoryPath]
							require.True(t, ok, "unexpected or duplicate directory %q", got.report.DirectoryPath)
							require.Equal(t, RunReport{DirectoryPath: got.report.DirectoryPath}, got.report)
							var retryErr *logfile.CheckpointSaveRetryError
							require.ErrorAs(t, got.err, &retryErr)
							require.Equal(t, want.sourceID, retryErr.SourceID)
							require.Equal(t, want.path, retryErr.Path)
							delete(expected, got.report.DirectoryPath)
						}
					}
					require.Empty(t, reports)
					for _, restore := range restoreCheckpoints {
						restore()
					}
					for _, lifecycle := range lifecycles {
						require.NoError(t, boundedwait.CloseInBubble(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose))
					}
					synctest.Wait()
					// 残留する通知workerがあればsynctestの終了時に検出します。
					time.Sleep(checkpointstore.SaveRecoveryInterval)
					synctest.Wait()
					require.Empty(t, reports, "closed factory stores must stop retry notifications")
				})
			})
		}
	}
}

func newCheckpointNotificationFactoryLifecycles(t *testing.T, factory string, reporter RunReporter) ([]*DirectoryLifecycle, pkgfsstore.ArchiveCheckpointConfig, []string) {
	t.Helper()
	if factory == "failure archive" {
		cfg := newFailureArchiveLifecycleTestConfig(t, validFailureArchiveUploadTargetConfig())
		lifecycles, err := NewFailureArchiveLifecycles(FailureArchiveOptions{Config: cfg, Reporter: reporter})
		require.NoError(t, err)
		return lifecycles, cfg.ArchiveCheckpoint(), []string{"retryable", "permanent"}
	}
	cfg := configuredCheckpointFactoryTestConfig(t)
	var lifecycles []*DirectoryLifecycle
	var err error
	if factory == "delivery" {
		deliveryCfg := deliveryRolePurgeTestConfig(cfg)
		deliveryCfg.ObjectStorageUploadConfig = cfg.ObjectStorageUploadConfig
		deliveryCfg.ArchiveCheckpointConfig = cfg.ArchiveCheckpointConfig
		lifecycles, err = NewDeliveryFileLifecycles(DeliveryFileOptions{Config: deliveryCfg, Reporter: reporter})
	} else {
		lifecycles, err = NewLogFileLifecycles(LogFileOptions{Config: cfg, Reporter: reporter})
	}
	require.NoError(t, err)
	return lifecycles, cfg.ArchiveCheckpointConfig, []string{"default"}
}

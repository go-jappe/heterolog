package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/shutdown"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestManualRecoveryRetryRetainsFileUntilAcceptedCheckpointIsSaved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		cfg := configuredCheckpointFactoryTestConfig(t)
		cfg.FsstoreConfig.Lifecycle.EnableFileCompress = true
		cfg.FsstoreConfig.Lifecycle.CompressFormat = pkgfsstore.CompressGzip
		// ReporterによるStore wrapperを使う場合も保存状況を取得できることを確認します。
		lifecycles, err := NewLogFileRecovery(LogFileOptions{
			Config: cfg, Reporter: runReporterFunc(func(context.Context, RunReport, error) {}),
		}, WithCheckpointSaveRetry())
		require.NoError(t, err)
		lifecycle := lifecycles[0]
		closeDirectoryLifecycleOnCleanup(t, lifecycle)
		manager := lifecycle.fileSeriesManagers[0]
		destinations := manager.objectStorageUpload.uploader.(uploader.DestinationUploader).DestinationIDs()
		remote := newDestinationUploaderRecorder(destinations...)
		manager.objectStorageUpload.uploader = remote
		source := writeFileSeriesCheckpointLog(t, manager.fileName.Dir, manager.fileName.Target, currentLogDate(), 1)
		checkpointPath := filepath.Join(cfg.ArchiveCheckpointConfig.Directory,
			archiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, "default", manager.fileName)+".log-file-uploads.json")
		restore := blockRunnerCheckpointDocument(t, checkpointPath)

		first, err := lifecycle.RecoverOnce(ctx)
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		require.Len(t, first.ObjectStorageUploads, 1)
		require.Empty(t, first.UploadedFileRemovals)
		require.FileExists(t, source+".gz")
		second, err := lifecycle.RecoverOnce(ctx)
		require.ErrorAs(t, err, &pending)
		require.Zero(t, second.ScannedFiles, "保存復旧中はmemoryの完了cursorだけを根拠に削除しない")
		require.Empty(t, second.UploadedFileRemovals)
		require.FileExists(t, source+".gz")
		require.False(t, manager.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey))
		require.False(t, manager.logFileProcessingRecovery.Has(logFileProcessingRecoveryKeyForFile(fileGeneration{Period: currentLogDate(), Index: 1}, true)))

		restore()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		third, err := lifecycle.RecoverOnce(ctx)
		require.NoError(t, err)
		require.Empty(t, third.ObjectStorageUploads)
		require.Len(t, third.UploadedFileRemovals, 1)
		require.Equal(t, PurgeReport{}, third.Purge)
		require.NoFileExists(t, source+".gz")
		for _, destination := range destinations {
			require.Equal(t, []string{source + ".gz"}, remote.pathsFor(destination), "保存復旧後も再uploadしない")
		}
		require.NoError(t, boundedwait.CloseInBubble(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose))
		synctest.Wait()
	})
}

func TestManualRecoveryFactoriesEnableSaveRetryOnlyWhenRequested(t *testing.T) {
	for _, factory := range []string{"normal", "delivery", "failure archive"} {
		for _, retry := range []bool{false, true} {
			name := factory + "/once"
			if retry {
				name = factory + "/retry"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var options []RecoveryOption
					if retry {
						options = append(options, WithCheckpointSaveRetry())
					}
					var lifecycles []*DirectoryLifecycle
					var err error
					if factory == "failure archive" {
						cfg := newFailureArchiveLifecycleTestConfig(t, validFailureArchiveUploadTargetConfig())
						lifecycles, err = NewFailureArchiveRecovery(FailureArchiveOptions{Config: cfg}, options...)
					} else {
						cfg := configuredCheckpointFactoryTestConfig(t)
						if factory == "delivery" {
							deliveryCfg := deliveryRolePurgeTestConfig(cfg)
							deliveryCfg.ObjectStorageUploadConfig = cfg.ObjectStorageUploadConfig
							deliveryCfg.ArchiveCheckpointConfig = cfg.ArchiveCheckpointConfig
							lifecycles, err = NewDeliveryFileRecovery(DeliveryFileOptions{Config: deliveryCfg}, options...)
						} else {
							lifecycles, err = NewLogFileRecovery(LogFileOptions{Config: cfg}, options...)
						}
					}
					require.NoError(t, err)
					for _, lifecycle := range lifecycles {
						forceCloseLifecycleOnFailure(t, lifecycle)
					}
					for _, lifecycle := range lifecycles {
						require.Equal(t, retry, lifecycle.checkpointSaveRetryEnabled)
						manager := lifecycle.fileSeriesManagers[0]
						require.True(t, manager.manualRecovery)
						store := checkpointStoreForFactoryTest(t, manager)
						restore := blockRunnerCheckpointDocument(t, filepath.Join(store.Directory(), store.SourceID()+".log-file-uploads.json"))
						_, err := store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20261011-1", Compressed: true},
							manager.objectStorageUpload.uploader.(uploader.DestinationUploader).DestinationIDs())
						var pending *checkpointstore.PendingSaveError
						require.ErrorAs(t, err, &pending)
						restore()
						time.Sleep(checkpointstore.SaveRecoveryInterval)
						synctest.Wait()
						statusErr := store.PendingSaveError(context.Background())
						if retry {
							require.NoError(t, statusErr)
							require.NoError(t, boundedwait.CloseInBubble(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose))
						} else {
							require.ErrorAs(t, statusErr, &pending)
							require.Error(t, boundedwait.CloseInBubble(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose))
						}
						synctest.Wait()
					}
				})
			})
		}
	}
}

type manualRegistrationFailureUploader struct {
	*destinationUploaderRecorder
	beforeDestinationIDs func()
}

func (u *manualRegistrationFailureUploader) DestinationIDs() []objectstorage.DestinationID {
	u.beforeDestinationIDs()
	return u.destinationUploaderRecorder.DestinationIDs()
}

func TestManualRecoveryRetryPublishesAcceptedRegistrationSoItCanBeStopped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := configuredCheckpointFactoryTestConfig(t)
		directory := filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir)
		pattern := logpattern.FileNamePattern{Dir: directory, Target: cfg.FsstoreConfig.Layout.Target, Extension: "log"}
		storePath := filepath.Join(cfg.ArchiveCheckpointConfig.Directory,
			archiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, "default", pattern)+".log-file-uploads.json")
		remote := &manualRegistrationFailureUploader{
			destinationUploaderRecorder: newDestinationUploaderRecorder("primary"),
			beforeDestinationIDs:        func() { blockRunnerCheckpointDocument(t, storePath) },
		}
		store, err := newArchiveCheckpointStore(cfg.ArchiveCheckpointConfig, "default", pattern,
			testFileSeriesManagerConfig(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, 1, 1, 1, true).Layout(),
			remote, nil, archiveCheckpointFactoryOptions{acceptPendingSave: true, activePeriod: currentLogDate()})
		require.NoError(t, err)
		require.NotNil(t, store)
		// 途中で失敗しても、保存の再試行を止めてからbubbleを終了します。
		t.Cleanup(func() {
			if t.Failed() {
				_ = store.(interface{ ForceClose() error }).ForceClose()
				_ = boundedwait.CloseInBubble(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store))
			}
		})
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, store.(interface{ PendingSaveError(context.Context) error }).PendingSaveError(context.Background()), &pending)
		require.ErrorIs(t, store.(interface{ ForceClose() error }).ForceClose(), shutdown.ErrForcedClose)
		require.ErrorIs(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)), shutdown.ErrForcedClose)
		synctest.Wait()
		require.Empty(t, remote.pathsFor("primary"))
	})
}

func TestManualRecoveryRetryRejectsStoreWithoutSaveStatus(t *testing.T) {
	cfg := configuredCheckpointFactoryTestConfig(t)
	lifecycles, err := NewLogFileRecovery(LogFileOptions{Config: cfg}, WithCheckpointSaveRetry())
	require.NoError(t, err)
	lifecycle := lifecycles[0]
	closeDirectoryLifecycleOnCleanup(t, lifecycle)
	manager := lifecycle.fileSeriesManagers[0]
	manager.objectStorageUpload.checkpointStore = struct{ checkpointstore.Store }{manager.objectStorageUpload.checkpointStore}
	report, err := lifecycle.RecoverOnce(context.Background())
	require.ErrorIs(t, err, ErrOfflineRecoveryPendingSaveStatusUnavailable)
	require.Zero(t, report.ScannedFiles)
	require.False(t, errors.Is(err, ErrManualRecoveryRequired))
}

func TestArchiveCheckpointFactoryPanicClosesUnpublishedStoreBeforeRetry(t *testing.T) {
	for _, retry := range []bool{false, true} {
		name := "normal"
		if retry {
			name = "manual retry"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := configuredCheckpointFactoryTestConfig(t)
				config := appendlog.NewConfigAdapter(cfg.FsstoreConfig)
				pattern := logpattern.FileNamePattern{
					Dir:    filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir),
					Target: cfg.FsstoreConfig.Layout.Target, Extension: "log",
				}
				failure := errors.New("destination resolution panic")
				calls := 0
				remote := &manualRegistrationFailureUploader{
					destinationUploaderRecorder: newDestinationUploaderRecorder("primary"),
					beforeDestinationIDs: func() {
						calls++
						if calls == 1 {
							panic(failure)
						}
					},
				}
				options := archiveCheckpointFactoryOptions{acceptPendingSave: retry, activePeriod: currentLogDate()}
				reporter := runReporterFunc(func(context.Context, RunReport, error) {})
				require.PanicsWithValue(t, failure, func() {
					_, _ = newArchiveCheckpointStore(cfg.ArchiveCheckpointConfig, "default", pattern, config.Layout(), remote, reporter, options)
				})
				store, err := newArchiveCheckpointStore(cfg.ArchiveCheckpointConfig, "default", pattern, config.Layout(), remote, reporter, options)
				require.NoError(t, err)
				require.NoError(t, boundedwait.CloseInBubble(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
				boundedwait.ReceiveInBubble(t, store.(interface{ Done() <-chan struct{} }).Done(), "receive from store.(interface{ Done() <-chan struct{} }).Done()")
				boundedwait.ReceiveInBubble(t, store.(interface{ NotificationsDone() <-chan struct{} }).NotificationsDone(), "receive from store.(interface{ NotificationsDone() <-chan struct{} }).NotificationsDone()")
				require.Equal(t, 2, calls)
				synctest.Wait() // 最初の構築が通知workerを残していれば、このbubbleは終了できません。
			})
		})
	}
}

type manualPanicAfterStoreConfig struct {
	appendlog.ConfigProvider
	readyPath string
	failure   error
	panicked  bool
}

func (c *manualPanicAfterStoreConfig) Purge() appendlog.FilePurgeConfigProvider {
	if !c.panicked {
		if _, err := os.Stat(c.readyPath); err == nil {
			c.panicked = true
			panic(c.failure)
		}
	}
	return c.ConfigProvider.Purge()
}

func TestManualRecoveryFactoryPanicClosesAcquiredManagerBeforeRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := configuredCheckpointFactoryTestConfig(t)
		cfg.FsstoreConfig.Layout.IsSeparateErrorLogsEnabled = true
		cfg.FsstoreConfig.Layout.SeparateErrorLogsPrefix = "error_"
		cfg.FsstoreConfig.Layout.ErrorLogDir = "error-logs"
		cfg.ObjectStorageUploadConfig.Error = cfg.ObjectStorageUploadConfig.Default
		pattern := logpattern.FileNamePattern{
			Dir:    filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir),
			Target: cfg.FsstoreConfig.Layout.Target, Extension: "log",
		}
		failure := errors.New("configuration panic after store creation")
		provider := &manualPanicAfterStoreConfig{
			ConfigProvider: appendlog.NewConfigAdapter(cfg.FsstoreConfig),
			readyPath: filepath.Join(cfg.ArchiveCheckpointConfig.Directory,
				archiveCheckpointSourceID(cfg.ArchiveCheckpointConfig.SourcePrefix, "default", pattern)+".log-file-uploads.json"),
			failure: failure,
		}
		options := directoryLifecycleFactoryOptions{
			manualRecovery: true, checkpointSaveRetry: true, Config: provider,
			ObjectStorageUpload: cfg.ObjectStorageUploadConfig, ArchiveCheckpoint: cfg.ArchiveCheckpointConfig,
			Reporter: runReporterFunc(func(context.Context, RunReport, error) {}),
		}
		require.PanicsWithValue(t, failure, func() { _, _ = newDirectoryLifecycles(options) })
		require.True(t, provider.panicked)
		lifecycles, err := newDirectoryLifecycles(options)
		require.NoError(t, err)
		for _, lifecycle := range lifecycles {
			require.NoError(t, boundedwait.CloseInBubble(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose))
			boundedwait.ReceiveInBubble(t, lifecycle.NotificationsDone(), "receive from lifecycle.NotificationsDone()")
		}
		synctest.Wait()
	})
}

type manualFailureArchivePanicAfterStoreConfig struct {
	FailureArchiveRunnerConfigProvider
	readyPath string
	failure   error
	panicked  bool
}

func (c *manualFailureArchivePanicAfterStoreConfig) Target() string {
	if !c.panicked {
		if _, err := os.Stat(c.readyPath); err == nil {
			c.panicked = true
			panic(c.failure)
		}
	}
	return c.FailureArchiveRunnerConfigProvider.Target()
}

func TestManualFailureArchiveFactoryPanicClosesAcquiredManagerBeforeRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := newFailureArchiveLifecycleTestConfig(t, validFailureArchiveUploadTargetConfig())
		directory, err := failurearchive.GetFileArchiveDirectoryPath(cfg, cfg.DirCaseRetryable())
		require.NoError(t, err)
		pattern := logpattern.FileNamePattern{Dir: directory, Target: cfg.Target(), Extension: failurearchive.FileExtension}
		failure := errors.New("archive configuration panic after store creation")
		provider := &manualFailureArchivePanicAfterStoreConfig{
			FailureArchiveRunnerConfigProvider: cfg,
			readyPath: filepath.Join(cfg.ArchiveCheckpoint().Directory,
				archiveCheckpointSourceID(cfg.ArchiveCheckpoint().SourcePrefix, "retryable", pattern)+".log-file-uploads.json"),
			failure: failure,
		}
		options := FailureArchiveOptions{Config: provider, Reporter: runReporterFunc(func(context.Context, RunReport, error) {})}
		require.PanicsWithValue(t, failure, func() { _, _ = NewFailureArchiveRecovery(options, WithCheckpointSaveRetry()) })
		require.True(t, provider.panicked)
		lifecycles, err := NewFailureArchiveRecovery(options, WithCheckpointSaveRetry())
		require.NoError(t, err)
		for _, lifecycle := range lifecycles {
			require.NoError(t, boundedwait.CloseInBubble(t, "lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes()", lifecycle.CloseObjectStorageCompressedLogFileUploadFailureRoutes, lifecycle.ForceClose))
			boundedwait.ReceiveInBubble(t, lifecycle.NotificationsDone(), "receive from lifecycle.NotificationsDone()")
		}
		synctest.Wait()
	})
}

func TestUnpublishedRetryStoreCleanupWaitsForAcceptedNotifications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		directory := resolvedTempDirForTest(t)
		notificationStarted := make(chan struct{})
		releaseNotification := make(chan struct{})
		store, err := logfile.NewCheckpointStore(directory, "unpublished-notifications", func(error) {
			close(notificationStarted)
			<-releaseNotification
		})
		require.NoError(t, err)
		release := sync.OnceFunc(func() { close(releaseNotification) })
		// 途中で失敗しても、通知と保存の再試行を止めてからbubbleを終了します。
		t.Cleanup(func() {
			if t.Failed() {
				release()
				_ = store.(interface{ ForceClose() error }).ForceClose()
			}
		})
		blockRunnerCheckpointDocument(t, filepath.Join(directory, "unpublished-notifications.log-file-uploads.json"))
		_, err = store.AdvanceCompletionCursor(context.Background(), checkpointstore.CheckpointKey{Generation: "20261012-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		var pending *checkpointstore.PendingSaveError
		require.ErrorAs(t, err, &pending)
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		boundedwait.ReceiveInBubble(t, notificationStarted, "receive from notificationStarted")
		cleanupComplete := make(chan error, 1)
		go func() { cleanupComplete <- closeUnpublishedCheckpointStore(store, true, true) }()
		boundedwait.ReceiveInBubble(t, store.(interface{ Done() <-chan struct{} }).Done(), "receive from store.(interface{ Done() <-chan struct{} }).Done()")
		synctest.Wait()
		select {
		case err := <-cleanupComplete:
			t.Fatalf("cleanup returned while an accepted notification was still running: %v", err)
		default:
		}
		release()
		require.ErrorIs(t, boundedwait.ReceiveInBubble(t, cleanupComplete, "receive from cleanupComplete"), shutdown.ErrForcedClose)
		boundedwait.ReceiveInBubble(t, store.(interface{ NotificationsDone() <-chan struct{} }).NotificationsDone(), "receive from store.(interface{ NotificationsDone() <-chan struct{} }).NotificationsDone()")
		synctest.Wait()
	})
}

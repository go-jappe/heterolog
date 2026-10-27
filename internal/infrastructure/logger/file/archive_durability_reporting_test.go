package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	logfilelifecycle "github.com/go-jappe/heterolog/internal/infrastructure/logfile/lifecycle"
	logfilerunner "github.com/go-jappe/heterolog/internal/infrastructure/logfile/runner"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgclock "github.com/go-jappe/heterolog/pkg/clock"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgobserver "github.com/go-jappe/heterolog/pkg/observer"
)

func TestFileLoggerReportsUnconfirmedArchiveDurabilityAndContinuesWriting(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		now := time.Now().UTC()
		logger := newInlineCompressionCheckpointTestLogger(t, now, "unconfirmed-inline-archive")
		checkpointDirectory := t.TempDir()
		const sourceID = "unconfirmed_inline_archive"
		logger.archiveUploadCheckpoints = newCompressedLogCheckpointTestStore(t, checkpointDirectory, sourceID)
		// durable handoff が同期未確認を返す境界を既存 uploader fake で再現します。
		// 実 OS の同期失敗分類は fsstore 側の試験で検証します。
		syncErr := fmt.Errorf("publish archive: %w: %w", pkgfsstore.ErrDurabilityUnconfirmed, os.ErrPermission)
		remote := &sequenceArchiveUploader{errs: []error{syncErr}}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "primary", Uploader: remote},
		)
		notified := make(chan error, 1)
		logger.OnError = func(_ context.Context, err error, _ *logging.LogEntry, _ pkgobserver.Tracer, _ pkgobserver.Meter) {
			if errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
				notified <- err
			}
		}

		sourcePath := logger.Fsstore.ActiveFilePath
		// Windows の追記専用 handle は Truncate できないため、通常記録で上限まで埋めます。
		// fixture の上限は最小許容サイズの 1 MiB です。
		require.NoError(t, logger.HandleLogEntry(ctx, &logging.LogEntry{
			OccurredAt: now, Level: pkglogger.LogLevelInfo,
			Message: strings.Repeat("a", int(logger.config.Fsstore().Lifecycle().MaxFileSize())),
		}))
		waitBackgroundForTest(t, logger)
		// 通常の記録を入口に rotation/background archive を実行します。
		require.NoError(t, logger.HandleLogEntry(ctx, &logging.LogEntry{
			OccurredAt: now, Level: pkglogger.LogLevelInfo, Message: "record accepted before archive result",
		}))
		waitBackgroundForTest(t, logger)
		err := receiveForTest(t, notified, "OnError did not receive the unconfirmed archive durability error")
		require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
		require.ErrorIs(t, err, os.ErrPermission)
		compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
		require.FileExists(t, compressedPath)
		require.Equal(t, 1, remote.attemptCount())
		// 同期未確認の宛先を成功として永続化していないことを別 Store から確認します。
		restarted := newCompressedLogCheckpointTestStore(t, checkpointDirectory, sourceID)
		checkpoint, err := restarted.Load(ctx, inlineCompressedLogCheckpointKey(t, logger, compressedPath))
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.PendingDestinationIDs([]objectstorage.DestinationID{"primary"}))

		require.NoError(t, logger.HandleLogEntry(ctx, &logging.LogEntry{
			OccurredAt: now, Level: pkglogger.LogLevelInfo, Message: "normal write after unconfirmed archive",
		}))
		waitBackgroundForTest(t, logger)
		assertLogFileContains(t, logger.Fsstore.ActiveFilePath, "normal write after unconfirmed archive")
		require.FileExists(t, compressedPath)

		logger.Clock = pkgclock.NewFixedClock(now.Add(2 * time.Hour))
		require.NoError(t, purgeFileLoggerForTest(ctx, logger))
		requireFileLoggerClosesForTest(t, logger)
		require.NoFileExists(t, compressedPath)
		require.Equal(t, 2, remote.attemptCount())
	})
}

func TestDeliveryArchiveDurabilityFailureIsReportedAfterSuccessfulDelivery(t *testing.T) {
	synctestFromWallClock(t, func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		const target = "unconfirmed-delivery-archive"
		cfg := newTestDeliveryFileLoggerConfig(root, "logs", target)
		cfg.FsstoreConfig.Rotation.EnableRotate = true
		logger, err := NewDeliveryFileLogger(ctx, NewDeliveryFileLoggerConfigAdapter(cfg), nil, nil)
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "logger Close", func() closerForTest { return logger }, true)
		period, err := time.Parse(pkgfsstore.FileDateFormat, logger.Fsstore.FileName.Date)
		require.NoError(t, err)
		fileClock := pkgclock.NewFixedClock(period.Add(12 * time.Hour))
		logger.Clock = fileClock

		sourcePath := logger.Fsstore.ActiveFilePath
		// Delivery も通常の追記で上限へ到達させ、次の記録で実際に rotation します。
		filledDelivery, err := logger.HandleLogEntry(ctx, &logging.LogEntry{
			OccurredAt: fileClock.Now(), Level: pkglogger.LogLevelInfo,
			Message: strings.Repeat("a", int(cfg.FsstoreConfig.Rotation.MaxFileSize)),
		})
		requireDeliverySucceeded(t, filledDelivery, err)
		delivery, deliveryErr := logger.HandleLogEntry(ctx, &logging.LogEntry{
			OccurredAt: fileClock.Now(), Level: pkglogger.LogLevelInfo, Message: "delivery before archive maintenance",
		})
		requireDeliverySucceeded(t, delivery, deliveryErr)
		require.NotEqual(t, sourcePath, logger.Fsstore.ActiveFilePath)

		lifecycleConfig := newTestFileLoggerConfig(root, "logs", target).FsstoreConfig
		lifecycleConfig.Lifecycle.EnableFileCompress = true
		lifecycleConfig.Lifecycle.MaxFileAge = 36500
		lifecycleConfig.Duration.CompressWarnThresholdDuration = time.Hour
		lifecycleConfig.Duration.CompressErrorThresholdDuration = 2 * time.Hour
		syncErr := fmt.Errorf("publish delivery archive: %w: %w", pkgfsstore.ErrDurabilityUnconfirmed, os.ErrPermission)
		remote := &sequenceArchiveUploader{errs: []error{syncErr}}
		destinations := newMultiArchiveUploader(t, uploader.Destination{ID: "primary", Uploader: remote})
		store := newCompressedLogCheckpointTestStore(t, t.TempDir(), "unconfirmed_delivery_archive")
		require.NoError(t, store.RegisterDestinations(ctx, destinations.DestinationIDs(), nil))
		manager := logfilelifecycle.NewFileSeriesManager(logfilelifecycle.FileSeriesManagerOptions{
			FileName: logpattern.FileNamePattern{Dir: logger.Fsstore.FileName.Dir, Target: target, Extension: "log"},
			Config:   appendlog.NewConfigAdapter(lifecycleConfig), Clock: fileClock,
			Uploader: destinations, CheckpointStore: store,
		})
		t.Cleanup(func() {
			_ = manager.ForceClose()
			closeWithinBoundForTest(t, boundedwait.BubbleTimeout, "file series manager Close", manager, false)
		})
		lifecycle, err := logfilelifecycle.NewDirectoryLifecycle(manager)
		require.NoError(t, err)
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		type result struct {
			report logfilelifecycle.RunReport
			err    error
		}
		reported := make(chan result, 1)
		runner := logfilerunner.NewRunner(lifecycle,
			logfilerunner.StaticRunnerConfig{IntervalValue: time.Millisecond},
			archiveDurabilityRunReporter(func(_ context.Context, report logfilelifecycle.RunReport, err error) {
				reported <- result{report: report, err: err}
				cancel()
			}),
		)
		finished := make(chan error, 1)
		go func() { finished <- runner.Run(runCtx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-finished:
			case <-time.After(hangTimeoutForTest):
				t.Error("archive runner did not stop")
			}
		})
		got := receiveForTest(t, reported, "archive runner did not report unconfirmed durability")
		require.ErrorIs(t, got.err, pkgfsstore.ErrDurabilityUnconfirmed)
		require.ErrorIs(t, got.err, os.ErrPermission)
		require.Empty(t, got.report.ObjectStorageUploads)
		require.Empty(t, got.report.UploadedFileRemovals)
		// 保守側の失敗は受理済み DeliveryReport を後から失敗へ書き換えません。
		requireDeliverySucceeded(t, delivery, deliveryErr)
		compressedPath := sourcePath + pkgfsstore.ExtensionCompressGzip
		require.FileExists(t, compressedPath)
		nextDelivery, err := logger.HandleLogEntry(ctx, &logging.LogEntry{
			OccurredAt: fileClock.Now(), Level: pkglogger.LogLevelInfo, Message: "delivery after archive maintenance error",
		})
		requireDeliverySucceeded(t, nextDelivery, err)
		assertLogFileContains(t, logger.Fsstore.ActiveFilePath, "delivery after archive maintenance error")

		require.NoError(t, manager.CloseLogFileProcessingRecovery())
		require.NoFileExists(t, compressedPath)
		require.Equal(t, 2, remote.attemptCount())
	})
}

type archiveDurabilityRunReporter func(context.Context, logfilelifecycle.RunReport, error)

func (f archiveDurabilityRunReporter) ReportRun(ctx context.Context, report logfilelifecycle.RunReport, err error) {
	f(ctx, report, err)
}

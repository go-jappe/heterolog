package file

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	checkpointtest "github.com/go-jappe/heterolog/internal/testutil/checkpoint"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"

	"github.com/stretchr/testify/require"
)

func TestInlineCheckpointPersistenceFailureDoesNotBlockCompletedFileRemoval(t *testing.T) {
	for _, method := range []string{"AdvanceCompletionCursor", "OtherFile"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			synctestFromWallClock(t, func(t *testing.T) {
				logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
				directory := t.TempDir()
				filesystem, err := checkpointstore.NewFilesystemStore(directory, "nonblocking_save")
				require.NoError(t, err)
				closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "filesystem Close", func() closerForTest { return filesystem }, false)
				store.Store = filesystem
				ctx := context.Background()
				checkpointPath := filepath.Join(directory, "nonblocking_save.log-file-uploads.json")
				restore := checkpointtest.BlockPublication(t, checkpointPath)
				if method == "OtherFile" {
					_, err = filesystem.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: "20260919-98", Compressed: true}, []objectstorage.DestinationID{"primary"})
					require.Error(t, err)
				}
				notifications := &backgroundTaskNotificationQueue{}
				require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), notifications.enqueue))
				require.NotEmpty(t, notifications.notifications, "persistence failure must still be reported")
				require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
				require.NoFileExists(t, path)
				checkpoint, err := store.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
				logger.lifecycleGuard.WithProtectedPaths(func(protected map[string]struct{}) { require.NotContains(t, protected, path) })

				// Another file progresses while the same checkpoint file remains unwritable.
				nextPath := writePendingCompressedLog(t, logger, "20260920", 2, time.Now())
				require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{nextPath}, logger.Fsstore.FileName), notifications.enqueue))
				require.NoFileExists(t, nextPath)
				restore()
				require.EqualValues(t, 2, store.records.Load())
				require.Equal(t, 2, remote.attemptCount())
				expected := []checkpointstore.Checkpoint{{Key: inlineCompressedLogCheckpointKey(t, logger, nextPath), CompletedDestinationIDs: []objectstorage.DestinationID{"primary"}}}
				// No caller retry or new mutation: the Store saves the latest state on its own.
				requireArchiveRecoveryClosesForTest(t, logger)
				restarted := checkpointtest.WaitForContentsAndRestart(t, checkpointPath, "nonblocking_save", filesystem, expected)
				checkpoint, err = restarted.Load(ctx, key)
				require.NoError(t, err)
				require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
			})
		})
	}
}

func TestInlineCheckpointSaveFailureRetainsPartialUploadAndSkipsCompletedDestination(t *testing.T) {
	t.Parallel()
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, primary, path, key := newInlineLogFileProcessingRecoveryFixture(t)
		directory := t.TempDir()
		filesystem, err := checkpointstore.NewFilesystemStore(directory, "partial_save")
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "filesystem Close", func() closerForTest { return filesystem }, false)
		store.Store = filesystem
		secondary := &sequenceArchiveUploader{errs: []error{errors.New("remote unavailable")}}
		logger.ArchiveUploader = newMultiArchiveUploader(t,
			uploader.Destination{ID: "primary", Uploader: primary},
			uploader.Destination{ID: "secondary", Uploader: secondary},
		)
		checkpointPath := filepath.Join(directory, "partial_save.log-file-uploads.json")
		restore := checkpointtest.BlockPublication(t, checkpointPath)
		ctx := context.Background()
		require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		require.FileExists(t, path)
		require.True(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		checkpoint, err := store.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
		require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		requireArchiveRecoveryClosesForTest(t, logger)
		require.NoFileExists(t, path)
		require.Equal(t, 1, primary.attemptCount())
		require.Equal(t, 2, secondary.attemptCount())
		restore()
		checkpointtest.WaitForContentsAndRestart(t, checkpointPath, "partial_save", filesystem, []checkpointstore.Checkpoint{{Key: key, CompletedDestinationIDs: []objectstorage.DestinationID{"primary", "secondary"}}})
	})
}

func TestInlineCheckpointCloseWaitsForPersistence(t *testing.T) {
	t.Parallel()
	synctestFromWallClock(t, func(t *testing.T) {
		logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
		directory := t.TempDir()
		filesystem, err := checkpointstore.NewFilesystemStore(directory, "close_pending")
		require.NoError(t, err)
		store.Store = filesystem
		checkpointPath := filepath.Join(directory, "close_pending.log-file-uploads.json")
		restore := checkpointtest.BlockPublication(t, checkpointPath)
		require.False(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
		require.NoFileExists(t, path)
		require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
		closed := make(chan error, 1)
		go func() { closed <- logger.Close() }()
		synctest.Wait()
		time.Sleep(2 * checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		require.Empty(t, closed, "shutdown waits until accepted completion is durable")
		require.Equal(t, 1, remote.attemptCount())
		restore()
		time.Sleep(checkpointstore.SaveRecoveryInterval)
		synctest.Wait()
		requireStartedCloseCompletesForTest(t, logger, closed)
		restarted, err := checkpointstore.NewFilesystemStore(directory, "close_pending")
		require.NoError(t, err)
		closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "restarted Close", func() closerForTest { return restarted }, true)
		checkpoint, err := restarted.Load(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, []objectstorage.DestinationID{"primary"}, checkpoint.CompletedDestinationIDs)
		paths, err := filepath.Glob(filepath.Join(logger.Fsstore.FileName.Dir, "*.log.gz"))
		require.NoError(t, err)
		require.Empty(t, paths, "the removed file cannot be rediscovered for upload after restart")
	})
}

func TestInlineDurableHandoffDoesNotWaitForCheckpointPersistence(t *testing.T) {
	for _, types := range [][]failurestore.DurableType{
		{failurestore.DurableTypeSpool},
		{failurestore.DurableTypeFileArchive},
		{failurestore.DurableTypeSpool, failurestore.DurableTypeFileArchive},
	} {
		t.Run(fmt.Sprint(types), func(t *testing.T) {
			t.Parallel()
			synctestFromWallClock(t, func(t *testing.T) {
				logger, store, _, path, key := newInlineLogFileProcessingRecoveryFixture(t)
				base := t.TempDir()
				cfg := archiveUploaderCompressedLogFileUploadFailureStoreConfig(base, pkglogger.LoggerContractSync, time.Hour)
				cfg.Default.CompressedLogFileUploadFailureStore.DurableType = types
				cfg.Default.CompressedLogFileUploadFailureStore.DurableStoreConfig.FileArchiveConfig = objectstorage.CompressedLogFileUploadFailureArchiveConfig{
					RootPath:       base,
					LoggerContract: pkglogger.LoggerContractSync.String(), LifecycleInterval: time.Hour,
				}
				remote := &archiveUploadDestinationStub{ids: []objectstorage.DestinationID{"s3-current"}, err: errors.New("remote unavailable")}
				managed, err := newLogFileUploaderWithFailureStorage(objectstorage.NewUploadTargetConfigAdapter(cfg.Default), logger.Fsstore.FileName, remote, nil, archiveUploaderTestSpoolRuntimes(t, cfg))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, closeArchiveUploader(managed)) })
				logger.ArchiveUploader = managed
				payload, err := os.ReadFile(path)
				require.NoError(t, err)
				directory := t.TempDir()
				filesystem, err := checkpointstore.NewFilesystemStore(directory, "handoff_save")
				require.NoError(t, err)
				closeOnCleanupWithinForTest(t, boundedwait.BubbleTimeout, "filesystem Close", func() closerForTest { return filesystem }, false)
				store.Store = filesystem
				checkpointPath := filepath.Join(directory, "handoff_save.log-file-uploads.json")
				restore := checkpointtest.BlockPublication(t, checkpointPath)
				require.False(t, logger.processRemainingLogFiles(context.Background(), archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
				require.NoFileExists(t, path)
				require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
				require.Equal(t, 1, remote.callCount())
				if slices.Contains(types, failurestore.DurableTypeSpool) {
					root, err := objectstoragespool.NewRoot(base, "heterolog", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
					require.NoError(t, err)
					root, err = root.ForDestination("s3-current")
					require.NoError(t, err)
					stored, err := os.ReadFile(filepath.Join(root.ActiveDir(), filepath.Base(path)))
					require.NoError(t, err)
					require.Equal(t, payload, stored)
				}
				if slices.Contains(types, failurestore.DurableTypeFileArchive) {
					root, err := compressedlogarchive.NewRoot(base, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
					require.NoError(t, err)
					root, err = root.ForDestination("s3-current")
					require.NoError(t, err)
					stored, err := os.ReadFile(filepath.Join(root.RetryableDir(), filepath.Base(path)))
					require.NoError(t, err)
					require.Equal(t, payload, stored)
				}
				restore()
				requireArchiveRecoveryClosesForTest(t, logger)
				checkpointtest.WaitForContentsAndRestart(t, checkpointPath, "handoff_save", filesystem, []checkpointstore.Checkpoint{{Key: key, CompletedDestinationIDs: remote.ids}})
			})
		})
	}
}

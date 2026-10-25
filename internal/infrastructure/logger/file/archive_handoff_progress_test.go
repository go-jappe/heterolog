package file

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgs3 "github.com/go-jappe/heterolog/pkg/objectstorage/uploader/s3"
)

func TestInlineHandoffResumesOnlyUnfinishedStoreAfterRestart(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		name := "retryable"
		category := uploader.FailureCategoryRetryable
		var remoteErr error = errors.New("temporary upload failure")
		if permanent {
			name = "permanent"
			category = uploader.FailureCategoryPermanent
			remoteErr = &pkgs3.HTTPStatusError{Code: http.StatusForbidden}
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			const destination = objectstorage.DestinationID("s3-current")
			const sourceID = "inline-handoff-progress"
			now := time.Date(2026, time.September, 28, 12, 0, 0, 0, time.UTC)
			logger := newInlineUsageTestLogger(t, now, "handoff-progress")
			checkpointDirectory := t.TempDir()
			store := newCompressedLogCheckpointTestStore(t, checkpointDirectory, sourceID)
			logger.archiveUploadCheckpoints = store
			require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{destination}, nil))
			path := writePendingCompressedLog(t, logger, "20260927", 1, now)
			key := inlineCompressedLogCheckpointKey(t, logger, path)
			payload, err := os.ReadFile(path)
			require.NoError(t, err)

			base := t.TempDir()
			cfg := archiveUploaderCompressedLogFileUploadFailureStoreConfig(base, pkglogger.LoggerContractSync, time.Hour)
			cfg.Default.CompressedLogFileUploadFailureStore.DurableType = []failurestore.DurableType{
				failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool,
			}
			cfg.Default.CompressedLogFileUploadFailureStore.DurableStoreConfig.FileArchiveConfig = objectstorage.CompressedLogFileUploadFailureArchiveConfig{
				RootPath:       base,
				LoggerContract: pkglogger.LoggerContractSync.String(), LifecycleInterval: time.Hour,
			}
			runtime := archiveUploaderTestSpoolRuntimes(t, cfg)
			remote := &archiveUploadDestinationStub{ids: []objectstorage.DestinationID{destination}, err: remoteErr, failureCategory: category}
			managed, err := newLogFileUploaderWithFailureStorage(objectstorage.NewUploadTargetConfigAdapter(cfg.Default), logger.Fsstore.FileName, remote, logger, runtime)
			require.NoError(t, err)
			logger.ArchiveUploader = managed
			t.Cleanup(func() { require.NoError(t, closeArchiveUploader(logger.ArchiveUploader)) })

			spoolRoot, err := objectstoragespool.NewRoot(base, "heterolog", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			spoolRoot, err = spoolRoot.ForDestination(destination)
			require.NoError(t, err)
			staging := filepath.Join(spoolRoot.Path(), ".staging")
			require.NoError(t, os.Remove(staging))
			require.NoError(t, os.WriteFile(staging, []byte("blocks spool saves"), 0o600))
			require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
			require.FileExists(t, path)
			require.Equal(t, 1, remote.callCount())
			current, err := store.Load(ctx, key)
			require.NoError(t, err)
			require.False(t, current.DestinationComplete(destination), "archive alone must not advance the cursor")

			archiveRoot, err := compressedlogarchive.NewRoot(base, objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			archiveRoot, err = archiveRoot.ForDestination(destination)
			require.NoError(t, err)
			archiveDirectory, spoolDirectory := archiveRoot.RetryableDir(), spoolRoot.ActiveDir()
			if permanent {
				archiveDirectory, spoolDirectory = archiveRoot.PermanentDir(), spoolRoot.DeadDir()
			}
			archivedPath := filepath.Join(archiveDirectory, filepath.Base(path))
			archived, err := os.ReadFile(archivedPath)
			require.NoError(t, err)
			require.Equal(t, payload, archived)
			// Reusing the archive would refresh its timestamp. A saved step must be skipped entirely.
			unchangedTime := now.AddDate(-1, 0, 0)
			require.NoError(t, os.Chtimes(archivedPath, unchangedTime, unchangedTime))
			require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
			require.Equal(t, 1, remote.callCount(), "a failed spool save must not repeat the remote upload")
			info, err := os.Stat(archivedPath)
			require.NoError(t, err)
			require.True(t, info.ModTime().Equal(unchangedTime))

			// Reopen both the producer checkpoint and its uploader, as on process restart.
			restartInlineRecoveryForTest(t, logger)
			require.NoError(t, closeArchiveUploader(managed))
			require.NoError(t, store.Close())
			require.NoError(t, os.Remove(staging))
			require.NoError(t, os.Mkdir(staging, 0o700))
			reopened := newCompressedLogCheckpointTestStore(t, checkpointDirectory, sourceID)
			logger.archiveUploadCheckpoints = reopened
			restartedRemote := &archiveUploadDestinationStub{ids: []objectstorage.DestinationID{destination}, err: remoteErr, failureCategory: category}
			logger.ArchiveUploader, err = newLogFileUploaderWithFailureStorage(objectstorage.NewUploadTargetConfigAdapter(cfg.Default), logger.Fsstore.FileName, restartedRemote, logger, runtime)
			require.NoError(t, err)
			require.False(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
			require.Zero(t, restartedRemote.callCount())
			require.NoFileExists(t, path)
			spooled, err := os.ReadFile(filepath.Join(spoolDirectory, filepath.Base(path)))
			require.NoError(t, err)
			require.Equal(t, payload, spooled)
			info, err = os.Stat(archivedPath)
			require.NoError(t, err)
			require.True(t, info.ModTime().Equal(unchangedTime), "restart must retain the completed archive step")
			current, err = reopened.Load(ctx, key)
			require.NoError(t, err)
			require.True(t, current.DestinationComplete(destination))
			_, found, err := reopened.LoadUploadProgress(ctx, key, destination)
			require.NoError(t, err)
			require.False(t, found, "completed cursor replaces the intermediate progress")
		})
	}
}

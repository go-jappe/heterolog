package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	infraobjectstorage "github.com/go-jappe/heterolog/internal/infrastructure/objectstorage"
	"github.com/go-jappe/heterolog/internal/testutil/uploadertest"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/stretchr/testify/require"
)

type parallelDestinationUploader struct {
	*testDestinationUploader
	entered chan objectstorage.DestinationID
	release <-chan struct{}
}

func (u *parallelDestinationUploader) UploadDestinations(ctx context.Context, input uploader.UploadRequest, ids []objectstorage.DestinationID) uploader.UploadReport {
	u.entered <- ids[0]
	select {
	case <-u.release:
		return u.testDestinationUploader.UploadDestinations(ctx, input, ids)
	case <-ctx.Done():
		return failedUploadReport(ids, ctx.Err())
	}
}

func TestManagedUploaderMultipleDestinationsResumeOnlyFailedSpoolAfterRestart(t *testing.T) {
	ctx := context.Background()
	base, checkpointDir := t.TempDir(), t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260929_1.log.gz")
	ids := []objectstorage.DestinationID{"destination-a", "destination-b"}
	store := newUploadProgressStore(t, checkpointDir)
	require.NoError(t, store.RegisterDestinations(ctx, ids, nil))
	runtime := newTestSpoolRuntimes(t, filepath.Join(base, "spool"))
	config := ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive, failurestore.DurableTypeSpool})),
		SpoolRuntimes:                       runtime, CheckpointStore: store,
		FileName: logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
	}
	release := make(chan struct{})
	remote := &parallelDestinationUploader{testDestinationUploader: &testDestinationUploader{
		ids: ids, errs: map[objectstorage.DestinationID]error{ids[0]: errors.New("offline-a"), ids[1]: errors.New("offline-b")},
	}, entered: make(chan objectstorage.DestinationID, 2), release: release}
	managed := newTestManagedUploader(t, config, remote)
	rootA, rootB := spoolRouteForTest(t, managed, ids[0]).Root(), spoolRouteForTest(t, managed, ids[1]).Root()
	stagingB := filepath.Join(rootB.Path(), ".staging")
	require.NoError(t, os.Remove(stagingB))
	require.NoError(t, os.WriteFile(stagingB, []byte("block spool b"), 0o600))
	result := make(chan uploader.UploadReport, 1)
	go func() { result <- managed.UploadDestinations(ctx, uploadertest.Request(t, source), ids) }()
	for range 2 {
		select {
		case <-remote.entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("destinations were not started in parallel")
		}
	}
	close(release)
	report := <-result
	require.NoError(t, report.Results[0].Err)
	require.Error(t, report.Results[1].Err)
	assertFileBytes(t, filepath.Join(rootA.ActiveDir(), filepath.Base(source)), payload)
	require.NoFileExists(t, filepath.Join(rootB.ActiveDir(), filepath.Base(source)))
	archiveA := archiveRouteForTest(t, managed, ids[0]).store.Root()
	archiveB := archiveRouteForTest(t, managed, ids[1]).store.Root()
	require.NotEqual(t, archiveA.Path(), archiveB.Path())
	assertFileBytes(t, filepath.Join(archiveA.RetryableDir(), filepath.Base(source)), payload)
	assertFileBytes(t, filepath.Join(archiveB.RetryableDir(), filepath.Base(source)), payload)
	key := checkpoint.CheckpointKey{Generation: "20260929-1", Compressed: true}
	for _, id := range ids {
		progress, found, err := store.LoadUploadProgress(ctx, key, id)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, checkpoint.UploadResultRetryableFailure, progress.UploadResult)
		if id == ids[0] {
			require.Len(t, progress.SavedTargets, 2)
		} else {
			require.Len(t, progress.SavedTargets, 1)
		}
	}
	// producerは成功した宛先だけ完了させます。次回はBの未完了spoolだけを保存します。
	_, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{ids[0]})
	require.NoError(t, err)
	require.NoError(t, managed.Close())
	require.NoError(t, runtime.Close())
	require.NoError(t, store.Close())
	require.NoError(t, os.Remove(stagingB))
	config.CheckpointStore = newUploadProgressStore(t, checkpointDir)
	config.SpoolRuntimes = newTestSpoolRuntimes(t, filepath.Join(base, "spool"))
	restartedRemote := &testDestinationUploader{ids: ids}
	restarted := newTestManagedUploader(t, config, restartedRemote)
	require.NoError(t, restarted.UploadDestinations(ctx, uploadertest.Request(t, source), ids).Err())
	require.Zero(t, restartedRemote.callCount(), "neither destination may be uploaded again")
	assertFileBytes(t, filepath.Join(rootB.ActiveDir(), filepath.Base(source)), payload)
}

func TestManagedUploaderUsesEachBackendFailureClassification(t *testing.T) {
	base := t.TempDir()
	source, payload := writeCompressedLog(t, base, "app_20260930_2.log.gz")
	backendErr := errors.New("backend-specific failure")
	var destinations []uploader.Destination
	for _, category := range []uploader.FailureCategory{uploader.FailureCategoryRetryable, uploader.FailureCategoryPermanent} {
		id := objectstorage.DestinationID("backend-" + string(category))
		destinations = append(destinations, uploader.Destination{
			ID:              id,
			Service:         objectstorage.UploadTargetS3,
			Uploader:        &testDestinationUploader{ids: []objectstorage.DestinationID{id}, errs: map[objectstorage.DestinationID]error{id: backendErr}},
			ClassifyFailure: func(context.Context, error) uploader.FailureCategory { return category },
		})
	}
	remote, err := infraobjectstorage.NewMultiUploader(destinations...)
	require.NoError(t, err)
	var notifications []Notification
	managed := newTestManagedUploader(t, ManagedUploaderConfig{
		CompressedLogFileUploadFailureStore: NewConfigAdapter(testFailureStoreConfig(filepath.Join(base, "archive"), []failurestore.DurableType{failurestore.DurableTypeFileArchive})),
		FileName:                            logpattern.FileNamePattern{Dir: base, Target: "app", Extension: ".log"},
		Notifier: func(_ context.Context, notification Notification) {
			notifications = append(notifications, notification)
		},
	}, remote)
	require.NoError(t, managed.Upload(context.Background(), uploadertest.Request(t, source)))
	require.Len(t, notifications, 2)
	for _, notification := range notifications {
		wantCategory := uploader.FailureCategoryRetryable
		if notification.DestinationID == "backend-permanent" {
			wantCategory = uploader.FailureCategoryPermanent
		}
		require.Equal(t, wantCategory, notification.FailureCategory)
		archive := archiveDetailsForTest(notification)
		require.Equal(t, wantCategory, archive.FailureCategory)
		require.Equal(t, string(wantCategory), filepath.Base(filepath.Dir(archive.FilePath)))
		assertFileBytes(t, archive.FilePath, payload)
	}
}

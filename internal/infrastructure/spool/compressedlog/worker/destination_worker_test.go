package worker

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	replaytest "github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog/internal/testutil"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestDestinationWorkerRetainsOrphansAndResumesOnlyMatchingDestination(t *testing.T) {
	ctx := context.Background()
	stores, err := objectstoragespool.NewDestinationStores(replaytest.NewStore(t).Root())
	require.NoError(t, err)
	oldStore, err := stores.ForDestination("old-bucket")
	require.NoError(t, err)
	currentStore, err := stores.ForDestination("new-bucket")
	require.NoError(t, err)
	const name = "app_20260929_1.log.gz"
	replaytest.SaveFile(t, oldStore, name)
	orphan, claimed, err := oldStore.ClaimOldestActive(ctx)
	require.NoError(t, err)
	require.True(t, claimed)
	originalInfo, err := os.Stat(orphan.Path)
	require.NoError(t, err)
	replaytest.SaveFile(t, currentStore, name)
	currentUpload := &replaytest.Uploader{DestinationID: "new-bucket"}
	routes := RouteRegistry{{FileName: replaytest.FileNamePattern("app_"), Uploader: currentUpload}}
	w, err := NewDestinationWorker(stores, nil, time.Minute, WithRouteSnapshot(func() RouteRegistry { return routes }))
	require.NoError(t, err)
	report, err := w.RunOnce(ctx)
	require.ErrorIs(t, err, ErrNoMatchingRoute)
	require.Equal(t, objectstorage.DestinationID("new-bucket"), report.File.DestinationID)
	require.True(t, report.File.UploadSucceeded, "orphan must not block a configured destination")
	unchanged, err := os.Stat(orphan.Path)
	require.NoError(t, err)
	require.Equal(t, originalInfo.ModTime(), unchanged.ModTime(), "orphan must not be recovered or requeued")
	require.Equal(t, 1, currentUpload.CallCount())
	oldUpload := &replaytest.Uploader{DestinationID: "old-bucket"}
	routes = append(routes, RouteRegistration{FileName: replaytest.FileNamePattern("app_"), Uploader: oldUpload})
	report, err = w.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, report.RecoveredProcessingCount)
	require.Equal(t, objectstorage.DestinationID("old-bucket"), report.File.DestinationID)
	require.Equal(t, 1, oldUpload.CallCount())
	require.Equal(t, 1, currentUpload.CallCount(), "must not replay to the replacement destination")
	require.Equal(t, name, replaytest.OnlyFile(t, oldStore, pkgspool.StateDone).FileName)
}

func TestDestinationWorkerFailureDoesNotStarveOtherDestination(t *testing.T) {
	stores, err := objectstoragespool.NewDestinationStores(replaytest.NewStore(t).Root())
	require.NoError(t, err)
	routes := RouteRegistry{}
	for _, id := range []objectstorage.DestinationID{"a", "b"} {
		store, err := stores.ForDestination(id)
		require.NoError(t, err)
		replaytest.SaveFile(t, store, "app_20260929_1.log.gz")
		upload := &replaytest.Uploader{DestinationID: id}
		if id == "a" {
			upload.Err = errors.New("unavailable")
		}
		routes = append(routes, RouteRegistration{FileName: replaytest.FileNamePattern("app_"), Uploader: upload})
	}
	w, err := NewDestinationWorker(stores, routes, time.Minute)
	require.NoError(t, err)
	first, err := w.RunOnce(context.Background())
	require.Error(t, err)
	require.Equal(t, objectstorage.DestinationID("a"), first.File.DestinationID)
	second, err := w.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, objectstorage.DestinationID("b"), second.File.DestinationID)
	require.True(t, second.File.UploadSucceeded)
}

func TestWorkerRetainsFileWhenUploaderDoesNotMatchSpoolDestination(t *testing.T) {
	stores, err := objectstoragespool.NewDestinationStores(replaytest.NewStore(t).Root())
	require.NoError(t, err)
	store, err := stores.ForDestination("original")
	require.NoError(t, err)
	replaytest.SaveFile(t, store, "app_20260929_1.log.gz")
	upload := &replaytest.Uploader{DestinationID: "other"}
	worker, err := NewWorker(store, RouteRegistry{{
		FileName: replaytest.FileNamePattern("app_"), Uploader: upload,
	}}, time.Minute)
	require.NoError(t, err)

	report, err := worker.RunOnce(context.Background())
	require.ErrorIs(t, err, ErrNoMatchingRoute)
	require.Equal(t, objectstorage.DestinationID("original"), report.File.DestinationID)
	require.Equal(t, pkgspool.StateActive, report.File.State)
	require.Equal(t, DeferredReasonRouteUnavailable, report.File.DeferredReason)
	require.Equal(t, 0, upload.CallCount(), "must not send the file to a different destination")
	_ = replaytest.OnlyFile(t, store, pkgspool.StateActive)
}

package logger

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	objectstoragespool "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/objectstorage"
	"github.com/go-jappe/heterolog/internal/infrastructure/http/retryable"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"

	"github.com/stretchr/testify/require"
)

func TestCompressedLogSpoolAggregateWaitsForActualForcedShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		runtime, route := newAggregateShutdownRuntime(t, func(context.Context, *compressedlog.RunReport, *objectstoragespool.TerminalPurgeReport, error) {
			close(entered)
			<-release
		})
		owner := &compressedLogSpoolRuntimes{runtimes: []*compressedlog.RootRuntime{runtime}}
		reported := make(chan struct{})
		go func() {
			runtime.ReportSpoolRunnerRun(context.Background(), compressedlog.RunReport{}, nil)
			close(reported)
		}()
		<-entered
		require.ErrorIs(t, runtime.RequestForceClose(), pkgshutdown.ErrForcedClose)
		closed := make(chan error, 1)
		go func() { closed <- owner.Close() }()
		routeClosed := make(chan error, 1)
		go func() { routeClosed <- route.Close() }()
		synctest.Wait()
		require.Empty(t, closed, "the aggregate must retain the root until its accepted notification finishes")
		require.False(t, runtime.ShutdownComplete())
		unblock()
		<-reported
		require.NoError(t, <-routeClosed)
		require.ErrorIs(t, <-closed, pkgshutdown.ErrForcedClose)
		require.True(t, runtime.ShutdownComplete())
		select {
		case <-runtime.ShutdownDone():
		default:
			t.Fatal("aggregate returned before actual root cleanup completed")
		}
	})
}

func TestCompressedLogSpoolAggregateNormalOwnerReleaseKeepsRoutesAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runtime, route := newAggregateShutdownRuntime(t, nil)
		owner := &compressedLogSpoolRuntimes{runtimes: []*compressedlog.RootRuntime{runtime}}
		closed := make(chan error, 1)
		go func() { closed <- owner.Close() }()
		synctest.Wait()
		select {
		case err := <-closed:
			require.NoError(t, err)
		default:
			t.Fatal("normal owner release must return while registered routes remain")
		}
		require.False(t, runtime.ShutdownComplete())
		require.NoError(t, route.Close())
		<-runtime.ShutdownDone()
	})
}

func newAggregateShutdownRuntime(t *testing.T, observer compressedlog.ManagedRunObserver) (*compressedlog.RootRuntime, *compressedlog.ManagedRoute) {
	t.Helper()
	upload := sharedCompressedLogSpoolTestUpload(t)
	runtime, err := compressedlog.NewRootRuntime(compressedlog.NewConfigAdapter(upload.CompressedLogFileUploadSpool, objectstorage.UploadTargetS3))
	require.NoError(t, err)
	route, err := runtime.RegisterRoute(compressedlog.ManagedRouteConfig{
		DisableMaintenance: true,
		FileName:           logpattern.FileNamePattern{Target: "aggregate", Extension: "log"},
		RunObserver:        observer,
	}, aggregateShutdownUploader{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = route.Close()
		_ = runtime.Close()
		<-runtime.ShutdownDone()
	})
	return runtime, route
}

type aggregateShutdownUploader struct{}

func (aggregateShutdownUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"aggregate"}
}

func (aggregateShutdownUploader) Upload(context.Context, uploader.UploadRequest) error { return nil }

func (aggregateShutdownUploader) UploadDestinations(_ context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	results := make([]uploader.DestinationResult, len(destinations))
	for index, destination := range destinations {
		results[index].DestinationID = destination
	}
	return uploader.UploadReport{Results: results}
}

func (aggregateShutdownUploader) ClassifyRetry(context.Context, error) retryable.RetryDecision {
	return retryable.RetryDecision{}
}

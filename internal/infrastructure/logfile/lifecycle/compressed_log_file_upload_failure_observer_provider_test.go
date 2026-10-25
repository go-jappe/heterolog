package lifecycle

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/compressedlog"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

type observerSpoolConfigProvider struct {
	compressedlog.ConfigAdapter
	path string
}

func (p *observerSpoolConfigProvider) SpoolPath() string { return p.path }

func TestLifecycleCompressedLogFileUploadSpoolObserverUsesRuntimeRootSnapshot(t *testing.T) {
	provider := &observerSpoolConfigProvider{
		ConfigAdapter: compressedlog.NewConfigAdapter(objectstorage.NewCompressedLogFileUploadSpoolConfigAdapter(runnerCompressedLogFileUploadSpoolTestConfig(
			filepath.Join(resolvedTempDirForTest(t), "unused-adapter-path"), "provider-observer", pkglogger.LoggerContractSync,
		)), objectstorage.UploadTargetS3),
		path: resolvedTempDirForTest(t),
	}
	runtime, err := compressedlog.NewRootRuntime(provider)
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "runtime.Close()", runtime.Close, boundedwait.ForceCloserOf(runtime), boundedwait.NoError)
	wantPath := runtime.Root().Path()
	provider.path = resolvedTempDirForTest(t)
	var report RunReport
	var reportErr error
	calls := 0
	reporter := lifecycleRunReporterFunc(func(_ context.Context, got RunReport, err error) {
		calls++
		report, reportErr = got, err
	})
	observer := compressedLogFileUploadFailureSpoolRunObserver(reporter, runtime)
	require.NotNil(t, observer)

	observer(context.Background(), nil, nil, nil)
	require.Zero(t, calls)
	wantErr := errors.New("spool maintenance failed")
	observer(context.Background(), nil, nil, wantErr)
	require.Equal(t, 1, calls)
	require.Equal(t, pathidentity.AbsoluteOrCleanPath(wantPath), report.DirectoryPath)
	require.ErrorIs(t, reportErr, wantErr)
}

func TestLifecycleCompressedLogFileUploadArchivePurgeObserverUsesDestinationRoot(t *testing.T) {
	for _, service := range []objectstorage.UploadTarget{objectstorage.UploadTargetS3, "another-backend"} {
		t.Run(string(service), func(t *testing.T) {
			root, err := compressedlogarchive.NewRoot(resolvedTempDirForTest(t), service, pkglogger.LoggerContractSync)
			require.NoError(t, err)
			root, err = root.ForDestination("opaque-destination")
			require.NoError(t, err)
			wantPath := root.Path()
			var report RunReport
			var reportErr error
			calls := 0
			reporter := lifecycleRunReporterFunc(func(_ context.Context, got RunReport, err error) {
				calls++
				report, reportErr = got, err
			})
			observer := compressedLogFileUploadFailureArchivePurgeRunObserver(reporter, root)
			require.NotNil(t, observer)

			wantReport := compressedlogarchive.PurgeReport{}
			observer(context.Background(), wantReport, nil)
			require.Equal(t, 1, calls)
			require.Equal(t, pathidentity.AbsoluteOrCleanPath(wantPath), report.DirectoryPath)
			require.Equal(t, &wantReport, report.UploadFailureArchivePurge)
			require.NoError(t, reportErr)
			wantErr := errors.New("archive lifecycle failed")
			observer(context.Background(), wantReport, wantErr)
			require.Equal(t, 2, calls)
			require.Equal(t, pathidentity.AbsoluteOrCleanPath(wantPath), report.DirectoryPath)
			require.Equal(t, &wantReport, report.UploadFailureArchivePurge)
			require.ErrorIs(t, reportErr, wantErr)
		})
	}
}

func TestLifecycleCompressedLogFileUploadObserversSkipWithoutReporter(t *testing.T) {
	require.Nil(t, compressedLogFileUploadFailureSpoolRunObserver(nil, nil))
	require.Nil(t, compressedLogFileUploadFailureArchivePurgeRunObserver(nil, compressedlogarchive.Root{}))
}

package compressedlogfailure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	compressedlogarchive "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive/compressedlog"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

func TestArchivePurgeObserversReceiveSuccessAndErrorsDespiteAnotherObserverPanic(t *testing.T) {
	store, err := compressedlogarchive.NewStore(newArchiveRoot(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	purger, err := compressedlogarchive.NewPurger(store, compressedlogarchive.NewPurgeConfigAdapter(compressedlogarchive.PurgeConfig{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	panickingCalls, otherCalls := 0, 0
	var receivedReport compressedlogarchive.PurgeReport
	var receivedError error
	coordinator := &sharedArchiveLifecycle{
		purger: purger,
		purgeObservers: map[*archiveLifecycleOwner]ArchivePurgeObserver{
			{}: nil,
			{}: func(context.Context, compressedlogarchive.PurgeReport, error) {
				panickingCalls++
				panic("notification failed")
			},
			{}: func(receivedContext context.Context, report compressedlogarchive.PurgeReport, err error) {
				otherCalls++
				receivedReport, receivedError = report, err
				if receivedContext != ctx {
					t.Error("purge notification lost its context")
				}
			},
		},
	}
	coordinator.runOnce(ctx)
	if panickingCalls != 1 || otherCalls != 1 {
		t.Fatalf("successful purge observer calls = %d, %d; want one each", panickingCalls, otherCalls)
	}
	if receivedError != nil || len(receivedReport.ByFailureCategory) != 2 {
		t.Fatalf("successful purge result = %+v, %v; want both disabled categories", receivedReport, receivedError)
	}
	for _, categoryReport := range receivedReport.ByFailureCategory {
		if categoryReport.Enabled {
			t.Fatalf("disabled purge was reported as enabled: %+v", categoryReport)
		}
	}

	cancel()
	coordinator.runOnce(ctx)
	if panickingCalls != 2 || otherCalls != 2 {
		t.Fatalf("observer calls = %d, %d; want two each", panickingCalls, otherCalls)
	}
	if !errors.Is(receivedError, context.Canceled) || len(receivedReport.ByFailureCategory) != 0 {
		t.Fatalf("canceled purge result = %+v, %v; want empty report and cancellation", receivedReport, receivedError)
	}
}

func TestArchivePurgeObserverReportsPreservedFileAfterStoreRestart(t *testing.T) {
	root := newArchiveRoot(t, t.TempDir())
	store, err := compressedlogarchive.NewStore(root)
	require.NoError(t, err)
	sourcePath := filepath.Join(t.TempDir(), "app_20260929_1.log.gz")
	require.NoError(t, os.WriteFile(sourcePath, []byte("old content"), 0o640))
	_, err = store.Save(context.Background(), compressedlogarchive.SaveInput{
		SourcePath: sourcePath, FailureCategory: uploader.FailureCategoryRetryable,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(sourcePath, []byte("new content"), 0o640))
	saved, err := store.Save(context.Background(), compressedlogarchive.SaveInput{
		SourcePath: sourcePath, FailureCategory: uploader.FailureCategoryRetryable,
	})
	require.NoError(t, err)
	require.Len(t, saved.ConflictRenames, 1)

	restartedStore, err := compressedlogarchive.NewStore(root)
	require.NoError(t, err)
	purger, err := compressedlogarchive.NewPurger(
		restartedStore,
		compressedlogarchive.NewPurgeConfigAdapter(compressedlogarchive.PurgeConfig{}),
		nil,
	)
	require.NoError(t, err)
	var observed []error
	coordinator := &sharedArchiveLifecycle{
		store:  restartedStore,
		purger: purger,
		purgeObservers: map[*archiveLifecycleOwner]ArchivePurgeObserver{
			{}: func(_ context.Context, report compressedlogarchive.PurgeReport, err error) {
				require.Equal(t, 1, report.PreservedFileCount)
				observed = append(observed, err)
			},
		},
	}
	coordinator.runOnce(context.Background())
	coordinator.runOnce(context.Background())
	require.Len(t, observed, 2)
	for _, err := range observed {
		var preserved *PreservedArchiveFilesError
		require.ErrorAs(t, err, &preserved)
		require.Equal(t, root.Path(), preserved.Root)
		require.Equal(t, 1, preserved.Count)
	}
}

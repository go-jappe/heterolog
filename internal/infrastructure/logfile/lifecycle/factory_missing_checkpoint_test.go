package lifecycle

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestLogFileFactoryRecoversMissingCheckpointWithoutSkippingExistingRawFiles(t *testing.T) {
	for _, recoverMissing := range []bool{false, true} {
		t.Run(fmt.Sprintf("recover=%t", recoverMissing), func(t *testing.T) {
			ctx := context.Background()
			cfg := configuredCheckpointFactoryTestConfig(t)
			cfg.ArchiveCheckpointConfig.AutoRecoverMissingCheckpoint = recoverMissing
			directory := filepath.Join(cfg.FsstoreConfig.Layout.LogPath, cfg.FsstoreConfig.Layout.Target, cfg.FsstoreConfig.Layout.LogDir)
			require.NoError(t, os.MkdirAll(directory, 0o750))
			period := currentLogDate()
			first := writeFileSeriesCheckpointLog(t, directory, cfg.FsstoreConfig.Layout.Target, period, 1)
			second := writeFileSeriesCheckpointLog(t, directory, cfg.FsstoreConfig.Layout.Target, period, 2)
			latest := writeFileSeriesCheckpointLog(t, directory, cfg.FsstoreConfig.Layout.Target, period, 3)
			destinations := []objectstorage.DestinationID{"first-archive", "second-archive"}

			original, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
			require.NoError(t, err)
			require.Len(t, original, 1)
			store := checkpointStoreForFactoryTest(t, original[0].fileSeriesManagers[0])
			require.NoError(t, store.RegisterDestinations(ctx, destinations, nil))
			for _, index := range []int{1, 2} {
				_, err = store.AdvanceCompletionCursor(ctx, checkpointstore.CheckpointKey{Generation: fmt.Sprintf("%s-%d", period, index), Compressed: false}, destinations)
				require.NoError(t, err)
			}
			missingPath := filepath.Join(store.Directory(), store.SourceID()+".log-file-uploads.json")
			markerPath := filepath.Join(store.Directory(), store.SourceID()+".destinations.json")
			require.NoError(t, boundedwait.Close(t, "original[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes()", original[0].CloseObjectStorageCompressedLogFileUploadFailureRoutes, boundedwait.ForceCloserOf(original[0])))
			require.NoError(t, os.Remove(missingPath))
			require.FileExists(t, markerPath)

			restarted, err := NewLogFileLifecycles(LogFileOptions{Config: cfg})
			if !recoverMissing {
				require.ErrorIs(t, err, checkpointstore.ErrRegisteredCheckpointMissing)
				require.Nil(t, restarted)
				require.NoFileExists(t, missingPath)
				return
			}
			require.NoError(t, err)
			require.Len(t, restarted, 1)
			lifecycle := restarted[0]
			closeDirectoryLifecycleOnCleanup(t, lifecycle)
			manager := lifecycle.fileSeriesManagers[0]
			remote := newDestinationUploaderRecorder(destinations...)
			manager.objectStorageUpload.uploader = remote
			_, err = lifecycle.ReconcileObjectStorageUploadDestinations(ctx)
			require.NoError(t, err)
			require.FileExists(t, missingPath)
			for _, index := range []int{1, 2} {
				state, loadErr := manager.objectStorageUpload.checkpointStore.Load(ctx, checkpointstore.CheckpointKey{Generation: fmt.Sprintf("%s-%d", period, index), Compressed: false})
				require.NoError(t, loadErr)
				require.Empty(t, state.CompletedDestinationIDs, "lost source state must not seed at the newest existing file")
			}

			_, err = lifecycle.RunOnce(ctx)
			require.NoError(t, err)
			for _, destination := range destinations {
				require.Equal(t, []string{first, second}, remote.pathsFor(destination))
			}
			require.FileExists(t, first)
			require.FileExists(t, second)
			require.FileExists(t, latest)
		})
	}
}

package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func recordedGenerationTestSeries(t *testing.T) (pkgfsstore.ArchiveCheckpointConfig, logpattern.FileNamePattern, string) {
	t.Helper()
	cfg := pkgfsstore.ArchiveCheckpointConfig{Directory: filepath.Join(t.TempDir(), "checkpoints"), SourcePrefix: "recorded"}
	fileName := logpattern.FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: "log"}
	return cfg, fileName, ArchiveCheckpointSourceID(cfg.SourcePrefix, "default", fileName)
}

func TestLatestRecordedGenerationReturnsNewestCheckpointRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg, fileName, sourceID := recordedGenerationTestSeries(t)
	require.NoError(t, PrepareDirectory(cfg.Directory))
	store, err := NewFilesystemStore(cfg.Directory, sourceID)
	require.NoError(t, err)
	// addedは登録前から存在した20261022-4までを対象外とし、primaryは20261022-6まで完了しています。
	require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{"added"}, []CheckpointKey{{Generation: "20261022-4", Compressed: true}}))
	_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20261022-6", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	recorded, err := LatestRecordedGeneration(cfg, "default", fileName)
	require.NoError(t, err)
	require.Equal(t, RecordedGeneration{Period: "20261022", Index: 6}, recorded)

	// 完了位置より新しい世代の途中経過も、送信を試みた世代として数えます。
	require.NoError(t, store.SaveUploadProgress(ctx, UploadProgress{
		Key: CheckpointKey{Generation: "20261023-2", Compressed: true}, DestinationID: "primary", UploadResult: UploadResultSuccess,
	}))
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	recorded, err = LatestRecordedGeneration(cfg, "default", fileName)
	require.NoError(t, err)
	require.Equal(t, RecordedGeneration{Period: "20261023", Index: 2}, recorded)

	// 別のlog系列の記録は使いません。
	other, err := LatestRecordedGeneration(cfg, "error", fileName)
	require.NoError(t, err)
	require.Zero(t, other)
}

func TestLatestRecordedGenerationReadsWithoutCreatingCheckpointFiles(t *testing.T) {
	t.Parallel()
	cfg, fileName, sourceID := recordedGenerationTestSeries(t)
	for _, cfg := range []pkgfsstore.ArchiveCheckpointConfig{
		{},
		{Directory: cfg.Directory},
		{SourcePrefix: cfg.SourcePrefix},
		cfg,
	} {
		recorded, err := LatestRecordedGeneration(cfg, "default", fileName)
		require.NoError(t, err)
		require.Zero(t, recorded)
	}
	require.NoDirExists(t, cfg.Directory, "the read-only lookup must not prepare the checkpoint directory")

	require.NoError(t, PrepareDirectory(cfg.Directory))
	recorded, err := LatestRecordedGeneration(cfg, "default", fileName)
	require.NoError(t, err)
	require.Zero(t, recorded)
	entries, err := os.ReadDir(cfg.Directory)
	require.NoError(t, err)
	require.Empty(t, entries, "the read-only lookup must not create checkpoint documents")

	// 登録だけで完了位置のないcursorは、記録済みの世代として扱いません。
	store, err := NewFilesystemStore(cfg.Directory, sourceID)
	require.NoError(t, err)
	require.NoError(t, store.RegisterDestinations(context.Background(), []objectstorage.DestinationID{"primary"}, nil))
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	recorded, err = LatestRecordedGeneration(cfg, "default", fileName)
	require.NoError(t, err)
	require.Zero(t, recorded)
}

func TestLatestRecordedGenerationRejectsCorruptDocument(t *testing.T) {
	t.Parallel()
	cfg, fileName, sourceID := recordedGenerationTestSeries(t)
	require.NoError(t, PrepareDirectory(cfg.Directory))
	path := filepath.Join(cfg.Directory, sourceID+documentFileSuffix)
	payload := []byte(`{"source":"` + sourceID + `","cursors":[{"destination_id":"primary","generation":"not-a-generation"}]}`)
	require.NoError(t, os.WriteFile(path, payload, fileMode))
	_, err := LatestRecordedGeneration(cfg, "default", fileName)
	require.ErrorIs(t, err, ErrCorruptCheckpoint)
	unchanged, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, unchanged)
}

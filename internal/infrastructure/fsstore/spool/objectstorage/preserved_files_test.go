package objectstoragespool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	pkglogger "github.com/go-jappe/heterolog/pkg/logger"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestDestinationStoresCountPreservedFilesDiscoversAllDestinationsAfterRestart(t *testing.T) {
	ctx := context.Background()
	root, err := NewRoot(t.TempDir(), "app", objectstorage.UploadTargetS3, pkglogger.LoggerContractSync)
	require.NoError(t, err)
	stores, err := NewDestinationStores(root)
	require.NoError(t, err)
	primary, err := stores.ForDestination("primary")
	require.NoError(t, err)
	secondary, err := stores.ForDestination("secondary")
	require.NoError(t, err)
	source := writeTestSource(t, t.TempDir(), "app_20261004_1.log.gz", "source bytes")
	_, err = primary.PreserveFile(ctx, source)
	require.NoError(t, err)
	_, err = secondary.SaveRetryableFailure(ctx, source)
	require.NoError(t, err)
	changed := writeTestSource(t, t.TempDir(), "app_20261004_1.log.gz", "changed bytes")
	conflict, err := secondary.SaveRetryableFailure(ctx, changed)
	require.NoError(t, err)
	require.Len(t, conflict.ConflictRenames, 1)
	_, err = secondary.PreserveFile(ctx, changed)
	require.NoError(t, err)

	// No routes or cached Stores are registered on this restarted owner.
	restarted, err := NewDestinationStores(root)
	require.NoError(t, err)
	count, err := restarted.CountPreservedFiles(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, count, "rename and source-copy artifacts from every destination must remain visible")
}

package objectstoragespool

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestStoreClaimReportsMovedFileAfterDirectorySyncFailure(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	const content = "claimed compressed bytes"
	published, err := store.SaveRetryableFailure(ctx,
		writeTestSource(t, t.TempDir(), "app_20261003_1.log.gz", content))
	require.NoError(t, err)
	restore := testutil.DenyDirectorySync(t, store.Root().ProcessingDir())

	file, claimed, err := store.ClaimOldestActive(ctx)
	// 移動済みのfileとclaimedを返す失敗は、移動後のdirectory同期だけです。
	require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
	require.True(t, claimed)
	require.Equal(t, CompressedLogFile{
		DestinationID: published.DestinationID,
		FileName:      published.FileName,
		State:         pkgspool.StateProcessing,
		Path:          filepath.Join(store.Root().ProcessingDir(), published.FileName),
	}, file)
	restore()
	requireMovedCompressedLogFile(t, published.Path, file.Path, content)

	// The returned identity remains usable after restoring directory sync.
	requeued, err := store.RequeueProcessing(ctx, file)
	require.NoError(t, err)
	require.Equal(t, published, requeued)
}

func TestStoreProcessingMovesReportMovedFileAfterDirectorySyncFailure(t *testing.T) {
	for _, state := range []pkgspool.State{
		pkgspool.StateActive,
		pkgspool.StateDone,
		pkgspool.StateDead,
		pkgspool.StateQuarantine,
	} {
		for _, syncSide := range []string{"destination", "source"} {
			t.Run(string(state)+"/"+syncSide, func(t *testing.T) {
				ctx := context.Background()
				store := newTestStore(t)
				const content = "moved compressed bytes"
				published, err := store.SaveRetryableFailure(ctx,
					writeTestSource(t, t.TempDir(), "app_20261003_2.log.gz", content))
				require.NoError(t, err)
				processing, claimed, err := store.ClaimOldestActive(ctx)
				require.NoError(t, err)
				require.True(t, claimed)
				destination, err := store.Root().StateDir(state)
				require.NoError(t, err)
				deniedDirectory := destination
				if syncSide == "source" {
					deniedDirectory = store.Root().ProcessingDir()
				}
				restore := testutil.DenyDirectorySync(t, deniedDirectory)

				var moved CompressedLogFile
				if state == pkgspool.StateActive {
					moved, err = store.RequeueProcessing(ctx, processing)
				} else {
					moved, err = store.TransitionProcessing(ctx, processing, state)
				}
				require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
				require.Equal(t, CompressedLogFile{
					DestinationID: published.DestinationID,
					FileName:      published.FileName,
					State:         state,
					Path:          filepath.Join(destination, published.FileName),
				}, moved)
				restore()
				requireMovedCompressedLogFile(t, processing.Path, moved.Path, content)

				// Continue using the returned identity once sync is available again.
				if state == pkgspool.StateActive {
					next, claimed, err := store.ClaimOldestActive(ctx)
					require.NoError(t, err)
					require.True(t, claimed)
					require.Equal(t, processing, next)
				} else {
					require.NoError(t, store.RemoveTerminal(ctx, moved))
					_, err := os.Lstat(moved.Path)
					require.ErrorIs(t, err, os.ErrNotExist)
				}
			})
		}
	}
}

func requireMovedCompressedLogFile(t *testing.T, source, destination, content string) {
	t.Helper()
	_, err := os.Lstat(source)
	require.ErrorIs(t, err, os.ErrNotExist)
	data, err := os.ReadFile(destination)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
}

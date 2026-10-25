package checkpoint

import (
	"context"
	"errors"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreLoadAndUnchangedUpdatesDoNotSaveWhilePending(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "load-during-save-recovery")
		_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		restore := blockCheckpointReplacement(t, store)
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"secondary"})
		pending := pendingSaveForTest(t, err)
		saveErr := errors.Unwrap(pending)
		synctest.Wait()
		restore()
		path := store.documentPath()
		before, err := os.Stat(path)
		require.NoError(t, err)
		checkpoint, err := store.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, checkpoint.CompletedDestinationIDs, "primary", "secondary")
		for _, update := range []func() error{
			func() error {
				_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"secondary"})
				return err
			},
			func() error {
				_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, nil)
				return err
			},
			func() error {
				_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
				return err
			},
		} {
			unchanged := pendingSaveForTest(t, update())
			require.Same(t, saveErr, errors.Unwrap(unchanged))
		}
		after, err := os.Stat(path)
		require.NoError(t, err)
		require.True(t, os.SameFile(before, after), "Load and unchanged updates must not write pending state")
		require.Same(t, saveErr, checkpointSaveErrorForTest(store))
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		checkpoint, err = store.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, checkpoint.CompletedDestinationIDs, "primary", "secondary")

		before, err = os.Stat(path)
		require.NoError(t, err)
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		after, err = os.Stat(path)
		require.NoError(t, err)
		require.True(t, os.SameFile(before, after), "saved state must not be rewritten by unchanged operations")
	})
}

func TestFilesystemStoreMethodLockWaitCanBeCanceled(t *testing.T) {
	t.Parallel()
	store := newCheckpointTestStore(t, "cancel-method-wait")
	for _, test := range []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "Load", run: func(ctx context.Context) error {
			_, err := store.Load(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true})
			return err
		}},
		{name: "AdvanceCompletionCursor", run: func(ctx context.Context) error {
			_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
			return err
		}},
		{name: "RegisterDestinations", run: func(ctx context.Context) error {
			return store.RegisterDestinations(ctx, []objectstorage.DestinationID{"primary"}, nil)
		}},
		{name: "DestinationCursors", run: func(ctx context.Context) error {
			_, err := store.DestinationCursors(ctx)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store.stateOrSaveMu.Lock()
			defer store.stateOrSaveMu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- test.run(ctx) }()
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(boundedwait.HangTimeout):
				t.Fatal("method did not cancel checkpoint lock wait")
			}
		})
	}
	require.NoError(t, checkpointSaveErrorForTest(store))
}

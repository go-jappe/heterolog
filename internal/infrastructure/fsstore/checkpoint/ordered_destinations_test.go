package checkpoint

import (
	"context"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/stretchr/testify/require"
)

func TestReadyDestinationsDoNotSkipOlderFailureAndKeepOtherDestinationMoving(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "ordered")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
	active := []objectstorage.DestinationID{"a", "b"}
	require.NoError(t, store.RegisterDestinations(ctx, active, nil))
	older := CheckpointKey{Generation: "20260927-2", Compressed: true}
	later := CheckpointKey{Generation: "20260927-10", Compressed: true}
	_, err = store.AdvanceCompletionCursor(ctx, older, active[:1])
	require.NoError(t, err)
	ready, err := ReadyDestinationIDs(ctx, store, later, active, []CheckpointKey{later, older, older})
	require.NoError(t, err)
	require.Equal(t, active[:1], ready)
	_, err = store.AdvanceCompletionCursor(ctx, later, ready)
	require.NoError(t, err)
	// Bが後から退避まで完了した場合も、同じ完了記録で後続へ進めます。
	_, err = store.AdvanceCompletionCursor(ctx, older, active[1:])
	require.NoError(t, err)
	ready, err = ReadyDestinationIDs(ctx, store, later, active[1:], []CheckpointKey{older, later})
	require.NoError(t, err)
	require.Equal(t, active[1:], ready)
	_, err = store.AdvanceCompletionCursor(ctx, later, ready)
	require.NoError(t, err)
	state, err := store.Load(ctx, older)
	require.NoError(t, err)
	require.True(t, state.Complete(active), "older retained file requires only deletion, not resending")
}

func TestReadyDestinationsIgnoreInitiallyCompletedOlderGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "scope")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
	old := CheckpointKey{Generation: "20260927-5", Compressed: true}
	next := CheckpointKey{Generation: "20260927-6", Compressed: true}
	active := []objectstorage.DestinationID{"new"}
	require.NoError(t, store.RegisterDestinations(ctx, active, []CheckpointKey{old}))
	ready, err := ReadyDestinationIDs(ctx, store, next, active, []CheckpointKey{old, next})
	require.NoError(t, err)
	require.Equal(t, active, ready)
	_, err = store.AdvanceCompletionCursor(ctx, next, active)
	require.NoError(t, err)
	state, err := store.Load(ctx, old)
	require.NoError(t, err)
	require.True(t, state.Complete(active), "initially out-of-scope files also need no further upload")
	require.Equal(t, active, state.CompletedDestinationIDs)
}

func TestCompareGenerationsUsesNumericIndex(t *testing.T) {
	t.Parallel()
	require.Negative(t, CompareGenerations("20260927-2", "20260927-10"))
	require.Negative(t, CompareGenerations("20260926-100", "20260927-1"))
	require.Zero(t, CompareGenerations("20260927-10", "20260927-10"))
}

type countingCheckpointStore struct {
	Store
	loadedGenerations []string
}

func (s *countingCheckpointStore) Load(ctx context.Context, key CheckpointKey) (Checkpoint, error) {
	s.loadedGenerations = append(s.loadedGenerations, key.Generation)
	return s.Store.Load(ctx, key)
}

func TestReadyDestinationsChecksEachGenerationOnceAcrossCompressionForms(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "shared-generation")
	active := []objectstorage.DestinationID{"primary"}
	require.NoError(t, store.RegisterDestinations(ctx, active, nil))
	uncompressed := CheckpointKey{Generation: "20260927-1"}
	compressed := CheckpointKey{Generation: uncompressed.Generation, Compressed: true}
	next := CheckpointKey{Generation: "20260927-2", Compressed: true}
	_, err := store.AdvanceCompletionCursor(ctx, uncompressed, active)
	require.NoError(t, err)
	counting := &countingCheckpointStore{Store: store}
	ready, err := ReadyDestinationIDs(ctx, counting, next, active, []CheckpointKey{compressed, uncompressed, compressed, next})
	require.NoError(t, err)
	require.Equal(t, active, ready, "uncompressed completion also covers the compressed representation")
	require.Equal(t, []string{uncompressed.Generation}, counting.loadedGenerations)
}

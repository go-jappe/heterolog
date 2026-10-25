package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/stretchr/testify/require"
)

func TestFilesystemStoreDestinationCursorsSurviveRestartAndReenable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "source")
	active := []objectstorage.DestinationID{"primary", "secondary"}
	old := CheckpointKey{Generation: "20260926-1", Compressed: true}
	latestUncompressed := CheckpointKey{Generation: "20260926-2"}
	between := CheckpointKey{Generation: "20260926-3", Compressed: true}
	fresh := CheckpointKey{Generation: "20260926-4", Compressed: true}
	existing := []CheckpointKey{old, latestUncompressed}
	require.NoError(t, store.RegisterDestinations(ctx, active[:1], existing))
	existing[0].Generation = "caller mutation"
	initial, err := store.Load(ctx, old)
	require.NoError(t, err)
	require.Equal(t, active[:1], initial.CompletedDestinationIDs)
	initial, err = store.Load(ctx, latestUncompressed)
	require.NoError(t, err)
	require.Empty(t, initial.CompletedDestinationIDs)
	_, err = store.AdvanceCompletionCursor(ctx, between, active[:1])
	require.NoError(t, err)
	require.NoError(t, store.RegisterDestinations(ctx, active, []CheckpointKey{old, latestUncompressed, between}))

	// primaryはold、secondaryはbetweenまでが登録前から存在した対象外です。
	// 対象外の世代もuploadは不要ですが、送信済みの宛先としては数えません。
	outOfScope := map[string][]objectstorage.DestinationID{
		old.Generation:                active,
		latestUncompressed.Generation: active[1:],
		between.Generation:            active[1:],
	}
	checkCursors := func(store *FilesystemStore) {
		t.Helper()
		for _, generation := range []string{old.Generation, latestUncompressed.Generation, between.Generation} {
			for _, compressed := range []bool{false, true} {
				snapshot, err := store.Load(ctx, CheckpointKey{Generation: generation, Compressed: compressed})
				require.NoError(t, err)
				require.Equal(t, active, snapshot.CompletedDestinationIDs)
				require.Empty(t, snapshot.PendingDestinationIDs(active))
				require.True(t, snapshot.Complete(active))
				require.Equal(t, outOfScope[generation], snapshot.OutOfScopeDestinationIDs)
				require.Equal(t, generation != old.Generation, snapshot.DeliveredToAny(active))
			}
		}
		snapshot, err := store.Load(ctx, fresh)
		require.NoError(t, err)
		require.Equal(t, active, snapshot.PendingDestinationIDs(active))
		cursors, err := store.DestinationCursors(ctx)
		require.NoError(t, err)
		require.Equal(t, []DestinationCursor{
			{DestinationID: "primary", Generation: between.Generation, OutOfScopeGeneration: old.Generation},
			{DestinationID: "secondary", Generation: between.Generation, OutOfScopeGeneration: between.Generation},
		}, cursors)
		cursors[0].Generation = "caller mutation"
		again, err := store.DestinationCursors(ctx)
		require.NoError(t, err)
		require.Equal(t, between.Generation, again[0].Generation)
	}
	checkCursors(store)
	snapshot, err := store.AdvanceCompletionCursor(ctx, old, active)
	require.NoError(t, err)
	require.True(t, snapshot.Complete(active))
	checkCursors(store)
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	restarted, err := NewFilesystemStore(store.directory, "source")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, boundedwait.Close(t, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted)))
	}()
	require.NoError(t, restarted.RegisterDestinations(ctx, active, []CheckpointKey{fresh}))
	checkCursors(restarted)
}

func TestFilesystemStoreInitialRegistrationLeavesLatestUncompressedGenerationEligible(t *testing.T) {
	t.Parallel()
	for _, existing := range [][]CheckpointKey{
		nil,
		{{Generation: "20260926-0"}},
		{{Generation: "20260926-0"}, {Generation: "20260926-0", Compressed: true}},
	} {
		store := newCheckpointTestStore(t, "source")
		ctx := context.Background()
		active := []objectstorage.DestinationID{"primary"}
		require.NoError(t, store.RegisterDestinations(ctx, active, existing))
		cursors, err := store.DestinationCursors(ctx)
		require.NoError(t, err)
		require.Equal(t, []DestinationCursor{{DestinationID: "primary"}}, cursors)
		for _, compressed := range []bool{false, true} {
			snapshot, err := store.Load(ctx, CheckpointKey{Generation: "20260926-0", Compressed: compressed})
			require.NoError(t, err)
			require.Equal(t, active, snapshot.PendingDestinationIDs(active))
		}
		require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
		restarted, err := NewFilesystemStore(store.directory, "source", WithMissingCheckpointRecovery(false))
		require.NoError(t, err, "an explicit empty cursor is valid, not missing")
		require.NoError(t, boundedwait.Close(t, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted)))
	}
}

func TestFilesystemStoreMissingCursorRecoveryResendsPreviouslySkippedAndUploadedFiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "source")
	active := []objectstorage.DestinationID{"primary", "secondary"}
	first := CheckpointKey{Generation: "20260926-1", Compressed: true}
	latest := CheckpointKey{Generation: "20260926-2", Compressed: true}
	require.NoError(t, store.RegisterDestinations(ctx, active[:1], []CheckpointKey{first}))
	_, err := store.AdvanceCompletionCursor(ctx, latest, active[:1])
	require.NoError(t, err)
	require.NoError(t, store.RegisterDestinations(ctx, active[1:], []CheckpointKey{first, latest}))
	manifest, err := os.ReadFile(store.registrationsPath())
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	require.NoError(t, os.Remove(store.documentPath()))
	strict, err := NewFilesystemStore(store.directory, "source", WithMissingCheckpointRecovery(false))
	require.ErrorIs(t, err, ErrRegisteredCheckpointMissing)
	require.Nil(t, strict)
	recovered, err := NewFilesystemStore(store.directory, "source")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, boundedwait.Close(t, "recovered.Close()", recovered.Close, boundedwait.ForceCloserOf(recovered)))
	}()
	// Known IDs must not become new destinations with a newly computed initial cursor.
	require.NoError(t, recovered.RegisterDestinations(ctx, active, []CheckpointKey{first, latest}))
	for _, key := range []CheckpointKey{first, latest} {
		for _, compressed := range []bool{false, true} {
			key.Compressed = compressed
			snapshot, err := recovered.Load(ctx, key)
			require.NoError(t, err)
			require.Equal(t, active, snapshot.PendingDestinationIDs(active))
		}
	}
	current, err := os.ReadFile(store.registrationsPath())
	require.NoError(t, err)
	require.Equal(t, manifest, current, "recovery preserves registered IDs only")
}

func TestFilesystemStoreInterruptedRegistrationRecoversOnlyMissingCursor(t *testing.T) {
	t.Parallel()
	for _, recovery := range []bool{false, true} {
		t.Run(fmt.Sprint(recovery), func(t *testing.T) {
			ctx := context.Background()
			store := newCheckpointTestStore(t, "source")
			active := []objectstorage.DestinationID{"known", "new"}
			key := CheckpointKey{Generation: "20260926-3", Compressed: true}
			_, err := store.AdvanceCompletionCursor(ctx, key, active[:1])
			require.NoError(t, err)
			// Crash after registration metadata publication but before the cursor document.
			store.stateOrSaveMu.Lock()
			err = store.registerDestinationIDsLocked(ctx, active[1:])
			store.stateOrSaveMu.Unlock()
			require.NoError(t, err)
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			restarted, err := NewFilesystemStore(store.directory, "source", WithMissingCheckpointRecovery(recovery))
			if !recovery {
				require.ErrorIs(t, err, ErrRegisteredCheckpointMissing)
				require.Nil(t, restarted)
				return
			}
			require.NoError(t, err)
			defer func() {
				require.NoError(t, boundedwait.Close(t, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted)))
			}()
			require.NoError(t, restarted.RegisterDestinations(ctx, active, []CheckpointKey{key}))
			snapshot, err := restarted.Load(ctx, key)
			require.NoError(t, err)
			require.Equal(t, active[:1], snapshot.CompletedDestinationIDs)
			require.Equal(t, active[1:], snapshot.PendingDestinationIDs(active))
		})
	}
}

func TestFilesystemStoreRegistrationFailureDoesNotAcceptCompletion(t *testing.T) {
	t.Parallel()
	for _, viaMark := range []bool{false, true} {
		t.Run(fmt.Sprint(viaMark), func(t *testing.T) {
			ctx := context.Background()
			store := newCheckpointTestStore(t, "source")
			key := CheckpointKey{Generation: "20260926-1", Compressed: true}
			active := []objectstorage.DestinationID{"new"}
			backup := filepath.Join(t.TempDir(), "registrations.json")
			require.NoError(t, os.Rename(store.registrationsPath(), backup))
			require.NoError(t, os.Mkdir(store.registrationsPath(), 0o700))
			var err error
			if viaMark {
				_, err = store.AdvanceCompletionCursor(ctx, key, active)
			} else {
				err = store.RegisterDestinations(ctx, active, []CheckpointKey{key})
			}
			// 空でないdirectoryが宛先登録文書の公開(rename)を拒否した失敗です。
			var publishErr *os.LinkError
			require.ErrorAs(t, err, &publishErr)
			require.Equal(t, store.registrationsPath(), publishErr.New)
			var pending *PendingSaveError
			require.NotErrorAs(t, err, &pending, "registration failure cannot accept a cursor update")
			snapshot, err := store.Load(ctx, key)
			require.NoError(t, err)
			require.Empty(t, snapshot.CompletedDestinationIDs)
			require.Empty(t, store.registrations.DestinationIDs)
			require.NoError(t, checkpointSaveErrorForTest(store))
			require.NoError(t, os.Remove(store.registrationsPath()))
			require.NoError(t, os.Rename(backup, store.registrationsPath()))
			require.NoError(t, store.RegisterDestinations(ctx, active, []CheckpointKey{key}))
			snapshot, err = store.Load(ctx, key)
			require.NoError(t, err)
			require.Equal(t, active, snapshot.CompletedDestinationIDs)
		})
	}
}

func TestFilesystemStoreRegistrationDuringRecoveryPreservesLatestMemory(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "source")
		active := []objectstorage.DestinationID{"primary", "secondary"}
		require.NoError(t, store.RegisterDestinations(ctx, active[:1], nil))
		restore := blockCheckpointReplacement(t, store)
		latest := CheckpointKey{Generation: "20260926-3", Compressed: true}
		_, err := store.AdvanceCompletionCursor(ctx, latest, active[:1])
		pendingSaveForTest(t, err)
		synctest.Wait()
		initial := CheckpointKey{Generation: "20260926-2", Compressed: true}
		err = store.RegisterDestinations(ctx, active, []CheckpointKey{initial})
		pendingSaveForTest(t, err)
		accepted := checkpointStateForTest(store)
		require.Equal(t, []DestinationCursor{
			{DestinationID: "primary", Generation: latest.Generation},
			{DestinationID: "secondary", Generation: initial.Generation, OutOfScopeGeneration: initial.Generation},
		}, accepted.Cursors)
		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		persisted, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, accepted, persisted)
	})
}

func TestFilesystemStoreRegistrationLoadedOnceAndStoresOnlyIDs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "source")
	key := CheckpointKey{Generation: "20260926-1", Compressed: true}
	require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{"new"}, []CheckpointKey{key}))
	snapshot, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"original"})
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"new", "original"}, snapshot.CompletedDestinationIDs)
	require.Equal(t, []objectstorage.DestinationID{"new"}, snapshot.OutOfScopeDestinationIDs)
	require.False(t, snapshot.DestinationDelivered("new"))
	require.True(t, snapshot.DestinationDelivered("original"))
	clone := cloneCheckpoint(snapshot)
	clone.CompletedDestinationIDs[0] = "mutation"
	clone.OutOfScopeDestinationIDs[0] = "mutation"
	require.Equal(t, objectstorage.DestinationID("new"), snapshot.CompletedDestinationIDs[0])
	require.Equal(t, objectstorage.DestinationID("new"), snapshot.OutOfScopeDestinationIDs[0])
	payload, err := os.ReadFile(store.registrationsPath())
	require.NoError(t, err)
	require.JSONEq(t, `{"source":"source","destination_ids":["new","original"]}`, string(payload))
	payload, err = os.ReadFile(store.documentPath())
	require.NoError(t, err)
	var document checkpointDocument
	require.NoError(t, json.Unmarshal(payload, &document))
	require.Equal(t, []DestinationCursor{
		{DestinationID: "new", Generation: key.Generation, OutOfScopeGeneration: key.Generation},
		{DestinationID: "original", Generation: key.Generation},
	}, document.Cursors)
	require.NoError(t, os.WriteFile(store.registrationsPath(), []byte("{broken"), fileMode))
	before, err := os.Stat(store.registrationsPath())
	require.NoError(t, err)
	require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{"new"}, []CheckpointKey{{Generation: "20260926-3", Compressed: true}}))
	snapshot, err = store.Load(ctx, CheckpointKey{Generation: "20260926-3", Compressed: true})
	require.NoError(t, err)
	require.Empty(t, snapshot.CompletedDestinationIDs)
	after, err := os.Stat(store.registrationsPath())
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after), "known IDs must not reread or rewrite the manifest")
}

func TestFilesystemStoreRejectsMissingRegistrationForNonemptyCheckpoint(t *testing.T) {
	t.Parallel()
	store := newCheckpointTestStore(t, "source")
	_, err := store.AdvanceCompletionCursor(context.Background(), CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	require.NoError(t, os.Remove(store.registrationsPath()))
	for _, recovery := range []bool{false, true} {
		restarted, err := NewFilesystemStore(store.directory, "source", WithMissingCheckpointRecovery(recovery))
		require.ErrorIs(t, err, ErrRegisteredDestinationsMissing)
		require.Nil(t, restarted)
		require.NoFileExists(t, store.registrationsPath())
	}
}

func TestFilesystemStoreRejectsUnregisteredPersistedCursor(t *testing.T) {
	t.Parallel()
	store := newCheckpointTestStore(t, "source")
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	payload := []byte(`{"source":"source","cursors":[{"destination_id":"unknown","generation":"20260926-1"}]}`)
	require.NoError(t, os.WriteFile(store.documentPath(), payload, fileMode))
	for _, recovery := range []bool{false, true} {
		restarted, err := NewFilesystemStore(store.directory, "source", WithMissingCheckpointRecovery(recovery))
		require.ErrorIs(t, err, ErrCorruptCheckpoint)
		require.Nil(t, restarted)
		unchanged, err := os.ReadFile(store.documentPath())
		require.NoError(t, err)
		require.Equal(t, payload, unchanged)
	}
}

func TestFilesystemStoreRejectsInvalidRegistrationDocuments(t *testing.T) {
	t.Parallel()
	for name, payload := range map[string]string{
		"source":        `{"source":"other","destination_ids":[]}`,
		"missing list":  `{"source":"source"}`,
		"null list":     `{"source":"source","destination_ids":null}`,
		"unknown field": `{"source":"source","destination_ids":[],"unknown":true}`,
		"duplicate ID":  `{"source":"source","destination_ids":["a","a"]}`,
		"empty ID":      `{"source":"source","destination_ids":[""]}`,
		"blank ID":      `{"source":"source","destination_ids":[" "]}`,
		"null ID":       `{"source":"source","destination_ids":[null]}`,
		"object ID":     `{"source":"source","destination_ids":[{}]}`,
		"trailing data": `{"source":"source","destination_ids":[]} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := newCheckpointTestStore(t, "source")
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			require.NoError(t, os.WriteFile(store.registrationsPath(), []byte(payload), fileMode))
			for _, recovery := range []bool{false, true} {
				restarted, err := NewFilesystemStore(store.directory, "source", WithMissingCheckpointRecovery(recovery))
				require.ErrorIs(t, err, ErrCorruptCheckpoint)
				require.Nil(t, restarted)
				unchanged, err := os.ReadFile(store.registrationsPath())
				require.NoError(t, err)
				require.Equal(t, payload, string(unchanged))
			}
		})
	}
}

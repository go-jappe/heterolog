package checkpoint

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreSeparatesPreRegistrationScopeFromDelivery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "source")
	active := []objectstorage.DestinationID{"added"}
	first := CheckpointKey{Generation: "20261001-1", Compressed: true}
	boundary := CheckpointKey{Generation: "20261001-2", Compressed: true}
	writing := CheckpointKey{Generation: "20261001-3"}
	require.NoError(t, store.RegisterDestinations(ctx, active, []CheckpointKey{first, boundary, writing}))

	check := func(store *FilesystemStore) {
		t.Helper()
		// 登録前の世代は送信しませんが、送信済みとも扱いません。
		for _, key := range []CheckpointKey{first, boundary, {Generation: first.Generation}} {
			snapshot, err := store.Load(ctx, key)
			require.NoError(t, err)
			require.Equal(t, active, snapshot.CompletedDestinationIDs)
			require.Equal(t, active, snapshot.OutOfScopeDestinationIDs)
			require.Empty(t, snapshot.PendingDestinationIDs(active))
			require.True(t, snapshot.Complete(active))
			require.False(t, snapshot.DestinationDelivered("added"))
			require.False(t, snapshot.DeliveredToAny(active))
		}
		// 書き込み中だった最終世代は対象に残り、完了後は送信済みとして数えます。
		for _, key := range []CheckpointKey{writing, {Generation: writing.Generation, Compressed: true}} {
			snapshot, err := store.Load(ctx, key)
			require.NoError(t, err)
			require.Equal(t, active, snapshot.CompletedDestinationIDs)
			require.Empty(t, snapshot.OutOfScopeDestinationIDs)
			require.True(t, snapshot.DeliveredToAny(active))
		}
	}

	pending, err := store.Load(ctx, writing)
	require.NoError(t, err)
	require.Equal(t, active, pending.PendingDestinationIDs(active))
	delivered, err := store.AdvanceCompletionCursor(ctx, writing, active)
	require.NoError(t, err)
	require.Empty(t, delivered.OutOfScopeDestinationIDs)
	require.True(t, delivered.DestinationDelivered("added"))
	check(store)

	payload, err := os.ReadFile(store.documentPath())
	require.NoError(t, err)
	var document struct {
		Cursors []map[string]any `json:"cursors"`
	}
	require.NoError(t, json.Unmarshal(payload, &document))
	require.Equal(t, []map[string]any{{
		"destination_id": "added", "generation": writing.Generation, "out_of_scope_generation": boundary.Generation,
	}}, document.Cursors)

	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	restarted, err := NewFilesystemStore(store.Directory(), "source", WithMissingCheckpointRecovery(false))
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted), boundedwait.NoError)
	// 登録済みの宛先は、再登録時の既存名で対象外の範囲を決め直しません。
	require.NoError(t, restarted.RegisterDestinations(ctx, active, []CheckpointKey{first, boundary, writing, {Generation: "20261001-4", Compressed: true}}))
	check(restarted)
}

func TestFilesystemStoreRegistrationWithoutExistingFilesKeepsLegacyCursorFormat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "source")
	active := []objectstorage.DestinationID{"primary"}
	require.NoError(t, store.RegisterDestinations(ctx, active, nil))
	key := CheckpointKey{Generation: "20261001-1", Compressed: true}
	snapshot, err := store.AdvanceCompletionCursor(ctx, key, active)
	require.NoError(t, err)
	require.Nil(t, snapshot.OutOfScopeDestinationIDs)
	require.True(t, snapshot.DeliveredToAny(active))
	payload, err := os.ReadFile(store.documentPath())
	require.NoError(t, err)
	require.JSONEq(t, `{"source":"source","cursors":[{"destination_id":"primary","generation":"20261001-1"}]}`, string(payload))
}

func TestFilesystemStoreReadsLegacyCursorAsDelivered(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "source")
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	// 対象外の範囲を保存しない旧形式では、cursor以前を従来どおり送信・退避済みとして扱います。
	legacy := `{"source":"source","cursors":[{"destination_id":"primary","generation":"20261001-2"}]}`
	require.NoError(t, os.WriteFile(store.registrationsPath(), []byte(`{"source":"source","destination_ids":["primary"]}`), fileMode))
	require.NoError(t, os.WriteFile(store.documentPath(), []byte(legacy), fileMode))
	restarted, err := NewFilesystemStore(store.Directory(), "source", WithMissingCheckpointRecovery(false))
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "restarted.Close()", restarted.Close, boundedwait.ForceCloserOf(restarted), boundedwait.NoError)
	active := []objectstorage.DestinationID{"primary"}
	snapshot, err := restarted.Load(ctx, CheckpointKey{Generation: "20261001-1", Compressed: true})
	require.NoError(t, err)
	require.Equal(t, active, snapshot.CompletedDestinationIDs)
	require.Empty(t, snapshot.OutOfScopeDestinationIDs)
	require.True(t, snapshot.DeliveredToAny(active))
	cursors, err := restarted.DestinationCursors(ctx)
	require.NoError(t, err)
	require.Equal(t, []DestinationCursor{{DestinationID: "primary", Generation: "20261001-2"}}, cursors)
}

func TestFilesystemStoreRejectsInvalidOutOfScopeCursorWithoutRepair(t *testing.T) {
	t.Parallel()
	for name, cursor := range map[string]string{
		"null":             `{"destination_id":"primary","generation":"20261001-2","out_of_scope_generation":null}`,
		"not string":       `{"destination_id":"primary","generation":"20261001-2","out_of_scope_generation":1}`,
		"malformed":        `{"destination_id":"primary","generation":"20261001-2","out_of_scope_generation":"20261001-01"}`,
		"after completion": `{"destination_id":"primary","generation":"20261001-2","out_of_scope_generation":"20261001-3"}`,
		"no completion":    `{"destination_id":"primary","generation":"","out_of_scope_generation":"20261001-1"}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := newCheckpointTestStore(t, "source")
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			require.NoError(t, os.WriteFile(store.registrationsPath(), []byte(`{"source":"source","destination_ids":["primary"]}`), fileMode))
			payload := []byte(`{"source":"source","cursors":[` + cursor + `]}`)
			require.NoError(t, os.WriteFile(store.documentPath(), payload, fileMode))
			for _, recovery := range []bool{false, true} {
				restarted, err := NewFilesystemStore(store.Directory(), "source", WithMissingCheckpointRecovery(recovery))
				require.ErrorIs(t, err, ErrCorruptCheckpoint)
				require.Nil(t, restarted)
				unchanged, err := os.ReadFile(store.documentPath())
				require.NoError(t, err)
				require.Equal(t, payload, unchanged)
			}
			original := DestinationCursor{DestinationID: "registered", Generation: "20261001-1"}
			decoded := original
			require.Error(t, json.Unmarshal([]byte(cursor), &decoded))
			require.Equal(t, original, decoded, "failed decoding must not replace the receiver")
		})
	}
}

func TestCheckpointDeliveredToAnyIgnoresOutOfScopeAndInactiveDestinations(t *testing.T) {
	t.Parallel()
	snapshot := Checkpoint{
		Key:                      CheckpointKey{Generation: "20261001-1", Compressed: true},
		CompletedDestinationIDs:  []objectstorage.DestinationID{"delivered", "scoped-out"},
		OutOfScopeDestinationIDs: []objectstorage.DestinationID{"scoped-out"},
	}
	require.True(t, snapshot.DestinationDelivered("delivered"))
	require.False(t, snapshot.DestinationDelivered("scoped-out"))
	require.False(t, snapshot.DestinationDelivered("pending"))
	for _, test := range []struct {
		name      string
		active    []objectstorage.DestinationID
		complete  bool
		delivered bool
	}{
		{name: "out of scope only", active: []objectstorage.DestinationID{"scoped-out"}, complete: true},
		{name: "delivered and out of scope", active: []objectstorage.DestinationID{"scoped-out", "delivered"}, complete: true, delivered: true},
		{name: "pending destination", active: []objectstorage.DestinationID{"delivered", "pending"}, delivered: true},
		{name: "inactive delivery", active: []objectstorage.DestinationID{"pending"}},
		{name: "no destination"},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.complete, snapshot.Complete(test.active))
			require.Equal(t, test.delivered, snapshot.DeliveredToAny(test.active))
		})
	}
}

// checkpointの欠落を自動復旧すると、登録時に決めた対象外の範囲も空に戻ります。
// 宛先登録manifestから登録済みと分かる宛先は新規登録として扱わず、残存する世代を再送対象にします。
func TestFilesystemStoreMissingCheckpointRecoveryClearsOutOfScopeRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "source")
	active := []objectstorage.DestinationID{"primary"}
	preRegistration := CheckpointKey{Generation: "20261001-1", Compressed: true}
	writing := CheckpointKey{Generation: "20261001-2"}
	require.NoError(t, store.RegisterDestinations(ctx, active, []CheckpointKey{preRegistration, writing}))
	scoped, err := store.Load(ctx, preRegistration)
	require.NoError(t, err)
	require.Equal(t, active, scoped.OutOfScopeDestinationIDs)
	require.True(t, scoped.Complete(active))
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	require.NoError(t, os.Remove(store.documentPath()))

	recovered, err := NewFilesystemStore(store.Directory(), "source", WithMissingCheckpointRecovery(true))
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "recovered.Close()", recovered.Close, boundedwait.ForceCloserOf(recovered), boundedwait.NoError)
	cursors, err := recovered.DestinationCursors(ctx)
	require.NoError(t, err)
	require.Equal(t, []DestinationCursor{{DestinationID: "primary"}}, cursors)
	// 登録済みの宛先を再登録しても、現在のfile一覧から対象外の範囲を作り直しません。
	require.NoError(t, recovered.RegisterDestinations(ctx, active, []CheckpointKey{preRegistration, writing}))
	for _, key := range []CheckpointKey{preRegistration, writing} {
		snapshot, err := recovered.Load(ctx, key)
		require.NoError(t, err)
		require.Empty(t, snapshot.CompletedDestinationIDs)
		require.Empty(t, snapshot.OutOfScopeDestinationIDs)
		require.Equal(t, active, snapshot.PendingDestinationIDs(active))
	}
	// 再送した世代は送信済みとして数え、送信後に削除できます。
	delivered, err := recovered.AdvanceCompletionCursor(ctx, preRegistration, active)
	require.NoError(t, err)
	require.Empty(t, delivered.OutOfScopeDestinationIDs)
	require.True(t, delivered.DestinationDelivered("primary"))
	require.True(t, delivered.DeliveredToAny(active))
}

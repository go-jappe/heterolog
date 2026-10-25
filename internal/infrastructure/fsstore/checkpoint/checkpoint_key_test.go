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

func TestCheckpointKeyJSONIncludesGenerationAndCompression(t *testing.T) {
	t.Parallel()
	for _, compressed := range []bool{false, true} {
		key := CheckpointKey{Generation: "20260926-1", Compressed: compressed}
		payload, err := json.Marshal(key)
		require.NoError(t, err)
		var fields map[string]any
		require.NoError(t, json.Unmarshal(payload, &fields))
		require.Equal(t, map[string]any{"generation": "20260926-1", "compressed": compressed}, fields)
		var decoded CheckpointKey
		require.NoError(t, json.Unmarshal(payload, &decoded))
		require.Equal(t, key, decoded)
	}
}

func TestCheckpointDocumentsRejectMalformedTypedKeys(t *testing.T) {
	t.Parallel()
	for name, keyJSON := range map[string]string{
		"string":             `"20260926-1"`,
		"null":               `null`,
		"number":             `1`,
		"array":              `[]`,
		"empty object":       `{}`,
		"missing generation": `{"compressed":false}`,
		"null generation":    `{"generation":null,"compressed":false}`,
		"wrong generation":   `{"generation":1,"compressed":false}`,
		"empty generation":   `{"generation":"","compressed":false}`,
		"padded generation":  `{"generation":" 20260926-1 ","compressed":false}`,
		"missing compressed": `{"generation":"20260926-1"}`,
		"null compressed":    `{"generation":"20260926-1","compressed":null}`,
		"string compressed":  `{"generation":"20260926-1","compressed":"false"}`,
		"number compressed":  `{"generation":"20260926-1","compressed":0}`,
		"array compressed":   `{"generation":"20260926-1","compressed":[]}`,
		"object compressed":  `{"generation":"20260926-1","compressed":{}}`,
		"unknown field":      `{"generation":"20260926-1","compressed":false,"unknown":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			var key CheckpointKey
			err := json.Unmarshal([]byte(keyJSON), &key)
			require.Error(t, err)
		})
	}
}

func TestFilesystemStoreTypedKeysShareGenerationCompletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "source")
	require.NoError(t, err)
	keys := []CheckpointKey{
		{Generation: "20260926-1", Compressed: false},
		{Generation: "20260926-1", Compressed: true},
	}
	destinations := []objectstorage.DestinationID{"raw-only", "compressed-only"}
	for index, key := range keys {
		checkpoint, err := store.AdvanceCompletionCursor(ctx, key, destinations[index:index+1])
		require.NoError(t, err)
		require.Equal(t, key, checkpoint.Key)
		loaded, err := store.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, checkpoint, loaded)
	}
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	reopened, err := NewFilesystemStore(store.Directory(), "source")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "reopened.Close()", reopened.Close, boundedwait.ForceCloserOf(reopened), boundedwait.NoError)
	for _, key := range keys {
		checkpoint, err := reopened.Load(ctx, key)
		require.NoError(t, err)
		require.ElementsMatch(t, destinations, checkpoint.CompletedDestinationIDs)
	}
	// 未圧縮で完了した世代は、後から圧縮しても再送しません。
	_, err = reopened.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260926-2"}, []objectstorage.DestinationID{"raw-only"})
	require.NoError(t, err)
	compressed, err := reopened.Load(ctx, CheckpointKey{Generation: "20260926-2", Compressed: true})
	require.NoError(t, err)
	require.Equal(t, []objectstorage.DestinationID{"raw-only"}, compressed.CompletedDestinationIDs)
	require.Len(t, reopened.state.Cursors, 2)
}

func TestFilesystemStoreRegistersOneCursorFromDistinctTypedKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "source")
	require.NoError(t, err)
	input := []CheckpointKey{
		{Generation: "20260926-2", Compressed: true},
		{Generation: "20260926-1", Compressed: true},
		{Generation: "20260926-1", Compressed: false},
		{Generation: "20260926-1", Compressed: true},
	}
	active := []objectstorage.DestinationID{"primary"}
	require.NoError(t, store.RegisterDestinations(ctx, active, input))
	want := []CheckpointKey{
		{Generation: "20260926-1", Compressed: false},
		{Generation: "20260926-1", Compressed: true},
		{Generation: "20260926-2", Compressed: true},
	}
	require.Equal(t, []DestinationCursor{{DestinationID: "primary", Generation: "20260926-2", OutOfScopeGeneration: "20260926-2"}}, store.state.Cursors)
	payload, err := os.ReadFile(store.registrationsPath())
	require.NoError(t, err)
	require.JSONEq(t, `{"source":"source","destination_ids":["primary"]}`, string(payload))
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	reopened, err := NewFilesystemStore(store.Directory(), "source")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "reopened.Close()", reopened.Close, boundedwait.ForceCloserOf(reopened), boundedwait.NoError)
	// 再起動後も、登録前の世代を送信不要かつ未送信の対象外として読み直します。
	for _, key := range append(want, CheckpointKey{Generation: "20260926-2", Compressed: false}) {
		checkpoint, err := reopened.Load(ctx, key)
		require.NoError(t, err)
		require.Equal(t, active, checkpoint.CompletedDestinationIDs)
		require.Equal(t, active, checkpoint.OutOfScopeDestinationIDs)
		require.False(t, checkpoint.DeliveredToAny(active))
	}
}

func TestFilesystemStoreRejectsEmptyTypedGenerations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "source")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
	for _, compressed := range []bool{false, true} {
		key := CheckpointKey{Generation: " \t\u3000", Compressed: compressed}
		_, err := store.Load(ctx, key)
		require.ErrorIs(t, err, ErrInvalidGroupKey)
		_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
		require.ErrorIs(t, err, ErrInvalidGroupKey)
		require.ErrorIs(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{"primary"}, []CheckpointKey{key}), ErrInvalidGroupKey)
	}
	require.Empty(t, store.state.Cursors)
	require.Empty(t, store.registrations.DestinationIDs)
}

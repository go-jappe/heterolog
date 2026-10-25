package checkpoint

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestCheckpointGenerationRejectsInvalidInputWithoutMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "source")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), boundedwait.NoError)
	valid := CheckpointKey{Generation: "20260928-1", Compressed: true}
	_, err = store.AdvanceCompletionCursor(ctx, valid, []objectstorage.DestinationID{"registered"})
	require.NoError(t, err)
	wantState := cloneDocument(store.state)
	wantRegistrations, err := os.ReadFile(store.registrationsPath())
	require.NoError(t, err)
	wantDocument, err := os.ReadFile(store.documentPath())
	require.NoError(t, err)

	for _, generation := range []string{
		"", " ", "\t\u3000", " 20260928-1", "20260928-1 ", "20260928-1\n",
		"abc", "20260928/1", "20260928", "-1", "20260928-", "202609-1",
		"2026-09-28-1", "2026092812-1", "20261301-1", "20260229-1", "20260931-1",
		"20260928--1", "20260928-+1", "20260928-01", "20260928-00", "20260928-1.0",
		"20260928-１", "20260928-18446744073709551616", "20260928-1-extra",
	} {
		t.Run(generation, func(t *testing.T) {
			for _, compressed := range []bool{false, true} {
				key := CheckpointKey{Generation: generation, Compressed: compressed}
				_, err := store.Load(ctx, key)
				require.ErrorIs(t, err, ErrInvalidGroupKey)
				_, err = store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"new"})
				require.ErrorIs(t, err, ErrInvalidGroupKey)
				for _, destinations := range [][]objectstorage.DestinationID{{"new"}, {"registered"}, nil} {
					require.ErrorIs(t, store.RegisterDestinations(ctx, destinations, []CheckpointKey{valid, key}), ErrInvalidGroupKey)
				}
				payload, err := json.Marshal(key)
				require.NoError(t, err)
				decoded := valid
				require.ErrorIs(t, json.Unmarshal(payload, &decoded), ErrInvalidGroupKey)
				require.Equal(t, valid, decoded, "failed decoding must not replace the receiver")
			}
			if generation != "" {
				payload, err := json.Marshal(DestinationCursor{DestinationID: "new", Generation: generation})
				require.NoError(t, err)
				original := DestinationCursor{DestinationID: "registered", Generation: valid.Generation}
				decoded := original
				require.ErrorIs(t, json.Unmarshal(payload, &decoded), ErrInvalidGroupKey)
				require.Equal(t, original, decoded)
			}
		})
	}

	require.Equal(t, wantState, store.state)
	document, err := os.ReadFile(store.documentPath())
	require.NoError(t, err)
	require.Equal(t, wantDocument, document)
	registrations, err := os.ReadFile(store.registrationsPath())
	require.NoError(t, err)
	require.Equal(t, wantRegistrations, registrations)
	require.Nil(t, store.lastSaveErr)
}

func TestCheckpointGenerationAcceptsCalendarDatesAndUint64Indices(t *testing.T) {
	t.Parallel()
	for _, generation := range []string{
		"20240229-1", "20260901-0", "20260921-10", "20260928-18446744073709551615",
	} {
		t.Run(generation, func(t *testing.T) {
			ctx := context.Background()
			store, err := NewFilesystemStore(t.TempDir(), "source")
			require.NoError(t, err)
			for _, compressed := range []bool{false, true} {
				key := CheckpointKey{Generation: generation, Compressed: compressed}
				require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{"initial"}, []CheckpointKey{key}))
				snapshot, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"uploaded"})
				require.NoError(t, err)
				require.Equal(t, key, snapshot.Key)
				payload, err := json.Marshal(key)
				require.NoError(t, err)
				var decoded CheckpointKey
				require.NoError(t, json.Unmarshal(payload, &decoded))
				require.Equal(t, key, decoded)
			}
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			reopened, err := NewFilesystemStore(store.Directory(), "source")
			require.NoError(t, err)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "reopened.Close()", reopened.Close, boundedwait.ForceCloserOf(reopened), boundedwait.NoError)
			state, err := reopened.Load(ctx, CheckpointKey{Generation: generation})
			require.NoError(t, err)
			require.True(t, state.DestinationComplete("uploaded"))
		})
	}
	var cursor DestinationCursor
	require.NoError(t, json.Unmarshal([]byte(`{"destination_id":"primary","generation":""}`), &cursor))
	require.Empty(t, cursor.Generation, "only a cursor may use the empty initial position")
}

func TestReadyDestinationIDsRejectsMalformedInputBeforeFiltering(t *testing.T) {
	t.Parallel()
	valid := CheckpointKey{Generation: "20260928-1"}
	invalid := CheckpointKey{Generation: "invalid"}
	for _, pending := range [][]objectstorage.DestinationID{nil, {"primary"}} {
		_, err := ReadyDestinationIDs(context.Background(), nil, invalid, pending, nil)
		require.ErrorIs(t, err, ErrInvalidGroupKey)
		_, err = ReadyDestinationIDs(context.Background(), nil, valid, pending, []CheckpointKey{valid, invalid})
		require.ErrorIs(t, err, ErrInvalidGroupKey)
	}
}

func TestFilesystemStoreRejectsMalformedPersistedGenerationWithoutRepair(t *testing.T) {
	t.Parallel()
	for _, generation := range []string{" 20260928-1 ", "abc", "20260229-1", "20260928/1", "20260928-01"} {
		t.Run(generation, func(t *testing.T) {
			store, err := NewFilesystemStore(t.TempDir(), "source")
			require.NoError(t, err)
			_, err = store.AdvanceCompletionCursor(context.Background(), CheckpointKey{Generation: "20260928-1"}, []objectstorage.DestinationID{"primary"})
			require.NoError(t, err)
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			corrupt := cloneDocument(store.state)
			corrupt.Cursors[0].Generation = generation
			payload, err := json.Marshal(corrupt)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(store.documentPath(), payload, fileMode))
			for _, recoverMissing := range []bool{false, true} {
				reopened, err := NewFilesystemStore(store.Directory(), "source", WithMissingCheckpointRecovery(recoverMissing))
				require.Nil(t, reopened)
				require.ErrorIs(t, err, ErrCorruptCheckpoint)
				unchanged, err := os.ReadFile(store.documentPath())
				require.NoError(t, err)
				require.Equal(t, payload, unchanged)
			}
		})
	}
}

// 存在しない日付の世代キーは、ErrInvalidGroupKeyと共に日付の解析errorを原因として返します。
// checkpoint fileのdecodeも同じ検証を通るため、json.Unmarshalの結果でも確認します。
func TestCheckpointGenerationInvalidDateKeepsParseErrorAsCause(t *testing.T) {
	for _, generation := range []string{"20261301-1", "20260229-1", "20260931-1"} {
		t.Run(generation, func(t *testing.T) {
			period, _, _ := strings.Cut(generation, "-")
			payload, err := json.Marshal(CheckpointKey{Generation: generation, Compressed: true})
			require.NoError(t, err)
			var decoded CheckpointKey
			for name, err := range map[string]error{
				"validate": validateCheckpointGeneration(generation),
				"decode":   json.Unmarshal(payload, &decoded),
			} {
				require.ErrorIs(t, err, ErrInvalidGroupKey, name)
				var parseErr *time.ParseError
				require.ErrorAs(t, err, &parseErr, name)
				require.Equal(t, pkgfsstore.FileDateFormat, parseErr.Layout, name)
				require.Equal(t, period, parseErr.Value, name)
			}
		})
	}
}

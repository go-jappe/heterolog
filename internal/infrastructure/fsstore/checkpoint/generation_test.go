package checkpoint

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func TestFilesystemStoreScopesSuccessToSourceAndGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	first, err := NewFilesystemStore(directory, "first-series")
	require.NoError(t, err)
	second, err := NewFilesystemStore(directory, "second-series")
	require.NoError(t, err)
	_, err = first.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	empty, err := second.Load(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true})
	require.NoError(t, err)
	require.Empty(t, empty.CompletedDestinationIDs)
	_, err = second.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"secondary"})
	require.NoError(t, err)
	for source, destination := range map[string]objectstorage.DestinationID{"first-series": "primary", "second-series": "secondary"} {
		restarted, err := NewFilesystemStore(directory, source)
		require.NoError(t, err)
		checkpoint, err := restarted.Load(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, checkpoint.CompletedDestinationIDs, destination)
		other, err := restarted.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
		require.NoError(t, err)
		require.Empty(t, other.CompletedDestinationIDs)
		payload, err := os.ReadFile(restarted.documentPath())
		require.NoError(t, err)
		var saved map[string]any
		require.NoError(t, json.Unmarshal(payload, &saved))
		require.NotContains(t, saved, "version")
		require.Len(t, saved, 2)
		require.Equal(t, source, saved["source"])
		entry := saved["cursors"].([]any)[0].(map[string]any)
		require.Len(t, entry, 2)
		require.Contains(t, entry, "destination_id")
		require.Contains(t, entry, "generation")
	}
}

func TestFilesystemStoreRejectsPersistedSourceChangeWithoutChangingDocumentPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "original-source")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), nil)
	_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260923-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	before := checkpointStateForTest(store)
	path := store.documentPath()
	require.Equal(t, filepath.Join(store.directory, "original-source"+documentFileSuffix), path)
	candidate := cloneDocument(before)
	candidate.Source = "other-source"
	payload, err := json.Marshal(candidate)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, payload, fileMode))
	reopened, err := NewFilesystemStore(store.directory, "original-source")
	require.ErrorIs(t, err, ErrCorruptCheckpoint)
	require.ErrorIs(t, err, ErrCheckpointSourceMismatch)
	require.Nil(t, reopened)
	require.Equal(t, before, checkpointStateForTest(store))
	require.Equal(t, path, store.documentPath())
	require.NoError(t, checkpointSaveErrorForTest(store))
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, after)
	require.NoFileExists(t, filepath.Join(store.directory, "other-source"+documentFileSuffix))
}

func TestFilesystemStoreRejectsInvalidGenerationDocumentsWithoutOverwriting(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*checkpointDocument)
	}{
		{name: "wrong source", mutate: func(doc *checkpointDocument) { doc.Source = "other-source" }},
		{name: "null cursors", mutate: func(doc *checkpointDocument) { doc.Cursors = nil }},
		{name: "blank cursor", mutate: func(doc *checkpointDocument) { doc.Cursors[0].Generation = " " }},
		{name: "padded cursor", mutate: func(doc *checkpointDocument) { doc.Cursors[0].Generation = " 20260915-1 " }},
		{name: "duplicate destination", mutate: func(doc *checkpointDocument) { doc.Cursors = append(doc.Cursors, doc.Cursors[0]) }},
		{name: "empty destination", mutate: func(doc *checkpointDocument) { doc.Cursors[0].DestinationID = "" }},
		{name: "blank destination", mutate: func(doc *checkpointDocument) { doc.Cursors[0].DestinationID = " " }},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "validation-source"+documentFileSuffix)
			doc := newDocument("validation-source")
			doc.Cursors = []DestinationCursor{{DestinationID: "primary", Generation: "20260915-1"}}
			test.mutate(&doc)
			payload, err := json.Marshal(doc)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(path, payload, fileMode))
			store, err := NewFilesystemStore(directory, "validation-source")
			require.ErrorIs(t, err, ErrCorruptCheckpoint)
			require.Nil(t, store)
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, payload, after)
		})
	}
}

func TestFilesystemStoreRejectsInvalidJSONWithoutOverwriting(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(string) string
	}{
		{name: "unknown document field", mutate: func(payload string) string {
			return strings.Replace(payload, `"source":`, `"unknown":true,"source":`, 1)
		}},
		{name: "unknown checkpoint field", mutate: func(payload string) string {
			return strings.Replace(payload, `"destination_id":`, `"unknown":true,"destination_id":`, 1)
		}},
		{name: "missing destination", mutate: func(payload string) string {
			return strings.Replace(payload, `"destination_id":"primary",`, "", 1)
		}},
		{name: "null destination", mutate: func(payload string) string {
			return strings.Replace(payload, `"destination_id":"primary"`, `"destination_id":null`, 1)
		}},
		{name: "missing generation", mutate: func(payload string) string {
			return strings.Replace(payload, `,"generation":"20260915-1"`, "", 1)
		}},
		{name: "null generation", mutate: func(payload string) string {
			return strings.Replace(payload, `"generation":"20260915-1"`, `"generation":null`, 1)
		}},
		{name: "truncated document", mutate: func(payload string) string { return payload[:len(payload)-1] }},
		{name: "multiple JSON values", mutate: func(payload string) string { return payload + "\n{}" }},
		{name: "trailing garbage", mutate: func(payload string) string { return payload + "\n!" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "json-validation-source"+documentFileSuffix)
			doc := newDocument("json-validation-source")
			doc.Cursors = []DestinationCursor{{DestinationID: "primary", Generation: "20260915-1"}}
			validPayload, err := json.Marshal(doc)
			require.NoError(t, err)
			payload := []byte(test.mutate(string(validPayload)))
			require.NotEqual(t, validPayload, payload)
			require.NoError(t, os.WriteFile(path, payload, fileMode))
			for _, recovery := range []bool{false, true} {
				store, err := NewFilesystemStore(directory, "json-validation-source", WithMissingCheckpointRecovery(recovery))
				require.ErrorIs(t, err, ErrCorruptCheckpoint)
				require.Nil(t, store)
				after, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, payload, after)
			}
		})
	}
}

package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
)

func (s *FilesystemStore) readDocument() (checkpointDocument, error) {
	document, _, err := s.readDocumentState()
	return document, err
}

func TestNewFilesystemStoreRejectsBlankDirectory(t *testing.T) {
	for _, test := range []struct {
		name      string
		directory string
	}{
		{name: "empty", directory: ""},
		{name: "ASCII whitespace", directory: " \t\r\n"},
		{name: "Unicode whitespace", directory: "\u3000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			workingDirectory := t.TempDir()
			t.Chdir(workingDirectory)
			store, err := NewFilesystemStore(test.directory, "runner_default-source")
			if !errors.Is(err, ErrInvalidDirectory) {
				t.Fatalf("NewFilesystemStore(%q) error = %v, want ErrInvalidDirectory", test.directory, err)
			}
			if store != nil {
				t.Fatal("NewFilesystemStore() returned a store for a blank directory")
			}
			entries, err := os.ReadDir(workingDirectory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("working directory after rejected input: entries = %v, error = %v", entries, err)
			}
		})
	}
}

func TestFilesystemStoreLoadDoesNotRegisterOrWriteUnrecordedGenerations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	store, err := NewFilesystemStore(directory, "load-only-source")
	require.NoError(t, err)
	before, err := os.Stat(store.documentPath())
	require.NoError(t, err)
	checkpoint, err := store.Load(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true})
	require.NoError(t, err)
	require.Equal(t, CheckpointKey{Generation: "20260915-1", Compressed: true}, checkpoint.Key)
	require.NotNil(t, checkpoint.CompletedDestinationIDs)
	require.Empty(t, checkpoint.CompletedDestinationIDs)
	assertDestinationIDs(t, checkpoint.PendingDestinationIDs([]objectstorage.DestinationID{"new-destination"}), "new-destination")
	for _, successes := range [][]objectstorage.DestinationID{nil, {}} {
		empty, err := store.AdvanceCompletionCursor(ctx, checkpoint.Key, successes)
		require.NoError(t, err)
		require.Equal(t, checkpoint, empty)
	}
	require.Empty(t, store.state.Cursors)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 2, "Load must leave only the constructor's document and registrations")
	after, err := os.Stat(store.documentPath())
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after), "Load and empty updates must not rewrite the document")
	require.NoError(t, checkpointSaveErrorForTest(store))
}

func TestNewFilesystemStoreInitializesStateBeforeFirstOperation(t *testing.T) {
	t.Parallel()
	for _, existing := range []bool{false, true} {
		name := "missing checkpoint"
		if existing {
			name = "existing checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			const source = "eager-load-source"
			key := CheckpointKey{Generation: "20260923-1", Compressed: true}
			path := filepath.Join(directory, source+documentFileSuffix)
			want := newDocument(source)
			checkpoint := emptyCheckpoint(key)
			if existing {
				initialized, err := NewFilesystemStore(directory, source)
				require.NoError(t, err)
				require.NoError(t, initialized.RegisterDestinations(context.Background(), []objectstorage.DestinationID{"primary"}, nil))
				require.NoError(t, boundedwait.Close(t, "initialized.Close()", initialized.Close, boundedwait.ForceCloserOf(initialized)))
				checkpoint.CompletedDestinationIDs = []objectstorage.DestinationID{"primary"}
				want.Cursors = []DestinationCursor{{DestinationID: "primary", Generation: key.Generation}}
				payload, err := json.Marshal(want)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, payload, fileMode))
			}

			store, err := NewFilesystemStore(directory, source)
			require.NoError(t, err)
			require.Equal(t, want, store.state, "constructor must finish loading before returning")
			require.NoError(t, checkpointSaveErrorForTest(store))
			require.FileExists(t, path, "initialization saves even an empty checkpoint")
			require.FileExists(t, store.registrationsPath())

			// 初回操作より前にファイルを書き換えても、構築時に読み込んだstateを使います。
			// 外部変更の監視ではなく、構築後に再読込しない契約を確認します。
			corrupt := []byte(`{"source":`)
			require.NoError(t, os.WriteFile(path, corrupt, fileMode))
			got, err := store.Load(context.Background(), key)
			require.NoError(t, err)
			require.Equal(t, checkpoint, got)
			payload, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, corrupt, payload, "Load must not write checkpoint state")

			_, err = store.AdvanceCompletionCursor(context.Background(), key, []objectstorage.DestinationID{"secondary"})
			require.NoError(t, err)
			reopened, err := NewFilesystemStore(directory, source)
			require.NoError(t, err)
			got, err = reopened.Load(context.Background(), key)
			require.NoError(t, err)
			if existing {
				assertDestinationIDs(t, got.CompletedDestinationIDs, "primary", "secondary")
			} else {
				assertDestinationIDs(t, got.CompletedDestinationIDs, "secondary")
			}
		})
	}
}

func TestNewFilesystemStoreRejectsCheckpointReadFailure(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "read-failure-source"+documentFileSuffix)
	require.NoError(t, os.Mkdir(path, 0o700))
	store, err := NewFilesystemStore(directory, "read-failure-source")
	require.ErrorIs(t, err, ErrUnreadableCheckpoint)
	require.Nil(t, store)
	require.DirExists(t, path)

	// 読み込みに失敗したStoreは返さず、原因解消後は改めて構築できます。
	require.NoError(t, os.Remove(path))
	store, err = NewFilesystemStore(directory, "read-failure-source")
	require.NoError(t, err)
	require.Equal(t, newDocument("read-failure-source"), store.state)
}

func TestFilesystemStoreRecordsActualSuccessesWithoutDestinationSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "actual-success-source")
	require.NoError(t, err)
	successes := []objectstorage.DestinationID{"primary"}
	checkpoint, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true}, successes)
	require.NoError(t, err)
	successes[0] = "changed"
	assertDestinationIDs(t, checkpoint.CompletedDestinationIDs, "primary")
	require.True(t, checkpoint.Complete([]objectstorage.DestinationID{"primary"}))
	require.False(t, checkpoint.Complete([]objectstorage.DestinationID{"primary", "secondary"}))
	require.False(t, checkpoint.Complete(nil))
	assertDestinationIDs(t, checkpoint.PendingDestinationIDs([]objectstorage.DestinationID{"secondary", "primary", "new"}), "secondary", "new")

	// A new active destination needs no reconciliation or registration before its success can be recorded.
	checkpoint, err = store.AdvanceCompletionCursor(ctx, checkpoint.Key, []objectstorage.DestinationID{"secondary"})
	require.NoError(t, err)
	require.True(t, checkpoint.Complete([]objectstorage.DestinationID{"secondary"}))
	require.True(t, checkpoint.DestinationComplete("primary"))
	checkpoint.CompletedDestinationIDs[0] = "changed"
	loaded, err := store.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
	require.NoError(t, err)
	assertDestinationIDs(t, loaded.CompletedDestinationIDs, "primary", "secondary")
	restarted, err := NewFilesystemStore(store.directory, "actual-success-source")
	require.NoError(t, err)
	afterRestart, err := restarted.Load(ctx, CheckpointKey{Generation: "20260915-2", Compressed: true})
	require.NoError(t, err)
	require.Equal(t, loaded, afterRestart)
}

func TestFilesystemStoreLoadAndDuplicateSuccessesDoNotRewriteDocument(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "no-rewrite-source")
	require.NoError(t, err)
	expected, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-3", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	before, err := os.Stat(store.documentPath())
	require.NoError(t, err)
	loaded, err := store.Load(ctx, expected.Key)
	require.NoError(t, err)
	require.Equal(t, expected, loaded)
	loaded, err = store.AdvanceCompletionCursor(ctx, expected.Key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	require.Equal(t, expected, loaded)
	loaded, err = store.AdvanceCompletionCursor(ctx, expected.Key, nil)
	require.NoError(t, err)
	require.Equal(t, expected, loaded)
	after, err := os.Stat(store.documentPath())
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after), "read-only and duplicate reports must not atomically replace the document")
}

func TestFilesystemStoreKeepsDestinationCursorsIndependent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "destination-cursors")
	for _, update := range []struct {
		generation  string
		destination objectstorage.DestinationID
	}{{"20260915-2", "primary"}, {"20260915-10", "secondary"}} {
		_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: update.generation, Compressed: true}, []objectstorage.DestinationID{update.destination})
		require.NoError(t, err)
	}
	for _, test := range []struct {
		generation string
		want       []objectstorage.DestinationID
	}{
		{"20260915-1", []objectstorage.DestinationID{"primary", "secondary"}},
		{"20260915-2", []objectstorage.DestinationID{"primary", "secondary"}},
		{"20260915-3", []objectstorage.DestinationID{"secondary"}},
		{"20260915-10", []objectstorage.DestinationID{"secondary"}},
		{"20260915-11", nil},
	} {
		snapshot, err := store.Load(ctx, CheckpointKey{Generation: test.generation, Compressed: true})
		require.NoError(t, err)
		assertDestinationIDs(t, snapshot.CompletedDestinationIDs, test.want...)
	}
}

func TestFilesystemStoreOlderCompletionsNeverRegressOrRewriteCursor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "monotonic-cursor")
	key := CheckpointKey{Generation: "20260915-10", Compressed: true}
	_, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	before, err := os.Stat(store.documentPath())
	require.NoError(t, err)
	for _, generation := range []string{"20260915-2", "20260915-1", "20260915-10"} {
		snapshot, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: generation, Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.NoError(t, err)
		require.Equal(t, generation, snapshot.Key.Generation)
		require.True(t, snapshot.DestinationComplete("primary"))
	}
	require.Equal(t, []DestinationCursor{{DestinationID: "primary", Generation: key.Generation}}, store.state.Cursors)
	after, err := os.Stat(store.documentPath())
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after))
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	reopened, err := NewFilesystemStore(store.directory, "monotonic-cursor")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, boundedwait.Close(t, "reopened.Close()", reopened.Close, boundedwait.ForceCloserOf(reopened)))
	}()
	require.Equal(t, store.state, reopened.state)
}

func TestFilesystemStoreCursorStateIsBoundedByDestinationCount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "bounded-cursors")
	for index := 1; index <= 30; index++ {
		for _, compressed := range []bool{false, true} {
			key := CheckpointKey{Generation: fmt.Sprintf("20260915-%d", index), Compressed: compressed}
			_, err := store.AdvanceCompletionCursor(ctx, key, []objectstorage.DestinationID{"secondary", "primary"})
			require.NoError(t, err)
		}
	}
	require.Equal(t, []DestinationCursor{
		{DestinationID: "primary", Generation: "20260915-30"},
		{DestinationID: "secondary", Generation: "20260915-30"},
	}, store.state.Cursors)
	persisted, err := store.readDocument()
	require.NoError(t, err)
	require.Equal(t, store.state, persisted)
}

func TestFilesystemStoreRejectsInvalidGenerationWithoutChangingCursor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "invalid-cursor")
	_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: "20260915-1", Compressed: true}, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	before := checkpointStateForTest(store)
	payload, err := os.ReadFile(store.documentPath())
	require.NoError(t, err)
	for _, generation := range []string{"", " "} {
		_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: generation, Compressed: true}, []objectstorage.DestinationID{"primary"})
		require.ErrorIs(t, err, ErrInvalidGroupKey)
		require.Equal(t, before, checkpointStateForTest(store))
		require.NoError(t, checkpointSaveErrorForTest(store))
		after, err := os.ReadFile(store.documentPath())
		require.NoError(t, err)
		require.Equal(t, payload, after)
	}
}

func TestFilesystemStoreConcurrentGenerationsPreserveGreatestCursor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "concurrent-cursor")
	const updates = 20
	results := make(chan error, updates)
	var callers sync.WaitGroup
	for index := 1; index <= updates; index++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			_, err := store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: fmt.Sprintf("20260915-%d", index), Compressed: true}, []objectstorage.DestinationID{"primary"})
			results <- err
		}()
	}
	finished := make(chan struct{})
	go func() {
		callers.Wait()
		close(finished)
	}()
	boundedwait.Receive(t, finished, "concurrent cursor updates")
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, []DestinationCursor{{DestinationID: "primary", Generation: "20260915-20"}}, checkpointStateForTest(store).Cursors)
	persisted, err := store.readDocument()
	require.NoError(t, err)
	require.Equal(t, checkpointStateForTest(store), persisted)
}

func TestFilesystemStoreSerializesConcurrentUploadSuccesses(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := NewFilesystemStore(t.TempDir(), "runner_race-source")
	if err != nil {
		t.Fatalf("NewFilesystemStore() error = %v", err)
	}

	var wait sync.WaitGroup
	errorsByDestination := make(chan error, 2)
	for _, destination := range []objectstorage.DestinationID{"primary", "secondary"} {
		destination := destination
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, recordErr := store.AdvanceCompletionCursor(
				ctx,
				CheckpointKey{Generation: "20260903-6", Compressed: true},
				[]objectstorage.DestinationID{destination})
			errorsByDestination <- recordErr
		}()
	}
	finished := make(chan struct{})
	go func() {
		wait.Wait()
		close(finished)
	}()
	boundedwait.Receive(t, finished, "concurrent upload success records")
	close(errorsByDestination)
	for recordErr := range errorsByDestination {
		if recordErr != nil {
			t.Fatalf("AdvanceCompletionCursor() error = %v", recordErr)
		}
	}

	checkpoint, err := store.Load(ctx, CheckpointKey{Generation: "20260903-6", Compressed: true})
	if err != nil {
		t.Fatalf("AdvanceCompletionCursor(final) error = %v", err)
	}
	if !checkpoint.Complete([]objectstorage.DestinationID{"primary", "secondary"}) {
		t.Fatalf("Complete() = false, checkpoint = %+v", checkpoint)
	}
}

func TestFilesystemStoreRejectsCorruptDocumentAfterRestart(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	path := filepath.Join(directory, "runner_corrupt-source"+documentFileSuffix)
	if err := os.WriteFile(path, []byte(`{"source":"runner_corrupt-source"`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store, err := NewFilesystemStore(directory, "runner_corrupt-source")
	if !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("NewFilesystemStore() error = %v, want ErrCorruptCheckpoint", err)
	}
	require.Nil(t, store)
}

func assertDestinationIDs(
	t *testing.T,
	got []objectstorage.DestinationID,
	want ...objectstorage.DestinationID,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("destination IDs = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("destination IDs = %v, want %v", got, want)
		}
	}
}

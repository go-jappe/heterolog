package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func failedUploadProgressForTest(destination objectstorage.DestinationID) UploadProgress {
	return UploadProgress{
		Key:           CheckpointKey{Generation: "20260928-1", Compressed: true},
		DestinationID: destination,
		UploadResult:  UploadResultRetryableFailure,
		SavedTargets: []SavedFailureTarget{SavedArchiveTarget{
			Target:          "/failure/archive",
			FailureCategory: uploader.FailureCategoryRetryable,
		}},
	}
}

func TestUploadProgressRejectsEmptyDestinationWithoutChangingState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-invalid-destination")
	original := checkpointStateForTest(store)
	for _, destination := range []objectstorage.DestinationID{"", " \t\u3000"} {
		progress := failedUploadProgressForTest(destination)
		loaded, found, err := store.LoadUploadProgress(ctx, progress.Key, destination)
		require.False(t, found)
		require.ErrorIs(t, err, ErrInvalidDestinationID)
		require.Equal(t, UploadProgress{}, loaded)
		require.ErrorIs(t, store.SaveUploadProgress(ctx, progress), ErrInvalidDestinationID)
		require.Equal(t, original, checkpointStateForTest(store))
	}
}

func TestUploadProgressPersistsPartialHandoffAndSeparatesDestinations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-reopen")
	primary := failedUploadProgressForTest("primary")
	primary.SavedTargets = append(primary.SavedTargets, SavedSpoolTarget{
		Target: "/failure/spool", State: pkgspool.StateActive,
	})
	secondary := UploadProgress{Key: primary.Key, DestinationID: "secondary", UploadResult: UploadResultSuccess}
	require.NoError(t, store.SaveUploadProgress(ctx, primary))
	require.NoError(t, store.SaveUploadProgress(ctx, secondary))
	payload, err := os.ReadFile(store.documentPath())
	require.NoError(t, err)
	var document struct {
		UploadProgress []map[string]json.RawMessage `json:"upload_progress"`
	}
	require.NoError(t, json.Unmarshal(payload, &document))
	require.JSONEq(t, `"retryable_failure"`, string(document.UploadProgress[0]["upload_result"]))
	require.JSONEq(t, `"success"`, string(document.UploadProgress[1]["upload_result"]))
	for _, progress := range document.UploadProgress {
		require.NotContains(t, progress, "failure_category")
	}
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))

	reopened, err := NewFilesystemStore(store.directory, "progress-reopen")
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "reopened.Close()", reopened.Close, boundedwait.ForceCloserOf(reopened), boundedwait.NoError)
	loaded, found, err := reopened.LoadUploadProgress(ctx, primary.Key, "primary")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, primary, loaded)
	require.IsType(t, SavedArchiveTarget{}, loaded.SavedTargets[0])
	require.IsType(t, SavedSpoolTarget{}, loaded.SavedTargets[1])
	loaded, found, err = reopened.LoadUploadProgress(ctx, primary.Key, "secondary")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, secondary, loaded)
	loaded, found, err = reopened.LoadUploadProgress(ctx, primary.Key, "unregistered")
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, UploadProgress{}, loaded)
	completed, err := reopened.Load(ctx, primary.Key)
	require.NoError(t, err)
	require.Empty(t, completed.CompletedDestinationIDs, "a saved upload outcome alone must not advance the cursor")
}

func TestUploadProgressReopensLegacyResultsAndWritesExplicitResultsOnResume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-legacy-results")
	key := CheckpointKey{Generation: "20260928-1", Compressed: true}
	previous := []UploadProgress{
		{Key: key, DestinationID: "missing-category", UploadResult: UploadResultSuccess},
		{Key: key, DestinationID: "empty-category", UploadResult: UploadResultSuccess},
		failedUploadProgressForTest("retryable"),
		failedUploadProgressForTest("permanent"),
	}
	previous[3].UploadResult = UploadResultPermanentFailure
	for _, progress := range previous {
		require.NoError(t, store.SaveUploadProgress(ctx, progress))
	}
	payload, err := os.ReadFile(store.documentPath())
	require.NoError(t, err)
	require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
	var document map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &document))
	var legacy []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(document["upload_progress"], &legacy))
	for _, progress := range legacy {
		delete(progress, "upload_result")
	}
	legacy[1]["failure_category"] = json.RawMessage(`""`)
	legacy[2]["failure_category"] = json.RawMessage(`"retryable"`)
	legacy[3]["failure_category"] = json.RawMessage(`"permanent"`)
	document["upload_progress"], err = json.Marshal(legacy)
	require.NoError(t, err)
	payload, err = json.Marshal(document)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(store.documentPath(), payload, fileMode))

	reopened, err := NewFilesystemStore(store.directory, store.SourceID())
	require.NoError(t, err)
	boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "reopened.Close()", reopened.Close, boundedwait.ForceCloserOf(reopened), boundedwait.NoError)
	for _, progress := range previous {
		loaded, found, err := reopened.LoadUploadProgress(ctx, key, progress.DestinationID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, progress, loaded)
	}
	resumed := previous[2]
	resumed.SavedTargets = append(resumed.SavedTargets, SavedSpoolTarget{Target: "/failure/spool", State: pkgspool.StateActive})
	require.NoError(t, reopened.SaveUploadProgress(ctx, resumed))
	loaded, found, err := reopened.LoadUploadProgress(ctx, key, resumed.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, resumed, loaded)
	payload, err = os.ReadFile(reopened.documentPath())
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &document))
	var rewritten []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(document["upload_progress"], &rewritten))
	for index, progress := range rewritten {
		result, err := json.Marshal(previous[index].UploadResult)
		require.NoError(t, err)
		require.JSONEq(t, string(result), string(progress["upload_result"]))
		require.NotContains(t, progress, "failure_category")
	}
	completion, err := reopened.Load(ctx, key)
	require.NoError(t, err)
	require.Empty(t, completion.CompletedDestinationIDs, "legacy progress must not advance completion cursors")
}

func TestUploadProgressKeepsUploadAndReusedArchiveCategoriesSeparate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-reused-archive")
	progress := failedUploadProgressForTest("primary")
	// 今回の送信はpermanentでも、同じ内容のarchiveが以前retryableに保存されていれば
	// その既存ファイルを再利用するため、実際の保存先分類はretryableのままです。
	progress.UploadResult = UploadResultPermanentFailure
	require.NoError(t, store.SaveUploadProgress(ctx, progress))
	loaded, found, err := store.LoadUploadProgress(ctx, progress.Key, progress.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, UploadResultPermanentFailure, loaded.UploadResult)
	require.Equal(t, SavedArchiveTarget{Target: "/failure/archive", FailureCategory: uploader.FailureCategoryRetryable}, loaded.SavedTargets[0])
}

func TestUploadProgressMergesSavedTargetsWithoutAliasingOrLosingEarlierSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-merge")
	archive := failedUploadProgressForTest("primary")
	wantArchive := archive.SavedTargets[0]
	require.NoError(t, store.SaveUploadProgress(ctx, archive))
	archive.SavedTargets[0] = SavedArchiveTarget{Target: "changed input", FailureCategory: uploader.FailureCategoryRetryable}
	loaded, found, err := store.LoadUploadProgress(ctx, archive.Key, "primary")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wantArchive, loaded.SavedTargets[0])
	loaded.SavedTargets[0] = SavedArchiveTarget{Target: "changed result", FailureCategory: uploader.FailureCategoryRetryable}

	// A later parallel result contains only its own success and must preserve archive.
	spool := failedUploadProgressForTest("primary")
	spool.SavedTargets = []SavedFailureTarget{SavedSpoolTarget{Target: "/failure/spool", State: pkgspool.StateActive}}
	require.NoError(t, store.SaveUploadProgress(ctx, spool))
	loaded, found, err = store.LoadUploadProgress(ctx, archive.Key, "primary")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []SavedFailureTarget{wantArchive, spool.SavedTargets[0]}, loaded.SavedTargets)
	// A changed configured root is a distinct save, not a success at the old root.
	spool.SavedTargets[0] = SavedSpoolTarget{Target: "/failure/new-spool", State: pkgspool.StateActive}
	require.NoError(t, store.SaveUploadProgress(ctx, spool))
	loaded, found, err = store.LoadUploadProgress(ctx, archive.Key, "primary")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, loaded.SavedTargets, 3)

	conflicting := failedUploadProgressForTest("primary")
	conflicting.UploadResult = UploadResultPermanentFailure
	require.Error(t, store.SaveUploadProgress(ctx, conflicting))
}

func TestUploadProgressRejectsNilSavedTargetsWithoutChangingState(t *testing.T) {
	t.Parallel()
	store := newCheckpointTestStore(t, "progress-nil-target")
	original := checkpointStateForTest(store)
	for _, saved := range []SavedFailureTarget{nil, (*SavedArchiveTarget)(nil), (*SavedSpoolTarget)(nil), (*testSavedReplicaTarget)(nil)} {
		progress := failedUploadProgressForTest("primary")
		progress.SavedTargets = []SavedFailureTarget{saved}
		require.Error(t, store.SaveUploadProgress(context.Background(), progress))
		require.Equal(t, original, checkpointStateForTest(store))
	}
}

func TestUploadProgressDelegatesTargetValidationEqualityAndCopy(t *testing.T) {
	t.Parallel()
	// 第三の種類がsliceを保持していても、共通処理はその型を知らずに扱えます。
	first := &testSavedReplicaTarget{root: "/replica/first", receipts: []string{"receipt-1"}}
	previous := failedUploadProgressForTest("primary")
	previous.SavedTargets = []SavedFailureTarget{first}
	require.NoError(t, validateUploadProgress(previous))
	cloned := cloneUploadProgress(previous)
	require.True(t, equalUploadProgress(previous, cloned))
	cloned.SavedTargets[0].(*testSavedReplicaTarget).receipts[0] = "changed result"
	require.Equal(t, []string{"receipt-1"}, first.receipts)
	require.False(t, equalUploadProgress(previous, cloned))

	second := &testSavedReplicaTarget{root: "/replica/second", receipts: []string{"receipt-2"}}
	next := previous
	next.SavedTargets = []SavedFailureTarget{second}
	merged, err := mergeUploadProgress(previous, next)
	require.NoError(t, err)
	require.Empty(t, merged.SavedTargetConflicts)
	require.Len(t, merged.SavedTargets, 2)
	first.receipts[0] = "changed first input"
	second.receipts[0] = "changed second input"
	require.Equal(t, []string{"receipt-1"}, merged.SavedTargets[0].(*testSavedReplicaTarget).receipts)
	require.Equal(t, []string{"receipt-2"}, merged.SavedTargets[1].(*testSavedReplicaTarget).receipts)
	unchanged, err := mergeUploadProgress(merged, next)
	require.NoError(t, err)
	require.False(t, equalUploadProgress(merged, unchanged))
	require.Len(t, unchanged.SavedTargetConflicts, 1)
	require.Equal(t, merged.SavedTargets[1], unchanged.SavedTargetConflicts[0].Recorded)
	require.Equal(t, next.SavedTargets[0], unchanged.SavedTargetConflicts[0].Received)
	unchanged.SavedTargetConflicts[0].Recorded.(*testSavedReplicaTarget).receipts[0] = "changed conflict copy"
	require.Equal(t, []string{"receipt-2"}, merged.SavedTargets[1].(*testSavedReplicaTarget).receipts)

	invalid := previous
	invalid.SavedTargets = []SavedFailureTarget{&testSavedReplicaTarget{root: "/replica/invalid"}}
	require.ErrorIs(t, validateUploadProgress(invalid), errTestReplicaReceiptRequired)
}

func TestUploadProgressPersistsConflictingTargetUntilCallerPreservesData(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		original SavedFailureTarget
		changed  SavedFailureTarget
	}{
		{"archive", SavedArchiveTarget{Target: "/archive", FailureCategory: uploader.FailureCategoryRetryable}, SavedArchiveTarget{Target: "/archive", FailureCategory: uploader.FailureCategoryPermanent}},
		{"spool", SavedSpoolTarget{Target: "/spool", State: pkgspool.StateActive}, SavedSpoolTarget{Target: "/spool", State: pkgspool.StateDead}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := NewFilesystemStore(t.TempDir(), "progress-target-conflict")
			require.NoError(t, err)
			progress := failedUploadProgressForTest("primary")
			progress.SavedTargets = []SavedFailureTarget{test.original}
			require.NoError(t, store.SaveUploadProgress(ctx, progress))
			require.NoError(t, store.SaveUploadProgress(ctx, progress))
			progress.SavedTargets = []SavedFailureTarget{test.changed}
			err = store.SaveUploadProgress(ctx, progress)
			var conflict *SavedTargetConflictError
			require.ErrorAs(t, err, &conflict)
			require.Equal(t, progress.Key, conflict.Key)
			require.Equal(t, progress.DestinationID, conflict.DestinationID)
			require.Equal(t, []SavedTargetConflict{{Recorded: test.original, Received: test.changed}}, conflict.Conflicts)
			loaded, found, err := store.LoadUploadProgress(ctx, progress.Key, progress.DestinationID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []SavedFailureTarget{test.original}, loaded.SavedTargets)
			require.Equal(t, conflict.Conflicts, loaded.SavedTargetConflicts)

			// 未解決差分はJSONにも残り、再起動後の呼び出しへ同じtyped errorを返します。
			directory := store.directory
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			store, err = NewFilesystemStore(directory, "progress-target-conflict")
			require.NoError(t, err)
			boundedwait.CloseOnCleanup(t, boundedwait.HangTimeout, "store.Close()", store.Close, boundedwait.ForceCloserOf(store), nil)
			loaded, found, err = store.LoadUploadProgress(ctx, progress.Key, progress.DestinationID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, conflict.Conflicts, loaded.SavedTargetConflicts)
			var restartedConflict *SavedTargetConflictError
			require.ErrorAs(t, store.SaveUploadProgress(ctx, loaded), &restartedConflict)
			require.Equal(t, conflict.Conflicts, restartedConflict.Conflicts)

			// 矛盾していない別の保存先は成功記録へ追加し、未解決差分も保持します。
			additional := SavedSpoolTarget{Target: "/additional-spool", State: pkgspool.StateActive}
			progress.SavedTargets = append(progress.SavedTargets, additional)
			require.ErrorAs(t, store.SaveUploadProgress(ctx, progress), &conflict)
			loaded, found, err = store.LoadUploadProgress(ctx, progress.Key, progress.DestinationID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []SavedFailureTarget{test.original, additional}, loaded.SavedTargets)
			require.Equal(t, conflict.Conflicts, loaded.SavedTargetConflicts)
			// 呼び出し側が保全を完了した後、既存値と完全一致の差分を明示すると解消します。
			loaded.PreservedTargetConflicts = cloneSavedTargetConflicts(loaded.SavedTargetConflicts)
			loaded.SavedTargetConflicts = nil
			require.NoError(t, store.SaveUploadProgress(ctx, loaded))
			document, err := store.readDocument()
			require.NoError(t, err)
			require.Equal(t, []SavedFailureTarget{test.original, additional}, document.UploadProgress[0].SavedTargets)
			require.Empty(t, document.UploadProgress[0].SavedTargetConflicts)

			completed, err := store.AdvanceCompletionCursor(ctx, progress.Key, []objectstorage.DestinationID{"primary"})
			require.NoError(t, err)
			require.True(t, completed.Complete([]objectstorage.DestinationID{"primary"}))
		})
	}
}

func TestUploadProgressConflictKeepsCanonicalSuccessAndRecordsPendingDifference(t *testing.T) {
	t.Parallel()
	store := newCheckpointTestStore(t, "progress-conflict-no-warning-handler")
	progress := failedUploadProgressForTest("primary")
	require.NoError(t, store.SaveUploadProgress(context.Background(), progress))
	original, found, err := store.LoadUploadProgress(context.Background(), progress.Key, progress.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	progress.SavedTargets = []SavedFailureTarget{SavedArchiveTarget{
		Target: "/failure/archive", FailureCategory: uploader.FailureCategoryPermanent,
	}}
	var conflict *SavedTargetConflictError
	require.ErrorAs(t, store.SaveUploadProgress(context.Background(), progress), &conflict)
	loaded, found, err := store.LoadUploadProgress(context.Background(), progress.Key, progress.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, original.SavedTargets, loaded.SavedTargets)
	require.Equal(t, conflict.Conflicts, loaded.SavedTargetConflicts)
}

func TestUploadProgressStaleCanonicalSnapshotCannotClearPendingConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-stale-conflict")
	original := failedUploadProgressForTest("primary")
	require.NoError(t, store.SaveUploadProgress(ctx, original))
	stale, found, err := store.LoadUploadProgress(ctx, original.Key, original.DestinationID)
	require.NoError(t, err)
	require.True(t, found)

	changed := original
	changed.SavedTargets = []SavedFailureTarget{SavedArchiveTarget{
		Target: original.SavedTargets[0].Root(), FailureCategory: uploader.FailureCategoryPermanent,
	}}
	var conflict *SavedTargetConflictError
	require.ErrorAs(t, store.SaveUploadProgress(ctx, changed), &conflict)

	// conflict発生前のsnapshotが別targetを追加しても、差分の省略を解消とは扱いません。
	additional := SavedSpoolTarget{Target: "/additional-spool", State: pkgspool.StateActive}
	stale.SavedTargets = append(stale.SavedTargets, additional)
	require.ErrorAs(t, store.SaveUploadProgress(ctx, stale), &conflict)
	loaded, found, err := store.LoadUploadProgress(ctx, original.Key, original.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []SavedFailureTarget{original.SavedTargets[0], additional}, loaded.SavedTargets)
	require.Equal(t, conflict.Conflicts, loaded.SavedTargetConflicts)
	require.Nil(t, loaded.PreservedTargetConflicts)
	_, err = store.AdvanceCompletionCursor(ctx, original.Key, []objectstorage.DestinationID{original.DestinationID})
	require.ErrorAs(t, err, &conflict)
}

func TestUploadProgressOnlyExactPreservationProofClearsLatestConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-exact-preservation-proof")
	// 複数の結果を表現できるtest targetで、同じkeyの最新差分への置換を検証します。
	replicaProgress := failedUploadProgressForTest("secondary")
	replicaRecorded := &testSavedReplicaTarget{root: "/replica", receipts: []string{"recorded"}}
	replicaProgress.SavedTargets = []SavedFailureTarget{replicaRecorded}
	require.NoError(t, store.SaveUploadProgress(ctx, replicaProgress))
	replicaProgress.SavedTargets = []SavedFailureTarget{&testSavedReplicaTarget{root: "/replica", receipts: []string{"received-1"}}}
	var oldConflict *SavedTargetConflictError
	require.ErrorAs(t, store.SaveUploadProgress(ctx, replicaProgress), &oldConflict)
	replicaProgress.SavedTargets = []SavedFailureTarget{&testSavedReplicaTarget{root: "/replica", receipts: []string{"received-2"}}}
	var latestConflict *SavedTargetConflictError
	require.ErrorAs(t, store.SaveUploadProgress(ctx, replicaProgress), &latestConflict)
	require.NotEqual(t, oldConflict.Conflicts, latestConflict.Conflicts)

	resolution, found, err := store.LoadUploadProgress(ctx, replicaProgress.Key, replicaProgress.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	resolution.PreservedTargetConflicts = cloneSavedTargetConflicts(oldConflict.Conflicts)
	var stillPending *SavedTargetConflictError
	require.ErrorAs(t, store.SaveUploadProgress(ctx, resolution), &stillPending)
	require.Equal(t, latestConflict.Conflicts, stillPending.Conflicts)

	resolution.PreservedTargetConflicts = cloneSavedTargetConflicts(latestConflict.Conflicts)
	require.NoError(t, store.SaveUploadProgress(ctx, resolution))
	loaded, found, err := store.LoadUploadProgress(ctx, replicaProgress.Key, replicaProgress.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, loaded.SavedTargetConflicts)
	require.Nil(t, loaded.PreservedTargetConflicts)
	// 保存成功応答を失って同じ確認を再送しても、解消済みなら冪等に受理します。
	require.NoError(t, store.SaveUploadProgress(ctx, resolution))
}

func TestPreservedTargetConflictsAreInputOnly(t *testing.T) {
	t.Parallel()
	progress := failedUploadProgressForTest("primary")
	progress.PreservedTargetConflicts = []SavedTargetConflict{{
		Recorded: progress.SavedTargets[0],
		Received: SavedArchiveTarget{Target: progress.SavedTargets[0].Root(), FailureCategory: uploader.FailureCategoryPermanent},
	}}
	require.NoError(t, validateUploadProgress(progress))
	payload, err := json.Marshal(progress)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "preserved_target_conflicts")
	require.Nil(t, cloneUploadProgress(progress).PreservedTargetConflicts)
}

func TestUploadProgressRejectsInvalidSavedTargetConflicts(t *testing.T) {
	t.Parallel()
	recorded := SavedArchiveTarget{Target: "/failure/archive", FailureCategory: uploader.FailureCategoryRetryable}
	received := SavedArchiveTarget{Target: recorded.Target, FailureCategory: uploader.FailureCategoryPermanent}
	valid := failedUploadProgressForTest("primary")
	valid.SavedTargets = []SavedFailureTarget{recorded}
	valid.SavedTargetConflicts = []SavedTargetConflict{{Recorded: recorded, Received: received}}
	require.NoError(t, validateUploadProgress(valid))

	for _, test := range []struct {
		name   string
		mutate func(*UploadProgress)
	}{
		{"nil recorded", func(progress *UploadProgress) { progress.SavedTargetConflicts[0].Recorded = nil }},
		{"nil received", func(progress *UploadProgress) { progress.SavedTargetConflicts[0].Received = nil }},
		{"different kind", func(progress *UploadProgress) {
			progress.SavedTargetConflicts[0].Received = SavedSpoolTarget{Target: recorded.Target, State: pkgspool.StateActive}
		}},
		{"different root", func(progress *UploadProgress) {
			progress.SavedTargetConflicts[0].Received = SavedArchiveTarget{Target: "/failure/other", FailureCategory: uploader.FailureCategoryPermanent}
		}},
		{"same result", func(progress *UploadProgress) { progress.SavedTargetConflicts[0].Received = recorded }},
		{"recorded is not canonical", func(progress *UploadProgress) {
			progress.SavedTargets[0] = received
		}},
		{"duplicate", func(progress *UploadProgress) {
			progress.SavedTargetConflicts = append(progress.SavedTargetConflicts, progress.SavedTargetConflicts[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			progress := cloneUploadProgress(valid)
			test.mutate(&progress)
			require.Error(t, validateUploadProgress(progress))
		})
	}
}

func TestUploadProgressRejectsInvalidPreservationProofs(t *testing.T) {
	t.Parallel()
	recorded := SavedArchiveTarget{Target: "/failure/archive", FailureCategory: uploader.FailureCategoryRetryable}
	received := SavedArchiveTarget{Target: recorded.Target, FailureCategory: uploader.FailureCategoryPermanent}
	newValid := func() UploadProgress {
		progress := failedUploadProgressForTest("primary")
		progress.SavedTargets = []SavedFailureTarget{recorded}
		progress.PreservedTargetConflicts = []SavedTargetConflict{{Recorded: recorded, Received: received}}
		return progress
	}
	require.NoError(t, validateUploadProgress(newValid()))
	for _, test := range []struct {
		name   string
		mutate func(*UploadProgress)
	}{
		{"recorded is not canonical", func(progress *UploadProgress) { progress.SavedTargets[0] = received }},
		{"duplicate", func(progress *UploadProgress) {
			progress.PreservedTargetConflicts = append(progress.PreservedTargetConflicts, progress.PreservedTargetConflicts[0])
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			progress := newValid()
			test.mutate(&progress)
			require.Error(t, validateUploadProgress(progress))
		})
	}
}

func TestAdvanceCompletionCursorDoesNotCoverUnresolvedSavedTargetConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-conflict-cursor")
	progress := failedUploadProgressForTest("primary")
	recorded := progress.SavedTargets[0]
	received := SavedArchiveTarget{Target: recorded.Root(), FailureCategory: uploader.FailureCategoryPermanent}
	require.NoError(t, store.SaveUploadProgress(ctx, progress))
	progress.SavedTargets = []SavedFailureTarget{received}
	var conflict *SavedTargetConflictError
	require.ErrorAs(t, store.SaveUploadProgress(ctx, progress), &conflict)

	// 同じ宛先でも未解決世代より古い完了位置は進められます。
	older := CheckpointKey{Generation: "20260927-9", Compressed: true}
	checkpoint, err := store.AdvanceCompletionCursor(ctx, older, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	require.True(t, checkpoint.DestinationComplete("primary"))

	// 別宛先は同じ世代まで進めても、primaryの未解決差分に影響しません。
	checkpoint, err = store.AdvanceCompletionCursor(ctx, progress.Key, []objectstorage.DestinationID{"secondary"})
	require.NoError(t, err)
	require.True(t, checkpoint.DestinationComplete("secondary"))

	before := checkpointStateForTest(store)
	for _, generation := range []string{progress.Key.Generation, "20260928-2"} {
		_, err = store.AdvanceCompletionCursor(ctx, CheckpointKey{Generation: generation, Compressed: true}, []objectstorage.DestinationID{"primary"})
		var blocked *SavedTargetConflictError
		require.ErrorAs(t, err, &blocked)
		require.Equal(t, progress.Key, blocked.Key)
		require.Equal(t, progress.DestinationID, blocked.DestinationID)
		require.Equal(t, conflict.Conflicts, blocked.Conflicts)
		require.Equal(t, before, checkpointStateForTest(store))
	}

	loaded, found, err := store.LoadUploadProgress(ctx, progress.Key, progress.DestinationID)
	require.NoError(t, err)
	require.True(t, found)
	loaded.PreservedTargetConflicts = cloneSavedTargetConflicts(loaded.SavedTargetConflicts)
	loaded.SavedTargetConflicts = nil
	require.NoError(t, store.SaveUploadProgress(ctx, loaded))
	_, err = store.AdvanceCompletionCursor(ctx, progress.Key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
}

func TestUploadProgressConflictResolutionDoesNotMaskPendingSaveOrValidationErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store, err := NewFilesystemStore(t.TempDir(), "progress-conflict-pending")
		require.NoError(t, err)
		boundedwait.CloseOnCleanup(t, boundedwait.BubbleTimeout, "store.Close()", store.Close, store.ForceClose, nil)
		progress := failedUploadProgressForTest("primary")
		original := progress.SavedTargets[0]
		require.NoError(t, store.SaveUploadProgress(ctx, progress))
		restore := blockCheckpointReplacement(t, store)
		progress.SavedTargets = []SavedFailureTarget{
			SavedArchiveTarget{Target: original.Root(), FailureCategory: uploader.FailureCategoryPermanent},
			SavedSpoolTarget{Target: "/spool", State: pkgspool.StateActive},
		}
		var conflict *SavedTargetConflictError
		require.ErrorAs(t, store.SaveUploadProgress(ctx, progress), &conflict)
		progress.SavedTargets[0] = conflict.Conflicts[0].Recorded.Clone()
		progress.PreservedTargetConflicts = cloneSavedTargetConflicts(conflict.Conflicts)
		pendingSaveForTest(t, store.SaveUploadProgress(ctx, progress))
		progress.PreservedTargetConflicts = nil
		loaded, found, err := store.LoadUploadProgress(ctx, progress.Key, progress.DestinationID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, original, loaded.SavedTargets[0])
		require.Len(t, loaded.SavedTargets, 2)

		// 不正値・送信結果の変更は、保存待ちへ弱めません。
		progress.UploadResult = UploadResultPermanentFailure
		err = store.SaveUploadProgress(ctx, progress)
		require.ErrorIs(t, err, ErrUploadResultChanged)
		var pending *PendingSaveError
		require.False(t, errors.As(err, &pending))
		progress.UploadResult = UploadResultRetryableFailure
		progress.SavedTargets = []SavedFailureTarget{SavedSpoolTarget{Target: "/spool", State: "invalid"}}
		err = store.SaveUploadProgress(ctx, progress)
		require.Error(t, err)
		require.False(t, errors.As(err, &pending))

		synctest.Wait()
		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		document, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, loaded, document.UploadProgress[0])
	})
}

type testSavedReplicaTarget struct {
	root     string
	receipts []string
}

func (*testSavedReplicaTarget) Kind() failurestore.DurableType { return "replica" }
func (target *testSavedReplicaTarget) Root() string            { return target.root }

var errTestReplicaReceiptRequired = errors.New("replica receipt is required")

func (target *testSavedReplicaTarget) Validate() error {
	if len(target.receipts) == 0 {
		return errTestReplicaReceiptRequired
	}
	return nil
}

func (target *testSavedReplicaTarget) Equal(other SavedFailureTarget) bool {
	right, ok := other.(*testSavedReplicaTarget)
	return ok && target.root == right.root && slices.Equal(target.receipts, right.receipts)
}

func (target *testSavedReplicaTarget) Clone() SavedFailureTarget {
	return &testSavedReplicaTarget{root: target.root, receipts: slices.Clone(target.receipts)}
}

func TestAdvanceCompletionCursorSavesCursorAndClearsOnlyCoveredProgressTogether(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "progress-complete")
	old := failedUploadProgressForTest("primary")
	other := failedUploadProgressForTest("secondary")
	newer := failedUploadProgressForTest("primary")
	newer.Key.Generation = "20260928-2"
	uncompressed := failedUploadProgressForTest("primary")
	uncompressed.Key.Compressed = false
	for _, progress := range []UploadProgress{old, other, newer, uncompressed} {
		require.NoError(t, store.SaveUploadProgress(ctx, progress))
	}
	_, err := store.AdvanceCompletionCursor(ctx, old.Key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	document, err := store.readDocument()
	require.NoError(t, err)
	require.Equal(t, "20260928-1", document.Cursors[cursorIndex(document.Cursors, "primary")].Generation)
	require.Equal(t, []UploadProgress{other, newer}, document.UploadProgress)
	// A delayed partial save must not recreate progress covered by the completed cursor.
	require.NoError(t, store.SaveUploadProgress(ctx, old))
	loaded, found, err := store.LoadUploadProgress(ctx, old.Key, "primary")
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, UploadProgress{}, loaded)

	// Cleanup is not conditional on cursor movement itself.
	store.stateOrSaveMu.Lock()
	store.state.UploadProgress = append(store.state.UploadProgress, old)
	store.stateOrSaveMu.Unlock()
	_, err = store.AdvanceCompletionCursor(ctx, old.Key, []objectstorage.DestinationID{"primary"})
	require.NoError(t, err)
	document, err = store.readDocument()
	require.NoError(t, err)
	require.Equal(t, []UploadProgress{other, newer}, document.UploadProgress)
}

func TestUploadProgressConcurrentTargetSavesKeepBothResults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newCheckpointTestStore(t, "concurrent-progress")
	archive := failedUploadProgressForTest("primary")
	spool := failedUploadProgressForTest("primary")
	spool.SavedTargets = []SavedFailureTarget{SavedSpoolTarget{Target: "/failure/spool", State: pkgspool.StateActive}}
	start := make(chan struct{})
	errors := make(chan error, 2)
	for _, progress := range []UploadProgress{archive, spool} {
		go func() {
			<-start
			errors <- store.SaveUploadProgress(ctx, progress)
		}()
	}
	close(start)
	for range 2 {
		require.NoError(t, boundedwait.Receive(t, errors, "concurrent upload progress save"))
	}
	loaded, found, err := store.LoadUploadProgress(ctx, archive.Key, "primary")
	require.NoError(t, err)
	require.True(t, found)
	require.ElementsMatch(t, []SavedFailureTarget{archive.SavedTargets[0], spool.SavedTargets[0]}, loaded.SavedTargets)
}

func TestUploadProgressSaveRecoveryWritesLatestCursorAndProgress(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newCheckpointTestStore(t, "progress-save-recovery")
		first := failedUploadProgressForTest("primary")
		require.NoError(t, store.RegisterDestinations(ctx, []objectstorage.DestinationID{"primary"}, nil))
		restore := blockCheckpointReplacement(t, store)
		pendingSaveForTest(t, store.SaveUploadProgress(ctx, first))
		loaded, found, err := store.LoadUploadProgress(ctx, first.Key, "primary")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, first, loaded, "memory remains usable while its save is pending")
		first.SavedTargets = append(first.SavedTargets, SavedSpoolTarget{Target: "/failure/spool", State: pkgspool.StateActive})
		pendingSaveForTest(t, store.SaveUploadProgress(ctx, first))
		loaded, found, err = store.LoadUploadProgress(ctx, first.Key, "primary")
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, loaded.SavedTargets, 2)
		_, err = store.AdvanceCompletionCursor(ctx, first.Key, []objectstorage.DestinationID{"primary"})
		pendingSaveForTest(t, err)
		second := failedUploadProgressForTest("primary")
		second.Key.Generation = "20260928-2"
		pendingSaveForTest(t, store.SaveUploadProgress(ctx, second))
		synctest.Wait()
		restore()
		advanceCheckpointRecoveryForTest()
		require.NoError(t, checkpointSaveErrorForTest(store))
		document, err := store.readDocument()
		require.NoError(t, err)
		require.Equal(t, []DestinationCursor{{DestinationID: "primary", Generation: first.Key.Generation}}, document.Cursors)
		require.Equal(t, []UploadProgress{second}, document.UploadProgress)
	})
}

func TestNewFilesystemStoreRejectsCorruptUploadProgress(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name               string
		mutate             func(*checkpointDocument)
		mutateSavedTarget  func(map[string]any)
		mutateProgressJSON func(map[string]any)
	}{
		{name: "unknown upload result", mutate: func(doc *checkpointDocument) { doc.UploadProgress[0].UploadResult = "other" }},
		{name: "empty upload result", mutate: func(doc *checkpointDocument) { doc.UploadProgress[0].UploadResult = "" }},
		{name: "null upload result", mutateProgressJSON: func(progress map[string]any) { progress["upload_result"] = nil }},
		{name: "mixed result schemas", mutateProgressJSON: func(progress map[string]any) { progress["failure_category"] = "permanent" }},
		{name: "bad key", mutate: func(doc *checkpointDocument) { doc.UploadProgress[0].Key.Generation = "bad" }},
		{name: "duplicate record", mutate: func(doc *checkpointDocument) { doc.UploadProgress = append(doc.UploadProgress, doc.UploadProgress[0]) }},
		{name: "duplicate target", mutate: func(doc *checkpointDocument) {
			progress := &doc.UploadProgress[0]
			progress.SavedTargets = append(progress.SavedTargets, progress.SavedTargets[0])
		}},
		{name: "unknown target type", mutateSavedTarget: func(target map[string]any) { target["type"] = "none" }},
		{name: "empty target root", mutateSavedTarget: func(target map[string]any) { target["target"] = "" }},
		{name: "archive without category", mutateSavedTarget: func(target map[string]any) { delete(target, "archive_failure_category") }},
		{name: "archive invalid category", mutateSavedTarget: func(target map[string]any) { target["archive_failure_category"] = "unknown" }},
		{name: "archive with spool state", mutateSavedTarget: func(target map[string]any) { target["spool_state"] = string(pkgspool.StateActive) }},
		{name: "spool without state", mutateSavedTarget: func(target map[string]any) {
			target["type"] = string(failurestore.DurableTypeSpool)
			delete(target, "archive_failure_category")
		}},
		{name: "spool invalid state", mutateSavedTarget: func(target map[string]any) {
			target["type"] = string(failurestore.DurableTypeSpool)
			delete(target, "archive_failure_category")
			target["spool_state"] = "unknown"
		}},
		{name: "spool with archive category", mutateSavedTarget: func(target map[string]any) {
			target["type"] = string(failurestore.DurableTypeSpool)
			target["spool_state"] = string(pkgspool.StateActive)
		}},
		{name: "missing destination cursor", mutate: func(doc *checkpointDocument) { doc.UploadProgress[0].DestinationID = "unknown" }},
		{name: "completed progress", mutate: func(doc *checkpointDocument) { doc.Cursors[0].Generation = doc.UploadProgress[0].Key.Generation }},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newCheckpointTestStore(t, "corrupt-progress")
			require.NoError(t, store.SaveUploadProgress(context.Background(), failedUploadProgressForTest("primary")))
			doc := checkpointStateForTest(store)
			if test.mutate != nil {
				test.mutate(&doc)
			}
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			payload, err := json.Marshal(doc)
			require.NoError(t, err)
			if test.mutateSavedTarget != nil || test.mutateProgressJSON != nil {
				// 型では表現できない不正な組み合わせを、読み込み対象のJSONに作ります。
				var raw map[string]any
				require.NoError(t, json.Unmarshal(payload, &raw))
				progress := raw["upload_progress"].([]any)[0].(map[string]any)
				if test.mutateSavedTarget != nil {
					target := progress["saved_targets"].([]any)[0].(map[string]any)
					test.mutateSavedTarget(target)
				}
				if test.mutateProgressJSON != nil {
					test.mutateProgressJSON(progress)
				}
				payload, err = json.Marshal(raw)
				require.NoError(t, err)
			}
			require.NoError(t, os.WriteFile(filepath.Join(store.directory, "corrupt-progress"+documentFileSuffix), payload, fileMode))
			_, err = NewFilesystemStore(store.directory, "corrupt-progress")
			require.ErrorIs(t, err, ErrCorruptCheckpoint)
		})
	}
}

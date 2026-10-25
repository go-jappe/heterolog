package processing

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type uploadCheckpointStub struct {
	state checkpoint.Checkpoint
	mark  func(context.Context, checkpoint.CheckpointKey, []objectstorage.DestinationID) (checkpoint.Checkpoint, error)
}

func (store *uploadCheckpointStub) Load(context.Context, checkpoint.CheckpointKey) (checkpoint.Checkpoint, error) {
	return store.state, nil
}

func (store *uploadCheckpointStub) AdvanceCompletionCursor(ctx context.Context, key checkpoint.CheckpointKey, destinations []objectstorage.DestinationID) (checkpoint.Checkpoint, error) {
	if store.mark != nil {
		return store.mark(ctx, key, destinations)
	}
	store.state.Key = key
	store.state.CompletedDestinationIDs = append(store.state.CompletedDestinationIDs, destinations...)
	return store.state, nil
}

func successfulUpload(destinations []objectstorage.DestinationID) uploader.UploadReport {
	var report uploader.UploadReport
	for _, destination := range destinations {
		report.Results = append(report.Results, uploader.DestinationResult{DestinationID: destination})
	}
	return report
}

func TestRunUploadRetainsSuccessBeforeCheckpointFailureOrPanic(t *testing.T) {
	for _, failure := range []string{"error", "panic"} {
		t.Run(failure, func(t *testing.T) {
			operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-1", Compressed: true}, UploadFilePath: "/logs/app_20261011_1.log.gz", DestinationIDs: []objectstorage.DestinationID{"primary"}}
			store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key}}
			store.mark = func(context.Context, checkpoint.CheckpointKey, []objectstorage.DestinationID) (checkpoint.Checkpoint, error) {
				if failure == "panic" {
					panic("checkpoint interrupted")
				}
				return checkpoint.Checkpoint{}, context.Canceled
			}
			attempts := 0
			dependencies := UploadDependencies{Checkpoints: store, Uploader: DestinationUploadFunc(func(_ context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
				attempts++
				return successfulUpload(destinations)
			})}
			if failure == "panic" {
				require.Panics(t, func() { _, _ = RunUpload(context.Background(), &operation, dependencies) })
			} else {
				result, err := RunUpload(context.Background(), &operation, dependencies)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, UploadFailureCompletionCursor, result.FailureStage)
			}
			require.Equal(t, operation.DestinationIDs, operation.UploadedDestinationIDs)
			store.mark = nil
			result, err := RunUpload(context.Background(), &operation, dependencies)
			require.NoError(t, err)
			require.True(t, result.Complete)
			require.Empty(t, operation.UploadedDestinationIDs)
			require.Equal(t, 1, attempts, "checkpoint retry must not repeat a successful upload")
		})
	}
}

func TestRunUploadAcceptsOnlyPendingSaveErrors(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending only", true: "mixed failure"}[mixed], func(t *testing.T) {
			operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-1"}, UploadFilePath: "/logs/app_20261011_1.log", DestinationIDs: []objectstorage.DestinationID{"primary"}}
			pending := &checkpoint.PendingSaveError{Err: errors.New("disk full")}
			store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key}}
			store.mark = func(context.Context, checkpoint.CheckpointKey, []objectstorage.DestinationID) (checkpoint.Checkpoint, error) {
				state := checkpoint.Checkpoint{Key: operation.Key, CompletedDestinationIDs: operation.DestinationIDs}
				if mixed {
					return state, errors.Join(pending, context.Canceled)
				}
				return state, pending
			}
			result, err := RunUpload(context.Background(), &operation, UploadDependencies{Checkpoints: store, Uploader: DestinationUploadFunc(func(_ context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
				return successfulUpload(destinations)
			})})
			if mixed {
				require.ErrorIs(t, err, context.Canceled)
				require.False(t, result.Complete)
				require.Equal(t, operation.DestinationIDs, operation.UploadedDestinationIDs)
			} else {
				require.NoError(t, err)
				require.True(t, result.Complete)
				require.Equal(t, []error{pending}, result.PendingSaveErrors)
				require.Empty(t, operation.UploadedDestinationIDs)
			}
		})
	}
}

func TestRunUploadCommitsReadySuccessesAndContinuesOtherDestinations(t *testing.T) {
	operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-2", Compressed: true}, UploadFilePath: "/logs/app_20261011_2.log.gz",
		DestinationIDs: []objectstorage.DestinationID{"ready", "blocked", "new"}, UploadedDestinationIDs: []objectstorage.DestinationID{"ready", "blocked"}}
	store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key}}
	var sent []objectstorage.DestinationID
	result, err := RunUpload(context.Background(), &operation, UploadDependencies{
		Checkpoints: store, CommitReadySubset: true,
		ReadyDestinations: func(_ context.Context, destinations []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
			return slices.DeleteFunc(destinations, func(destination objectstorage.DestinationID) bool { return destination == "blocked" }), nil
		},
		Uploader: DestinationUploadFunc(func(_ context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
			sent = slices.Clone(destinations)
			return successfulUpload(destinations)
		}),
	})
	require.NoError(t, err)
	require.False(t, result.Complete)
	require.Equal(t, []objectstorage.DestinationID{"new"}, sent)
	require.Equal(t, []objectstorage.DestinationID{"blocked"}, operation.UploadedDestinationIDs)
	require.ElementsMatch(t, []objectstorage.DestinationID{"ready", "new"}, store.state.CompletedDestinationIDs)
}

func TestCommitPendingUploadsDoesNotCompleteWhileRememberedSuccessIsBlocked(t *testing.T) {
	operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-2", Compressed: true}, DestinationIDs: []objectstorage.DestinationID{"primary"}, UploadedDestinationIDs: []objectstorage.DestinationID{"primary"}}
	store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key, CompletedDestinationIDs: []objectstorage.DestinationID{"primary"}}}
	result, err := CommitPendingUploads(context.Background(), &operation, UploadDependencies{
		Checkpoints: store, CommitReadySubset: true,
		ReadyDestinations: func(context.Context, []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
			return nil, nil
		},
	})
	require.NoError(t, err)
	require.False(t, result.Complete, "記録待ちの成功は世代順で反映できるまで保持する")
	require.Empty(t, result.CommittedDestinationIDs)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, operation.UploadedDestinationIDs)
}

func TestRunUploadReturnsOnlyMatchingConflictToUploader(t *testing.T) {
	operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-1", Compressed: true}, UploadFilePath: "/logs/app_20261011_1.log.gz",
		DestinationIDs: []objectstorage.DestinationID{"primary", "secondary"}, UploadedDestinationIDs: []objectstorage.DestinationID{"primary", "secondary"}}
	conflict := &checkpoint.SavedTargetConflictError{Key: operation.Key, DestinationID: "primary"}
	store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key}, mark: func(context.Context, checkpoint.CheckpointKey, []objectstorage.DestinationID) (checkpoint.Checkpoint, error) {
		return checkpoint.Checkpoint{}, errors.Join(conflict, &checkpoint.PendingSaveError{Err: errors.New("pending")})
	}}
	_, err := RunUpload(context.Background(), &operation, UploadDependencies{Checkpoints: store})
	require.ErrorIs(t, err, conflict)
	require.Equal(t, []objectstorage.DestinationID{"secondary"}, operation.UploadedDestinationIDs)
	store.mark = nil
	var sent []objectstorage.DestinationID
	result, err := RunUpload(context.Background(), &operation, UploadDependencies{Checkpoints: store, Uploader: DestinationUploadFunc(func(_ context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
		sent = slices.Clone(destinations)
		return successfulUpload(destinations)
	})})
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, sent)
}

func TestRunUploadWithoutStoreRetainsPartialSuccess(t *testing.T) {
	operation := UploadState{UploadFilePath: "/logs/app_20261011_1.log.gz", DestinationIDs: []objectstorage.DestinationID{"primary", "secondary"}}
	var calls [][]objectstorage.DestinationID
	dependencies := UploadDependencies{Uploader: DestinationUploadFunc(func(_ context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
		calls = append(calls, slices.Clone(destinations))
		report := successfulUpload(destinations)
		if len(calls) == 1 {
			report.Results[1].Err = errors.New("secondary unavailable")
		}
		return report
	})}
	result, err := RunUpload(context.Background(), &operation, dependencies)
	require.Error(t, err)
	require.False(t, result.Complete)
	require.False(t, result.Delivered)
	result, err = RunUpload(context.Background(), &operation, dependencies)
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.True(t, result.Delivered, "Storeなしの完了は、この状態内で送信した宛先だけから作ります")
	require.Equal(t, [][]objectstorage.DestinationID{{"primary", "secondary"}, {"secondary"}}, calls)
}

func TestRunUploadReportsDeliverySeparatelyFromOutOfScopeCompletion(t *testing.T) {
	const (
		added    objectstorage.DestinationID = "added"
		existing objectstorage.DestinationID = "existing"
	)
	key := checkpoint.CheckpointKey{Generation: "20261011-1", Compressed: true}
	for _, test := range []struct {
		name      string
		state     checkpoint.Checkpoint
		active    []objectstorage.DestinationID
		sent      []objectstorage.DestinationID
		delivered bool
	}{
		{
			name:   "out of scope only",
			state:  checkpoint.Checkpoint{CompletedDestinationIDs: []objectstorage.DestinationID{added}, OutOfScopeDestinationIDs: []objectstorage.DestinationID{added}},
			active: []objectstorage.DestinationID{added},
		},
		{
			name:      "delivered by another active destination",
			state:     checkpoint.Checkpoint{CompletedDestinationIDs: []objectstorage.DestinationID{added, existing}, OutOfScopeDestinationIDs: []objectstorage.DestinationID{added}},
			active:    []objectstorage.DestinationID{added, existing},
			delivered: true,
		},
		{
			name:      "uploaded to the pending destination",
			state:     checkpoint.Checkpoint{CompletedDestinationIDs: []objectstorage.DestinationID{added}, OutOfScopeDestinationIDs: []objectstorage.DestinationID{added}},
			active:    []objectstorage.DestinationID{added, existing},
			sent:      []objectstorage.DestinationID{existing},
			delivered: true,
		},
		{
			name:      "completion without scope information",
			state:     checkpoint.Checkpoint{CompletedDestinationIDs: []objectstorage.DestinationID{existing}},
			active:    []objectstorage.DestinationID{existing},
			delivered: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.state.Key = key
			operation := UploadState{Key: key, UploadFilePath: "/logs/app_20261011_1.log.gz", DestinationIDs: test.active}
			store := &uploadCheckpointStub{state: test.state}
			var sent []objectstorage.DestinationID
			result, err := RunUpload(context.Background(), &operation, UploadDependencies{Checkpoints: store, Uploader: DestinationUploadFunc(func(_ context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
				sent = append(sent, destinations...)
				return successfulUpload(destinations)
			})})
			require.NoError(t, err)
			require.True(t, result.Complete, "対象外の宛先にも送信しないまま完了します")
			require.Equal(t, test.delivered, result.Delivered)
			require.Equal(t, test.sent, sent)
			require.Equal(t, test.delivered, CanRemoveCompletedFile(true, test.active, result.Delivered))
		})
	}
}

func TestRunUploadRejectsIncompleteReportBeforeAdvancingCheckpoint(t *testing.T) {
	for _, test := range []struct {
		name    string
		results []uploader.DestinationResult
	}{
		{name: "missing", results: []uploader.DestinationResult{{DestinationID: "primary"}}},
		{name: "unexpected", results: []uploader.DestinationResult{{DestinationID: "primary"}, {DestinationID: "unknown"}}},
		{name: "duplicate", results: []uploader.DestinationResult{{DestinationID: "primary"}, {DestinationID: "primary"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-1"}, UploadFilePath: "/logs/app_20261011_1.log", DestinationIDs: []objectstorage.DestinationID{"primary", "secondary"}}
			store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key}}
			result, err := RunUpload(context.Background(), &operation, UploadDependencies{Checkpoints: store, Uploader: DestinationUploadFunc(func(context.Context, uploader.UploadRequest, []objectstorage.DestinationID) uploader.UploadReport {
				return uploader.UploadReport{Results: test.results}
			})})
			require.ErrorIs(t, err, uploader.ErrInvalidUploadReport)
			require.Equal(t, UploadFailureReport, result.FailureStage)
			require.Empty(t, store.state.CompletedDestinationIDs)
			require.Empty(t, operation.UploadedDestinationIDs)
		})
	}
}

func TestRunUploadRejectsDuplicateRequestedDestinations(t *testing.T) {
	for _, results := range [][]uploader.DestinationResult{
		{{DestinationID: "primary"}},
		{{DestinationID: "primary"}, {DestinationID: "primary"}},
	} {
		operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-1"}, UploadFilePath: "/logs/app_20261011_1.log", DestinationIDs: []objectstorage.DestinationID{"primary", "primary"}}
		store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key}}
		calls := 0
		result, err := RunUpload(context.Background(), &operation, UploadDependencies{Checkpoints: store, Uploader: DestinationUploadFunc(func(context.Context, uploader.UploadRequest, []objectstorage.DestinationID) uploader.UploadReport {
			calls++
			return uploader.UploadReport{Results: results}
		})})
		require.ErrorIs(t, err, uploader.ErrInvalidUploadReport)
		require.ErrorIs(t, err, uploader.ErrDuplicateDestinationID)
		require.Equal(t, UploadFailureReport, result.FailureStage)
		require.False(t, result.Complete)
		require.Empty(t, store.state.CompletedDestinationIDs)
		require.Empty(t, operation.UploadedDestinationIDs)
		require.Zero(t, calls, "宛先指定の重複は送信・完了記録の更新前に拒否する")
	}
}

func TestCommitPendingUploadsRejectsDuplicateRememberedDestinations(t *testing.T) {
	operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-1"}, DestinationIDs: []objectstorage.DestinationID{"primary"}, UploadedDestinationIDs: []objectstorage.DestinationID{"primary", "primary"}}
	store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key}}
	result, err := CommitPendingUploads(context.Background(), &operation, UploadDependencies{Checkpoints: store})
	require.ErrorIs(t, err, uploader.ErrDuplicateDestinationID)
	require.Equal(t, UploadFailureReport, result.FailureStage)
	require.Empty(t, store.state.CompletedDestinationIDs)
	require.Len(t, operation.UploadedDestinationIDs, 2)
}

func TestRunUploadRejectsDuplicateReadyDestinations(t *testing.T) {
	operation := UploadState{Key: checkpoint.CheckpointKey{Generation: "20261011-1"}, DestinationIDs: []objectstorage.DestinationID{"primary"}}
	store := &uploadCheckpointStub{state: checkpoint.Checkpoint{Key: operation.Key}}
	result, err := RunUpload(context.Background(), &operation, UploadDependencies{
		Checkpoints: store,
		ReadyDestinations: func(context.Context, []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
			return []objectstorage.DestinationID{"primary", "primary"}, nil
		},
	})
	require.ErrorIs(t, err, uploader.ErrDuplicateDestinationID)
	require.Equal(t, UploadFailureReport, result.FailureStage)
	require.False(t, result.Complete)
	require.Empty(t, store.state.CompletedDestinationIDs)
}

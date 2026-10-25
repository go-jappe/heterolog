package fsarchivehandler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type archiveConflictRecoveryStore struct {
	checkpointstore.Store
	joinPending bool
}

func (s *archiveConflictRecoveryStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	checkpoint, err := s.Store.AdvanceCompletionCursor(ctx, key, destinations)
	var conflict *checkpointstore.SavedTargetConflictError
	if s.joinPending && errors.As(err, &conflict) {
		err = errors.Join(err, &checkpointstore.PendingSaveError{Err: errors.New("pending conflicting checkpoint save")})
	}
	return checkpoint, err
}

// ManagedUploaderを再度呼び、保全後に保存結果の矛盾を解消する動作を再現します。
// 実ファイルの保全と、送信済み結果を使って再送を防ぐ動作はuploader側で試験します。
type archiveConflictRecoveryUploader struct {
	store            checkpointstore.Store
	key              checkpointstore.CheckpointKey
	destinations     []objectstorage.DestinationID
	attempts         map[objectstorage.DestinationID]int
	failPreservation bool
}

func (u *archiveConflictRecoveryUploader) DestinationIDs() []objectstorage.DestinationID {
	return slices.Clone(u.destinations)
}

func (u *archiveConflictRecoveryUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, request, u.DestinationIDs()).Err()
}

func (u *archiveConflictRecoveryUploader) UploadDestinations(ctx context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	var report uploader.UploadReport
	for _, destination := range destinations {
		u.attempts[destination]++
		attempt := u.attempts[destination]
		result := uploader.DestinationResult{DestinationID: destination}
		switch {
		case destination == "failure" && attempt == 1:
			result.Err = errors.New("remote upload failed")
		case destination != "failure" && attempt >= 2:
			if u.failPreservation && attempt == 2 {
				result.Err = errors.New("conflict preservation failed")
				break
			}
			progress, found, err := u.store.LoadUploadProgress(ctx, u.key, destination)
			if err == nil && !found {
				err = errors.New("missing conflicting upload progress")
			}
			if err == nil {
				progress.PreservedTargetConflicts = progress.SavedTargetConflicts
				progress.SavedTargetConflicts = nil
				err = u.store.SaveUploadProgress(ctx, progress)
			}
			result.Err = err
		}
		report.Results = append(report.Results, result)
	}
	return report
}

// newArchiveConflictRecoveryFixtureは、構築時にuploaderを渡し、checkpoint設定から作ったStoreを
// conflict用に包んだPipelineと、一世代の処理状態を返します。
// wrapStoreを指定すると、handlerと処理状態が使うStoreを障害注入用に包みます。
func newArchiveConflictRecoveryFixture(
	t *testing.T,
	destinations []objectstorage.DestinationID,
	wrapStore ...func(checkpointstore.Store) checkpointstore.Store,
) (*FailureArchivePipeline, *archiveConflictRecoveryStore, *archiveConflictRecoveryUploader, *archiveFileOperation) {
	t.Helper()
	store := &archiveConflictRecoveryStore{}
	var handlerStore checkpointstore.Store
	remote := &archiveConflictRecoveryUploader{
		store: store, destinations: destinations,
		attempts: make(map[objectstorage.DestinationID]int),
	}
	pipeline := newTestArchiveHandler(t, time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC), func(cfg *testArchiveConfig) {
		cfg.setArchiveUploader(failurearchive.FailureCategoryRetryable, remote)
		cfg.wrapArchiveCheckpointStore(failurearchive.FailureCategoryRetryable, func(production checkpointstore.Store) checkpointstore.Store {
			store.Store = production
			handlerStore = store
			for _, wrap := range wrapStore {
				handlerStore = wrap(production)
			}
			return handlerStore
		})
	})
	handler := pipeline.handler
	fileName := handler.Fsstore.FileNameRetryable
	path := filepath.Join(fileName.Dir, "test-app_20260922_1.jsonl.gz")
	require.NoError(t, os.WriteFile(path, []byte("compressed log"), 0o600))
	key := failureArchiveCompressedLogCheckpointKey(t, handler, failurearchive.FailureCategoryRetryable, path)
	// 世代のkeyは構築後に決まります。uploadの開始前に渡します。
	remote.key = key
	operation := &archiveFileOperation{
		store: handlerStore, key: key, path: path, category: failurearchive.FailureCategoryRetryable,
		fileName: fileName, uploader: remote,
	}
	return pipeline, store, remote, operation
}

func seedArchiveSavedTargetConflict(t *testing.T, store checkpointstore.Store, key checkpointstore.CheckpointKey, destination objectstorage.DestinationID) {
	t.Helper()
	progress := checkpointstore.UploadProgress{
		Key: key, DestinationID: destination, UploadResult: checkpointstore.UploadResultRetryableFailure,
		SavedTargets: []checkpointstore.SavedFailureTarget{checkpointstore.SavedArchiveTarget{
			Target: "/failure/archive", FailureCategory: uploader.FailureCategoryRetryable,
		}},
	}
	require.NoError(t, store.SaveUploadProgress(context.Background(), progress))
	progress.SavedTargets[0] = checkpointstore.SavedArchiveTarget{
		Target: "/failure/archive", FailureCategory: uploader.FailureCategoryPermanent,
	}
	var conflict *checkpointstore.SavedTargetConflictError
	require.ErrorAs(t, store.SaveUploadProgress(context.Background(), progress), &conflict)
}

func TestFailureArchiveCheckpointConflictRecoveryReentersOnlyConflictingDestination(t *testing.T) {
	for _, stage := range []string{"initial queue", "resume cursor update", "resume upload cursor update"} {
		for _, peers := range []struct {
			name         string
			destinations []objectstorage.DestinationID
		}{
			{"sole destination", []objectstorage.DestinationID{"primary"}},
			{"successful peer", []objectstorage.DestinationID{"primary", "secondary"}},
			{"successful and failed peers", []objectstorage.DestinationID{"primary", "secondary", "failure"}},
		} {
			for _, pendingSave := range []bool{false, true} {
				name := stage + "/" + peers.name
				if pendingSave {
					name += "/with pending save"
				}
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						pipeline, store, remote, operation := newArchiveConflictRecoveryFixture(t, peers.destinations)
						handler := pipeline.handler
						store.joinPending = pendingSave
						seedArchiveSavedTargetConflict(t, store, operation.key, "primary")
						ctx := context.Background()
						failedPeer := slices.Contains(peers.destinations, "failure")
						if stage == "initial queue" {
							outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{operation.path}, operation.category, operation.fileName)
							require.True(t, outcome.retryRequired)
							recoveryKey := failureArchiveRecoveryKey(operation.category, operation.key)
							require.True(t, handler.logFileProcessingRecovery.Has(recoveryKey), "a sole conflict remains pending even when joined with PendingSaveError")
							require.Contains(t, handler.protectedArchivePaths(operation.category), operation.path)
							require.FileExists(t, operation.path)
							synctest.Wait()
							time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
							synctest.Wait()
							require.False(t, handler.logFileProcessingRecovery.Has(recoveryKey), "checkpoint recovery must reenter preservation and finish")
							require.NotContains(t, handler.protectedArchivePaths(operation.category), operation.path)
						} else {
							if stage == "resume cursor update" {
								for _, destination := range peers.destinations {
									remote.attempts[destination] = 1 // 送信済みで、cursor更新前に中断した状態です。
									if destination != "failure" {
										operation.uploadedDestinationIDs = append(operation.uploadedDestinationIDs, destination)
									}
								}
							}
							_, pending, err := handler.runArchiveFileOperation(ctx, operation)
							var conflict *checkpointstore.SavedTargetConflictError
							require.True(t, pending)
							require.ErrorAs(t, err, &conflict)
							require.NotContains(t, operation.uploadedDestinationIDs, objectstorage.DestinationID("primary"))
							if slices.Contains(peers.destinations, "secondary") {
								require.Equal(t, []objectstorage.DestinationID{"secondary"}, operation.uploadedDestinationIDs, "successful peers remain queued for their cursor update")
							}
							require.FileExists(t, operation.path)
							outcome, pending, err := handler.runArchiveFileOperation(ctx, operation)
							require.NoError(t, err)
							require.False(t, pending)
							require.False(t, outcome.retryRequired)
						}
						require.Equal(t, 2, remote.attempts["primary"])
						if slices.Contains(peers.destinations, "secondary") {
							require.Equal(t, 1, remote.attempts["secondary"], "successful peers must not be reuploaded")
						}
						checkpoint, err := store.Load(ctx, operation.key)
						require.NoError(t, err)
						require.Contains(t, checkpoint.CompletedDestinationIDs, objectstorage.DestinationID("primary"))
						if failedPeer {
							require.Equal(t, 2, remote.attempts["failure"])
							require.Equal(t, 2, remote.attempts["primary"])
							require.Equal(t, 1, remote.attempts["secondary"])
							checkpoint, err = store.Load(ctx, operation.key)
							require.NoError(t, err)
						}
						require.True(t, checkpoint.Complete(peers.destinations))
						require.NoFileExists(t, operation.path, "remove the local file only after all destinations complete")
					})
				})
			}
		}
	}
}

func TestFailureArchiveCheckpointConflictRecoveryKeepsMismatchedKeyPending(t *testing.T) {
	for _, mismatch := range []string{"older generation", "different representation"} {
		for _, pendingSave := range []bool{false, true} {
			name := mismatch
			if pendingSave {
				name += "/with pending save"
			}
			t.Run(name, func(t *testing.T) {
				pipeline, store, remote, operation := newArchiveConflictRecoveryFixture(t, []objectstorage.DestinationID{"primary"})
				store.joinPending = pendingSave
				conflictingKey := operation.key
				if mismatch == "older generation" {
					conflictingKey.Generation = "20260921-1"
				} else {
					conflictingKey.Compressed = false
				}
				seedArchiveSavedTargetConflict(t, store, conflictingKey, "primary")
				operation.uploadedDestinationIDs = []objectstorage.DestinationID{"primary"}
				for range 2 {
					_, pending, err := pipeline.handler.runArchiveFileOperation(context.Background(), operation)
					var conflict *checkpointstore.SavedTargetConflictError
					require.True(t, pending)
					require.ErrorAs(t, err, &conflict)
					require.Equal(t, conflictingKey, conflict.Key)
					require.Equal(t, []objectstorage.DestinationID{"primary"}, operation.uploadedDestinationIDs)
					require.Empty(t, remote.attempts, "the current payload cannot preserve a different checkpoint key")
					require.FileExists(t, operation.path)
				}
			})
		}
	}
}

func TestFailureArchiveCheckpointConflictRecoveryRetainsFailedPreservationUntilComplete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pipeline, store, remote, operation := newArchiveConflictRecoveryFixture(t, []objectstorage.DestinationID{"primary", "secondary", "failure"})
		handler := pipeline.handler
		seedArchiveSavedTargetConflict(t, store, operation.key, "primary")
		remote.failPreservation = true
		ctx := context.Background()
		outcome := handler.uploadCheckpointedArchiveGroup(ctx, []string{operation.path}, operation.category, operation.fileName)
		require.True(t, outcome.retryRequired)
		recoveryKey := failureArchiveRecoveryKey(operation.category, operation.key)
		require.True(t, handler.logFileProcessingRecovery.Has(recoveryKey))
		synctest.Wait()
		time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.True(t, handler.logFileProcessingRecovery.Has(recoveryKey), "a failed preservation attempt remains accepted work")
		require.Equal(t, map[objectstorage.DestinationID]int{"primary": 2, "secondary": 1, "failure": 2}, remote.attempts)
		require.FileExists(t, operation.path)
		checkpoint, err := store.Load(ctx, operation.key)
		require.NoError(t, err)
		require.ElementsMatch(t, []objectstorage.DestinationID{"secondary", "failure"}, checkpoint.CompletedDestinationIDs)
		progress, found, err := store.LoadUploadProgress(ctx, operation.key, "primary")
		require.NoError(t, err)
		require.True(t, found)
		require.NotEmpty(t, progress.SavedTargetConflicts)
		time.Sleep(2 * logfile.DefaultLogFileProcessingRecoveryInterval)
		synctest.Wait()
		require.False(t, handler.logFileProcessingRecovery.Has(recoveryKey))
		outcome = handler.uploadCheckpointedArchiveGroup(ctx, []string{operation.path}, operation.category, operation.fileName)
		require.False(t, outcome.retryRequired)
		require.Equal(t, map[objectstorage.DestinationID]int{"primary": 3, "secondary": 1, "failure": 2}, remote.attempts)
		require.NoFileExists(t, operation.path)
	})
}

func TestFailureArchiveCheckpointConflictRecoveryRetainsMultiplePreservationDestinations(t *testing.T) {
	pipeline, store, remote, operation := newArchiveConflictRecoveryFixture(t, []objectstorage.DestinationID{"primary", "secondary", "failure"})
	seedArchiveSavedTargetConflict(t, store, operation.key, "primary")
	seedArchiveSavedTargetConflict(t, store, operation.key, "secondary")
	ctx := context.Background()
	for _, destination := range []objectstorage.DestinationID{"primary", "secondary"} {
		_, pending, err := pipeline.handler.runArchiveFileOperation(ctx, operation)
		var conflict *checkpointstore.SavedTargetConflictError
		require.True(t, pending)
		require.ErrorAs(t, err, &conflict)
		require.Equal(t, destination, conflict.DestinationID)
		require.NotContains(t, operation.uploadedDestinationIDs, destination)
		require.FileExists(t, operation.path)
	}
	require.Empty(t, operation.uploadedDestinationIDs)
	outcome, pending, err := pipeline.handler.runArchiveFileOperation(ctx, operation)
	require.NoError(t, err)
	require.False(t, pending)
	require.False(t, outcome.retryRequired)
	require.Equal(t, map[objectstorage.DestinationID]int{"primary": 2, "secondary": 2, "failure": 2}, remote.attempts)
	checkpoint, err := store.Load(ctx, operation.key)
	require.NoError(t, err)
	require.ElementsMatch(t, []objectstorage.DestinationID{"primary", "secondary", "failure"}, checkpoint.CompletedDestinationIDs)
	require.NoFileExists(t, operation.path)
}

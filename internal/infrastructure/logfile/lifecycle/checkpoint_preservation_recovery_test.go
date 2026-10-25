package lifecycle

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type preservationRecoveryStore struct {
	checkpointstore.Store
	joinPending bool
}

// ForceCloseは、包んだ実Storeへ所有者のForceCloseを届けます（forceCloseWrappedCheckpointStore）。
func (s *preservationRecoveryStore) ForceClose() error {
	return forceCloseWrappedCheckpointStore(s.Store)
}

func (s *preservationRecoveryStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	value, err := s.Store.AdvanceCompletionCursor(ctx, key, destinations)
	var conflict *checkpointstore.SavedTargetConflictError
	if s.joinPending && errors.As(err, &conflict) {
		err = errors.Join(err, &checkpointstore.PendingSaveError{Err: errors.New("pending conflicting checkpoint save")})
	}
	return value, err
}

// 保全処理への再入力だけを検証します。実コピーとremoteの再送抑止は
// compressedlogfailureのManagedUploaderテストで検証します。
type preservationRecoveryUploader struct {
	store          checkpointstore.Store
	key            checkpointstore.CheckpointKey
	resolveAttempt int
	attempts       map[objectstorage.DestinationID]int
}

func (*preservationRecoveryUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"primary", "secondary"}
}

func (u *preservationRecoveryUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, request, u.DestinationIDs()).Err()
}

func (u *preservationRecoveryUploader) UploadDestinations(ctx context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
	var report uploader.UploadReport
	for _, destination := range destinations {
		u.attempts[destination]++
		result := uploader.DestinationResult{DestinationID: destination}
		if destination == "primary" && u.attempts[destination] == u.resolveAttempt {
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

func seedRecoveryTargetConflict(t *testing.T, store checkpointstore.Store, key checkpointstore.CheckpointKey) {
	t.Helper()
	progress := checkpointstore.UploadProgress{
		Key: key, DestinationID: "primary", UploadResult: checkpointstore.UploadResultRetryableFailure,
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

func TestCheckpointPreservationRecoveryReentersOnlyConflictingDestination(t *testing.T) {
	for _, stage := range []string{"initial upload", "resume cursor update", "resume upload cursor update"} {
		for _, pendingSave := range []bool{false, true} {
			name := stage
			if pendingSave {
				name += "/with pending save"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				f := newLogFileProcessingRecoveryFixture(t, "primary", "secondary")
				key := fileCheckpointKey(f.cursor, true)
				store := &preservationRecoveryStore{Store: f.store.Store, joinPending: pendingSave}
				seedRecoveryTargetConflict(t, store, key)
				resolveAttempt := 2
				if stage == "resume cursor update" {
					resolveAttempt = 1
				}
				next := &preservationRecoveryUploader{store: store, key: key, resolveAttempt: resolveAttempt,
					attempts: make(map[objectstorage.DestinationID]int)}
				manager := f.manager.objectStorageUpload
				manager.uploader = next
				manager.checkpointStore = store
				resumeUpload := true
				var successes []objectstorage.DestinationID
				var report RunReport
				var err error
				if stage == "initial upload" {
					state, prepareErr := manager.prepareRun(ctx)
					require.NoError(t, prepareErr)
					result, uploadErr := manager.uploadIfPending(ctx, f.path, f.cursor, state, true)
					err = uploadErr
					require.Equal(t, uploadAttemptResumeUpload, result.state)
					successes = manager.uploadedPathDestinationIDs(f.path)
				} else {
					if stage == "resume cursor update" {
						successes = []objectstorage.DestinationID{"primary", "secondary"}
						manager.recordUploadedPathDestinations(f.path, successes)
						resumeUpload = false
					}
					done, recoveryErr := f.manager.recoverLogFileProcessing(ctx, f.path, f.cursor, true, &resumeUpload, &successes, &report)
					err = recoveryErr
					require.False(t, done)
				}
				var conflict *checkpointstore.SavedTargetConflictError
				require.ErrorAs(t, err, &conflict)
				require.Equal(t, []objectstorage.DestinationID{"secondary"}, successes)
				require.True(t, resumeUpload)
				require.FileExists(t, f.path, "an unresolved conflict must not remove the source")
				done, err := f.manager.recoverLogFileProcessing(ctx, f.path, f.cursor, true, &resumeUpload, &successes, &report)
				require.NoError(t, err)
				require.True(t, done)
				require.Equal(t, resolveAttempt, next.attempts["primary"])
				secondaryAttempts := 1
				if stage == "resume cursor update" {
					secondaryAttempts = 0
				}
				require.Equal(t, secondaryAttempts, next.attempts["secondary"])
				require.NoFileExists(t, f.path)
			})
		}
	}
}

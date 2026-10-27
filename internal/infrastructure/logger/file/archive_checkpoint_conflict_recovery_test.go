package file

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

type inlineConflictRecoveryStore struct {
	checkpointstore.Store
	joinPending bool
}

func (s *inlineConflictRecoveryStore) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, destinations []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	checkpoint, err := s.Store.AdvanceCompletionCursor(ctx, key, destinations)
	var conflict *checkpointstore.SavedTargetConflictError
	if s.joinPending && errors.As(err, &conflict) {
		err = errors.Join(err, &checkpointstore.PendingSaveError{Err: errors.New("pending conflicting checkpoint save")})
	}
	return checkpoint, err
}

// このuploaderは、ManagedUploaderへの再進入を模します。conflictした宛先だけが、
// そのreceiptを保全して解決するために、もう一度uploaderの呼び出しを必要とします。
// 物理的な保全とremote結果の再利用は、ManagedUploaderのtestで確認します。
type inlineConflictRecoveryUploader struct {
	store          checkpointstore.Store
	key            checkpointstore.CheckpointKey
	resolveAttempt int
	attempts       map[objectstorage.DestinationID]int
}

func (*inlineConflictRecoveryUploader) DestinationIDs() []objectstorage.DestinationID {
	return []objectstorage.DestinationID{"primary", "secondary"}
}

func (u *inlineConflictRecoveryUploader) Upload(ctx context.Context, request uploader.UploadRequest) error {
	return u.UploadDestinations(ctx, request, u.DestinationIDs()).Err()
}

func (u *inlineConflictRecoveryUploader) UploadDestinations(ctx context.Context, _ uploader.UploadRequest, destinations []objectstorage.DestinationID) uploader.UploadReport {
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

func seedInlineSavedTargetConflict(t *testing.T, store checkpointstore.Store, key checkpointstore.CheckpointKey) {
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

func TestInlineCheckpointConflictRecoveryReentersOnlyUnresolvedDestination(t *testing.T) {
	for _, stage := range []string{"initial queue", "resume cursor update", "resume upload cursor update"} {
		for _, pendingSave := range []bool{false, true} {
			name := stage
			if pendingSave {
				name += "/with pending save"
			}
			t.Run(name, func(t *testing.T) {
				synctestFromWallClock(t, func(t *testing.T) {
					logger, wrappedStore, _, path, key := newInlineLogFileProcessingRecoveryFixture(t)
					store := &inlineConflictRecoveryStore{Store: wrappedStore.Store, joinPending: pendingSave}
					logger.archiveUploadCheckpoints = store
					seedInlineSavedTargetConflict(t, store, key)
					resolveAttempt := 2
					if stage == "resume cursor update" {
						resolveAttempt = 1
					}
					nextUploader := &inlineConflictRecoveryUploader{store: store, key: key, resolveAttempt: resolveAttempt,
						attempts: make(map[objectstorage.DestinationID]int)}
					logger.ArchiveUploader = nextUploader
					ctx := context.Background()
					if stage == "initial queue" {
						require.True(t, logger.processRemainingLogFiles(ctx, archiveUploadGroupsForTest(t, []string{path}, logger.Fsstore.FileName), runNotificationImmediately))
						require.True(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}),
							"a conflict must enter recovery even when joined with PendingSaveError")
						time.Sleep(logfile.DefaultLogFileProcessingRecoveryInterval)
						synctest.Wait()
						require.False(t, logger.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}))
					} else {
						next := archiveFileOperationContinuation{
							group:        archiveUploadGroup{key: key, path: path, fileName: logger.Fsstore.FileName},
							destinations: logger.archiveUploadDestinationsSnapshot(),
						}
						if stage == "resume cursor update" {
							next.uploadState().UploadedDestinationIDs = []objectstorage.DestinationID{"primary", "secondary"}
						}
						done, err := next.resume(ctx, logger, store, runNotificationImmediately)
						var conflict *checkpointstore.SavedTargetConflictError
						require.False(t, done)
						require.ErrorAs(t, err, &conflict)
						require.Equal(t, []objectstorage.DestinationID{"secondary"}, next.uploadState().UploadedDestinationIDs,
							"other successful destinations remain queued for cursor update")
						require.FileExists(t, path)
						done, err = next.resume(ctx, logger, store, runNotificationImmediately)
						require.NoError(t, err)
						require.True(t, done)
					}
					require.Equal(t, resolveAttempt, nextUploader.attempts["primary"])
					secondaryAttempts := 1
					if stage == "resume cursor update" {
						secondaryAttempts = 0 // 最初の成功したuploadは、このcontinuationより前に済んでいます。
					}
					require.Equal(t, secondaryAttempts, nextUploader.attempts["secondary"], "successful peers must not be reuploaded")
					checkpoint, err := store.Load(ctx, key)
					require.NoError(t, err)
					require.True(t, checkpoint.Complete(nextUploader.DestinationIDs()))
					require.NoFileExists(t, path, "local removal resumes only after resolving the conflict")
				})
			})
		}
	}
}

func TestInlineCheckpointConflictRecoveryDoesNotUseNewerPayloadForOlderConflict(t *testing.T) {
	logger, store, remote, path, key := newInlineLogFileProcessingRecoveryFixture(t)
	olderKey := key
	olderKey.Generation = "20260919-1"
	seedInlineSavedTargetConflict(t, store, olderKey)
	next := archiveFileOperationContinuation{
		group:        archiveUploadGroup{key: key, path: path, fileName: logger.Fsstore.FileName},
		destinations: logger.archiveUploadDestinationsSnapshot(),
	}
	next.uploadState().UploadedDestinationIDs = []objectstorage.DestinationID{"primary"}
	done, err := next.resume(context.Background(), logger, store, runNotificationImmediately)
	var conflict *checkpointstore.SavedTargetConflictError
	require.False(t, done)
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, olderKey, conflict.Key)
	require.Equal(t, []objectstorage.DestinationID{"primary"}, next.uploadState().UploadedDestinationIDs)
	require.Zero(t, remote.attemptCount())
	require.FileExists(t, path)
}

func TestInlineCheckpointConflictRecoveryRejectsUnknownDestination(t *testing.T) {
	key := checkpointstore.CheckpointKey{Generation: "20260920-1", Compressed: true}
	next := archiveFileOperationContinuation{
		group:        archiveUploadGroup{key: key},
		destinations: archiveUploadDestinationSnapshot{destinationIDs: []objectstorage.DestinationID{"primary"}},
	}
	next.uploadState().UploadedDestinationIDs = []objectstorage.DestinationID{"primary"}
	next.retrySavedTargetConflict(&checkpointstore.SavedTargetConflictError{Key: key, DestinationID: "unknown"})
	require.Equal(t, []objectstorage.DestinationID{"primary"}, next.uploadState().UploadedDestinationIDs)
}

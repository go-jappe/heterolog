package compressedlogfailure

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

type uploadAttemptKey struct {
	// pathKeyは比較用に正規化した送信元のフルパスです。ファイル操作には使いません。
	pathKey     string
	destination objectstorage.DestinationID
}

// uploadAttemptは、実際に完了した処理をcheckpointへ受理させるまで保持します。
// muは並列のarchive/spool保存が同じ途中経過を更新する間だけ使います。
// uploadErrは退避前の送信エラーです。同一プロセス内での通知用に保持します。
type uploadAttempt struct {
	mu sync.Mutex
	// progressは確定した送信結果です。nilは未試行を表します。
	// Store未受理の結果やStoreなしの途中経過も保持し、再送を防ぎます。
	progress      *checkpoint.UploadProgress
	uploadErr     error
	saveErr       error
	checkpointErr error
	// preservationWarningsは保全完了後の通知です。muで保護し、結果通知時に取り出します。
	preservationWarnings []error
}

// uploadWithProgressは、呼び出し元で確認済みの一つの登録宛先について送信・退避を進めます。
func (u *ManagedUploader) uploadWithProgress(ctx context.Context, request uploader.UploadRequest, destination objectstorage.DestinationID) uploader.UploadReport {
	// 委譲先のuploadと結果reportは共通の複数宛先APIを使うため、ここで一要素にします。
	destinations := []objectstorage.DestinationID{destination}
	key := uploadAttemptKey{pathKey: pathidentity.ComparisonKey(request.FilePath()), destination: destination}
	var fileKey checkpoint.CheckpointKey
	if u.checkpointStore != nil {
		generation, err := checkpoint.ArchiveCheckpointGenerationKeyForPath(request.FilePath(), u.fileName)
		if err != nil {
			return failedUploadReport(destinations, err)
		}
		fileKey = checkpoint.CheckpointKey{Generation: generation, Compressed: true}
	}
	attempt, completed, err := u.loadAttempt(ctx, key, fileKey)
	if err != nil {
		return failedUploadReport(destinations, err)
	}
	if completed {
		u.forgetAttempt(key)
		return successfulUploadReport(destinations)
	}
	defer func() {
		// Storeへ受理済みの途中経過は、次回もStoreの最新値を読みます。
		// 手元に残すのは未受理の実行結果、またはStoreなしの途中経過だけです。
		if u.checkpointStore != nil && attempt.saveErr == nil {
			u.forgetAttempt(key)
		}
	}()
	// 送信contextが期限切れでも、その結果は保存します。保存自体には独立した期限を与えます。
	saveAttempt := func() error {
		progressContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), u.saveTimeout)
		defer cancel()
		progressContext, forceCancel := u.contextUntilForced(progressContext)
		defer forceCancel()
		return u.saveProgress(progressContext, request, attempt)
	}
	if attempt.saveErr != nil || (attempt.progress != nil && len(attempt.progress.SavedTargetConflicts) > 0) {
		// 再起動後も、checkpointに残った矛盾を先に保全します。
		// 旧SavedTargetsだけを見て、保全失敗を完了済みと取り違えません。
		if err := saveAttempt(); err != nil {
			u.notifyProgressError(ctx, request, attempt)
			return failedUploadReport(destinations, err)
		}
	}
	if attempt.progress == nil {
		if err := ctx.Err(); err != nil {
			u.forgetAttempt(key)
			return failedUploadReport(destinations, err)
		}
		storageCompleted, conflicts, err := u.confirmFailureStorageCompleted(ctx, request, destination)
		if err != nil || (len(conflicts) != 0 && !storageCompleted) {
			u.notify(context.WithoutCancel(ctx), Notification{
				DestinationID: destination, SourcePath: request.FilePath(), ExistingFileCheckErr: err,
				ConflictRenames: conflicts,
			})
		}
		if err != nil {
			// 既存の退避ファイルを確認・保全できなければ、送信が成功して
			// 元ファイルだけ消える状態を避けます。後続の周期で再試行します。
			return failedUploadReport(destinations, err)
		}
		if storageCompleted {
			u.forgetAttempt(key)
			return successfulUploadReport(destinations)
		}
		if err := ctx.Err(); err != nil {
			return failedUploadReport(destinations, err)
		}
		report := u.next.UploadDestinations(ctx, request, destinations)
		results, contractErr := normalizeUploadResults(destinations, report)
		uploadErr := errors.Join(report.Err(), contractErr)
		attempt.uploadErr = uploadErr
		attempt.progress = &checkpoint.UploadProgress{Key: fileKey, DestinationID: destination, UploadResult: checkpoint.UploadResultSuccess}
		if uploadErr != nil {
			attempt.progress.UploadResult = classifyUploadFailure(destinations, results, contractErr)
		}
		if err := saveAttempt(); err != nil {
			u.notifyProgressError(ctx, request, attempt)
			return failedUploadReport(destinations, errors.Join(uploadErr, err))
		}
	}
	if attempt.progress.UploadResult == checkpoint.UploadResultSuccess {
		u.notifyProgressError(ctx, request, attempt)
		u.forgetAttempt(key)
		return successfulUploadReport(destinations)
	}
	notification := u.saveFailedUpload(ctx, request, attempt)
	if notification.SaveErr != nil {
		return failedUploadReport(destinations, errors.Join(attempt.uploadErr, notification.SaveErr))
	}
	// 永続途中経過はproducerがcursorを進める際に消します。保存済みspoolがすでに
	// 再送・削除されていても、その受理成功を再確認したり作り直したりしません。
	u.forgetAttempt(key)
	return successfulUploadReport(destinations)
}

func (u *ManagedUploader) loadAttempt(ctx context.Context, key uploadAttemptKey, fileKey checkpoint.CheckpointKey) (*uploadAttempt, bool, error) {
	u.attemptsMu.Lock()
	defer u.attemptsMu.Unlock()
	if existing := u.attempts[key]; existing != nil {
		existing.checkpointErr = nil
		return existing, false, nil
	}
	attempt := &uploadAttempt{}
	if u.checkpointStore != nil {
		completed, err := u.checkpointStore.Load(ctx, fileKey)
		if err != nil {
			return nil, false, err
		}
		if slices.Contains(completed.CompletedDestinationIDs, key.destination) {
			return nil, true, nil
		}
		progress, found, err := u.checkpointStore.LoadUploadProgress(ctx, fileKey, key.destination)
		if err != nil {
			return nil, false, err
		}
		if found {
			attempt.progress = &progress
		}
	}
	if u.attempts == nil {
		u.attempts = make(map[uploadAttemptKey]*uploadAttempt)
	}
	u.attempts[key] = attempt
	return attempt, false, nil
}

func (u *ManagedUploader) forgetAttempt(key uploadAttemptKey) {
	u.attemptsMu.Lock()
	delete(u.attempts, key)
	u.attemptsMu.Unlock()
}

func (a *uploadAttempt) savedTarget(kind failurestore.DurableType, target string) (checkpoint.SavedFailureTarget, bool) {
	for _, saved := range a.progress.SavedTargets {
		if saved.Kind() == kind && saved.Root() == target {
			return saved, true
		}
	}
	return nil, false
}

func (u *ManagedUploader) recordSavedTarget(ctx context.Context, request uploader.UploadRequest, a *uploadAttempt, target checkpoint.SavedFailureTarget) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.progress.SavedTargets = append(a.progress.SavedTargets, target.Clone())
	return u.saveProgress(ctx, request, a)
}

// saveProgressは矛盾した保存結果を受けた場合、データを保全してから既存の記録へ戻します。
// 並列の退避保存から呼ぶ場合はa.muを保持します。保全失敗時は未受理の記録を残して再試行します。
func (u *ManagedUploader) saveProgress(ctx context.Context, request uploader.UploadRequest, a *uploadAttempt) error {
	if u.checkpointStore == nil {
		return nil
	}
	var err error
	for {
		if err = ctx.Err(); err != nil {
			break
		}
		err = u.checkpointStore.SaveUploadProgress(ctx, *a.progress)
		var conflict *checkpoint.SavedTargetConflictError
		if !errors.As(err, &conflict) {
			break
		}
		if preserveErr := u.preserveConflictingProgress(ctx, request, a, conflict); preserveErr != nil {
			err = errors.Join(err, preserveErr)
			break
		}
	}
	a.saveErr = err
	if err != nil {
		a.checkpointErr = errors.Join(a.checkpointErr, err)
		if checkpoint.IsPendingSaveOnly(err) {
			// メモリの正本へ受理済みです。Storeの復旧処理が最新内容を保存します。
			// 未解決の矛盾は、保存待ちも併発していても完了扱いにしません。
			a.saveErr = nil
			a.progress.PreservedTargetConflicts = nil
			return nil
		}
	} else {
		a.progress.PreservedTargetConflicts = nil
	}
	return err
}

// preserveConflictingProgressは保存結果の矛盾に備え、元の圧縮ファイルを対象rootの
// .preservedへコピー・同期します。他の退避先への並列保存でも使うため、元ファイルは移動しません。
// 保全後は既存の受理済み記録を維持し、保全した差分を次のSaveUploadProgressへ渡します。
// 保全成功はPreservationWarningで通知し、保全失敗時は矛盾を解消しません。
func (u *ManagedUploader) preserveConflictingProgress(ctx context.Context, request uploader.UploadRequest, a *uploadAttempt, conflict *checkpoint.SavedTargetConflictError) error {
	if conflict.Key != a.progress.Key || conflict.DestinationID != a.progress.DestinationID || len(conflict.Conflicts) == 0 {
		return errors.New("checkpoint returned a conflict for a different upload progress")
	}
	for _, difference := range conflict.Conflicts {
		var target failureTarget
		for _, configured := range u.failureTargets[conflict.DestinationID] {
			if configured.kind() == difference.Recorded.Kind() && configured.root() == difference.Recorded.Root() {
				target = configured
				break
			}
		}
		if target == nil {
			return fmt.Errorf("cannot preserve conflicting failure target %q of type %q: target is not configured", difference.Recorded.Root(), difference.Recorded.Kind())
		}
		path, err := target.preserve(ctx, request.FilePath())
		if err != nil {
			return &ConflictPreservationError{SourcePath: request.FilePath(), TargetName: target.name(), Err: err}
		}
		if path == "" {
			return fmt.Errorf("preserve source %q: %s returned no preservation path", request.FilePath(), target.name())
		}
		a.preservationWarnings = append(a.preservationWarnings, &ConflictPreservationWarning{
			SourcePath: request.FilePath(), Destination: conflict.DestinationID, Target: target.root(),
			Recorded: difference.Recorded, Received: difference.Received, PreservedPath: path,
		})
		for index, saved := range a.progress.SavedTargets {
			if saved.Kind() == difference.Recorded.Kind() && saved.Root() == difference.Recorded.Root() {
				a.progress.SavedTargets[index] = difference.Recorded.Clone()
				break
			}
		}
		// 保全した差分そのものを指定します。古いsnapshotの送信や差分の省略で、
		// まだ保全していない別の矛盾まで解消済みにしないためです。
		a.progress.PreservedTargetConflicts = slices.DeleteFunc(a.progress.PreservedTargetConflicts, func(preserved checkpoint.SavedTargetConflict) bool {
			return preserved.Recorded.Kind() == difference.Recorded.Kind() && preserved.Recorded.Root() == difference.Recorded.Root()
		})
		a.progress.PreservedTargetConflicts = append(a.progress.PreservedTargetConflicts, checkpoint.SavedTargetConflict{
			Recorded: difference.Recorded.Clone(), Received: difference.Received.Clone(),
		})
	}
	a.progress.SavedTargetConflicts = nil
	return nil
}

func (a *uploadAttempt) takePreservationWarnings() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	warning := errors.Join(a.preservationWarnings...)
	a.preservationWarnings = nil
	return warning
}

// notifyProgressErrorはcheckpointの保存エラーと、保全済みの矛盾の警告をNotifierへ渡します。
// 既存の通知先では保存エラーをOnErrorへ、PreservationWarningをOnWarnへ通知します。
func (u *ManagedUploader) notifyProgressError(ctx context.Context, request uploader.UploadRequest, attempt *uploadAttempt) {
	warning := attempt.takePreservationWarnings()
	if attempt.checkpointErr != nil || warning != nil {
		// 退避処理の結果ではなくcheckpoint保存だけの通知です。UploadErrを渡すと
		// 利用側が「送信失敗を退避で受理済み」と誤認するため、ここには含めません。
		u.notify(context.WithoutCancel(ctx), Notification{
			DestinationID: attempt.progress.DestinationID, SourcePath: request.FilePath(), CheckpointErr: attempt.checkpointErr, PreservationWarning: warning,
		})
	}
}

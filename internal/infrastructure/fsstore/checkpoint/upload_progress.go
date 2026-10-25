package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-jappe/heterolog/pkg/failurestore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
)

// UploadResultは、送信を試みて確定した成功または失敗の結果です。
// 未試行はUploadProgressの記録がない状態で表します。
type UploadResult string

const (
	UploadResultSuccess          UploadResult = "success"
	UploadResultRetryableFailure UploadResult = "retryable_failure"
	UploadResultPermanentFailure UploadResult = "permanent_failure"
)

// ErrUploadResultChangedは、記録済みの送信結果を別の結果へ変える途中経過の保存を拒否したことを表します。
// 保存待ちではなく入力の誤りとして返し、メモリの記録も変更しません。
var ErrUploadResultChanged = errors.New("upload progress cannot change a recorded upload result")

// UploadProgressは、一つのファイル・宛先について、確定した送信結果と退避処理の途中経過を保持します。
// 再開時は送信を繰り返さず、未完了の退避先への保存を進めます。未試行は記録がない状態で表します。
// 全処理の完了時はAdvanceCompletionCursorがcursor更新と同時にこの記録を削除します。
type UploadProgress struct {
	// Keyは対象ファイルの期間・世代番号と圧縮の有無です。
	// 例: app_20260915_3.log.gzならGeneration="20260915-3", Compressed=trueです。
	Key CheckpointKey `json:"key"`
	// DestinationIDは、この記録が対象とするobject storageの送信先IDです。
	DestinationID objectstorage.DestinationID `json:"destination_id"`
	// UploadResultは送信成功・再試行可能な失敗・再試行対象外の失敗のいずれかです。
	// 退避先への保存が完了しても、確定した送信結果は変更しません。
	UploadResult UploadResult `json:"upload_result"`
	// SavedTargetsは、保存に成功した退避先と保存時点の結果です。以前の試行の成功も含みます。
	// 再試行では、ここに記録された退避先への保存を繰り返しません。
	SavedTargets []SavedFailureTarget `json:"saved_targets,omitempty"`
	// SavedTargetConflictsは、同じ種類・保存先について、既存の成功記録と後続の結果が食い違った差分です。
	// checkpointへ保存し、元データの保全が確認できるまで対象ファイル・宛先を完了扱いにしません。
	SavedTargetConflicts []SavedTargetConflict `json:"saved_target_conflicts,omitempty"`
	// PreservedTargetConflictsは、呼び出し元が元データを.preservedへコピー・同期した差分です。
	// SaveUploadProgressへの入力にだけ使い、現在の未解決差分と完全に一致するものだけを解消します。
	// JSONやStoreの正本には保存しません。
	PreservedTargetConflicts []SavedTargetConflict `json:"-"`
}

// UnmarshalJSONはtypeに応じて保存済み退避先を具体型へ復元します。
// upload_resultがない旧JSONだけは、failure_categoryの欠落・空文字を成功として読み替えます。
// 新たに保存するJSONでは、成功もupload_resultへ明記します。
func (progress *UploadProgress) UnmarshalJSON(payload []byte) error {
	var record struct {
		Key                  CheckpointKey               `json:"key"`
		DestinationID        objectstorage.DestinationID `json:"destination_id"`
		UploadResult         json.RawMessage             `json:"upload_result"`
		FailureCategory      json.RawMessage             `json:"failure_category,omitempty"`
		SavedTargets         []json.RawMessage           `json:"saved_targets,omitempty"`
		SavedTargetConflicts []struct {
			Recorded json.RawMessage `json:"recorded"`
			Received json.RawMessage `json:"received"`
		} `json:"saved_target_conflicts,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return err
	}
	decoded := UploadProgress{
		Key: record.Key, DestinationID: record.DestinationID,
	}
	if len(record.UploadResult) > 0 {
		if len(record.FailureCategory) > 0 {
			return errors.New("upload progress must not contain both upload_result and legacy failure_category")
		}
		if err := json.Unmarshal(record.UploadResult, &decoded.UploadResult); err != nil {
			return fmt.Errorf("upload result: %w", err)
		}
	} else {
		// 旧形式の互換処理は読み込み時に限定し、以後は明示的な送信結果だけを保持します。
		var category uploader.FailureCategory
		if len(record.FailureCategory) > 0 {
			if err := json.Unmarshal(record.FailureCategory, &category); err != nil {
				return fmt.Errorf("legacy upload progress failure category: %w", err)
			}
		}
		switch category {
		case "":
			decoded.UploadResult = UploadResultSuccess
		case uploader.FailureCategoryRetryable:
			decoded.UploadResult = UploadResultRetryableFailure
		case uploader.FailureCategoryPermanent:
			decoded.UploadResult = UploadResultPermanentFailure
		default:
			return fmt.Errorf("legacy upload progress failure category %q is invalid", category)
		}
	}
	for index, raw := range record.SavedTargets {
		target, err := decodeSavedFailureTarget(raw)
		if err != nil {
			return fmt.Errorf("saved failure target[%d]: %w", index, err)
		}
		decoded.SavedTargets = append(decoded.SavedTargets, target)
	}
	for index, raw := range record.SavedTargetConflicts {
		recorded, err := decodeSavedFailureTarget(raw.Recorded)
		if err != nil {
			return fmt.Errorf("saved target conflict[%d].recorded: %w", index, err)
		}
		received, err := decodeSavedFailureTarget(raw.Received)
		if err != nil {
			return fmt.Errorf("saved target conflict[%d].received: %w", index, err)
		}
		decoded.SavedTargetConflicts = append(decoded.SavedTargetConflicts, SavedTargetConflict{
			Recorded: recorded,
			Received: received,
		})
	}
	*progress = decoded
	return nil
}

// LoadUploadProgressはメモリの記録をコピーして返します。記録がなければゼロ値とfound=falseを返します。
// found=trueでUploadResultがUploadResultSuccessなら直接送信の成功記録です。
func (s *FilesystemStore) LoadUploadProgress(ctx context.Context, key CheckpointKey, destination objectstorage.DestinationID) (progress UploadProgress, found bool, err error) {
	if s == nil {
		return UploadProgress{}, false, ErrInvalidDirectory
	}
	if err := validateCheckpointGeneration(key.Generation); err != nil {
		return UploadProgress{}, false, err
	}
	if err := validateDestinationID(destination); err != nil {
		return UploadProgress{}, false, err
	}
	if err := s.lockContext(ctx); err != nil {
		return UploadProgress{}, false, err
	}
	defer s.stateOrSaveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return UploadProgress{}, false, err
	}
	for _, progress := range s.state.UploadProgress {
		if progress.Key == key && progress.DestinationID == destination {
			return cloneUploadProgress(progress), true, nil
		}
	}
	return UploadProgress{}, false, nil
}

// SavedTargetConflictは同じ退避先について食い違った保存結果です。
// Recordedは先に受理した記録、Receivedは今回受け取った記録です。
type SavedTargetConflict struct {
	Recorded SavedFailureTarget `json:"recorded"`
	Received SavedFailureTarget `json:"received"`
}

// SavedTargetConflictErrorはデータの保全が必要な保存結果の矛盾です。
// Storeは既存の成功記録をSavedTargetsへ保持し、未解決の差分を
// SavedTargetConflictsへ保存します。Store自身は圧縮ファイルを移動・保全しません。
// 呼び出し側は元の圧縮ファイルを対象退避先のroot直下の.preservedへコピー・同期し、
// 保全後にSavedTargetsへRecordedを、PreservedTargetConflictsへ保全した差分を指定して
// 保存し直してください。保全ファイルは自動削除・再送せず、管理者の確認用に残します。
type SavedTargetConflictError struct {
	Key           CheckpointKey
	DestinationID objectstorage.DestinationID
	Conflicts     []SavedTargetConflict
}

func (e *SavedTargetConflictError) Error() string {
	return fmt.Sprintf("conflicting saved failure results for generation %q (compressed=%t, destination=%q): %+v", e.Key.Generation, e.Key.Compressed, e.DestinationID, e.Conflicts)
}

// SavedTargetConflictDestinationは、errに含まれる保存結果の矛盾がkeyとdestinationsの
// 対象なら、その宛先IDとtrueを返します。keyは世代と圧縮の有無の両方を照合します。
// 呼び出し元は完了記録の更新を待っている宛先一覧を渡し、該当した宛先だけを保全処理へ戻します。
func SavedTargetConflictDestination(err error, key CheckpointKey, destinations []objectstorage.DestinationID) (objectstorage.DestinationID, bool) {
	var conflict *SavedTargetConflictError
	if !errors.As(err, &conflict) || conflict == nil || conflict.Key != key || !slices.Contains(destinations, conflict.DestinationID) {
		return "", false
	}
	return conflict.DestinationID, true
}

// SaveUploadProgressは確定した送信結果と、成功した退避先を保存します。送信前には呼び出しません。
// 同じ種類・保存先の記録が食い違う場合は既存の成功記録を保持し、未解決の差分を保存して
// SavedTargetConflictErrorを返します。保全せずに矛盾を無視して完了扱いにすることはありません。
func (s *FilesystemStore) SaveUploadProgress(ctx context.Context, progress UploadProgress) error {
	if s == nil {
		return ErrInvalidDirectory
	}
	if err := validateUploadProgress(progress); err != nil {
		return err
	}
	if err := s.lockContext(ctx); err != nil {
		return err
	}
	defer s.stateOrSaveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if index := cursorIndex(s.state.Cursors, progress.DestinationID); index >= 0 && s.state.Cursors[index].Covers(progress.Key) {
		// 完了後に到着した途中経過で再送を必要な状態へ戻しません。
		return joinSavedTargetConflictError(progress, s.pendingSaveErrorLocked())
	}
	if err := s.registerDestinationIDsLocked(ctx, s.unregisteredDestinationIDs([]objectstorage.DestinationID{progress.DestinationID})); err != nil {
		return err
	}
	candidate := cloneDocument(s.state)
	if cursorIndex(candidate.Cursors, progress.DestinationID) < 0 {
		candidate.Cursors = append(candidate.Cursors, DestinationCursor{DestinationID: progress.DestinationID})
		sortCursors(candidate.Cursors)
	}
	for index, previous := range candidate.UploadProgress {
		if previous.Key != progress.Key || previous.DestinationID != progress.DestinationID {
			continue
		}
		merged, err := mergeUploadProgress(previous, progress)
		if err != nil {
			return err
		}
		var saveErr error
		if equalUploadProgress(previous, merged) {
			saveErr = s.pendingSaveErrorLocked()
		} else {
			candidate.UploadProgress[index] = merged
			saveErr = s.updateStateAndSaveLocked(ctx, candidate)
		}
		return joinSavedTargetConflictError(merged, saveErr)
	}
	candidate.UploadProgress = append(candidate.UploadProgress, cloneUploadProgress(progress))
	saveErr := s.updateStateAndSaveLocked(ctx, candidate)
	return joinSavedTargetConflictError(progress, saveErr)
}

// 保存先を並列に処理しても、先に保存した成功を後から到着した結果で消しません。
// 同じ退避先の異なる保存結果は既存の記録を優先し、両方の差分を
// SavedTargetConflictsへ残します。通常の入力でこの差分を省略しても解消しません。
// 保全済みとして明示された完全一致の差分だけを解消します。
func mergeUploadProgress(previous, next UploadProgress) (UploadProgress, error) {
	if previous.UploadResult != next.UploadResult {
		return UploadProgress{}, ErrUploadResultChanged
	}
	result := cloneUploadProgress(previous)
	type targetKey struct {
		kind failurestore.DurableType
		root string
	}
	keyFor := func(target SavedFailureTarget) targetKey {
		return targetKey{kind: target.Kind(), root: target.Root()}
	}

	pending := make(map[targetKey]SavedTargetConflict, len(previous.SavedTargetConflicts))
	order := make([]targetKey, 0, len(previous.SavedTargetConflicts))
	ordered := make(map[targetKey]struct{}, len(previous.SavedTargetConflicts))
	setPending := func(conflict SavedTargetConflict) {
		key := keyFor(conflict.Recorded)
		if _, exists := ordered[key]; !exists {
			order = append(order, key)
			ordered[key] = struct{}{}
		}
		pending[key] = cloneSavedTargetConflict(conflict)
	}
	for _, conflict := range previous.SavedTargetConflicts {
		setPending(conflict)
	}
	// SavedTargetConflictsはStoreが保持する状態です。呼び出し側の古いsnapshotで
	// 現在の差分を上書きせず、保全済みの完全一致ペアだけを確認情報として受け取ります。
	for _, preserved := range next.PreservedTargetConflicts {
		key := keyFor(preserved.Recorded)
		if current, exists := pending[key]; exists && equalSavedTargetConflict(current, preserved) {
			delete(pending, key)
		}
	}
	for _, saved := range next.SavedTargets {
		index := slices.IndexFunc(result.SavedTargets, func(existing SavedFailureTarget) bool {
			return existing.Kind() == saved.Kind() && existing.Root() == saved.Root()
		})
		if index < 0 {
			result.SavedTargets = append(result.SavedTargets, saved.Clone())
			continue
		}
		canonical := result.SavedTargets[index]
		if canonical.Equal(saved) {
			continue
		}
		setPending(SavedTargetConflict{Recorded: canonical, Received: saved})
	}
	result.SavedTargetConflicts = nil
	for _, key := range order {
		if conflict, exists := pending[key]; exists {
			result.SavedTargetConflicts = append(result.SavedTargetConflicts, cloneSavedTargetConflict(conflict))
		}
	}
	return result, nil
}

func cloneUploadProgress(progress UploadProgress) UploadProgress {
	progress.SavedTargets = slices.Clone(progress.SavedTargets)
	for index, saved := range progress.SavedTargets {
		progress.SavedTargets[index] = saved.Clone()
	}
	progress.SavedTargetConflicts = cloneSavedTargetConflicts(progress.SavedTargetConflicts)
	// 保全確認はSave呼び出しだけの入力で、永続stateやLoad結果へ持ち越しません。
	progress.PreservedTargetConflicts = nil
	return progress
}

func equalUploadProgress(left, right UploadProgress) bool {
	return left.Key == right.Key && left.DestinationID == right.DestinationID &&
		left.UploadResult == right.UploadResult &&
		slices.EqualFunc(left.SavedTargets, right.SavedTargets, SavedFailureTarget.Equal) &&
		slices.EqualFunc(left.SavedTargetConflicts, right.SavedTargetConflicts, equalSavedTargetConflict)
}

func cloneSavedTargetConflict(conflict SavedTargetConflict) SavedTargetConflict {
	return SavedTargetConflict{Recorded: conflict.Recorded.Clone(), Received: conflict.Received.Clone()}
}

func cloneSavedTargetConflicts(conflicts []SavedTargetConflict) []SavedTargetConflict {
	result := slices.Clone(conflicts)
	for index, conflict := range result {
		result[index] = cloneSavedTargetConflict(conflict)
	}
	return result
}

func equalSavedTargetConflict(left, right SavedTargetConflict) bool {
	return left.Recorded.Equal(right.Recorded) && left.Received.Equal(right.Received)
}

func joinSavedTargetConflictError(progress UploadProgress, saveErr error) error {
	if len(progress.SavedTargetConflicts) == 0 {
		return saveErr
	}
	conflictErr := &SavedTargetConflictError{
		Key:           progress.Key,
		DestinationID: progress.DestinationID,
		Conflicts:     cloneSavedTargetConflicts(progress.SavedTargetConflicts),
	}
	if saveErr == nil {
		return conflictErr
	}
	return errors.Join(conflictErr, saveErr)
}

func validateUploadProgress(progress UploadProgress) error {
	if err := validateCheckpointGeneration(progress.Key.Generation); err != nil {
		return err
	}
	if err := validateDestinationID(progress.DestinationID); err != nil {
		return err
	}
	switch progress.UploadResult {
	case UploadResultSuccess:
		if len(progress.SavedTargets) > 0 {
			return errors.New("successful upload progress must not contain failure results")
		}
	case UploadResultRetryableFailure, UploadResultPermanentFailure:
	default:
		return fmt.Errorf("upload progress upload result %q is invalid", progress.UploadResult)
	}
	type targetKey struct {
		kind failurestore.DurableType
		root string
	}
	seen := make(map[targetKey]struct{}, len(progress.SavedTargets))
	for _, saved := range progress.SavedTargets {
		if isNilSavedFailureTarget(saved) {
			return fmt.Errorf("saved failure target type %T is invalid", saved)
		}
		if err := saved.Validate(); err != nil {
			return err
		}
		if strings.TrimSpace(saved.Root()) == "" {
			return errors.New("saved failure target root must not be empty")
		}
		key := targetKey{kind: saved.Kind(), root: saved.Root()}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("saved failure target %q of type %q is duplicated", saved.Root(), saved.Kind())
		}
		seen[key] = struct{}{}
	}
	validateConflicts := func(label string, values []SavedTargetConflict) error {
		conflicts := make(map[targetKey]struct{}, len(values))
		for index, conflict := range values {
			if isNilSavedFailureTarget(conflict.Recorded) || isNilSavedFailureTarget(conflict.Received) {
				return fmt.Errorf("%s[%d] contains an invalid target", label, index)
			}
			if err := conflict.Recorded.Validate(); err != nil {
				return fmt.Errorf("%s[%d].recorded: %w", label, index, err)
			}
			if err := conflict.Received.Validate(); err != nil {
				return fmt.Errorf("%s[%d].received: %w", label, index, err)
			}
			key := targetKey{kind: conflict.Recorded.Kind(), root: conflict.Recorded.Root()}
			if strings.TrimSpace(key.root) == "" {
				return fmt.Errorf("%s[%d] root must not be empty", label, index)
			}
			if conflict.Received.Kind() != key.kind || conflict.Received.Root() != key.root {
				return fmt.Errorf("%s[%d] must refer to one failure target", label, index)
			}
			if conflict.Recorded.Equal(conflict.Received) {
				return fmt.Errorf("%s[%d] does not contain different results", label, index)
			}
			canonicalIndex := slices.IndexFunc(progress.SavedTargets, func(saved SavedFailureTarget) bool {
				return saved.Kind() == key.kind && saved.Root() == key.root
			})
			if canonicalIndex < 0 || !progress.SavedTargets[canonicalIndex].Equal(conflict.Recorded) {
				return fmt.Errorf("%s[%d] recorded result does not match the canonical saved target", label, index)
			}
			if _, duplicate := conflicts[key]; duplicate {
				return fmt.Errorf("%s for %q of type %q is duplicated", label, key.root, key.kind)
			}
			conflicts[key] = struct{}{}
		}
		return nil
	}
	if err := validateConflicts("saved target conflict", progress.SavedTargetConflicts); err != nil {
		return err
	}
	if err := validateConflicts("preserved target conflict", progress.PreservedTargetConflicts); err != nil {
		return err
	}
	return nil
}

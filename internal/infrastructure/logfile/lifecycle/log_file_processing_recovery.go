package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

// logFileProcessingRecoveryKeyは送信・圧縮・後片付けと宛先準備の復旧を区別します。
type logFileProcessingRecoveryKey struct {
	File        checkpointstore.CheckpointKey
	Preparation bool
	Compression bool
	CleanupPath string
}

var uploadPreparationRecoveryKey = logFileProcessingRecoveryKey{Preparation: true}

func logFileProcessingRecoveryKeyForFile(generation fileGeneration, compressed bool) logFileProcessingRecoveryKey {
	return logFileProcessingRecoveryKey{File: fileCheckpointKey(generation, compressed)}
}

// CloseLogFileProcessingRecoveryは、受理済みRunOnceが再開処理を登録し終えた後に、復旧完了を待ちます。
// すべての再開処理が完了するまでcheckpoint Storeを利用可能に保ちます。
// supervisorによるrunnerの再起動では、この再試行処理もStoreも終了させません。
func (m *FileSeriesManager) CloseLogFileProcessingRecovery() error {
	if m == nil || m.logFileProcessingRecovery == nil {
		return nil
	}
	return m.logFileProcessingRecovery.Close()
}

// ForceCloseは受理済み周期・復旧・保存へ中止を通知します。利用中の保護やStoreは
// ここでは解放せず、実処理が戻った後のCloseで順序どおり終了します。
func (m *FileSeriesManager) ForceClose() error {
	if m == nil {
		return nil
	}
	m.initMu.Lock()
	if m.closed {
		err := m.closeErr
		m.initMu.Unlock()
		return err
	}
	if m.forceDone == nil {
		m.forceDone = make(chan struct{})
	}
	select {
	case <-m.forceDone:
	default:
		close(m.forceDone)
	}
	recovery := m.logFileProcessingRecovery
	m.initMu.Unlock()
	var errs []error
	errs = append(errs, pkgshutdown.ErrForcedClose)
	if recovery != nil {
		errs = append(errs, recovery.ForceClose())
	}
	if m.objectStorageUpload != nil {
		m.objectStorageUpload.stateMu.Lock()
		store := m.objectStorageUpload.checkpointStore
		m.objectStorageUpload.stateMu.Unlock()
		if force, ok := store.(interface{ ForceClose() error }); ok {
			errs = append(errs, force.ForceClose())
		}
	}
	return errors.Join(errs...)
}

// Closeは、このmanagerの終了処理です。直接利用する呼び出し元は、最後のRunOnceが
// 終了してから呼んでください。DirectoryLifecycleはrunnerの停止後に呼びます。
// 受理済みのログファイル処理が完了するまで待ってから、checkpoint Storeを閉じます。
// 通知の実行完了は別の段階です。直接の所有者はClose後にNotificationsDoneも待ち、
// その後で内部ログの出力先を閉じてください。通知内から所有者の終了完了を待たないでください。
func (m *FileSeriesManager) Close() error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		m.initMu.Lock()
		guard := m.directoryGuard
		m.initMu.Unlock()
		if guard != nil {
			// lifecycle lockを保持中の処理が再開処理を登録し終えるまで待ちます。
			// 復旧も同じlockを使用するため、復旧完了を待つ前に解放します。
			_ = guard.RunWithLifecycleLock(func() error { return nil })
		}
		m.closeErr = m.CloseLogFileProcessingRecovery()
		closeStore := func() error {
			if m.objectStorageUpload == nil {
				return nil
			}
			m.objectStorageUpload.stateMu.Lock()
			store := m.objectStorageUpload.checkpointStore
			m.objectStorageUpload.stateMu.Unlock()
			var err error
			if store != nil {
				err = store.Close()
			}
			return err
		}
		m.closeErr = errors.Join(m.closeErr, closeStore())
		m.initMu.Lock()
		select {
		case <-m.forceDone:
			m.closeErr = errors.Join(m.closeErr, pkgshutdown.ErrForcedClose)
		default:
		}
		m.closed = true
		m.initMu.Unlock()
	})
	return m.closeErr
}

// NotificationsDoneは、このmanagerの復旧・checkpoint保存が受理した通知の完了を返します。
// 所有者は最後のClose後に待ってください。圧縮ファイルの退避先の利用登録は
// DirectoryLifecycleなど、その登録の所有者が別に終了・通知待機します。
func (m *FileSeriesManager) NotificationsDone() <-chan struct{} {
	done := make(chan struct{})
	var upstream []<-chan struct{}
	if m != nil {
		m.initMu.Lock()
		recovery := m.logFileProcessingRecovery
		m.initMu.Unlock()
		if recovery != nil {
			upstream = append(upstream, recovery.NotificationsDone())
		}
		if m.objectStorageUpload != nil {
			m.objectStorageUpload.stateMu.Lock()
			store := m.objectStorageUpload.checkpointStore
			m.objectStorageUpload.stateMu.Unlock()
			if notifications, ok := store.(interface{ NotificationsDone() <-chan struct{} }); ok {
				upstream = append(upstream, notifications.NotificationsDone())
			}
		}
	}
	go func() {
		for _, notifications := range upstream {
			<-notifications
		}
		close(done)
	}()
	return done
}

func (m *FileSeriesManager) reportLogFileProcessingRecovery(report RunReport, err error) {
	if err == nil && len(report.Compressed) == 0 && len(report.RecoveredCompressionSourceDeletePendingPaths) == 0 && len(report.ObjectStorageUploads) == 0 && len(report.UploadedFileRemovals) == 0 {
		return
	}
	m.logFileProcessingRecoveryReportMu.RLock()
	reportResult := m.logFileProcessingRecoveryReport
	m.logFileProcessingRecoveryReportMu.RUnlock()
	if reportResult != nil {
		reportResult(report, err)
	}
}

// schedulePanickedFileOperationは開始済みの段階だけを保持し、未選択の後続fileを取り込みません。
// 送信成功後のcheckpoint panicでも、upload managerに記録した成功宛先をそのまま引き継ぎます。
func (m *FileSeriesManager) schedulePanickedFileOperation(sourcePath, compressedPath, uploadPath string, compressionSource bool, recovered any) {
	failure := fmt.Errorf("%w: accepted file operation %q: %v", ErrFileProcessingPanicked, sourcePath, recovered)
	if compressionSource && (uploadPath == "" || m.hasUncompressedExtension(uploadPath)) {
		m.scheduleCompressionOperationRecovery(sourcePath, compressedPath, failure)
		return
	}
	if uploadPath == "" {
		uploadPath = sourcePath
	}
	if generation, valid := m.parseFileGeneration(uploadPath); valid {
		m.scheduleLogFileProcessingRecovery(uploadPath, generation, !m.hasUncompressedExtension(uploadPath), true, failure)
	}
}

// 受理済みの送信・全宛先への退避・状態更新・file削除を完了まで再試行します。
// 他に未完了操作がない保存だけの失敗はStoreに任せます。
func (m *FileSeriesManager) scheduleLogFileProcessingRecovery(path string, generation fileGeneration, compressed, resumeUpload bool, failure error) {
	m.scheduleLogFileProcessingRecoveryForOwner(path, generation, compressed, resumeUpload, failure, m.directoryToken)
}

func (m *FileSeriesManager) scheduleLogFileProcessingRecoveryForOwner(path string, generation fileGeneration, compressed, resumeUpload bool, failure error, previousOwner *appendlog.DirectoryGuardToken) bool {
	if m.manualRecovery {
		return false
	}
	if checkpointstore.IsPendingSaveOnly(failure) && !resumeUpload {
		return true
	}
	key := logFileProcessingRecoveryKeyForFile(generation, compressed)
	if m.logFileProcessingRecovery.Has(key) {
		return true
	}
	token := appendlog.NewDirectoryGuardToken()
	paths := fsstore.CompressionProtectionPaths(path, m.fileName.Extension)
	successes := m.objectStorageUpload.uploadedPathDestinationIDs(path)
	if !m.directoryGuard.ProtectPathsIfNoOtherOwnerConflict(token, previousOwner, paths) {
		return true
	}
	release := func() { m.directoryGuard.UnprotectPaths(token, paths...) }
	var report RunReport
	if !m.logFileProcessingRecovery.Schedule(key, failure, func(ctx context.Context) (done bool, err error) {
		report = RunReport{DirectoryPath: m.fileName.Dir}
		err = m.directoryGuard.RunWithLifecycleLockContext(ctx, func() error {
			var attemptErr error
			done, attemptErr = m.recoverLogFileProcessing(ctx, path, generation, compressed, &resumeUpload, &successes, &report)
			return attemptErr
		})
		return done, err
	}, func(err error) func() {
		report := report
		return func() { m.reportLogFileProcessingRecovery(report, err) }
	}, release) {
		release()
		return false
	}
	return true
}

// preparation recoveryは、現在のdestinationと受理済みcheckpointの状態を取得し直します。
// 既存のgenerationを再構築し、compressionやretentionは実行しません。
// checkpoint preparationの成功後は、現在の走査で選択したすべてのfile（圧縮しない
// 設定ではlog file、それ以外では各sourceで選択された圧縮済みfile）へ、保護された
// continuationを用意します。個別の登録を断られたfileは、この準備処理の中で続けます。
func (m *FileSeriesManager) scheduleUploadPreparationRecovery(paths []string, failure error) {
	if m.manualRecovery {
		return
	}
	if m.logFileProcessingRecovery.Has(uploadPreparationRecoveryKey) {
		return
	}
	token := appendlog.NewDirectoryGuardToken()
	var protected []string
	for _, path := range paths {
		protected = append(protected, fsstore.CompressionProtectionPaths(path, m.fileName.Extension)...)
	}
	m.directoryGuard.ProtectPaths(token, protected...)
	release := func() { m.directoryGuard.UnprotectPaths(token, protected...) }
	var report RunReport
	// Closeが追加登録を止めた後も、受理済みの準備から派生する処理は完了を待ちます。
	// 個別の登録を断られた場合は、送信済み宛先を保持してこの準備処理の中で再開します。
	type continuation struct {
		generation   fileGeneration
		compressed   bool
		resumeUpload bool
		successes    []objectstorage.DestinationID
	}
	inline := make(map[string]*continuation)
	if !m.logFileProcessingRecovery.Schedule(uploadPreparationRecoveryKey, failure, func(ctx context.Context) (done bool, err error) {
		report = RunReport{DirectoryPath: m.fileName.Dir}
		err = m.directoryGuard.RunWithLifecycleLockContext(ctx, func() error {
			scan, err := m.scanLifecycleTargetFiles()
			if err != nil {
				return err
			}
			// 登録と順序付けには、元のpreparation失敗の後に現れたfileも含め、
			// 現在のscanを使います。
			_, err = m.prepareObjectStorageUploadRun(ctx, scan.files, scan.sourceGroups.Files.Paths)
			if err != nil && !checkpointstore.IsPendingSaveOnly(err) {
				return err
			}
			preparationErr := err
			selected, err := m.resolveLifecycleCompressedPaths(scan.files, scan.sourceGroups)
			if err != nil {
				return err
			}
			// preparationは完了しています。これらのcontinuationはすぐに実行できるため、
			// nilを渡して、最初の試行の前に再びretry intervalを待たないようにします。
			pending := false
			var continuationErrs []error
			resume := func(path string, generation fileGeneration, compressed bool) {
				state := inline[path]
				if state == nil {
					if m.scheduleLogFileProcessingRecoveryForOwner(path, generation, compressed, true, nil, token) {
						return
					}
					paths := fsstore.CompressionProtectionPaths(path, m.fileName.Extension)
					protected = append(protected, paths...)
					m.directoryGuard.ProtectPaths(token, paths...)
					state = &continuation{generation: generation, compressed: compressed, resumeUpload: true, successes: m.objectStorageUpload.uploadedPathDestinationIDs(path)}
					inline[path] = state
				}
				completed, recoveryErr := m.recoverLogFileProcessing(ctx, path, generation, compressed, &state.resumeUpload, &state.successes, &report)
				if recoveryErr != nil {
					continuationErrs = append(continuationErrs, recoveryErr)
				}
				if completed {
					delete(inline, path)
				} else {
					pending = true
				}
			}
			// 個別に受理済みの処理は今回の走査で選択できなくても保持します。
			// 不在の確認や検査失敗の再試行はrecoverLogFileProcessing自身が判断します。
			resumed := make(map[string]bool, len(inline))
			for path, state := range inline {
				resumed[path] = true
				resume(path, state.generation, state.compressed)
			}
			for _, path := range scan.files {
				if resumed[path] {
					continue
				}
				source, _ := fsstore.CompressionSourcePath(path, m.fileName.Extension)
				sourceGroup, _ := scan.sourceGroups.Source(source)
				if sourceGroup.Err != nil {
					continue
				}
				if m.hasUncompressedExtension(path) {
					if !m.Config.Lifecycle().EnableFileCompress() {
						if generation, ok := m.parseFileGeneration(path); ok {
							resume(path, generation, false)
						}
					}
					continue
				}
				if pathidentity.ComparisonKey(selected[pathidentity.ComparisonKey(source)]) != pathidentity.ComparisonKey(path) {
					continue
				}
				// sourceのretirementは、log file processing recoveryではなく通常のcompressionが担当します。
				if containsRecoveryPath(scan.sourceGroups.Files.Paths, source) {
					continue
				}
				generation, ok := m.parseFileGeneration(path)
				if ok {
					resume(path, generation, true)
				}
			}
			done = !pending
			return errors.Join(append(continuationErrs, preparationErr)...)
		})
		return done, err
	}, func(err error) func() {
		report := report
		return func() { m.reportLogFileProcessingRecovery(report, err) }
	}, release) {
		release()
	}
}

// recoverLogFileProcessingは受理済みの送信・退避・更新とlocal削除を再開します。
// 再走査した先行世代を確認し、未完了世代を飛び越えて完了位置を進めません。
func (m *FileSeriesManager) recoverLogFileProcessing(ctx context.Context, path string, generation fileGeneration, compressed bool, resumeUpload *bool, successes *[]objectstorage.DestinationID, report *RunReport) (bool, error) {
	upload := m.objectStorageUpload
	if upload == nil {
		return true, nil
	}
	scan, err := m.scanLifecycleTargetFiles()
	if err != nil {
		return false, err
	}
	if !containsRecoveryPath(scan.sourceGroups.Files.Paths, path) {
		// 削除後の同期失敗もこの登録が引き受けます。不在だけでは完了にしません。
		if err := fsstore.SyncDirectory(filepath.Dir(path)); err != nil {
			return false, err
		}
		upload.forgetUploadedPath(path)
		return true, nil
	}
	source, _ := fsstore.CompressionSourcePath(path, m.fileName.Extension)
	sourceGroup, _ := scan.sourceGroups.Source(source)
	if err := sourceGroup.Err; err != nil {
		return false, err
	}
	// 元ログのretirementは通常の圧縮処理が担当します。
	if compressed && containsRecoveryPath(scan.sourceGroups.Files.Paths, source) {
		return true, nil
	}
	state, err := upload.prepareRun(ctx)
	if err != nil {
		return false, err
	}
	activePeriod, err := currentRotationPeriod(m.Config, m.Clock.Now())
	if err != nil {
		return false, err
	}
	if err := upload.registerRunDestinations(ctx, m.fileName, activePeriod, state); err != nil {
		return false, err
	}
	m.setObservedUploadKeys(state, scan.sourceGroups.Files.Paths)
	var errs []error
	if upload.checkpointStore != nil && len(*successes) > 0 {
		key := fileCheckpointKey(generation, compressed)
		remembered := slices.Clone(*successes)
		destinations := state.destinations
		if len(destinations) == 0 {
			// 無効化後も、受理済みの送信結果の完了記録だけは片付けます。
			destinations = slices.Clone(*successes)
		}
		operation := processing.UploadState{
			Key: key, UploadFilePath: path, DestinationIDs: destinations, UploadedDestinationIDs: *successes,
		}
		outcome, markErr := func() (processing.UploadResult, error) {
			defer func() { *successes = operation.UploadedDestinationIDs }()
			return processing.CommitPendingUploads(ctx, &operation, processing.UploadDependencies{
				Checkpoints: managedUploadCheckpoint{manager: upload, path: path}, CommitReadySubset: true,
				ReadyDestinations: func(ctx context.Context, pending []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
					return upload.readyDestinationIDs(ctx, key, pending, state)
				},
			})
		}()
		if markErr != nil {
			if _, found := checkpointstore.SavedTargetConflictDestination(markErr, key, remembered); found {
				*resumeUpload = true
			}
			return false, markErr
		}
		errs = append(errs, outcome.PendingSaveErrors...)
		if len(outcome.CommittedDestinationIDs) == 0 || len(*successes) > 0 {
			return false, errors.Join(errs...)
		}
		upload.updateFileCompletionSnapshot(outcome.Checkpoint)
	}
	if state != nil && len(state.destinations) == 0 {
		return true, errors.Join(errs...)
	}
	if *resumeUpload {
		result, uploadErr := upload.uploadIfPending(ctx, path, generation, state, compressed)
		if len(result.uploadedDestinationIDs) > 0 {
			report.ObjectStorageUploads = append(report.ObjectStorageUploads, ObjectStorageUploadReport{Path: path, DestinationIDs: result.uploadedDestinationIDs})
		}
		if uploadErr != nil {
			errs = append(errs, uploadErr)
		}
		switch result.state {
		case uploadAttemptResumeUpload, uploadAttemptResumeCheckpoint:
			*successes = upload.uploadedPathDestinationIDs(path)
			*resumeUpload = result.state == uploadAttemptResumeUpload
			return false, errors.Join(errs...)
		}
		if result.state != uploadAttemptComplete {
			return false, errors.Join(errs...)
		}
		*resumeUpload = false
	}
	if !compressed {
		return true, errors.Join(errs...)
	}
	if state != nil {
		value, err := upload.loadFileCheckpoint(ctx, path, generation, compressed)
		if err != nil {
			return false, errors.Join(append(errs, err)...)
		}
		if !value.Complete(state.destinations) {
			*resumeUpload = true
			return false, errors.Join(errs...)
		}
		if !value.DeliveredToAny(state.destinations) {
			// 宛先の登録前から存在した対象外の世代だけで完了したfileは、どこにも送信していません。
			// upload完了を理由に削除せず、この再開処理を終えて通常の保持・purgeへ残します。
			return true, errors.Join(errs...)
		}
	}
	var destinations []objectstorage.DestinationID
	if state != nil {
		destinations = state.destinations
	}
	completion, directorySyncErr := processing.RemoveCompletedFile(ctx, processing.CompletionRequest{
		Path: path, Compressed: compressed, Complete: true, DestinationIDs: destinations,
	}, m.removeCompressedFileAndSyncDirectory)
	if !completion.Attempted {
		return directorySyncErr == nil, errors.Join(append(errs, directorySyncErr)...)
	}
	removal := completion.Removal
	report.UploadedFileRemovals = append(report.UploadedFileRemovals, removal)
	if removal.Err != nil {
		return false, fmt.Errorf("recover uploaded appendlog file removal %s: %w", path, removal.Err)
	}
	upload.forgetUploadedPath(path)
	return directorySyncErr == nil, errors.Join(append(errs, directorySyncErr)...)
}

// uploadedPathDestinationIDsは、このファイルの送信・退避が完了し、checkpointへの完了記録の
// 反映を待っている宛先IDをメモリからコピーして返します。通常周期や復旧処理の再開時に、
// 完了済み宛先への再uploadを避け、AdvanceCompletionCursorによる完了記録の更新を再試行するために使います。
// AdvanceCompletionCursorが更新を受理した宛先は、checkpointファイルの保存が未完了でもこの一覧から取り除かれます。
func (m *objectStorageUploadManager) uploadedPathDestinationIDs(path string) []objectstorage.DestinationID {
	if m == nil {
		return nil
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	var destinations []objectstorage.DestinationID
	for destination := range m.uploadedPathDestinations[pathidentity.ComparisonKey(path)] {
		destinations = append(destinations, destination)
	}
	return destinations
}

func containsRecoveryPath(paths []string, path string) bool {
	for _, candidate := range paths {
		if pathidentity.ComparisonKey(candidate) == pathidentity.ComparisonKey(path) {
			return true
		}
	}
	return false
}

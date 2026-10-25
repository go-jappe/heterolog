package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

var (
	ErrInvalidCheckpointConfiguration = errors.New("appendlog lifecycle checkpoint configuration is invalid")
	ErrInvalidUploadReport            = errors.New("appendlog lifecycle destination upload report is invalid")
)

// objectStorageUploadManagerは宛先ごとの送信・退避完了位置を管理します。
// 先行する既存世代が未完了の宛先だけを止め、他の宛先は独立して進めます。
// 送信が有効な場合は、宛先別の送信結果を扱うUploaderとcheckpoint Storeが必須です。
type objectStorageUploadManager struct {
	stateMu         sync.Mutex
	uploader        uploader.Uploader
	checkpointStore checkpointstore.Store
	// AdvanceCompletionCursorが更新を受理する前に失敗した場合の送信・退避の完了を保持します。
	uploadedPathDestinations map[string]map[objectstorage.DestinationID]struct{}
	// purgeはStoreを呼ばず、この周期に確認した成功記録と有効宛先を参照します。
	checkpointsByKey       map[checkpointstore.CheckpointKey]checkpointstore.Checkpoint
	checkpointDestinations []objectstorage.DestinationID
	checkpointReady        bool
}

type objectStorageUploadRunState struct {
	uploader     uploader.DestinationUploader
	destinations []objectstorage.DestinationID
	observedKeys []checkpointstore.CheckpointKey
}

// objectStorageUploadAttemptStateは、送信処理の完了状況と再開地点を表します。
type objectStorageUploadAttemptState uint8

const (
	// uploadAttemptInactiveは有効な宛先がない状態です。復旧は登録せず、送信完了による削除もしません。
	uploadAttemptInactive objectStorageUploadAttemptState = iota
	// uploadAttemptResumeUploadは送信・退避の処理経路から再開します。既存の完了記録は再利用します。
	uploadAttemptResumeUpload
	// uploadAttemptResumeCheckpointは送信・退避済みの結果をcheckpointのメモリへ反映するところから再開します。
	// 反映済みstateのファイル保存だけを再試行するSaveRecoveryとは異なります。
	uploadAttemptResumeCheckpoint
	// uploadAttemptCompleteは、このファイルについて送信処理を再開する必要がない状態です。
	uploadAttemptComplete
)

type objectStorageUploadAttemptResult struct {
	state objectStorageUploadAttemptState
	// uploadedDestinationIDsは今回の送信・退避に成功した宛先です。完了記録だけの再試行は含めません。
	uploadedDestinationIDs []objectstorage.DestinationID
	// deliveredはuploadAttemptCompleteであり、今回の有効宛先の少なくとも一つへ送信・退避済みかを表します。
	// 宛先の登録前から存在したため対象外とした宛先だけで完了したファイルはfalseで、削除しません。
	delivered                   bool
	localCompressedPathToRemove string
}

func newObjectStorageUploadManager(uploadClient uploader.Uploader, store checkpointstore.Store) *objectStorageUploadManager {
	if uploadClient == nil && store == nil {
		return nil
	}
	return &objectStorageUploadManager{
		uploader: uploadClient, checkpointStore: store,
		uploadedPathDestinations: make(map[string]map[objectstorage.DestinationID]struct{}),
		checkpointsByKey:         make(map[checkpointstore.CheckpointKey]checkpointstore.Checkpoint),
	}
}

func (m *objectStorageUploadManager) validate() error {
	if m == nil {
		return nil
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.uploadedPathDestinations == nil {
		m.uploadedPathDestinations = make(map[string]map[objectstorage.DestinationID]struct{})
	}
	if m.checkpointsByKey == nil {
		m.checkpointsByKey = make(map[checkpointstore.CheckpointKey]checkpointstore.Checkpoint)
	}
	if m.uploader != nil {
		if m.checkpointStore == nil {
			return fmt.Errorf("%w: object storage upload requires a checkpoint store", ErrInvalidCheckpointConfiguration)
		}
		if _, ok := m.uploader.(uploader.DestinationUploader); !ok {
			return fmt.Errorf("%w: uploader %T does not expose destination results", ErrInvalidCheckpointConfiguration, m.uploader)
		}
	}
	return nil
}

func (m *objectStorageUploadManager) uploadEnabled() bool {
	if m == nil {
		return false
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.uploader != nil
}

func (m *objectStorageUploadManager) tracksFiles() bool {
	if m == nil {
		return false
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.checkpointStore != nil
}

// prepareRunは今回の送信先を固定します。初期完了位置はStoreの登録時に決まります。
func (m *objectStorageUploadManager) prepareRun(ctx context.Context) (*objectStorageUploadRunState, error) {
	if m == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.checkpointReady = false
	m.checkpointDestinations = nil
	m.checkpointsByKey = make(map[checkpointstore.CheckpointKey]checkpointstore.Checkpoint)
	state := &objectStorageUploadRunState{}
	if m.uploader != nil {
		client, ok := m.uploader.(uploader.DestinationUploader)
		if !ok {
			return nil, fmt.Errorf("%w: uploader does not expose destination results", ErrInvalidCheckpointConfiguration)
		}
		state.uploader = client
		state.destinations = append([]objectstorage.DestinationID(nil), client.DestinationIDs()...)
		if len(state.destinations) == 0 {
			return nil, fmt.Errorf("%w: object storage uploader has no destinations", ErrInvalidCheckpointConfiguration)
		}
		seen := make(map[objectstorage.DestinationID]struct{}, len(state.destinations))
		for _, id := range state.destinations {
			if id == "" {
				return nil, fmt.Errorf("%w: empty destination", ErrInvalidCheckpointConfiguration)
			}
			if _, exists := seen[id]; exists {
				return nil, fmt.Errorf("%w: duplicate destination %q", ErrInvalidCheckpointConfiguration, id)
			}
			seen[id] = struct{}{}
		}
	}
	m.checkpointDestinations = append([]objectstorage.DestinationID(nil), state.destinations...)
	m.checkpointReady = true
	return state, nil
}

// registerRunDestinationsは、directoryのlifecycle lock内で圧縮・uploadより先に宛先を登録します。
// 通常周期だけでなく、未完了ファイル処理を再開する場合も呼びます。
// activePeriodは現在のrotation期間で、この期間以降の元ログだけを新しい宛先の対象に残します。
func (m *objectStorageUploadManager) registerRunDestinations(ctx context.Context, fileName logpattern.FileNamePattern, activePeriod string, state *objectStorageUploadRunState) error {
	if m == nil || state == nil || len(state.destinations) == 0 {
		return nil
	}
	existingKeys, err := checkpointstore.ScanRegistrationFileKeys(fileName, activePeriod)
	if err == nil {
		err = m.checkpointStore.RegisterDestinations(ctx, state.destinations, existingKeys)
	}
	if err != nil {
		return fmt.Errorf("register log file upload destinations: %w", err)
	}
	return nil
}

func fileCheckpointKey(generation fileGeneration, compressed bool) checkpointstore.CheckpointKey {
	return checkpointstore.CheckpointKey{
		Generation: checkpointstore.ArchiveCheckpointGenerationKey(generation.Period, generation.Index),
		Compressed: compressed,
	}
}

// uploadIfPendingは先行世代の完了を確認した未完了宛先だけへ送信します。
func (m *objectStorageUploadManager) uploadIfPending(ctx context.Context, path string, generation fileGeneration, state *objectStorageUploadRunState, compressed bool) (objectStorageUploadAttemptResult, error) {
	if m == nil || path == "" {
		return objectStorageUploadAttemptResult{state: uploadAttemptComplete}, nil
	}
	path = pathidentity.AbsoluteOrCleanPath(path)
	if state == nil {
		if !m.uploadEnabled() {
			return objectStorageUploadAttemptResult{state: uploadAttemptInactive}, nil
		}
		return objectStorageUploadAttemptResult{}, fmt.Errorf("%w: object storage upload run is not prepared", ErrInvalidCheckpointConfiguration)
	}
	return m.uploadFileDestinationsIfPending(ctx, path, generation, state, compressed)
}

func (m *objectStorageUploadManager) loadFileCheckpoint(ctx context.Context, path string, generation fileGeneration, compressed bool) (checkpointstore.Checkpoint, error) {
	key := fileCheckpointKey(generation, compressed)
	value, err := m.checkpointStore.Load(ctx, key)
	if err != nil {
		return checkpointstore.Checkpoint{}, fmt.Errorf("load log file upload checkpoint %+v: %w", key, err)
	}
	m.updateFileCompletionSnapshot(value)
	return value, nil
}

// managedUploadCheckpointは、未受理の送信結果とStoreの完了記録を共通処理へ接続します。
// StoreのAdvanceCompletionCursorが更新を受理するまで、送信済み宛先をmanagerにも保持します。
type managedUploadCheckpoint struct {
	manager *objectStorageUploadManager
	path    string
}

func (adapter managedUploadCheckpoint) Load(ctx context.Context, key checkpointstore.CheckpointKey) (checkpointstore.Checkpoint, error) {
	manager := adapter.manager
	value, err := manager.checkpointStore.Load(ctx, key)
	if err != nil {
		return checkpointstore.Checkpoint{}, fmt.Errorf("load log file upload checkpoint %+v: %w", key, err)
	}
	manager.updateFileCompletionSnapshot(value)
	return value, nil
}

// AdvanceCompletionCursorは、指定宛先の送信・退避の完了結果を保持してからStoreへ渡します。
// Storeがメモリ上の完了cursorを進め、cursor以前の同じ宛先の途中経過を削除し、
// checkpointファイルへ保存します。このadapterは受理前のエラーやpanicでも完了結果を
// 復旧へ引き継ぎます。更新受理後の保存失敗でもStoreが更新済みstateを維持し、
// adapterは保存の再試行をStoreへ委ねます。
func (adapter managedUploadCheckpoint) AdvanceCompletionCursor(ctx context.Context, key checkpointstore.CheckpointKey, ids []objectstorage.DestinationID) (checkpointstore.Checkpoint, error) {
	manager := adapter.manager
	// AdvanceCompletionCursorがpanicしても、成功済み宛先を復旧へ引き継ぎます。
	manager.recordUploadedPathDestinations(adapter.path, ids)
	// cursorの更新・途中経過の整理・保存をStoreへ委譲し、受理済みの完了結果をcacheから外します。
	return manager.advanceFileCompletionCursor(ctx, adapter.path, key, ids)
}

func (m *objectStorageUploadManager) uploadFileDestinationsIfPending(ctx context.Context, path string, generation fileGeneration, state *objectStorageUploadRunState, compressed bool) (objectStorageUploadAttemptResult, error) {
	key := fileCheckpointKey(generation, compressed)
	if !slices.Contains(state.observedKeys, key) {
		state.observedKeys = append(state.observedKeys, key)
	}
	if len(state.destinations) == 0 {
		_, err := m.loadFileCheckpoint(ctx, path, generation, compressed)
		return objectStorageUploadAttemptResult{state: uploadAttemptInactive}, err
	}
	operation := processing.UploadState{
		Key: key, UploadFilePath: path, DestinationIDs: state.destinations,
		UploadedDestinationIDs: m.uploadedPathDestinationIDs(path),
	}
	remembered := slices.Clone(operation.UploadedDestinationIDs)
	outcome, err := processing.RunUpload(ctx, &operation, processing.UploadDependencies{
		Checkpoints: managedUploadCheckpoint{manager: m, path: path},
		Uploader:    state.uploader,
		ReadyDestinations: func(ctx context.Context, pending []objectstorage.DestinationID) ([]objectstorage.DestinationID, error) {
			return m.readyDestinationIDs(ctx, key, pending, state)
		},
		CommitReadySubset: true,
	})
	if outcome.Checkpoint.Key.Generation != "" {
		m.updateFileCompletionSnapshot(outcome.Checkpoint)
	}
	result := objectStorageUploadAttemptResult{
		state:                  uploadAttemptResumeUpload,
		uploadedDestinationIDs: outcome.UploadedDestinationIDs,
	}
	if outcome.Complete && len(operation.UploadedDestinationIDs) == 0 {
		result.state = uploadAttemptComplete
		result.delivered = outcome.Delivered
	}
	var errs []error
	errs = append(errs, outcome.PendingSaveErrors...)
	if outcome.FailureStage != processing.UploadFailureReport {
		for _, destinationResult := range outcome.Report.Results {
			if destinationResult.Err != nil {
				errs = append(errs, fmt.Errorf("upload appendlog file %s to object storage destination %q: %w", path, destinationResult.DestinationID, destinationResult.Err))
			}
		}
	}
	if err != nil && outcome.FailureStage != processing.UploadFailureDestination {
		switch outcome.FailureStage {
		case processing.UploadFailureReport:
			err = errors.Join(ErrInvalidUploadReport, err)
		case processing.UploadFailureRequest:
			err = fmt.Errorf("prepare appendlog upload %s: %w", path, err)
		case processing.UploadFailureCompletionCursor:
			err = fmt.Errorf("advance log file completion cursor %+v for %s: %w", key, path, err)
		}
		errs = append(errs, err)
	}
	if result.state != uploadAttemptComplete && outcome.FailureStage == processing.UploadFailureCompletionCursor {
		// 共通処理が除いた宛先も、実際に完了記録を待っていた場合だけ保全へ戻します。
		completionIDs := append(remembered, outcome.UploadedDestinationIDs...)
		if _, conflict := checkpointstore.SavedTargetConflictDestination(err, key, completionIDs); !conflict {
			result.state = uploadAttemptResumeCheckpoint
		}
	}
	// 送信不要なだけでなく、有効宛先の少なくとも一つへ送信・退避したファイルだけを削除します。
	// 登録前から存在した対象外の世代は送信も削除もせず、通常の保持・purgeへ残します。
	if processing.CanRemoveCompletedFile(compressed, state.destinations, result.delivered) {
		result.localCompressedPathToRemove = path
	}
	return result, errors.Join(errs...)
}
func (m *objectStorageUploadManager) readyDestinationIDs(ctx context.Context, key checkpointstore.CheckpointKey, pending []objectstorage.DestinationID, state *objectStorageUploadRunState) ([]objectstorage.DestinationID, error) {
	return checkpointstore.ReadyDestinationIDs(ctx, m.checkpointStore, key, pending, state.observedKeys)
}

// updateFileCompletionSnapshotは、Storeから得た世代別・宛先別の完了情報をメモリへ反映します。
// 完了情報は、その世代を宛先へ送る必要がないことを表します。送信・退避済みだけでなく、
// 宛先の登録前に作られた対象外の世代も含みます。正本はStoreの完了記録です。
// この時点の完了情報を使い、未完了の宛先があるファイルをpurgeから除外します。
// 対象外の世代は送信待ちではないため保護せず、通常の保持・purgeの対象に戻します。
// 呼び出し元と宛先IDのスライスを共有しないようコピーし、完了cursorを共有する
// 同世代の未圧縮・圧縮ファイルの両方のキーへ同じ完了情報を登録します。
func (m *objectStorageUploadManager) updateFileCompletionSnapshot(value checkpointstore.Checkpoint) {
	if m == nil || m.checkpointStore == nil {
		return
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	value.CompletedDestinationIDs = append([]objectstorage.DestinationID(nil), value.CompletedDestinationIDs...)
	m.checkpointsByKey[value.Key] = value
	// 同じ世代の未圧縮・圧縮ファイルは完了cursorを共有するため、もう一方のキーにも登録します。
	value.Key.Compressed = !value.Key.Compressed
	m.checkpointsByKey[value.Key] = value
}

func (m *objectStorageUploadManager) recordUploadedPathDestinations(path string, ids []objectstorage.DestinationID) {
	if m == nil || path == "" || len(ids) == 0 {
		return
	}
	key := pathidentity.ComparisonKey(path)
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	recorded := m.uploadedPathDestinations[key]
	if recorded == nil {
		recorded = make(map[objectstorage.DestinationID]struct{}, len(ids))
		m.uploadedPathDestinations[key] = recorded
	}
	for _, id := range ids {
		recorded[id] = struct{}{}
	}
}

func (m *objectStorageUploadManager) forgetUploadedPath(path string) {
	if m == nil || path == "" {
		return
	}
	key := pathidentity.ComparisonKey(path)
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	delete(m.uploadedPathDestinations, key)
}

func validateAndIndexUploadReportForDestinations(requested []objectstorage.DestinationID, report uploader.UploadReport) (map[objectstorage.DestinationID]uploader.DestinationResult, error) {
	results, err := uploader.IndexUploadReport(requested, report)
	if err != nil {
		return results, fmt.Errorf("%w: %w", ErrInvalidUploadReport, err)
	}
	return results, nil
}

// uploadPendingは、指定世代について今回の有効宛先への送信・退避が未完了、
// または完了情報から送信不要と確認できない状態ならtrueを返します。
//
// 送信が無効、準備済みの有効宛先一覧が空、全有効宛先への送信が不要と確認済みのいずれかならfalseです。
// つまり確実にupload再試行が不要であればfalseです。
//
// generationは呼び出し元がファイル名から取得した期間と世代番号、validはその取得に成功したかを表します。
// compressedは完了情報を検索するキーの圧縮・未圧縮の区別で、この関数はファイル名や内容を検査しません。
// 今回の有効宛先checkpointDestinationsと、世代別の完了情報checkpointsByKeyを照合します。
// 完了情報には送信成功、全指定退避先への保存成功、宛先登録時の対象外のいずれかにより
// 送信不要になった宛先IDが含まれます。この理由の違いは判定しません。
// 送信が有効で判定の準備が未完了、または有効宛先があり世代や完了情報を取得できない場合もtrueです。
// メモリ上の情報だけを参照し、Storeやファイルの読み書き、送信・退避の実行は行いません。
//
// 呼び出し元は、この戻り値を使って未完了・未確認のファイルをpurge対象から除外します。
// 保護対象への登録や、保持期限などを含む削除可否の判断は呼び出し元の責務です。
func (m *objectStorageUploadManager) uploadPending(generation fileGeneration, valid, compressed bool) bool {
	if m == nil {
		// 送信を管理する部品がないため、このmanagerによる送信待ちはありません。
		return false
	}
	// checkpointReady・有効宛先一覧・世代別の完了情報を、同じlock内で参照します。
	// 他の処理が宛先や完了情報を更新する途中の状態を読まないようにします。
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.uploader == nil {
		// 送信が無効なら、完了情報の有無にかかわらず送信待ちはありません。
		return false
	}
	if !m.checkpointReady {
		// 今回の宛先と完了判定の準備ができていないため、送信不要と確認できません。
		// 宛先一覧が空でも、準備済みと確認するまではこの未確認の扱いを優先します。
		return true
	}
	if len(m.checkpointDestinations) == 0 {
		// 準備済みの宛先一覧が空なら、このファイルを送る必要のある宛先はありません。
		return false
	}
	if !valid {
		// 世代を取得できなければ、どの完了情報と照合すべきか分からないためtrueです。
		return true
	}
	// checkpointsByKeyには、Storeから得た完了情報のその時点のコピーが入っています。
	// generationとcompressedからキーを作り、このファイルに対応する情報を引きます。
	value, exists := m.checkpointsByKey[fileCheckpointKey(generation, compressed)]
	// 情報がなければ、完了を確認できないためtrueです。
	// PendingDestinationIDsは今回の有効宛先からvalue.CompletedDestinationIDsを除きます。
	// 一つでも宛先が残れば未完了としてtrue、すべて送信不要と確認できればfalseを返します。
	return !exists || len(value.PendingDestinationIDs(m.checkpointDestinations)) > 0
}

func (m *objectStorageUploadManager) pruneUploadedPaths(existing map[string]struct{}) {
	if m == nil {
		return
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	for path := range m.uploadedPathDestinations {
		if _, ok := existing[path]; !ok {
			delete(m.uploadedPathDestinations, path)
		}
	}
}

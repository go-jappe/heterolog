package file

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile/processing"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// archiveUploadDestinationSnapshotは、一回の送信開始時の送信実装と対象宛先を固定します。
// 送信実装には指定宛先への送信だけを要求し、宛先一覧を取り直しません。
type archiveUploadDestinationSnapshot struct {
	uploader       processing.DestinationUploader
	destinationIDs []objectstorage.DestinationID
}

// archiveUploadGroupは、一世代について選んだ一件のログファイルと、その解析結果です。
// 候補選択またはrotation時に確定し、送信・削除・再試行まで同じ情報を引き継ぎます。
type archiveUploadGroup struct {
	// keyはファイル名から取得した世代と、圧縮・未圧縮の区別です。
	key checkpointstore.CheckpointKey
	// pathは今回処理するファイルのパスです。同世代の別の圧縮形式へ差し替えません。
	path string
	// fileNameは対象を確定した時点の系列の命名規則です。古い未完了世代の確認に使います。
	fileName logpattern.FileNamePattern
}

// uploadEnabledは新しいStoreが必要かを決め、destinationIDsは起動時の宛先登録に使います。
// upload無効時、または送信対象の宛先がない場合は、空の一覧を渡します。
// fileName.Dateにはwriterが現在書き込むrotation期間を設定して渡します。新しい宛先の登録では、
// この期間以降の元ログだけを書き込み中の可能性がある世代として対象に残します。
func newArchiveCompressedLogCheckpointStore(
	cfg pkgfsstore.ArchiveCheckpointConfig,
	role loggerType,
	fileName logpattern.FileNamePattern,
	uploadEnabled bool,
	destinationIDs []objectstorage.DestinationID,
	onError func(error),
) (checkpointstore.Store, error) {
	directoryConfigured := strings.TrimSpace(cfg.Directory) != ""
	sourceConfigured := strings.TrimSpace(cfg.SourcePrefix) != ""
	if !directoryConfigured && !sourceConfigured {
		return nil, nil
	}
	if !directoryConfigured || !sourceConfigured {
		return nil, fmt.Errorf(
			"compressed log file upload checkpoint directory and source prefix must both be configured",
		)
	}
	sourceID := checkpointstore.ArchiveCheckpointSourceID(
		cfg.SourcePrefix,
		string(role),
		fileName,
	)
	// upload無効時は保存先の設定だけを確認し、残存checkpointを読みません。
	// 再有効化したときにStoreを構築し、その時点で既存の記録を読みます。
	if !uploadEnabled {
		if err := checkpointstore.ValidateSourceID(sourceID); err != nil {
			return nil, err
		}
		directory, err := filepath.Abs(cfg.Directory)
		if err != nil {
			return nil, err
		}
		if _, err := pathidentity.ResolveAndValidateManagedDirectory(filepath.Dir(directory), filepath.Base(directory)); err != nil {
			return nil, err
		}
		return nil, nil
	}
	store, err := logfile.NewCheckpointStore(
		cfg.Directory, sourceID, onError,
		checkpointstore.WithMissingCheckpointRecovery(cfg.AutoRecoverMissingCheckpoint),
	)
	if err != nil {
		return nil, err
	}
	if len(destinationIDs) > 0 {
		err := appendlog.DirectoryGuardForPath(fileName.Dir).RunWithLifecycleLock(func() error {
			keys, err := checkpointstore.ScanRegistrationFileKeys(fileName, fileName.Date)
			if err != nil {
				return err
			}
			return store.RegisterDestinations(context.Background(), destinationIDs, keys)
		})
		if err != nil {
			return nil, errors.Join(err, store.Close())
		}
	}
	return store, nil
}

func (l *FileLogger) archiveUploadDestinationsSnapshot() archiveUploadDestinationSnapshot {
	if l == nil || l.ArchiveUploader == nil {
		return archiveUploadDestinationSnapshot{}
	}
	return archiveUploadDestinationSnapshot{
		uploader:       l.ArchiveUploader,
		destinationIDs: append([]objectstorage.DestinationID(nil), l.ArchiveUploader.DestinationIDs()...),
	}
}

// usesPersistentArchiveUploadCheckpointは、有効な宛先別uploadがcheckpointを使うかを返します。
func (l *FileLogger) usesPersistentArchiveUploadCheckpoint(
	snapshot archiveUploadDestinationSnapshot,
) bool {
	return l != nil && l.archiveUploadCheckpoints != nil && snapshot.uploader != nil
}

// completeArchiveGroupUploadは、解析済みの一件を送信開始時に固定した宛先へ送ります。
// upload成功または失敗時の全指定退避先への保存成功を宛先ごとに記録します。
// 現在の全宛先の完了をメモリ上のcheckpointへ反映でき、少なくとも一つへ送信・退避して
// いれば圧縮済みfileの削除を許可します。宛先の登録前から存在したため対象外の宛先だけで
// 完了した世代は削除せず、通常の保持・purgeへ任せます。
// checkpointファイルの保存だけが失敗した場合は通知し、Storeの再保存に任せます。
// 初回処理から渡されたuploadは、失敗・panic後に登録する再試行でも同じものを使用します。
// パスの解析や宛先一覧の取得は行わず、呼び出し元が確定した情報を引き継ぎます。
// 第一戻り値は削除可能な圧縮ファイル一件のパスです。削除対象がない場合は空文字を返します。
func (l *FileLogger) completeArchiveGroupUpload(
	ctx context.Context,
	group archiveUploadGroup,
	notify taskNotificationDelivery,
	snapshot archiveUploadDestinationSnapshot,
	upload *processing.UploadState,
) (string, bool, *checkpointstore.Checkpoint) {
	if group.path == "" {
		return "", false, nil
	}
	if snapshot.uploader == nil || len(snapshot.destinationIDs) == 0 {
		return "", false, nil
	}
	if !l.usesPersistentArchiveUploadCheckpoint(snapshot) {
		if upload == nil {
			upload = &processing.UploadState{}
		}
		upload.UploadFilePath = group.path
		upload.DestinationIDs = snapshot.destinationIDs
		result, _ := l.uploadAcceptedDestinations(ctx, upload, snapshot.uploader, notify)
		if !result.Complete {
			return "", true, nil
		}
		return group.path, false, nil
	}
	active := snapshot.destinationIDs
	key := group.key
	if l.archiveLogFileProcessingRecovery.Has(archiveLogFileProcessingRecoveryKey{File: key}) {
		return "", true, nil
	}
	if upload != nil {
		// 初回処理の所有中に識別情報を確定し、復旧へ渡した後は書き換えません。
		upload.Key, upload.UploadFilePath, upload.DestinationIDs = group.key, group.path, snapshot.destinationIDs
	}
	operation := archiveFileOperationContinuation{group: group, destinations: snapshot, upload: upload}
	recoveryRegistered := false
	defer func() {
		if recovered := recover(); recovered != nil {
			// AdvanceCompletionCursor中のpanicでも、受理済みの成功宛先を再送しません。
			// 復旧へ渡した後の通知panicでは、同じoperationを再び変更・登録しません。
			if !recoveryRegistered {
				l.queueArchiveLogFileProcessingRecovery(operation, fmt.Errorf("archive operation panic: %v", recovered))
			}
			panic(recovered)
		}
	}()
	result, err := l.runArchiveUpload(ctx, &operation, l.archiveUploadCheckpoints, notify)
	if err != nil {
		recoveryRegistered = l.queueArchiveLogFileProcessingRecovery(operation, err)
		switch result.FailureStage {
		case processing.UploadFailureRequest, processing.UploadFailureReport:
			l.notifyArchiveUploadError(ctx, group.path, err, notify)
		case processing.UploadFailureDestination:
			// 宛先ごとの送信失敗はrunArchiveUploadが通知済みです。
		default:
			l.notifyArchiveUploadCheckpointError(ctx, group.path, err, notify)
		}
		return "", true, nil
	}
	if !result.Complete {
		// 送信失敗・必須退避先の保存失敗・先行世代待ちも受理済み処理として保持します。
		recoveryRegistered = l.queueArchiveLogFileProcessingRecovery(operation,
			fmt.Errorf("archive upload remains incomplete for %q", group.path))
		return "", true, nil
	}
	// 現在の全宛先の送信・退避完了がメモリ上のcheckpointへ反映されていれば、
	// 元ファイルからの再送は不要です。spoolからの再送とは別の判定です。
	// 削除は、少なくとも一つの宛先へ実際に送信・退避した場合だけ許可します。
	if processing.CanRemoveCompletedFile(group.key.Compressed, active, result.Delivered) {
		return group.path, false, &result.Checkpoint
	}
	return "", false, &result.Checkpoint
}

// parseArchiveUploadGroupは、一件のパスから世代・圧縮種別を取得し、ファイル操作用のパスへ整形します。
// rotationや圧縮準備でファイルが決まった境界で呼び、後続処理は解析済みの値を引き継ぎます。
func parseArchiveUploadGroup(
	path string,
	fileName logpattern.FileNamePattern,
) (archiveUploadGroup, error) {
	generation, canonical := logpattern.ParseCanonicalGenerationFilePath(path, fileName)
	if !canonical {
		return archiveUploadGroup{}, fmt.Errorf("path is not a canonical archive: %q", path)
	}
	return archiveUploadGroup{
		key: checkpointstore.CheckpointKey{
			Generation: checkpointstore.ArchiveCheckpointGenerationKey(generation.Period, generation.Index),
			Compressed: generation.Kind == logpattern.GenerationFileKindCompressed,
		},
		path:     pathidentity.AbsoluteOrCleanPath(path),
		fileName: fileName,
	}, nil
}

func (l *FileLogger) notifyArchiveUploadCheckpointError(
	ctx context.Context,
	path string,
	err error,
	notify taskNotificationDelivery,
) {
	message := fmt.Sprintf(ErrorMsgArchiveUploadCheckpoint, path)
	wrapper := l.wrapErrorByLogger(err, OperationArchiveUploadCheckpoint)
	entry := l.InternalErrorEntryWithContext(ctx, message, err)
	notify(func() {
		l.dispatchError(ctx, wrapper, entry)
	})
}

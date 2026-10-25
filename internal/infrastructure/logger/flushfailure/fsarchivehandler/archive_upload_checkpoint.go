package fsarchivehandler

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
	checkpointstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/checkpoint"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/infrastructure/logfile"
	"github.com/go-jappe/heterolog/internal/infrastructure/logger/handler"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/objectstorage/uploader"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

// newCompressedArchiveUploadCheckpointStoreは、系列ごとのcheckpoint Storeを構築し、宛先を登録します。
// 送信・退避や元ファイルの削除、ログ保存先の使用量の管理は行いません。
func newCompressedArchiveUploadCheckpointStore(
	cfg pkgfsstore.ArchiveCheckpointConfig,
	failureCategory failurearchive.FailureCategory,
	fileName logpattern.FileNamePattern,
	archiveUploader uploader.Uploader,
	onError func(error),
) (checkpointstore.Store, error) {
	directoryConfigured := strings.TrimSpace(cfg.Directory) != ""
	sourceConfigured := strings.TrimSpace(cfg.SourcePrefix) != ""
	if archiveUploader == nil && !directoryConfigured && !sourceConfigured {
		return nil, nil
	}
	if !directoryConfigured || !sourceConfigured {
		return nil, fmt.Errorf(
			"failure archive compressed log file checkpoint directory and source prefix must both be configured",
		)
	}
	sourceID := checkpointstore.ArchiveCheckpointSourceID(
		cfg.SourcePrefix,
		string(failureCategory),
		fileName,
	)
	// upload無効時は保存先の設定だけを確認し、残存checkpointを読みません。
	// 再有効化したときにStoreを構築し、その時点で既存の記録を読みます。
	if archiveUploader == nil {
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
	if err := registerCompressedArchiveUploadDestinations(store, fileName, archiveUploader); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

// registerCompressedArchiveUploadDestinationsは、構築時点の宛先と既存の世代をStoreへ登録します。
// 登録前から存在した世代は、新しく追加された宛先の対象外として記録されます。
// fileName.Dateはwriterが現在書き込むrotation期間で、この期間以降の元ファイルだけを
// 書き込み中の可能性がある世代として新しい宛先の対象に残します。
// 既存世代の走査と登録は、同じdirectoryのlifecycle lock内で行います。
func registerCompressedArchiveUploadDestinations(
	store checkpointstore.Store,
	fileName logpattern.FileNamePattern,
	archiveUploader uploader.Uploader,
) error {
	destinationUploader, ok := archiveUploader.(uploader.DestinationUploader)
	if !ok {
		return nil
	}
	return appendlog.DirectoryGuardForPath(fileName.Dir).RunWithLifecycleLock(func() error {
		keys, err := checkpointstore.ScanRegistrationFileKeys(fileName, fileName.Date)
		if err != nil {
			return err
		}
		return store.RegisterDestinations(context.Background(), destinationUploader.DestinationIDs(), keys)
	})
}

func (f *FsArchiveHandler) compressedArchiveUploadCheckpointStore(
	failureCategory failurearchive.FailureCategory,
) checkpointstore.Store {
	state := f.directoryForFailureCategory(failureCategory)
	if state == nil {
		return nil
	}
	return state.uploadCheckpoints
}

func (f *FsArchiveHandler) notifyCompressedArchiveUploadCheckpointError(
	ctx context.Context,
	filePath string,
	err error,
) {
	if f == nil || err == nil {
		return
	}
	// 保存・復旧が受理したエラー通知は、元の処理の期限や通常終了では中止しません。
	// 強制終了だけを通知先へ伝え、終了待機を打ち切れるようにします。
	ctx, cancel := f.forceWork.context(context.WithoutCancel(contextOrBackground(ctx)))
	defer cancel()
	message := fmt.Sprintf(ErrorMsgFileArchiveUpload, filePath)
	wrappedErr := &FailureArchiveInternalError{Err: err, Op: OperationArchiveUpload}
	entry := f.internalErrorEntryWithContext(ctx, message, err)
	handler.CallErrorHandlerIfPresent(
		f.OnError,
		ctx,
		wrappedErr,
		entry,
		f.Tracer,
		f.Meter,
	)
}

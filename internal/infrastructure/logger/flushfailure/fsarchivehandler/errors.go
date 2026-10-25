package fsarchivehandler

import (
	"errors"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/failurearchive"
)

var (
	// ErrFailureArchiveClosingは、Shutdownまたは強制終了の開始後に新しい保存要求を拒否したことを表します。
	ErrFailureArchiveClosing = errors.New("failure archive handler is closing")
	// ErrNilFileArchiveConfigは、failure archiveの構築に必要な設定がnilであることを表します。
	ErrNilFileArchiveConfig = errors.New("file archive config is nil")
	// ErrUnsupportedFileArchiveOverloadPolicyは、過負荷時のpolicyが対応外であることを表します。
	ErrUnsupportedFileArchiveOverloadPolicy = errors.New("unsupported file archive overload policy")
	// ErrFileArchiveMinAvailableDiskSpaceTooSmallは、過負荷と判定する空き容量の閾値が下限未満であることを表します。
	ErrFileArchiveMinAvailableDiskSpaceTooSmall = errors.New(
		"failure archive overload policy config: min available disk space is below the minimum",
	)
	// ErrFailureArchiveRotationDeferredは、新しいarchive fileを準備できず、現在のactive fileへ
	// 保存を続けてrotationを次回へ延期したことを表します。原因のerrorも同時に判定できます。
	ErrFailureArchiveRotationDeferred = errors.New("failure archive rotation deferred")
	// ErrFailureArchiveWriteは、failure recordをarchive fileへ保存できなかったことを表します。
	// 原因のerrorも同時に判定できます。
	ErrFailureArchiveWrite = errors.New("failed to write failure archive")
	// ErrFailureArchiveUploadDestinationReconciliationは、runner modeの構築時に、object storageの
	// 送信先とcheckpointの登録内容を照合できなかったことを表します。原因のerrorも同時に判定できます。
	ErrFailureArchiveUploadDestinationReconciliation = errors.New(
		"failed to reconcile failure archive object storage upload destinations",
	)

	// errNilOverloadHandlerは、過負荷時の処置を行うhandlerが構成されていないことを表します。
	errNilOverloadHandler = errors.New("overload handler is nil")
)

// FailureArchiveCheckpointErrorは、failure archive系列の送信完了を記録するcheckpoint Storeを
// 構築できなかったことを表します。Categoryは対象の系列です。
type FailureArchiveCheckpointError struct {
	Category failurearchive.FailureCategory
	Err      error
}

func (e *FailureArchiveCheckpointError) Error() string {
	return fmt.Sprintf("create %s failure archive compressed log file checkpoint: %v", e.Category, e.Err)
}

func (e *FailureArchiveCheckpointError) Unwrap() error {
	return e.Err
}

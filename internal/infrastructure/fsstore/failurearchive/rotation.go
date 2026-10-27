package failurearchive

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/pkg/retry"
)

// ErrRotationTargetUnavailableは、rotation先のpath解決またはfile openに失敗したため、
// 現在のactive fileを維持したままrotationを延期できることを表します。
var ErrRotationTargetUnavailable = errors.New("failure archive rotation target is unavailable")

// RotationResultは、active fileの切替結果と、切替前のfileを閉じた際の異常を分離します。
// PreviousFileCloseErrが設定されていても新しいactive fileへの切替は完了しています。
type RotationResult struct {
	// OldFilePathは切替前のactive file pathです。切替後のlifecycle処理に使用します。
	OldFilePath string
	// PreviousFileCloseErrは旧fileのCloseが返したerrorです。設定されていても旧handleは
	// 再利用せず、新しいactive fileへの切替は完了しています。
	PreviousFileCloseErr error
}

// Rotateはatが属する期間の新しいactive fileを先にopenし、旧fileを一度だけ閉じて
// 切り替えます。
// 新しいfileを準備できない場合だけerrorを返し、旧fileのclose異常はRotationResultへ
// 分離します。
func (f *FileStore) Rotate(
	ctx context.Context,
	failureCategory FailureCategory,
	at time.Time,
) (RotationResult, error) {
	return f.rotate(ctx, failureCategory, at, func(file *os.File) error {
		return file.Close()
	})
}

// rotateはproductionのRotateと障害試験で共通する処理本体です。closeFileがerrorを
// 返しても旧handleは再利用せず、先にopenしたfileを新しいactiveとして確定します。
func (f *FileStore) rotate(
	ctx context.Context,
	failureCategory FailureCategory,
	at time.Time,
	closeFile func(*os.File) error,
) (RotationResult, error) {
	var (
		activeFile **os.File
		activePath *string
		fileName   *logpattern.FileNamePattern
	)
	switch failureCategory {
	case FailureCategoryRetryable:
		activeFile = &f.FileRetryable
		activePath = &f.ActiveFilePathRetryable
		fileName = &f.FileNameRetryable
	case FailureCategoryPermanent:
		activeFile = &f.FilePermanent
		activePath = &f.ActiveFilePathPermanent
		fileName = &f.FileNamePermanent
	default:
		return RotationResult{}, fmt.Errorf("unsupported failure archive category: %q", failureCategory)
	}
	if *activeFile == nil {
		return RotationResult{}, fmt.Errorf("%s failure archive active file is nil", failureCategory)
	}

	oldFile := *activeFile
	result := RotationResult{OldFilePath: oldFile.Name()}

	// 現在のactive fileを閉じる前にrotation先を確定してopenする。openできなければ
	// oldFileを維持し、呼び出し側が今回のrecordをoldFileへ書き込めるようにする。
	newPath, err := logpattern.ResolveFilePathAt(fileName, f.Config, at)
	if err != nil {
		return result, fmt.Errorf(
			"%w: resolve %s failure archive path: %w",
			ErrRotationTargetUnavailable,
			failureCategory,
			err,
		)
	}
	// MaxWriteRetriesは初回に加える再試行の回数で、構築時に0以上と検証済みです。
	newFile, err := retry.RetryWithResult(
		func() (*os.File, error) { return OpenFile(newPath) },
		f.Config.MaxWriteRetries(),
		f.Config.FileOperationRetryWaitTime(),
	)
	if err != nil {
		return result, fmt.Errorf(
			"%w: open %s failure archive rotation target %q: %w",
			ErrRotationTargetUnavailable,
			failureCategory,
			newPath,
			err,
		)
	}

	// os.File.Closeはerrorを返してもhandleを再利用できる契約ではありません。
	// close結果とは分離して、新しいactive fileへの切替を必ず確定します。
	result.PreviousFileCloseErr = closeFile(oldFile)
	*activeFile = newFile
	*activePath = newPath
	return result, nil
}

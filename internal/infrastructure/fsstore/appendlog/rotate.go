package appendlog

import (
	"context"
	"fmt"

	"github.com/go-jappe/heterolog/internal/infrastructure/logger/logging"
)

// RotationPending reports whether rotation released the previous file handle,
// including when its Close returned an error, but has not opened its
// replacement. Callers must hold Mu, as for RotateWithRetry. The old path must
// remain protected from lifecycle operations until rotation or Close.
func (fs *FileStore) RotationPending() bool {
	return fs.rotationPendingPath != ""
}

// ファイルのローテーション　および　直前まで書き込み対象だったファイルを圧縮可能な設定であれば圧縮
func (fs *FileStore) Rotate(
	ctx context.Context,
	logFunc logging.RetryLogFunc,
	entryFactory logging.EntryFactory,
	openActiveFile func(ctx context.Context) error, // fsstore内メソッドにしてもOK
) (string, error) {
	return fs.RotateWithRetry(ctx, logFunc, entryFactory, openActiveFile, fs.Config.Retry())
}

// RotateWithRetryは旧active fileを閉じてから、openActiveFileで新しいactive fileを開きます。
// 旧handleのCloseは一度だけ呼びます。Closeがerrorを返した場合も旧handleは外し、
// 旧pathをrotation待ちとして保持したままCloseのerrorを返します。新fileのopenと、
// open失敗後の再試行は、次の呼び出し（通常は次のwrite）でopenの段階から再開します。
func (fs *FileStore) RotateWithRetry(
	ctx context.Context,
	logFunc logging.RetryLogFunc,
	entryFactory logging.EntryFactory,
	openActiveFile func(ctx context.Context) error,
	retry RetryConfigProvider,
) (string, error) {

	oldPath := fs.rotationPendingPath
	if oldPath == "" {
		if fs.File == nil {
			return "", fmt.Errorf("file is nil")
		}
		oldPath = fs.File.Name()
		// os.File.Closeはerrorを返してもhandleを解放済みとして扱い、再度のClose・Stat・
		// Writeはすべて失敗します。再試行や旧handleの再利用はせず、Closeの結果に
		// かかわらずrotation待ちへ移します。旧pathは新fileのopen成功またはCloseまで
		// active保護から外れません。
		closeErr := fs.File.Close()
		fs.File = nil
		fs.rotationPendingPath = oldPath
		if closeErr != nil {
			if logFunc != nil && entryFactory != nil {
				logFunc(ctx, closeErr, entryFactory(ctx, fmt.Sprintf(ErrorMesFailedFileClose, oldPath), closeErr))
			}
			// 旧fileへ書いた内容の永続化を確認できないことを、このrotationの呼び出し元へ
			// 返します。新fileのopenは次の呼び出しで再開し、以降のwriteを止め続けません。
			return oldPath, fmt.Errorf("failed to close log file %s: %w", oldPath, closeErr)
		}
	}

	// open失敗後も元pathを保持し、次のwriteでこの段階から再開します。
	// 旧handleはすでに閉じているため、Windowsでも成功後のarchiveを妨げません。
	if err := logging.RetryWithLog(
		func() error { return openActiveFile(ctx) },
		retry.MaxWriteAttempts(),
		retry.FileOperationRetryWaitTime(),
		logFunc,
		entryFactory,
		fmt.Sprintf(ErrorMesFailedFileOpen, fs.ActiveFilePath),
		ctx,
	); err != nil {
		return oldPath, fmt.Errorf(ErrFailedOpenFile, fs.ActiveFilePath, err)
	}
	fs.rotationPendingPath = ""

	return oldPath, nil
}

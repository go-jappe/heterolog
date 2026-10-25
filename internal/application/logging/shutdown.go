package logging

import (
	"errors"
	"io"

	pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"
)

var ErrForcedClose = pkgshutdown.ErrForcedClose

var ErrForceCloseUnsupported = errors.New("logger does not support forced shutdown")

// ForceCloserは正常終了の待機を打ち切り、復旧処理へ中止を要求できるLoggerの追加契約です。
// 未保存のcheckpointや未完了の退避処理が残る可能性があります。プロセスは終了しません。
// 実行中のOS I/Oは強制中断せず、その処理が戻るまで資源の後片付けが続く場合があります。
type ForceCloser interface {
	ForceClose() error
}

// ForceCloseは同期・非同期・DeliveryのLoggerに共通の強制終了入口です。
// Closeを別goroutineで待っている間にも呼べます。以後はそのLoggerを再利用できません。
// 強制終了に対応していない独自Loggerの場合はErrForceCloseUnsupportedを返します。
func ForceClose(logger io.Closer) error {
	if logger == nil {
		return nil
	}
	closer, ok := logger.(ForceCloser)
	if !ok {
		return ErrForceCloseUnsupported
	}
	return closer.ForceClose()
}

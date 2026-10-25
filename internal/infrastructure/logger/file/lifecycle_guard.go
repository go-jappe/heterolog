package file

import (
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/appendlog"
)

// initLifecycleGuardは、このFileLoggerを同じ保存directoryの別FileLogger・lifecycle処理と
// 同期させるため、logger固有のtokenとdirectory共有のguardを設定します。
// LoggerSet内で同じroleを共有するlogical loggerは同じFileLoggerを使うためtokenも共通です。
// 分離されたerror/debug loggerは別のactive fileを持つため、それぞれ固有のtokenを持ちます。
// constructorがfile名を解決した後に一度だけ呼び出します。
func (l *FileLogger) initLifecycleGuard() {
	l.lifecycleToken = appendlog.NewDirectoryGuardToken()
	// 同じ正規化済みdirectoryを使うFileLoggerとappendlog lifecycle runnerは
	// 同じguardを共有します。
	l.lifecycleGuard = appendlog.DirectoryGuardForPath(l.Fsstore.FileName.Dir)
}

// initLifecycleGuardは、このDeliveryFileLoggerを同じ保存directoryの別FileLogger・
// lifecycle処理と同期させるため、logger固有のtokenとdirectory共有のguardを設定します。
// 分離されたdefault/error/debug loggerは別のactive fileを持つため、それぞれ固有の
// tokenを持ちます。constructorがfile名を解決した後に一度だけ呼び出します。
func (l *DeliveryFileLogger) initLifecycleGuard() {
	l.lifecycleToken = appendlog.NewDirectoryGuardToken()
	l.lifecycleGuard = appendlog.DirectoryGuardForPath(l.Fsstore.FileName.Dir)
}

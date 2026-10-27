package logging

import pkglogger "github.com/go-jappe/heterolog/pkg/logger"

// CloseableLogger は終了時のflushやresource解放が必要なLoggerです。
// Close開始後の新しい記録要求は受理せず、ErrLoggerClosedを返します。
// 受理済みの処理と、登録済みの復旧（退避・checkpoint保存・後片付け）を期限なしで待ちます。
// spoolへ保存済みの全データの再送成功は待ちません。待機を打ち切る場合はForceCloseを使います。
type CloseableLogger interface {
	pkglogger.Logger
	Close() error
}

// AsyncCloseableLogger は受理済み処理の完了待ちとresource解放が必要なAsyncLoggerです。
// AsyncLoggerにはerrorの戻り値がないため、Close開始後の新しい要求は受理せず、拒否を
// 呼び出し元へは通知しないbest-effort契約です。拒否の観測はmetricsなど別経路で扱います。
// Closeの待機範囲とForceCloseの契約はCloseableLoggerと同じです。
type AsyncCloseableLogger interface {
	pkglogger.AsyncLogger
	Close() error
}

// DeliveryClosableLoggerは終了処理を持つDeliveryLoggerです。
// Close開始後の新しい要求はbackend送信前に拒否し、DeliveryRejectedと
// ErrLoggerClosedを原因に持つerrorを返します。
// Closeの待機範囲とForceCloseの契約はCloseableLoggerと同じです。
type DeliveryClosableLogger interface {
	pkglogger.DeliveryLogger
	Close() error
}

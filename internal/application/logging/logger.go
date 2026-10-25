package logging

import (
	"context"

	"github.com/go-jappe/heterolog/pkg/logger"
)

// Logger はapplicationから同期的にログを記録する契約です。
// trace_idとspan_idはctx、request_idなどの追加情報はfieldsから取得します。
type Logger interface {
	Debug(ctx context.Context, message string, fields ...Field) error
	Info(ctx context.Context, message string, fields ...Field) error
	Warn(ctx context.Context, message string, fields ...Field) error
	Error(ctx context.Context, message string, fields ...Field) error
	ErrorWithStack(ctx context.Context, message string, errorMessage error, fields ...Field) error
}

// CloseableLogger は終了時のflushやresource解放が必要なLoggerです。
// Close開始後の新しい記録要求は受理せず、ErrLoggerClosedを返します。
// 受理済みの処理と、登録済みの復旧（退避・checkpoint保存・後片付け）を期限なしで待ちます。
// spoolへ保存済みの全データの再送成功は待ちません。待機を打ち切る場合はForceCloseを使います。
type CloseableLogger interface {
	Logger
	Close() error
}

// AsyncLogger はapplicationの処理完了を待たずにログを受け付ける契約です。
type AsyncLogger interface {
	Debug(ctx context.Context, message string, fields ...Field)
	Info(ctx context.Context, message string, fields ...Field)
	Warn(ctx context.Context, message string, fields ...Field)
	Error(ctx context.Context, message string, fields ...Field)
	ErrorWithStack(ctx context.Context, message string, errorMessage error, fields ...Field)
}

// AsyncCloseableLogger は受理済み処理の完了待ちとresource解放が必要なAsyncLoggerです。
// AsyncLoggerにはerrorの戻り値がないため、Close開始後の新しい要求は受理せず、拒否を
// 呼び出し元へは通知しないbest-effort契約です。拒否の観測はmetricsなど別経路で扱います。
// Closeの待機範囲とForceCloseの契約はCloseableLoggerと同じです。
type AsyncCloseableLogger interface {
	AsyncLogger
	Close() error
}

// DeliveryLogger は、applicationが指定した送信先ごとの結果をDeliveryReportとして返します。
// backend送信失敗後のarchiveやspoolなどの内部処理は、この契約には含めません。
type DeliveryLogger interface {
	Debug(ctx context.Context, message string, fields ...Field) (logger.DeliveryReport, error)
	Info(ctx context.Context, message string, fields ...Field) (logger.DeliveryReport, error)
	Warn(ctx context.Context, message string, fields ...Field) (logger.DeliveryReport, error)
	Error(ctx context.Context, message string, fields ...Field) (logger.DeliveryReport, error)
	ErrorWithStack(ctx context.Context, message string, errorMessage error, fields ...Field) (logger.DeliveryReport, error)
}

// DeliveryClosableLoggerは終了処理を持つDeliveryLoggerです。
// Close開始後の新しい要求はbackend送信前に拒否し、DeliveryRejectedと
// ErrLoggerClosedを原因に持つerrorを返します。
// Closeの待機範囲とForceCloseの契約はCloseableLoggerと同じです。
type DeliveryClosableLogger interface {
	DeliveryLogger
	Close() error
}

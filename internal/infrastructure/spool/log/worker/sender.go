package worker

import (
	"context"

	"github.com/go-jappe/heterolog/pkg/spool"
)

type Sender interface {
	// Sendはrecordをbackendへ送り、各recordをsuccess、retryable、permanent（dead）、
	// invalid（quarantine）のいずれかへ分類します。
	//
	// reportはspool.ExportReportの契約を満たさなければなりません。
	// errorは個々のbackend送信失敗ではなく、全recordの送信結果を
	// 通常どおり確定できなかったことを表します。errorを返す場合も、
	// 結果不明または未送信のrecordをretryableとして全件分類します。
	Send(ctx context.Context, records []spool.SpoolRecord) (spool.ExportReport, error)
}

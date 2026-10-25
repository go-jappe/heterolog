package logger

import "strings"

// LoggerContractKindは、applicationへ公開するtop-level MultiLoggerを、呼出方法と
// 返却結果の契約によってsync、async best-effort、deliveryへ分類します。
// File、Loki、OpenSearchなどの子Loggerや出力先backendの種類は表しません。
// 値は診断sourceやprocess-local directory LeaseのScopeLabelを組み立てるcanonical IDです。
// 人間向けの表示名や、個々のMultiLogger instanceを一意に識別するIDではありません。
type LoggerContractKind string

const (
	// LoggerContractSyncは、呼出元が各子Loggerの処理完了を待ち、errorを受け取る契約です。
	LoggerContractSync LoggerContractKind = "logger"
	// LoggerContractAsyncBestEffortは、child Loggerを別goroutineで実行し、そのerrorを
	// 呼出元へ返さない契約です。並行数上限を理由にchildを破棄することは表しません。
	LoggerContractAsyncBestEffort LoggerContractKind = "async_logger"
	// LoggerContractDeliveryは、送信先ごとの送達結果をDeliveryReportで返す契約です。
	LoggerContractDelivery LoggerContractKind = "delivery_logger"
)

// StringはLoggerContractKindのcanonical IDを返します。
func (k LoggerContractKind) String() string {
	return string(k)
}

// DisplayNameは、error messageなど人間が読む文章へ埋め込むcontract名を返します。
// canonical IDそのものは変更せず、単語の区切りであるunderscoreだけをspaceへ
// 置き換えるため、表示名を別の定数として二重管理しません。
func (k LoggerContractKind) DisplayName() string {
	return strings.ReplaceAll(k.String(), "_", " ")
}

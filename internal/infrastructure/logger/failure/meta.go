package failure

import "time"

// Metaは、1回のbackend送信試行に共通する情報を保持します。
// 個々の失敗内容はFailedRecordが保持し、Metaにはbatch内の全recordで共通する
// 送信先、実行環境、関連ID、送信時刻、入力件数をまとめます。
type Meta struct {
	// Backendは、送信に失敗したbackend serviceを表します。
	Backend string
	// RequestIDは、backend送信時のcontextに設定されていたapplication request IDです。
	// batch内の各LogEntryが持つRequestIDとは別であり、timerによるflushでは空の場合があります。
	RequestID string
	// AppNameからVersionまでは、失敗が発生したapplication実行環境を識別します。
	AppName         string
	HostName        string
	Environment     string
	RuntimeType     string
	ExecutionUnitID string
	Version         string
	// TraceIDは、送信試行をtraceと照合するためのIDです。
	TraceID string
	// AttemptedAtは、backendへの送信を試みた時刻です。
	// FailedRecord.FailedAtが空の場合は、durable recordの失敗時刻にも使用します。
	AttemptedAt time.Time
	// InputCountは、この送信試行でbackendへ渡したrecord数です。
	// SendResultはこの値と失敗record数から成功件数を計算します。
	InputCount int
}

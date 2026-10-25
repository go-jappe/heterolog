package stdout

const (
	MsgFailedWriteStdout = "failed to write to stdout. err: %v"
	MsgFailedWriteStderr = "failed to write to stderr. err: %v"
	// MsgFailedWriteNotificationは、内部ログの通知先として作った専用instanceが通知を書き込めなかった
	// ことを、失敗ごとに一行、標準loggerへ残す文言です。%sは失敗した書き込みの説明
	// （MsgFailedWriteStdout・MsgFailedWriteStderrの文言）です。
	MsgFailedWriteNotification = "stdout internal log notification failed: %s"
	OperationWriteError        = "write error"
)

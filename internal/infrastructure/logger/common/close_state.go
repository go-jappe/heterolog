package common

// LoggerCloseStateは、Close可能なloggerの新規受付と終了処理の進行状態を表します。
// zero valueは通常受付中です。Closeに失敗した場合はStartedを維持することで、
// 新規要求を拒否したまま次のCloseで終了処理だけを再試行できます。
type LoggerCloseState uint8

const (
	// LoggerCloseStateOpenは、新しいログ要求を受け付ける通常状態です。
	LoggerCloseStateOpen LoggerCloseState = iota
	// LoggerCloseStateStartedは、Close開始後かつ終了処理が未完了の状態です。
	LoggerCloseStateStarted
	// LoggerCloseStateCompletedは、終了処理が正常に完了した状態です。
	LoggerCloseStateCompleted
)

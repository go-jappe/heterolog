package fsstoreconfig

import "time"

const (

	// 圧縮レイテンシエラー型警告閾値時間（ファイルサイズによるので一旦この値）
	CompressWarnThresholdDuration  = 3 * time.Second
	CompressErrorThresholdDuration = 10 * time.Second
	// ログをファイルに書き込む際のレイテンシエラー警告閾値時間
	WriteLogFileWarnThresholdDuration  = 20 * time.Millisecond
	WriteLogFileErrorThresholdDuration = 100 * time.Millisecond

	//　ファイル操作時のエラーリトライ数 MaxWriteAttemptsで呼び出される
	MaxFileOperationnRetryAttempts = 3
	FileOperationRetryWaitTime     = 100 * time.Millisecond
)

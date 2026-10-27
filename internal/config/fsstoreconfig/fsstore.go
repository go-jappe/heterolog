package fsstoreconfig

import "time"

const (

	// 圧縮レイテンシエラー型警告閾値時間（ファイルサイズによるので一旦この値）
	CompressWarnThresholdDuration  = 3 * time.Second
	CompressErrorThresholdDuration = 10 * time.Second
	// ログをファイルに書き込む際のレイテンシエラー警告閾値時間
	WriteLogFileWarnThresholdDuration  = 20 * time.Millisecond
	WriteLogFileErrorThresholdDuration = 100 * time.Millisecond

	// MaxFileOperationRetriesは、ファイル操作が失敗したときに初回に加えて再試行する回数です。
	// MaxWriteRetriesに使い、初回と合わせて最大3回操作します。
	MaxFileOperationRetries    = 2
	FileOperationRetryWaitTime = 100 * time.Millisecond
)

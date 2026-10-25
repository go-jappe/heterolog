package compressedlog

import "time"

// RunnerConfigProviderはrunnerの実行間隔を取得する契約です。
type RunnerConfigProvider interface {
	// Intervalは再送と結果通知が終わってから、次の再送まで待つ時間です。
	// 初回もこの時間だけ待ち、保存を契機とした即時再送は行いません。
	Interval() time.Duration

	// PurgeIntervalは再送とは独立したpurge周期です。terminal purgerを設定した
	// runnerでは正の値が必須で、purgerを設定しない場合は使いません。
	PurgeInterval() time.Duration
}

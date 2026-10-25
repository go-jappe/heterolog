package spool

import "time"

// RetryAfterClampは、送信先がRetry-Afterで指定した再送待機、またはspoolに保存済みの
// 再送下限時刻を、設定した上限まで短縮したことを表します。短縮は運用者へ通知します。
type RetryAfterClamp struct {
	// Requestedは短縮前の待機時間です。保存済みの下限時刻を短縮した場合は、
	// 読み込んだ時点から保存済みの下限時刻までの時間です。
	Requested time.Duration
	// MaxRetryAfterは適用した上限です。
	MaxRetryAfter time.Duration
	// NotBeforeは短縮後の再送下限時刻です。
	NotBefore time.Time
	// Storedは、spoolに保存済みの下限時刻を読み込み時に短縮したことを表します。
	// falseの場合は、送信先の応答が指定した待機を短縮したことを表します。
	Stored bool
}

// CapRetryAfterは、retryAfterをmaxRetryAfterまでに制限した待機時間と、短縮したかを返します。
// maxRetryAfterが0以下の場合は制限しません。呼び出し側は設定値を検証済みで渡してください。
func CapRetryAfter(retryAfter, maxRetryAfter time.Duration) (time.Duration, bool) {
	if maxRetryAfter > 0 && retryAfter > maxRetryAfter {
		return maxRetryAfter, true
	}
	return retryAfter, false
}

// Package shutdownは明示的な強制終了の結果を定義します。
package shutdown

import "errors"

// ErrForcedCloseは受理済み処理の完了保証を放棄したことを表します。
// 実行中のOS I/Oや資源解放の完了、プロセス終了を意味しません。
var ErrForcedClose = errors.New("shutdown forced before accepted work completed")

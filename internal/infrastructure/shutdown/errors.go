package shutdown

import pkgshutdown "github.com/go-jappe/heterolog/pkg/shutdown"

// ErrForcedCloseは明示的な強制終了により受理済み処理の完了保証を放棄したことを表します。
// このエラーだけでは実行中のI/Oや資源解放の完了を意味しません。
var ErrForcedClose = pkgshutdown.ErrForcedClose

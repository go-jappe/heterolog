package retryable

import "errors"

// isTransientDisconnectErrnoは、接続の切断・拒否をOSのerror番号で判定します。
// error messageはOSごとに異なるため、型付きのerrorを持つ場合は文字列による推定より先に判定します。
func isTransientDisconnectErrno(err error) bool {
	for _, errno := range transientDisconnectErrnos {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
}

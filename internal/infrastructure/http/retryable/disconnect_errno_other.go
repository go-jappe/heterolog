//go:build !unix && !windows

package retryable

// transientDisconnectErrnosは、接続の切断・拒否を表すerror番号です。
// 対象外のplatformでは、文字列による推定だけを使います。
var transientDisconnectErrnos []error

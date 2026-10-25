//go:build unix

package retryable

import "syscall"

// transientDisconnectErrnosは、接続の切断・拒否を表すerror番号です。
var transientDisconnectErrnos = []error{
	syscall.ECONNRESET,
	syscall.ECONNABORTED,
	syscall.ECONNREFUSED,
	syscall.EPIPE,
}

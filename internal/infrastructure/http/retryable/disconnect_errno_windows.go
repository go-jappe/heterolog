//go:build windows

package retryable

import "syscall"

// wsaeConnRefusedは、Windows Socketsの接続拒否(WSAECONNREFUSED)です。
// syscall packageに定数がないため、error番号を直接指定します。
const wsaeConnRefused syscall.Errno = 10061

// transientDisconnectErrnosは、接続の切断・拒否を表すerror番号です。
// Windowsのsocket操作はWinsockのerror番号を返します。syscall packageがPOSIX互換に
// 定義する番号も、同じ意味のerrorとして扱います。
var transientDisconnectErrnos = []error{
	syscall.WSAECONNRESET,
	syscall.WSAECONNABORTED,
	wsaeConnRefused,
	syscall.ECONNRESET,
	syscall.ECONNABORTED,
	syscall.ECONNREFUSED,
	syscall.EPIPE,
}

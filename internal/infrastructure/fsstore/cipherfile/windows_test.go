package cipherfile

import "runtime"

// isWindowsは、permissionの検査をWindowsで飛ばすためのものです。
func isWindows() bool { return runtime.GOOS == "windows" }

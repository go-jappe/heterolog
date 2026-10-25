//go:build !darwin && !linux

package pathidentity

import "runtime"

func filesystemPathKey(path string) string { return path }

// Darwin・Linux以外のUnix系では、filesystemPathKeyと同じく大小文字を区別します。
// WindowsはDirectoryNameCaseで標準のcase-insensitive directoryとして扱います。
func filesystemDirectoryNameCase(string) NameCase { return NameCaseSensitive }

// filesystemCaseSensitiveは、Darwin・Linux以外でOSの既定の規則を返します。
// Windowsは大小文字を区別せず、その他のOSは区別します。
func filesystemCaseSensitive(string) (bool, error) { return runtime.GOOS != "windows", nil }

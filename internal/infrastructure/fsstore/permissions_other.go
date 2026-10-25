//go:build !windows

package fsstore

import "os"

func isDirectoryReparsePoint(os.FileInfo) bool { return false }

// CreatePrivateDirectoryはapp所有directoryを1つ作ります。
// Unixではos.Mkdirと同じmode・process umaskを使用します。
func CreatePrivateDirectory(path string, mode os.FileMode) error {
	return os.Mkdir(path, mode)
}

// SetPrivateDirectoryPermissionsは指定したapp所有directoryだけの権限を設定します。
// callerは共有ancestorを渡さないでください。
func SetPrivateDirectoryPermissions(path string, mode os.FileMode) error {
	return os.Chmod(path, mode)
}

// SetPrivateFilePermissionsはcallerがpayloadを書き込む前にfileの権限を設定します。
func SetPrivateFilePermissions(file *os.File, mode os.FileMode) error {
	return file.Chmod(mode)
}

// ReadPrivateFileは既存checkpointを読みます。Unixではos.ReadFileと同じく
// 既存権限を変更せず、起動時の読み取りに書き込み権限を要求しません。
func ReadPrivateFile(path string, mode os.FileMode) ([]byte, error) {
	return os.ReadFile(path)
}

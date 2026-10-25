package testutil

import (
	"testing"

	"golang.org/x/sys/windows"
)

// DenyDirectorySyncは、配下の項目を名前で参照する操作と名前変更は許可したまま、
// 同期用にディレクトリを開けなくします。戻り値の関数とテスト終了時の後片付けは、
// どちらも元の権限を復元します。
func DenyDirectorySync(t *testing.T, directory string) func() {
	t.Helper()
	// ディレクトリの同期で要求するGENERIC_WRITEには、FILE_WRITE_EAが含まれます。
	// 配下の項目の変更には、このアクセス権は必要ありません。
	return DenyWindowsAccess(t, directory, windows.FILE_WRITE_EA)
}

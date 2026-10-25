//go:build !windows

package file

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// requireDrivePathDirectoryValidationは、ドライブ指定を含む保存先設定を受け付けることを確認します。
// Windows以外では`D:\ordinary-logs`も通常のdirectory名として結合されます。
func requireDrivePathDirectoryValidation(t *testing.T, err error) {
	t.Helper()
	require.NoError(t, err)
}

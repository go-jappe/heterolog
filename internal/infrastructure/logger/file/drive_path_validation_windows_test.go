package file

import (
	"testing"

	"github.com/stretchr/testify/require"

	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

// requireDrivePathDirectoryValidationは、colon検証がドライブ指定をファイル名部品の誤りとして
// 拒否しないことを確認します。HETEROLOG_LOG_DIR等はHETEROLOG_SYSTEM_LOG_DIRとHETEROLOG_TARGETの下へ結合するため、Windowsでは
// ドライブ指定を含む名前が不正なpathとなり、保存先の解決で拒否されます。拒否の理由はOSのpath
// 構文のerrorで、そのerror codeはこの試験の要件ではありません。
func requireDrivePathDirectoryValidation(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err, "a drive-qualified directory joined below LOG_PATH is not a valid Windows path")
	require.NotErrorIs(t, err, pkgfsstore.ErrInvalidLogFileNameComponent)
}

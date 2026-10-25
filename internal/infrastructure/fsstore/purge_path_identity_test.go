package fsstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 除外pathの別表記は、filesystem上で同じfileを指す場合だけ削除から保護します。
// OS名ではなく、作成したfileを別表記でos.Statした結果で期待値を決めます。
// 大小文字を区別しないDarwinのvolumeやWindowsでは保護され、区別するdirectoryでは別名です。
func TestDirectoryPurgePathIdentityFollowsFilesystemCaseAlias(t *testing.T) {
	for _, test := range []struct {
		name  string
		alias func(string) string
	}{
		{name: "uppercase", alias: strings.ToUpper},
		{name: "extended", alias: func(path string) string { return `\\?\` + path }},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "App_20260911_1.log")
			require.NoError(t, os.WriteFile(path, []byte("valuable log"), 0o600))
			info, err := os.Stat(path)
			require.NoError(t, err)
			alias := test.alias(path)
			aliasInfo, err := os.Stat(alias)
			sameFile := err == nil && os.SameFile(info, aliasInfo)
			if !sameFile && strings.HasPrefix(alias, `\\?\`) {
				t.Skip("extended path syntax is not an alias on this filesystem")
			}
			file := FileInfo{Path: path, Size: uint64(info.Size()), ModTime: info.ModTime()}
			result := PurgeByDirectorySizeFromFileNameIfNeeded([]FileInfo{file}, 1, map[string]struct{}{alias: {}})
			if !sameFile {
				// 大小文字を区別するdirectoryでは別名のfileであり、元のfileの保護には使いません。
				require.Empty(t, result.Remaining, "alias %q must not protect %q", alias, path)
				require.NoFileExists(t, path)
				return
			}
			require.Len(t, result.Remaining, 1, "alias %q", alias)
			require.Empty(t, result.RemovalResults)
			body, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, "valuable log", string(body))
		})
	}
}

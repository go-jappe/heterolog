package fsstore

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestCompressionSourcePath(t *testing.T) {
	for _, test := range []struct {
		name, base, source string
		valid              bool
	}{
		{"app_20260914_1.log", "log", "app_20260914_1.log", true},
		{"app_20260914_1.log.gz", "log", "app_20260914_1.log", true},
		{"app_20260914_1.log.zst", ".log", "app_20260914_1.log", true},
		{"backend_20260914_1.jsonl.custom", "jsonl", "backend_20260914_1.jsonl", true},
		{"app_20260914_1.log.gz.tmp", "log", "", false},
		{"app_20260914_1.log.tmp", "log", "", false},
		{"app_20260914_1.jsonl.gz", "log", "", false},
	} {
		t.Run(test.name+test.base, func(t *testing.T) {
			dir := t.TempDir()
			got, ok := CompressionSourcePath(filepath.Join(dir, test.name), test.base)
			require.Equal(t, test.valid, ok)
			if ok {
				require.Equal(t, filepath.Join(dir, test.source), got)
			} else {
				require.Empty(t, got)
			}
		})
	}
}

// 大小文字だけが異なるbase extensionは、OS名ではなく保存先directoryの規則で照合します。
// 規則を指定して、大小文字を区別する場合・しない場合の両方をどのOSでも確認します。
func TestCompressionSourcePathFollowsDirectoryNameCase(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "App_20260914_1.LOG.GZ")
	for _, test := range []struct {
		nameCase pathidentity.NameCase
		want     string
	}{
		{nameCase: pathidentity.NameCaseSensitive},
		{nameCase: pathidentity.NameCaseInsensitive, want: filepath.Join(dir, "App_20260914_1.LOG")},
	} {
		got, ok := CompressionSourcePathForNameCase(path, "log", test.nameCase)
		require.Equal(t, test.want != "", ok, "name case %d", test.nameCase)
		require.Equal(t, test.want, got, "元のpath表記を保持する")
	}
	// 規則を指定しない場合は、ComparisonKeyと共用するpathの親directoryの判定を使います。
	want, wantOK := CompressionSourcePathForNameCase(path, "log", pathidentity.DirectoryNameCase(dir))
	got, ok := CompressionSourcePath(path, "log")
	require.Equal(t, wantOK, ok)
	require.Equal(t, want, got)
}

func TestCompressionProtectionPaths(t *testing.T) {
	for _, test := range []struct {
		name, path, base string
		want             []string
	}{
		{name: "empty path", base: "log"},
		{name: "uncompressed once", path: "app_20261011_1.log", base: "log", want: []string{"app_20261011_1.log"}},
		{name: "compressed and source", path: "app_20261011_1.log.gz", base: "log", want: []string{"app_20261011_1.log.gz", "app_20261011_1.log"}},
		{name: "different format same source", path: "app_20261011_1.log.zst", base: ".log", want: []string{"app_20261011_1.log.zst", "app_20261011_1.log"}},
		{name: "custom jsonl format", path: "app_20261011_1.jsonl.custom", base: "jsonl", want: []string{"app_20261011_1.jsonl.custom", "app_20261011_1.jsonl"}},
		{name: "different base extension", path: "app_20261011_1.jsonl.gz", base: "log", want: []string{"app_20261011_1.jsonl.gz"}},
		{name: "temporary file", path: "app_20261011_1.log.gz.tmp", base: "log", want: []string{"app_20261011_1.log.gz.tmp"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := test.path
			var want []string
			if path != "" {
				path = filepath.Join("not-created", path)
			}
			for _, candidate := range test.want {
				want = append(want, filepath.Join("not-created", candidate))
			}
			require.Equal(t, want, CompressionProtectionPaths(path, test.base))
		})
	}
}

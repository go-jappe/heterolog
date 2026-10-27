package fsstore_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"

	"github.com/stretchr/testify/require"
)

func TestResolveCompressedFilePath(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "app_20260922_1.log")
	for _, test := range []struct {
		name     string
		observed []string
		format   pkgfsstore.CompressFormat
		want     string
		exists   bool
		wantErr  bool
	}{
		{
			name:   "new gzip output",
			format: pkgfsstore.CompressGzip,
			want:   source + ".gz",
		},
		{
			name:   "new zstd output",
			format: pkgfsstore.CompressZstd,
			want:   source + ".zst",
		},
		{
			name:   "new custom format output",
			format: pkgfsstore.CompressFormat("br"),
			want:   source + ".br",
		},
		{
			name:     "reuse previous gzip format",
			observed: []string{source + ".gz"},
			format:   pkgfsstore.CompressZstd,
			want:     source + ".gz",
			exists:   true,
		},
		{
			name:     "reuse previous custom format",
			observed: []string{source + ".custom"},
			format:   pkgfsstore.CompressGzip,
			want:     source + ".custom",
			exists:   true,
		},
		{
			name: "ignore raw and unrelated paths",
			observed: []string{
				source,
				filepath.Join(dir, "other", "app_20260922_1.log.gz"),
				filepath.Join(dir, "app_20260922_2.log.gz"),
				filepath.Join(dir, "app_20260922_1.jsonl.gz"),
			},
			format: pkgfsstore.CompressZstd,
			want:   source + ".zst",
		},
		{
			name:     "first observed match wins over current format",
			observed: []string{source, source + ".gz", source + ".zst"},
			format:   pkgfsstore.CompressZstd,
			want:     source + ".gz",
			exists:   true,
		},
		{
			name:     "first observed match can be current format",
			observed: []string{source + ".zst", source + ".gz"},
			format:   pkgfsstore.CompressZstd,
			want:     source + ".zst",
			exists:   true,
		},
		{
			name:    "empty format cannot generate output",
			format:  pkgfsstore.CompressFormat(""),
			wantErr: true,
		},
		{
			name:    "unsafe format cannot generate output",
			format:  pkgfsstore.CompressFormat("../invalid"),
			wantErr: true,
		},
		{
			name:     "existing output wins over invalid current format",
			observed: []string{source + ".gz"},
			format:   pkgfsstore.CompressFormat("../invalid"),
			want:     source + ".gz",
			exists:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// observedのpathには、意図的に実fileを用意していません。解決処理は呼び出し元の
			// snapshotを使い、fileの検査も作成もしてはいけません。
			path, exists, err := fsstore.ResolveCompressedFilePath(source, test.observed, test.format)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.want, path)
			require.Equal(t, test.exists, exists)

			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, entries, "resolving compressed paths must not create files")
		})
	}
}

func TestFindExistingCompressedFilePathReusesObservedFormat(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "app_20260914_1.log")
	for _, test := range []struct {
		name   string
		paths  []string
		want   string
		exists bool
	}{
		{"none", nil, "", false},
		{"old gzip", []string{source, source + ".gz"}, source + ".gz", true},
		{"unknown old format", []string{source + ".custom"}, source + ".custom", true},
		{"same observed path twice", []string{source + ".gz", source + ".gz"}, source + ".gz", true},
		{"first is old format", []string{source + ".gz", source + ".zst"}, source + ".gz", true},
		{"first is current format", []string{source + ".zst", source + ".gz"}, source + ".zst", true},
		{"unrelated and temporary", []string{
			source + ".gz.tmp", source + ".tmp", source + logpattern.CompressionSourceDeletePendingSuffix,
			filepath.Join(dir, "other", "app_20260914_1.log.gz"),
			filepath.Join(dir, "app_20260914_2.log.gz"),
			filepath.Join(dir, "app_20260914_1.jsonl.gz"),
		}, "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path, exists := fsstore.FindExistingCompressedFilePath(source, test.paths)
			require.Equal(t, test.want, path)
			require.Equal(t, test.exists, exists)
		})
	}
}

func TestFindExistingCompressedFilePathFollowsDirectoryNameCase(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "App_20260914_1.log")
	peer := filepath.Join(dir, "app_20260914_1.LOG.GZ")
	observed := []string{
		filepath.Join(dir, "other", "app_20260914_1.LOG.GZ"),
		filepath.Join(dir, "APP_20260914_2.LOG.GZ"),
		peer,
	}
	// OS名ではなく保存先directoryの規則を指定し、両方の規則をどのOSでも確認します。
	for _, test := range []struct {
		name     string
		nameCase pathidentity.NameCase
		want     string
	}{
		{name: "case-sensitive", nameCase: pathidentity.NameCaseSensitive},
		{name: "case-insensitive", nameCase: pathidentity.NameCaseInsensitive, want: peer},
	} {
		t.Run(test.name, func(t *testing.T) {
			path, exists := fsstore.FindExistingCompressedFilePathForNameCase(source, observed, test.nameCase)
			require.Equal(t, test.want != "", exists)
			require.Equal(t, test.want, path, "観測したパスの表記を保持する")
		})
	}
	t.Run("directory rule", func(t *testing.T) {
		// 規則を指定しない場合は、ComparisonKeyと共用する保存先directoryの判定を使います。
		wantPath, wantExists := fsstore.FindExistingCompressedFilePathForNameCase(
			source, observed, pathidentity.DirectoryNameCase(dir))
		path, exists := fsstore.FindExistingCompressedFilePath(source, observed)
		require.Equal(t, wantExists, exists)
		require.Equal(t, wantPath, path)
	})
}

func TestFindExistingCompressedFilePathPreservesSourceMatchingRules(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"app.log", "app.jsonl", "App.K", "app", "app.", "app.log.gz"} {
		source := filepath.Join(directory, name)
		for _, suffix := range []string{"", ".gz", ".custom", ".log", ".LOG", ".K", ".k", ".tmp", ".TMP", ".gz.tmp", ".gz.gz", ".bad*"} {
			t.Run(name+suffix, func(t *testing.T) {
				candidate := source + suffix
				// 共通化前の名前解析との一致を確認し、拡張子・大小文字・一時ファイルの扱いを維持します。
				raw, valid := fsstore.CompressionSourcePath(candidate, filepath.Ext(source))
				wantFound := valid && raw != candidate && pathidentity.ComparisonKey(raw) == pathidentity.ComparisonKey(source)
				path, found := fsstore.FindExistingCompressedFilePath(source, []string{candidate})
				require.Equal(t, wantFound, found)
				if wantFound {
					require.Equal(t, candidate, path, "観測したパスの表記を保持する")
				} else {
					require.Empty(t, path)
				}
			})
		}
	}
}

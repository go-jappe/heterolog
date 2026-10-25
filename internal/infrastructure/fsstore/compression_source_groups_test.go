package fsstore_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil/errortree"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
)

func TestCompressionSourceGroupsKeepsSourceFilesAndErrorsTogether(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "app_20260914_1.log")
	other := filepath.Join(dir, "app_20260914_2.log")
	rejected := errors.New("not a regular file")
	regular := []fsstore.FileInfo{
		{Path: source + ".zst", Size: 17, ModTime: time.Unix(10, 0)},
		{Path: other, Size: 25, ModTime: time.Unix(20, 0)},
		{Path: source, Size: 42, ModTime: time.Unix(30, 0)},
		{Path: source + ".gz", Size: 23, ModTime: time.Unix(40, 0)},
	}
	files := fsstore.FileInspection{
		Paths:        []string{source + ".zst", other, source + ".custom", source, source + ".gz"},
		RegularFiles: regular,
		PathErrors:   map[string]error{source + ".custom": rejected},
	}
	sourceGroups := fsstore.GroupFilesByCompressionSource(files, ".log")
	require.Equal(t, files, sourceGroups.Files)
	require.Len(t, sourceGroups.Sources, 2)
	group, exists := sourceGroups.Source(source)
	require.True(t, exists)
	require.Equal(t, source, group.SourcePath)
	require.Equal(t, []string{source + ".zst", source + ".custom", source, source + ".gz"}, group.Paths)
	require.Equal(t, []fsstore.FileInfo{regular[0], regular[2], regular[3]}, group.RegularFiles)
	require.ErrorIs(t, group.Err, rejected)
	require.Equal(t, []string{source + ".custom"}, compressionCandidateErrorPaths(group.Err))
	require.Equal(t, other, sourceGroups.Sources[1].SourcePath)

	// 通常の圧縮ファイルがあっても、同じ元ファイルに対応する検査エラーは無視しません。
	path, found, err := sourceGroups.FindExistingCompressedFile(source)
	require.ErrorIs(t, err, rejected)
	require.False(t, found)
	require.Empty(t, path)
}

func TestCompressionSourceGroupsFindsFirstInspectedRegularFormat(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260914_1.log")
	for _, test := range []struct {
		name  string
		paths []string
		want  string
	}{
		{name: "none"},
		{name: "raw only", paths: []string{source}},
		{name: "old gzip", paths: []string{source, source + ".gz"}, want: source + ".gz"},
		{name: "unknown old format", paths: []string{source + ".custom"}, want: source + ".custom"},
		{name: "old format first", paths: []string{source + ".gz", source + ".zst"}, want: source + ".gz"},
		{name: "current format first", paths: []string{source + ".zst", source + ".gz"}, want: source + ".zst"},
		{name: "duplicate peer", paths: []string{source + ".gz", source + ".gz"}, want: source + ".gz"},
	} {
		t.Run(test.name, func(t *testing.T) {
			// ファイルを作成せず、入力の検査結果だけで分類・選択することを確認します。
			files := fsstore.FileInspection{Paths: test.paths}
			for _, path := range test.paths {
				files.RegularFiles = append(files.RegularFiles, fsstore.FileInfo{Path: path})
			}
			sourceGroups := fsstore.GroupFilesByCompressionSource(files, "log")
			path, found, err := sourceGroups.FindExistingCompressedFile(source)
			require.NoError(t, err)
			require.Equal(t, test.want, path)
			require.Equal(t, test.want != "", found)
		})
	}
}

func TestCompressionSourceGroupsKeepsRegularFileOrderIndependentOfNames(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app.log")
	files := fsstore.FileInspection{
		Paths:        []string{source + ".gz", source, source + ".zst"},
		RegularFiles: []fsstore.FileInfo{{Path: source + ".zst"}, {Path: source}, {Path: source + ".gz"}},
	}
	sourceGroups := fsstore.GroupFilesByCompressionSource(files, "log")
	group, exists := sourceGroups.Source(source)
	require.True(t, exists)
	require.Equal(t, files.Paths, group.Paths)
	require.Equal(t, files.RegularFiles, group.RegularFiles)
	path, found, err := sourceGroups.FindExistingCompressedFile(source)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, source+".zst", path)
}

func TestCompressionSourceGroupsIgnoresMissingFilesAndUnrelatedSourceErrors(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "app_20260914_1.log")
	other := filepath.Join(dir, "app_20260914_2.log")
	missing := &os.PathError{Op: "lstat", Path: source + ".zst", Err: os.ErrNotExist}
	denied := &os.PathError{Op: "lstat", Path: other + ".gz", Err: os.ErrPermission}
	files := fsstore.FileInspection{
		Paths:        []string{other + ".gz", source + ".zst", source + ".gz"},
		RegularFiles: []fsstore.FileInfo{{Path: source + ".gz"}},
		PathErrors:   map[string]error{source + ".zst": missing, other + ".gz": denied},
	}
	sourceGroups := fsstore.GroupFilesByCompressionSource(files, "log")
	path, found, err := sourceGroups.FindExistingCompressedFile(source)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, source+".gz", path)
	group, exists := sourceGroups.Source(source)
	require.True(t, exists)
	require.Equal(t, []string{source + ".zst", source + ".gz"}, group.Paths)
	require.NoError(t, group.Err)
	path, found, err = sourceGroups.FindExistingCompressedFile(other)
	require.ErrorIs(t, err, os.ErrPermission)
	require.Empty(t, path)
	require.False(t, found)
}

func TestCompressionSourceGroupsAggregatesRawAndCompressedInspectionErrors(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app.log")
	rawErr := errors.New("raw inspection failed")
	peerErr := errors.New("compressed inspection failed")
	sourceGroups := fsstore.GroupFilesByCompressionSource(fsstore.FileInspection{
		Paths:        []string{source + ".gz", source, source + ".zst"},
		RegularFiles: []fsstore.FileInfo{{Path: source + ".zst"}},
		PathErrors:   map[string]error{source: rawErr, source + ".gz": peerErr},
	}, "log")
	path, found, err := sourceGroups.FindExistingCompressedFile(source)
	require.ErrorIs(t, err, rawErr)
	require.ErrorIs(t, err, peerErr)
	require.Equal(t, []string{source, source + ".gz"}, compressionCandidateErrorPaths(err))
	require.Empty(t, path)
	require.False(t, found)
}

func TestCompressionSourceGroupsKeepsUnfilteredFilesAndExcludesNonCandidates(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260914_1.log")
	files := fsstore.FileInspection{Paths: []string{
		source + ".tmp", source + ".gz.tmp", logpattern.CompressionSourceDeletePendingPath(source),
		source + ".compression-source-conflict-1.quarantine", source + ".gz.extra", source + ".jsonl.gz",
	}}
	files.PathErrors = make(map[string]error)
	for _, path := range files.Paths {
		files.RegularFiles = append(files.RegularFiles, fsstore.FileInfo{Path: path})
		files.PathErrors[path] = os.ErrPermission
	}
	sourceGroups := fsstore.GroupFilesByCompressionSource(files, "log")
	require.Equal(t, files, sourceGroups.Files)
	require.Empty(t, sourceGroups.Sources)
	_, exists := sourceGroups.Source(source)
	require.False(t, exists)
	path, found, err := sourceGroups.FindExistingCompressedFile(source)
	require.NoError(t, err)
	require.Empty(t, path)
	require.False(t, found)
}

func TestCompressionSourceGroupsSupportsOmittedPaths(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "app.log")
	other := filepath.Join(dir, "other.log")
	rejected := errors.New("not a regular file")
	files := fsstore.FileInspection{
		RegularFiles: []fsstore.FileInfo{{Path: source + ".gz"}, {Path: source}},
		PathErrors:   map[string]error{source + ".zst": rejected, other: rejected},
	}
	sourceGroups := fsstore.GroupFilesByCompressionSource(files, "log")
	require.Nil(t, sourceGroups.Files.Paths)
	group, exists := sourceGroups.Source(source)
	require.True(t, exists)
	require.Equal(t, []string{source + ".gz", source, source + ".zst"}, group.Paths)
	require.Equal(t, files.RegularFiles, group.RegularFiles)
	require.ErrorIs(t, group.Err, rejected)
	group, exists = sourceGroups.Source(other)
	require.True(t, exists)
	require.Equal(t, []string{other}, group.Paths)
	require.Empty(t, group.RegularFiles)
	require.ErrorIs(t, group.Err, rejected)
}

func TestCompressionSourceGroupsNormalizesSourcesAndPreservesOriginalSpelling(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "app.log")
	spelling := dir + string(filepath.Separator) + "." + string(filepath.Separator) + "app.log"
	sourceGroups := fsstore.GroupFilesByCompressionSource(fsstore.FileInspection{
		Paths:        []string{spelling + ".gz", source},
		RegularFiles: []fsstore.FileInfo{{Path: spelling}, {Path: source + ".gz"}},
	}, "log")
	require.Len(t, sourceGroups.Sources, 1)
	group, exists := sourceGroups.Source(source)
	require.True(t, exists)
	require.Equal(t, spelling, group.SourcePath)
	require.Equal(t, []string{spelling + ".gz", source, spelling, source + ".gz"}, group.Paths)
	path, found, err := sourceGroups.FindExistingCompressedFile(spelling)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, source+".gz", path)
}

func TestCompressionSourceGroupsFollowsDirectoryNameCase(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "App_20260914_1.log")
	alias := filepath.Join(dir, "app_20260914_1.LOG")
	peer := alias + ".GZ"
	inspection := fsstore.FileInspection{
		Paths:        []string{alias, peer},
		RegularFiles: []fsstore.FileInfo{{Path: alias}, {Path: peer}},
	}
	// OS名ではなく保存先directoryの規則を指定し、両方の規則をどのOSでも確認します。
	t.Run("case-insensitive", func(t *testing.T) {
		sourceGroups := fsstore.GroupFilesByCompressionSourceForNameCase(inspection, "log", pathidentity.NameCaseInsensitive)
		require.Len(t, sourceGroups.Sources, 1)
		require.Equal(t, alias, sourceGroups.Sources[0].SourcePath, "最初に観測した表記を保持する")
		require.Equal(t, []string{alias, peer}, sourceGroups.Sources[0].Paths)
		group, exists := sourceGroups.Source(source)
		require.True(t, exists)
		require.Equal(t, alias, group.SourcePath)
		path, found, err := sourceGroups.FindExistingCompressedFile(source)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, peer, path)
	})
	t.Run("case-sensitive", func(t *testing.T) {
		sourceGroups := fsstore.GroupFilesByCompressionSourceForNameCase(inspection, "log", pathidentity.NameCaseSensitive)
		// 大小文字が異なる拡張子は別の名前なので、元ログ・圧縮ファイルとして分類しません。
		require.Empty(t, sourceGroups.Sources)
		_, exists := sourceGroups.Source(source)
		require.False(t, exists)
		path, found, err := sourceGroups.FindExistingCompressedFile(source)
		require.NoError(t, err)
		require.False(t, found)
		require.Empty(t, path)
	})
	t.Run("directory rule", func(t *testing.T) {
		// 規則を指定しない場合は、ComparisonKeyと共用する保存先directoryの判定を使います。
		want := fsstore.GroupFilesByCompressionSourceForNameCase(inspection, "log", pathidentity.DirectoryNameCase(dir))
		got := fsstore.GroupFilesByCompressionSource(inspection, "log")
		require.Equal(t, want.Sources, got.Sources)
		wantPath, wantFound, wantErr := want.FindExistingCompressedFile(source)
		path, found, err := got.FindExistingCompressedFile(source)
		require.Equal(t, wantErr, err)
		require.Equal(t, wantFound, found)
		require.Equal(t, wantPath, path)
	})
}

func TestCompressionSourceGroupsRejectsNonRegularFilesBeforeResolution(t *testing.T) {
	for _, kind := range []string{"directory", "link", "dangling link"} {
		t.Run(kind, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "app_20260914_1.log")
			peer := source + ".gz"
			payload := []byte("keep original and link target")
			require.NoError(t, os.WriteFile(source, payload, 0o600))
			if kind == "directory" {
				require.NoError(t, os.Mkdir(peer, 0o700))
			} else {
				target := source
				if kind == "dangling link" {
					target = source + ".absent"
				}
				if err := os.Symlink(target, peer); err != nil {
					t.Skipf("symbolic links unavailable: %v", err)
				}
			}
			sourceGroups := fsstore.GroupFilesByCompressionSource(fsstore.InspectFilePaths([]string{source, peer}), ".log")
			group, exists := sourceGroups.Source(source)
			require.True(t, exists)
			require.Equal(t, []string{source, peer}, group.Paths)
			require.Len(t, group.RegularFiles, 1)
			require.Equal(t, source, group.RegularFiles[0].Path)
			path, found, err := sourceGroups.FindExistingCompressedFile(source)
			require.ErrorIs(t, err, fsstore.ErrNotRegularFile)
			var notRegular *fsstore.NotRegularFileError
			require.ErrorAs(t, err, &notRegular)
			require.Equal(t, peer, notRegular.Path)
			require.Equal(t, []string{peer}, compressionCandidateErrorPaths(err))
			require.Empty(t, path)
			require.False(t, found)
			got, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, payload, got)
			_, err = os.Lstat(peer)
			require.NoError(t, err)
		})
	}
}

func TestCompressionSourceGroupsKeepsInspectionError(t *testing.T) {
	// 権限設定に依存せず、OSのパス検査を失敗させます。
	source := filepath.Join(t.TempDir(), "app_20260914_\x00.log")
	sourceGroups := fsstore.GroupFilesByCompressionSource(fsstore.InspectFilePaths([]string{source + ".gz"}), ".log")
	_, found, err := sourceGroups.FindExistingCompressedFile(source)
	require.False(t, found)
	var pathErr *os.PathError
	require.ErrorAs(t, err, &pathErr)
	require.Equal(t, source+".gz", pathErr.Path)
}

func TestCompressionSourceGroupsDoesNotInspectFilesystemAgain(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "app_20260914_1.log")
	peer := source + ".gz"
	rejectedSource := filepath.Join(dir, "app_20260914_2.log")
	rejectedPeer := rejectedSource + ".zst"
	require.NoError(t, os.WriteFile(peer, []byte("observed archive"), 0o600))
	require.NoError(t, os.Mkdir(rejectedPeer, 0o700))
	files := fsstore.InspectFilePaths([]string{peer, rejectedPeer})
	// 分類前にファイルが変わっても、入力の検査結果を使います。
	// 分類時・検索時にディレクトリの再走査やファイルの再検査は行いません。
	require.NoError(t, os.Remove(peer))
	require.NoError(t, os.Remove(rejectedPeer))
	require.NoError(t, os.WriteFile(rejectedPeer, nil, 0o600))
	sourceGroups := fsstore.GroupFilesByCompressionSource(files, ".log")
	for range 2 {
		path, found, err := sourceGroups.FindExistingCompressedFile(source)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, peer, path)
		path, found, err = sourceGroups.FindExistingCompressedFile(rejectedSource)
		require.ErrorIs(t, err, fsstore.ErrNotRegularFile)
		require.Equal(t, []string{rejectedPeer}, compressionCandidateErrorPaths(err))
		require.Empty(t, path)
		require.False(t, found)
	}
}

// compressionCandidateErrorPathsは、errに含まれるCompressionCandidateErrorのpathを出現順に返します。
func compressionCandidateErrorPaths(err error) []string {
	var paths []string
	for _, candidate := range errortree.All[*fsstore.CompressionCandidateError](err) {
		paths = append(paths, candidate.Path)
	}
	return paths
}

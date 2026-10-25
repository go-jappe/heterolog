package appendlog

import (
	"os"
	"path/filepath"
	"testing"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/go-jappe/heterolog/internal/testutil"

	"github.com/stretchr/testify/require"
)

func TestRecoverCompressionSourceDeletePendingPathPreservesDifferentContent(t *testing.T) {
	for _, test := range []struct {
		name    string
		source  string
		pending string
	}{
		{name: "same size different bytes", source: "source log", pending: "unique log"},
		{name: "different size", source: "source log", pending: "only preserved in pending log"},
		{name: "empty source", source: "", pending: "must not lose this log"},
	} {
		t.Run(test.name, func(t *testing.T) {
			// 保全先の作成はリンクを拒否するため、macOSの/varなど一時ディレクトリの上位のリンクを解決します。
			directory := resolvedTempDirForTest(t)
			sourcePath := filepath.Join(directory, "app_20260910_1.log")
			pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
			compressedPath := sourcePath + ".gz"
			require.NoError(t, os.WriteFile(sourcePath, []byte(test.source), 0o600))
			require.NoError(t, os.WriteFile(pendingPath, []byte(test.pending), 0o600))
			require.NoError(t, os.WriteFile(compressedPath, []byte("published compressed file"), 0o600))

			durable, err := RecoverCompressionSourceDeletePendingPath(pendingPath)
			require.False(t, durable, "the original source still needs normal recovery")
			require.ErrorIs(t, err, ErrCompressionSourceDeletePendingConflict)
			preserved, globErr := filepath.Glob(compressionSourcePreservationPattern(sourcePath))
			require.NoError(t, globErr)
			require.Len(t, preserved, 1)
			var conflict *CompressionSourceDeletePendingConflictError
			require.ErrorAs(t, err, &conflict)
			require.True(t, conflict.ContentsDiffer)
			require.Equal(t, sourcePath, conflict.SourcePath)
			require.Equal(t, pendingPath, conflict.PendingPath)
			require.Equal(t, preserved[0], conflict.PreservedPath, "OnError must identify the preserved file")
			assertCompressionSourceContents(t, sourcePath, test.source)
			assertCompressionSourceContents(t, preserved[0], test.pending)
			assertCompressionSourceContents(t, compressedPath, "published compressed file")
			assertRetirementPathDoesNotExist(t, pendingPath)

			// 退避fileは通常の圧縮・upload・purgeにも、pending回復にも再投入しません。
			pattern := logpattern.FileNamePattern{Dir: directory, Target: "app", Extension: "log"}
			for _, mode := range []logpattern.GlobMode{logpattern.GlobByTarget, logpattern.GlobAllFiles} {
				files, scanErr := logpattern.GlobFileNames(pattern, mode)
				require.NoError(t, scanErr)
				require.ElementsMatch(t, []string{sourcePath, compressedPath}, files)
				pending, scanErr := logpattern.FindCompressionSourceDeletePendingPaths(pattern, mode)
				require.NoError(t, scanErr)
				require.Empty(t, pending)
			}
		})
	}
}

func TestRecoverCompressionSourceDeletePendingPathDoesNotOverwriteEarlierPreservation(t *testing.T) {
	sourcePath := filepath.Join(resolvedTempDirForTest(t), "app_20260910_1.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	require.NoError(t, os.WriteFile(sourcePath, []byte("canonical source"), 0o600))
	var preserved []string
	for _, contents := range []string{"first different log", "second different log"} {
		require.NoError(t, os.WriteFile(pendingPath, []byte(contents), 0o600))
		durable, err := RecoverCompressionSourceDeletePendingPath(pendingPath)
		require.False(t, durable)
		require.ErrorIs(t, err, ErrCompressionSourceDeletePendingConflict)
		preserved = append(preserved, contents)
	}
	paths, err := filepath.Glob(compressionSourcePreservationPattern(sourcePath))
	require.NoError(t, err)
	require.Len(t, paths, 2)
	var actual []string
	for _, path := range paths {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		actual = append(actual, string(content))
	}
	require.ElementsMatch(t, preserved, actual)
	assertCompressionSourceContents(t, sourcePath, "canonical source")
}

func TestRecoverCompressionSourceDeletePendingPathComparisonFailurePreservesBothPaths(t *testing.T) {
	for _, invalidSide := range []string{"source", "pending"} {
		for _, kind := range []string{"directory", "symlink"} {
			t.Run(invalidSide+" "+kind, func(t *testing.T) {
				directory := t.TempDir()
				sourcePath := filepath.Join(directory, "app_20260910_1.log")
				pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
				invalidPath, regularPath := sourcePath, pendingPath
				if invalidSide == "pending" {
					invalidPath, regularPath = pendingPath, sourcePath
				}
				require.NoError(t, os.WriteFile(regularPath, []byte("must remain"), 0o600))
				if kind == "directory" {
					require.NoError(t, os.Mkdir(invalidPath, 0o700))
					require.NoError(t, os.WriteFile(filepath.Join(invalidPath, "data"), []byte("must remain"), 0o600))
				} else {
					testutil.Symlink(t, regularPath, invalidPath)
				}

				durable, err := RecoverCompressionSourceDeletePendingPath(pendingPath)
				require.False(t, durable)
				// 通常fileでない入力は比較できず、内容の相違とは扱いません。
				var notRegular *infrafsstore.NotRegularFileError
				require.ErrorAs(t, err, &notRegular)
				require.Equal(t, invalidPath, notRegular.Path)
				assertCompressionSourceContents(t, regularPath, "must remain")
				_, statErr := os.Lstat(invalidPath)
				require.NoError(t, statErr)
				preserved, globErr := filepath.Glob(compressionSourcePreservationPattern(sourcePath))
				require.NoError(t, globErr)
				require.Empty(t, preserved, "unreadable inputs must not be treated as different content")
			})
		}
	}
}

func TestRecoverCompressionSourceDeletePendingPathRemovesIdenticalEmptyPending(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "app_20260910_1.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	require.NoError(t, os.WriteFile(sourcePath, nil, 0o600))
	require.NoError(t, os.WriteFile(pendingPath, nil, 0o600))
	durable, err := RecoverCompressionSourceDeletePendingPath(pendingPath)
	require.NoError(t, err)
	require.False(t, durable)
	assertCompressionSourceContents(t, sourcePath, "")
	assertRetirementPathDoesNotExist(t, pendingPath)
}

func TestPreserveCompressionSourceDeletePendingPathCleansReservationOnRenameFailure(t *testing.T) {
	directory := resolvedTempDirForTest(t)
	sourcePath := filepath.Join(directory, "app_20260910_1.log")
	require.NoError(t, os.WriteFile(sourcePath, []byte("canonical source"), 0o600))
	// rename元のpendingがない失敗を再現し、自分で予約した空fileだけを片付けることを確認します。
	preserved, err := preserveCompressionSourceDeletePendingPath(
		sourcePath,
		logpattern.CompressionSourceDeletePendingPath(sourcePath),
	)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Empty(t, preserved)
	entries, err := os.ReadDir(filepath.Join(directory, infrafsstore.PreservedDirectoryName))
	require.NoError(t, err)
	require.Empty(t, entries)
	assertCompressionSourceContents(t, sourcePath, "canonical source")
}

func TestCompressionSourceConflictKeepsBothFilesWhenPreservationDirectoryIsUnsafe(t *testing.T) {
	for _, kind := range []string{"file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			// 試験が置く.preservedだけが拒否の原因になるよう、一時ディレクトリの上位のリンクを解決します。
			directory := resolvedTempDirForTest(t)
			sourcePath := filepath.Join(directory, "app_20260910_1.log")
			pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
			require.NoError(t, os.WriteFile(sourcePath, []byte("source"), 0o600))
			require.NoError(t, os.WriteFile(pendingPath, []byte("different pending"), 0o600))
			preservedDirectory := filepath.Join(directory, infrafsstore.PreservedDirectoryName)
			if kind == "file" {
				require.NoError(t, os.WriteFile(preservedDirectory, []byte("blocked"), 0o600))
			} else {
				outside := t.TempDir()
				testutil.Symlink(t, outside, preservedDirectory)
			}
			durable, err := RecoverCompressionSourceDeletePendingPath(pendingPath)
			require.False(t, durable)
			require.ErrorIs(t, err, ErrCompressionSourceDeletePendingConflict)
			// 退避できなかったため、競合errorは退避先を示しません。
			var conflict *CompressionSourceDeletePendingConflictError
			require.ErrorAs(t, err, &conflict)
			require.Empty(t, conflict.PreservedPath)
			// 保全先の検査で、試験が置いた.preservedを拒否したことを確認します。
			var obstruction *infrafsstore.DirectoryObstructionError
			require.ErrorAs(t, err, &obstruction)
			require.Equal(t, preservedDirectory, obstruction.Path)
			assertCompressionSourceContents(t, sourcePath, "source")
			assertCompressionSourceContents(t, pendingPath, "different pending")
		})
	}
}

// resolvedTempDirForTestは、macOSの/varなど一時ディレクトリの上位のリンクを解決したパスを返します。
func resolvedTempDirForTest(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return directory
}

// cleanedTempDirForTestは、保存先のパスを正規化して扱う処理と比べられるよう、TMPDIRの
// 「//」などを残すt.TempDirを正規化したパスを返します。リンクは解決しません。
func cleanedTempDirForTest(t *testing.T) string {
	t.Helper()
	return filepath.Clean(t.TempDir())
}

func compressionSourcePreservationPattern(sourcePath string) string {
	return filepath.Join(filepath.Dir(sourcePath), infrafsstore.PreservedDirectoryName,
		filepath.Base(sourcePath)+compressionSourceConflictNamePattern)
}

func TestRemoveSourceAfterCompressionAlreadyRemovedUsesCommonSyncAndRemovalPath(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "app_20260910_1.log")
	pendingPath := logpattern.CompressionSourceDeletePendingPath(sourcePath)
	var operations []string
	err := removeSourceAfterCompression(
		sourcePath,
		sourcePath+".gz",
		func(string, string) error {
			t.Fatal("an absent source must not be renamed")
			return nil
		},
		func(path string) error {
			require.Equal(t, pendingPath, path)
			operations = append(operations, "remove")
			return &os.PathError{Op: "remove", Path: path, Err: os.ErrNotExist}
		},
		func(directoryPath string) error {
			require.Equal(t, filepath.Dir(pendingPath), directoryPath)
			operations = append(operations, "sync")
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"sync", "remove", "sync"}, operations)
}

func assertCompressionSourceContents(t *testing.T, path, contents string) {
	t.Helper()
	actual, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, contents, string(actual))
}

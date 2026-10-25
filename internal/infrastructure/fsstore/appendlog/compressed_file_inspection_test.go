package appendlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
)

func TestInspectCompressedFileReturnsOnlyExistingRegularFile(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260924_1.log")
	compressed := source + ".gz"
	require.NoError(t, os.WriteFile(source, []byte("source"), 0o600))

	info, err := InspectCompressedFile(source, compressed)
	require.NoError(t, err)
	require.Nil(t, info)
	info, err = InspectCompressedFile(source, source)
	require.NoError(t, err)
	require.Nil(t, info)

	// この段階は通常ファイルの存在確認だけで、圧縮内容の検証ではありません。
	require.NoError(t, os.WriteFile(compressed, []byte("not yet verified"), 0o600))
	info, err = InspectCompressedFile(source, compressed)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.True(t, info.Mode().IsRegular())
	require.EqualValues(t, len("not yet verified"), info.Size())
}

func TestInspectCompressedFileRejectsNonRegularPaths(t *testing.T) {
	for _, kind := range []string{"directory", "symlink", "dangling symlink"} {
		t.Run(kind, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "app_20260924_1.log")
			compressed := source + ".gz"
			target := filepath.Join(t.TempDir(), "other.gz")
			switch kind {
			case "directory":
				require.NoError(t, os.Mkdir(compressed, 0o700))
			case "symlink":
				require.NoError(t, os.WriteFile(target, []byte("other content"), 0o600))
				testutil.Symlink(t, target, compressed)
			case "dangling symlink":
				testutil.Symlink(t, target, compressed)
			}

			info, err := InspectCompressedFile(source, compressed)
			require.ErrorIs(t, err, ErrCompressedPeerNotRegular)
			require.Nil(t, info)
			_, err = os.Lstat(compressed)
			require.NoError(t, err, "inspection must not remove the rejected path")
		})
	}
}

func TestInspectCompressedFileReturnsInspectionFailure(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260924_1.log")
	info, err := InspectCompressedFile(source, source+"\x00.gz")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrCompressedPeerNotRegular)
	require.Nil(t, info)
}

func TestInspectEmptyCompressedFileRequiresEmptyOrRetiredSource(t *testing.T) {
	for _, kind := range []string{"nonempty", "empty", "retired", "directory", "symlink", "dangling symlink"} {
		t.Run(kind, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "app.log")
			peer := source + ".zst"
			require.NoError(t, os.WriteFile(peer, nil, 0o600))
			switch kind {
			case "nonempty":
				require.NoError(t, os.WriteFile(source, []byte("retained record\n"), 0o600))
			case "empty":
				require.NoError(t, os.WriteFile(source, nil, 0o600))
			case "directory":
				require.NoError(t, os.Mkdir(source, 0o700))
			case "symlink", "dangling symlink":
				target := filepath.Join(t.TempDir(), "target.log")
				if kind == "symlink" {
					require.NoError(t, os.WriteFile(target, nil, 0o600))
				}
				testutil.Symlink(t, target, source)
			}
			info, err := InspectCompressedFile(source, peer)
			switch kind {
			case "empty", "retired":
				require.NoError(t, err)
				require.NotNil(t, info)
			case "nonempty":
				require.ErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile)
				require.Nil(t, info)
			default:
				require.ErrorIs(t, err, fsstore.ErrNotRegularFile)
				require.NotErrorIs(t, err, pkgfsstore.ErrCorruptCompressedFile, "an uninspectable source does not prove the peer is corrupt")
				require.Nil(t, info)
			}
			require.FileExists(t, peer, "inspection must not delete either file")
			if kind != "retired" {
				_, err = os.Lstat(source)
				require.NoError(t, err)
			}
		})
	}
}

func TestRemoveInvalidCompressedFilePreservesSourceAndAllowsMissingPeer(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260924_1.log")
	compressed := source + ".gz"
	contents := []byte("original log retained for recompression")
	require.NoError(t, os.WriteFile(source, contents, 0o600))
	require.NoError(t, os.WriteFile(compressed, []byte("invalid gzip"), 0o600))

	require.NoError(t, RemoveInvalidCompressedFile(source, compressed))
	require.NoFileExists(t, compressed)
	require.NoError(t, RemoveInvalidCompressedFile(source, compressed))
	require.Error(t, RemoveInvalidCompressedFile(source, source))
	got, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, contents, got)
}

func TestRemoveInvalidCompressedFileReturnsRemovalFailure(t *testing.T) {
	source := filepath.Join(t.TempDir(), "app_20260924_1.log")
	err := RemoveInvalidCompressedFile(source, source+"\x00.gz")
	var pathErr *os.PathError
	require.ErrorAs(t, err, &pathErr)
	require.Equal(t, "remove", pathErr.Op)
	require.Equal(t, source+"\x00.gz", pathErr.Path)
}

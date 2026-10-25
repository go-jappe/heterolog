package fsstore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInspectFilePathsKeepsNamesMetadataAndErrorsSeparately(t *testing.T) {
	directory := t.TempDir()
	first := filepath.Join(directory, "first.log")
	second := filepath.Join(directory, "second.log.gz")
	nonRegular := filepath.Join(directory, "directory.log.gz")
	missing := filepath.Join(directory, "missing.log")
	require.NoError(t, os.WriteFile(first, []byte("first"), 0o600))
	require.NoError(t, os.WriteFile(second, []byte("second"), 0o600))
	require.NoError(t, os.Mkdir(nonRegular, 0o700))
	paths := []string{second, missing, nonRegular, first}
	observation := InspectFilePaths(paths)
	require.Equal(t, paths, observation.Paths)
	require.Len(t, observation.RegularFiles, 2)
	require.Equal(t, second, observation.RegularFiles[0].Path)
	require.Equal(t, uint64(6), observation.RegularFiles[0].Size)
	require.False(t, observation.RegularFiles[0].ModTime.IsZero())
	require.Equal(t, first, observation.RegularFiles[1].Path)
	require.Equal(t, uint64(5), observation.RegularFiles[1].Size)
	require.Len(t, observation.PathErrors, 2)
	require.ErrorIs(t, observation.PathErrors[missing], os.ErrNotExist)
	require.ErrorIs(t, observation.PathErrors[nonRegular], ErrNotRegularFile)
	paths[0] = "changed by caller"
	require.Equal(t, second, observation.Paths[0])
}

func TestInspectFilePathsDoesNotFollowLinks(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.log")
	require.NoError(t, os.WriteFile(target, []byte("keep"), 0o600))
	for _, test := range []struct{ name, target string }{
		{"link.log.gz", target},
		{"dangling.log.gz", filepath.Join(directory, "absent")},
	} {
		path := filepath.Join(directory, test.name)
		if err := os.Symlink(test.target, path); err != nil {
			t.Skipf("symbolic links unavailable: %v", err)
		}
		observation := InspectFilePaths([]string{path})
		require.Equal(t, []string{path}, observation.Paths)
		require.Empty(t, observation.RegularFiles)
		require.ErrorIs(t, observation.PathErrors[path], ErrNotRegularFile)
	}
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "keep", string(data))
}

func TestInspectFilePathsPreservesIOError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid\x00.log")
	observation := InspectFilePaths([]string{path})
	require.Equal(t, []string{path}, observation.Paths)
	require.Empty(t, observation.RegularFiles)
	var pathErr *os.PathError
	require.ErrorAs(t, observation.PathErrors[path], &pathErr)
	require.Equal(t, path, pathErr.Path)
}

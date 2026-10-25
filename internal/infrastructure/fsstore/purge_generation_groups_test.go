package fsstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// 同じkeyのファイルを一世代として数え、超過した古い世代のファイルをまとめて削除します。
// keyを返さないファイルは数えず、削除もしません。
func TestPurgeGenerationGroupsFromFileNameIfNeededRemovesWholeOldestGroups(t *testing.T) {
	dir := t.TempDir()
	write := func(name string) FileInfo {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(name), 0o600))
		return FileInfo{Path: path, Size: uint64(len(name))}
	}
	files := []FileInfo{
		write("app_20260829_10.log"),
		write("app_20260829_2.log.gz"),
		write("app_20260829_2.log"),
		write("ignored_20260801_1.txt"),
		write("app_20260828_7.log.zst"),
	}
	generationKey := func(file FileInfo) (string, bool) {
		name := filepath.Base(file.Path)
		if !strings.HasPrefix(name, "app_") {
			return "", false
		}
		stem, _, _ := strings.Cut(name, ".")
		return stem, true
	}

	notRun := PurgeGenerationGroupsFromFileNameIfNeeded(files, 3, generationKey)
	require.False(t, notRun.Ran, "three generations are within the limit")
	require.Equal(t, files, notRun.Remaining)

	result := PurgeGenerationGroupsFromFileNameIfNeeded(files, 1, generationKey)
	require.True(t, result.Ran)
	// 日付・数値indexの古い順に、20260828_7と20260829_2の全ファイルを削除します。
	removed := make([]string, 0, len(result.RemovalResults))
	for _, removal := range result.RemovalResults {
		require.NoError(t, removal.Err)
		removed = append(removed, filepath.Base(removal.File.Path))
	}
	require.Equal(t, []string{"app_20260828_7.log.zst", "app_20260829_2.log.gz", "app_20260829_2.log"}, removed)
	require.Equal(t, []FileInfo{files[0], files[3]}, result.Remaining)
	for _, file := range result.Remaining {
		require.FileExists(t, file.Path)
	}
}

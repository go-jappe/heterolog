package logpattern

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGlobLifecycleFileNamesCombinesManagedAndPendingNames(t *testing.T) {
	pattern := FileNamePattern{Dir: t.TempDir(), Prefix: "prefix_", Target: "App", Extension: "log"}
	for _, name := range []string{
		"prefix_App_20260914_1.log", "prefix_App_20260914_1.log.gz",
		"prefix_App_20260914_2.log.zst", "prefix_App_20260914_3.log.custom",
		"prefix_App_20260914_2.log" + CompressionSourceDeletePendingSuffix,
		"prefix_other_20260914_1.log", "prefix_other_20260914_1.log.gz",
		"prefix_other_20260914_2.log" + CompressionSourceDeletePendingSuffix,
		"prefix_app_20260914_5.LOG.GZ", "prefix_app_20260914_6.LOG" + CompressionSourceDeletePendingSuffix,
		"prefix_App_20260914_7.log.gz.tmp", "prefix_App_20260914_8.log.tmp",
		"prefix_App_20260914_9.jsonl" + CompressionSourceDeletePendingSuffix,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(pattern.Dir, name), nil, 0o600))
	}
	for _, mode := range []GlobMode{GlobAllFiles, GlobByTarget} {
		managed, err := GlobFileNames(pattern, mode)
		require.NoError(t, err)
		pending, err := FindCompressionSourceDeletePendingPaths(pattern, mode)
		require.NoError(t, err)
		got, err := GlobLifecycleFileNames(pattern, mode)
		require.NoError(t, err)
		require.Equal(t, append(managed, pending...), got)
		require.NotContains(t, got, filepath.Join(pattern.Dir, "prefix_App_20260914_7.log.gz.tmp"))
		require.NotContains(t, got, filepath.Join(pattern.Dir, "prefix_App_20260914_8.log.tmp"))
	}
}

func TestGlobLifecycleFileNamesKeepsNonRegularNames(t *testing.T) {
	pattern := FileNamePattern{Dir: t.TempDir(), Target: "app", Extension: "log"}
	directoryPath := filepath.Join(pattern.Dir, "app_20260914_98.log")
	require.NoError(t, os.Mkdir(directoryPath, 0o700))
	linkPath := filepath.Join(pattern.Dir, "app_20260914_99.log")
	if err := os.Symlink(filepath.Join(pattern.Dir, "absent"), linkPath); err != nil {
		t.Skipf("symbolic links unavailable: %v", err)
	}
	got, err := GlobLifecycleFileNames(pattern, GlobByTarget)
	require.NoError(t, err)
	require.Equal(t, []string{directoryPath, linkPath}, got)
}

func TestGlobLifecycleFileNamesReportsReadError(t *testing.T) {
	_, err := GlobLifecycleFileNames(FileNamePattern{Dir: "invalid\x00", Extension: "log"}, GlobAllFiles)
	require.Error(t, err)
	_, err = GlobLifecycleFileNames(FileNamePattern{}, GlobMode(-1))
	require.Error(t, err)
}

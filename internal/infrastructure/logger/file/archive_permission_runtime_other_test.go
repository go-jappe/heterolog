//go:build !windows

package file

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func denyArchiveRuntimeDirectoryAccess(t *testing.T, directory string, write bool) func() {
	t.Helper()
	info, err := os.Stat(directory)
	require.NoError(t, err)
	mode := os.FileMode(0o300)
	if write {
		mode = 0o500
	}
	restore := func() { require.NoError(t, os.Chmod(directory, info.Mode().Perm())) }
	t.Cleanup(restore)
	require.NoError(t, os.Chmod(directory, mode))
	if write {
		probe, probeErr := os.CreateTemp(directory, ".permission-probe-*")
		err = probeErr
		if probeErr == nil {
			require.NoError(t, probe.Close())
			require.NoError(t, os.Remove(probe.Name()))
		}
	} else {
		_, err = os.ReadDir(directory)
	}
	if err == nil {
		t.Skip("current user or filesystem bypasses Unix directory permissions")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("permission fixture failed with another error: %v", err)
	}
	return restore
}

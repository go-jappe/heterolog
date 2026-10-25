//go:build !windows

package compressedlogfailure

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func denyArchiveLifecycleDirectoryRead(t *testing.T, directory string) func() {
	t.Helper()
	info, err := os.Stat(directory)
	require.NoError(t, err)
	restore := func() { require.NoError(t, os.Chmod(directory, info.Mode().Perm())) }
	t.Cleanup(restore)
	require.NoError(t, os.Chmod(directory, 0o300))
	_, err = os.ReadDir(directory)
	if err == nil {
		t.Skip("current user or filesystem bypasses Unix directory read permissions")
	}
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("permission fixture failed with another error: %v", err)
	}
	return restore
}

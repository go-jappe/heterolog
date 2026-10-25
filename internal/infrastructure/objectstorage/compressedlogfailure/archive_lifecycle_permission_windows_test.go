package compressedlogfailure

import (
	"testing"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func denyArchiveLifecycleDirectoryRead(t *testing.T, directory string) func() {
	t.Helper()
	return testutil.DenyWindowsAccess(t, directory, windows.FILE_LIST_DIRECTORY)
}

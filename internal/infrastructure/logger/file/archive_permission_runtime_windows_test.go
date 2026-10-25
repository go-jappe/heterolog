package file

import (
	"testing"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func denyArchiveRuntimeDirectoryAccess(t *testing.T, directory string, write bool) func() {
	t.Helper()
	access := uint32(windows.FILE_LIST_DIRECTORY)
	if write {
		access = windows.FILE_WRITE_DATA // FILE_ADD_FILE for a directory.
	}
	return testutil.DenyWindowsAccess(t, directory, access)
}

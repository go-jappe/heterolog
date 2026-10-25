package file

import (
	"testing"

	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil"
)

// denyLogDirectoryOperationForTestは、directoryに対するoperationに必要なアクセス権を全員に拒否し、
// 試験の終了時に元のDACLへ戻します（testutil.DenyWindowsAccess）。拒否を確かめられない環境では
// skipします（requireLogDirectoryOperationDeniedForTest）。
func denyLogDirectoryOperationForTest(t *testing.T, directory string, operation logDirectoryOperationForTest) {
	t.Helper()
	// directoryに対するFILE_WRITE_DATAはFILE_ADD_FILE、FILE_APPEND_DATAはFILE_ADD_SUBDIRECTORYです。
	access := uint32(windows.FILE_WRITE_DATA)
	switch operation {
	case createSubdirectoryForTest:
		access = windows.FILE_APPEND_DATA
	case listEntriesForTest:
		access = windows.FILE_LIST_DIRECTORY
	}
	testutil.DenyWindowsAccess(t, directory, access)
	requireLogDirectoryOperationDeniedForTest(t, directory, operation)
}

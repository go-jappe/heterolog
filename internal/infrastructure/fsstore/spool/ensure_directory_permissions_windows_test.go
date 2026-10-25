package spool

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil"
)

func TestSpoolDirectoryPreparationKeepsExistingWindowsACLAndCreatesPrivateChildren(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "spool-root")
	// 以前の版が作った既存のrootは、親のACLを継承したままです。
	require.NoError(t, os.Mkdir(root, 0o755))
	before := windowsDACLForTest(t, root)
	partition := filepath.Join(root, "active", "2026")
	require.NoError(t, EnsureDirectoryNoSymlinks(root, partition))
	require.Equal(t, before, windowsDACLForTest(t, root), "existing spool root ACL must remain unchanged")
	for _, path := range []string{filepath.Join(root, "active"), partition} {
		testutil.RequirePrivatePermissions(t, path, directoryMode)
	}
	// spool fileの一時ファイルは、権限を設定する前から非公開のACLを継承します。
	file, err := os.CreateTemp(partition, "inherited-*.tmp")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	requireOnlyPrivateWindowsIdentities(t, file.Name())
}

func TestSpoolDirectoryPreparationCreatesMissingWindowsAncestorsPrivate(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	target := filepath.Join(base, "app")
	root := filepath.Join(target, DirectoryName, "opensearch")
	require.NoError(t, EnsureDirectoryNoSymlinks(root, filepath.Join(root, "active")))
	// Windowsでは以前と同じく、spool rootより上位で不足するdirectoryも非公開のACLで作成します。
	for _, path := range []string{target, filepath.Join(target, DirectoryName), root, filepath.Join(root, "active")} {
		testutil.RequirePrivatePermissions(t, path, directoryMode)
	}
}

func windowsDACLForTest(t *testing.T, path string) string {
	t.Helper()
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	return descriptor.String()
}

// requireOnlyPrivateWindowsIdentitiesは、継承したACEを含め、実行ユーザー・SYSTEM・
// Administrators以外へアクセスを許可していないことを確認します。
func requireOnlyPrivateWindowsIdentities(t *testing.T, path string) {
	t.Helper()
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	require.NoError(t, err)
	dacl, _, err := descriptor.DACL()
	require.NoError(t, err)
	require.NotNil(t, dacl, "DACL of %s is unrestricted", path)
	user, err := windows.GetCurrentThreadEffectiveToken().GetTokenUser()
	require.NoError(t, err)
	allowed := map[string]bool{user.User.Sid.String(): true, "S-1-5-18": true, "S-1-5-32-544": true}
	require.NotZero(t, dacl.AceCount, "DACL of %s grants no access", path)
	for index := uint32(0); index < uint32(dacl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		require.NoError(t, windows.GetAce(dacl, index, &ace))
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			t.Fatalf("unexpected ACE type %d at %d in %s", ace.Header.AceType, index, path)
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart)).String()
		require.True(t, allowed[sid], "ACE %d of %s grants access to %s", index, path, sid)
	}
	runtime.KeepAlive(descriptor)
}

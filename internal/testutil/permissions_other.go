//go:build !windows

package testutil

import (
	"os"
	"testing"
)

// RequirePrivatePermissionsは、指定パスの権限がunixModeと一致することを確認します。
func RequirePrivatePermissions(t testing.TB, path string, unixMode os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != unixMode {
		t.Fatalf("permissions of %q = %o, want %o", path, got, unixMode)
	}
}

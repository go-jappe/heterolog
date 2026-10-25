package fsstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/path/pathidentity"
	"golang.org/x/sys/windows"
)

func TestEnsureDirectoryRetriesSyncThroughWindowsPathAliases(t *testing.T) {
	for _, extended := range []bool{false, true} {
		name := "case"
		if extended {
			name = "extended"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			directory := filepath.Join(root, "state", "checkpoints")
			failure := errors.New("parent sync failed")
			if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(string) error { return failure }); !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
				t.Fatalf("initial ensure error = %v", err)
			}
			alias := strings.ToUpper(directory)
			if extended {
				alias = `\\?\` + alias
			}
			// 同じ実体を別表記で開いても、記録した親の同期を上位から順に再試行します。
			wantKeys := []string{pathidentity.ComparisonKey(root), pathidentity.ComparisonKey(filepath.Join(root, "state"))}
			retry := func(syncDirectory func(string) error) ([]string, error) {
				var keys []string
				err := ensureDirectoryWithPolicy(context.Background(), alias, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
					keys = append(keys, pathidentity.ComparisonKey(path))
					return syncDirectory(path)
				})
				return keys, err
			}
			keys, err := retry(func(string) error { return failure })
			if !errors.Is(err, failure) || !errors.Is(err, pkgfsstore.ErrDurabilityUnconfirmed) {
				t.Fatalf("alias retry while sync fails = %v, want recorded parent sync failure", err)
			}
			if !reflect.DeepEqual(keys, wantKeys) {
				t.Fatalf("alias retried parent syncs = %v, want %v", keys, wantKeys)
			}
			if keys, err = retry(SyncDirectory); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(keys, wantKeys) {
				t.Fatalf("alias recovered parent syncs = %v, want %v", keys, wantKeys)
			}
			// 同期できた記録は元の表記でも解除済みです。
			if err := ensureDirectoryWithPolicy(context.Background(), directory, 0o700, directorySetupPolicy{setDestinationPermissions: true}, func(path string) error {
				t.Errorf("recovered parent sync was retried through original path: %q", path)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEnsureDirectoryPreservesExistingWindowsACL(t *testing.T) {
	for _, policy := range []struct {
		name   string
		ensure func(string, os.FileMode) error
	}{
		{name: "normal", ensure: EnsureDirectory},
		{name: "no symlinks", ensure: EnsureDirectoryNoSymlinks},
		// 管理範囲内でも、Windowsでは既存ACLを変更しません。
		{name: "managed", ensure: func(directory string, mode os.FileMode) error {
			return EnsureManagedDirectoryNoSymlinks(directory, directory, mode, 0o755)
		}},
	} {
		t.Run(policy.name, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "state")
			if err := policy.ensure(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			testutil.RequirePrivatePermissions(t, directory, 0o700)
			// 既存ディレクトリには、ファイル作成権限があってもACL変更権限があるとは限りません。
			testutil.DenyWindowsAccess(t, directory, windows.WRITE_DAC)
			before := privateTestSecurity(t, directory).String()
			for _, path := range []string{directory, strings.ToUpper(directory), `\\?\` + strings.ToUpper(directory)} {
				for attempt := 0; attempt < 2; attempt++ {
					if err := policy.ensure(path, 0o750); err != nil {
						t.Fatalf("existing directory initialization %d for %q: %v", attempt, path, err)
					}
					if got := privateTestSecurity(t, directory).String(); got != before {
						t.Fatalf("existing directory ACL changed: before %s, after %s", before, got)
					}
				}
			}
			child := filepath.Join(directory, "child")
			if err := policy.ensure(child, 0o700); err != nil {
				t.Fatalf("create child under parent without WRITE_DAC: %v", err)
			}
			testutil.RequirePrivatePermissions(t, child, 0o700)
			if got := privateTestSecurity(t, directory).String(); got != before {
				t.Fatalf("child creation changed parent ACL: before %s, after %s", before, got)
			}
			if err := os.WriteFile(filepath.Join(child, "payload"), []byte("writable"), 0o600); err != nil {
				t.Fatalf("write in newly created directory: %v", err)
			}
		})
	}
}

func TestEnsureDirectoryWindowsPermissionFailureDoesNotPoisonLaterCalls(t *testing.T) {
	for _, policy := range []struct {
		name   string
		ensure func(string, os.FileMode) error
	}{
		{name: "normal", ensure: EnsureDirectory},
		{name: "no symlinks", ensure: EnsureDirectoryNoSymlinks},
		// 管理範囲内でも、Windowsでは既存ACLを変更しません。
		{name: "managed", ensure: func(directory string, mode os.FileMode) error {
			return EnsureManagedDirectoryNoSymlinks(directory, directory, mode, 0o755)
		}},
	} {
		t.Run(policy.name, func(t *testing.T) {
			root := t.TempDir()
			parent := filepath.Join(root, "parent")
			if err := policy.ensure(parent, 0o700); err != nil {
				t.Fatal(err)
			}
			// FILE_APPEND_DATAは、ディレクトリでは子ディレクトリの作成権限を表します。
			restore := testutil.DenyWindowsAccess(t, parent, windows.FILE_APPEND_DATA)
			before := privateTestSecurity(t, parent).String()
			directory := filepath.Join(parent, "child")
			if err := policy.ensure(directory, 0o700); !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
				t.Fatalf("creation error = %v, want access denied", err)
			}
			if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("denied directory was created: %v", err)
			}
			if err := policy.ensure(filepath.Join(root, "unrelated"), 0o700); err != nil {
				t.Fatalf("unrelated creation after permission error: %v", err)
			}
			if err := policy.ensure(parent, 0o700); err != nil {
				t.Fatalf("existing directory initialization after permission error: %v", err)
			}
			if got := privateTestSecurity(t, parent).String(); got != before {
				t.Fatalf("failed creation changed parent ACL: before %s, after %s", before, got)
			}
			restore()
			if err := policy.ensure(directory, 0o700); err != nil {
				t.Fatalf("creation after access restored: %v", err)
			}
			testutil.RequirePrivatePermissions(t, directory, 0o700)
		})
	}
}

func TestEnsureDirectoryDoesNotRequireExistingWindowsACLReadAccess(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "state")
	if err := EnsureDirectory(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	testutil.DenyWindowsAccess(t, directory, windows.READ_CONTROL|windows.WRITE_DAC)
	if _, err := windows.GetNamedSecurityInfo(directory, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION); !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		t.Fatalf("ACL read error = %v, want access denied", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := EnsureDirectory(directory, 0o700); err != nil {
			t.Fatalf("normal initialization %d without ACL read access: %v", attempt, err)
		}
		if err := EnsureDirectoryNoSymlinks(directory, 0o700); err != nil {
			t.Fatalf("strict initialization %d without ACL read access: %v", attempt, err)
		}
		if err := EnsureManagedDirectoryNoSymlinks(directory, directory, 0o700, 0o755); err != nil {
			t.Fatalf("managed initialization %d without ACL read access: %v", attempt, err)
		}
	}
}

func TestDirectorySetupPoliciesPreserveWindowsSymlinkRejection(t *testing.T) {
	root := t.TempDir()
	realDirectory := filepath.Join(root, "real")
	linkedDirectory := filepath.Join(root, "linked")
	if err := EnsureDirectory(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	testutil.Symlink(t, realDirectory, linkedDirectory)
	before := privateTestSecurity(t, realDirectory).String()
	if err := EnsureDirectory(linkedDirectory, 0o700); err == nil {
		t.Fatal("normal helper accepted a symlink as the destination directory")
	}
	if err := EnsureDirectoryNoSymlinks(linkedDirectory, 0o700); err == nil {
		t.Fatal("strict helper accepted a symlink as the destination directory")
	}
	child := filepath.Join(linkedDirectory, "child")
	if err := EnsureDirectoryNoSymlinks(child, 0o700); err == nil {
		t.Fatal("strict helper accepted an ancestor symlink")
	}
	if _, err := os.Stat(filepath.Join(realDirectory, "child")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("strict helper created a child through an ancestor symlink: %v", err)
	}
	if err := EnsureDirectory(child, 0o700); err != nil {
		t.Fatalf("normal helper rejected its existing ancestor-symlink policy: %v", err)
	}
	if got := privateTestSecurity(t, realDirectory).String(); got != before {
		t.Fatalf("directory checks changed symlink target ACL: before %s, after %s", before, got)
	}
}

package fsstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestDirectorySetupFilesystemRequiresPersistentACLsOnly(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		filesystem string
		flags      uint32
		allowed    bool
	}{
		{name: "ACL without hardlinks", filesystem: "ReFS", flags: windows.FILE_PERSISTENT_ACLS, allowed: true},
		{name: "ACL with hardlinks", filesystem: "NTFS", flags: windows.FILE_PERSISTENT_ACLS | windows.FILE_SUPPORTS_HARD_LINKS, allowed: true},
		{name: "unknown name with ACL", filesystem: "custom filesystem", flags: windows.FILE_PERSISTENT_ACLS, allowed: true},
		{name: "no ACL", filesystem: "exFAT"},
		{name: "hardlinks without ACL", filesystem: "custom filesystem", flags: windows.FILE_SUPPORTS_HARD_LINKS},
		{name: "name does not imply capability", filesystem: "NTFS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := `C:\application\logs`
			calls := 0
			err := validateDirectorySetupFilesystemWithQuery(directory, func(path string) (string, uint32, error) {
				calls++
				if path != directory {
					t.Fatalf("queried path = %q, want %q", path, directory)
				}
				return test.filesystem, test.flags, nil
			})
			if calls != 1 {
				t.Fatalf("volume query calls = %d, want 1", calls)
			}
			if test.allowed {
				if err != nil {
					t.Fatalf("ACL-capable filesystem rejected: %v", err)
				}
				return
			}
			if !errors.Is(err, errors.ErrUnsupported) {
				t.Fatalf("filesystem error = %v, want unsupported", err)
			}
			var filesystemErr *DirectoryFilesystemError
			if !errors.As(err, &filesystemErr) || !filesystemErr.Unsupported ||
				filesystemErr.Directory != directory || filesystemErr.Filesystem != test.filesystem {
				t.Fatalf("filesystem error = %v, want unsupported %q on %q", err, test.filesystem, directory)
			}
		})
	}
}

func TestDirectorySetupFilesystemPreservesQueryFailure(t *testing.T) {
	t.Parallel()
	failure := errors.New("volume query failed")
	directory := `C:\application\logs`
	err := validateDirectorySetupFilesystemWithQuery(directory, func(string) (string, uint32, error) {
		// Even apparently acceptable flags cannot hide a failed volume query.
		return "NTFS", windows.FILE_PERSISTENT_ACLS, failure
	})
	if !errors.Is(err, failure) || errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("query error = %v, want original failure only", err)
	}
	var filesystemErr *DirectoryFilesystemError
	if !errors.As(err, &filesystemErr) || filesystemErr.Unsupported || filesystemErr.Directory != directory {
		t.Fatalf("query error = %v, want query failure for %q", err, directory)
	}
}

func TestDirectorySetupFilesystemInspectsNativeDirectoryBeforeCreatingChildren(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	filesystem, flags, err := inspectDirectorySetupFilesystem(root)
	if err != nil {
		t.Fatalf("native directory filesystem inspection: %v", err)
	}
	for _, ensure := range []struct {
		name string
		run  func(string, os.FileMode) error
	}{
		{name: "normal", run: EnsureDirectory},
		{name: "no symlinks", run: EnsureDirectoryNoSymlinks},
	} {
		t.Run(ensure.name, func(t *testing.T) {
			first := filepath.Join(root, ensure.name)
			directory := filepath.Join(first, "nested", "logs")
			err := ensure.run(directory, 0o700)
			if flags&windows.FILE_PERSISTENT_ACLS == 0 {
				if !errors.Is(err, errors.ErrUnsupported) {
					t.Fatalf("filesystem %q without ACL accepted: %v", filesystem, err)
				}
				if _, err := os.Lstat(first); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("unsupported filesystem created first directory: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("initialize on filesystem %q: %v", filesystem, err)
			}
			if err := ensure.run(directory, 0o700); err != nil {
				t.Fatalf("inspect existing destination: %v", err)
			}
			if err := os.Remove(directory); err != nil {
				t.Fatalf("filesystem inspection retained an open directory handle: %v", err)
			}
		})
	}
}

// FSSTORE_NO_ACL_TEST_ROOTには、ACLを保持しない検証用volumeの既存directoryを指定します。
// 操作対象はその下にこの試験が作る一時directoryだけで、volumeの準備・formatは行いません。
func TestDirectorySetupRejectsNativeFilesystemWithoutACLs(t *testing.T) {
	root := os.Getenv("FSSTORE_NO_ACL_TEST_ROOT")
	if root == "" {
		t.Skip("FSSTORE_NO_ACL_TEST_ROOT is not set; a native filesystem without persistent ACLs is required")
	}
	filesystem, flags, err := inspectDirectorySetupFilesystem(root)
	if err != nil {
		t.Fatalf("inspect supplied filesystem: %v", err)
	}
	if flags&windows.FILE_PERSISTENT_ACLS != 0 {
		t.Fatalf("supplied filesystem %q supports persistent ACLs; a filesystem without ACL support is required", filesystem)
	}
	directory, err := os.MkdirTemp(root, "directory-setup-no-acl-")
	if err != nil {
		t.Fatalf("create isolated test directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("remove isolated test directory: %v", err)
		}
	})
	for _, ensure := range []struct {
		name string
		run  func(string, os.FileMode) error
	}{
		{name: "normal", run: EnsureDirectory},
		{name: "no symlinks", run: EnsureDirectoryNoSymlinks},
	} {
		t.Run(ensure.name, func(t *testing.T) {
			if err := ensure.run(directory, 0o700); !errors.Is(err, errors.ErrUnsupported) {
				t.Fatalf("existing directory on filesystem %q: %v, want unsupported", filesystem, err)
			}
			first := filepath.Join(directory, "must-not-be-created")
			if err := ensure.run(filepath.Join(first, "child"), 0o700); !errors.Is(err, errors.ErrUnsupported) {
				t.Fatalf("new directory on filesystem %q: %v, want unsupported", filesystem, err)
			}
			if _, err := os.Lstat(first); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsupported filesystem created a directory: %v", err)
			}
		})
	}
}

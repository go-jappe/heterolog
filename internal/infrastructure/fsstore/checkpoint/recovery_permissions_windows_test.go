//go:build windows

package checkpoint

import (
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"

	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
)

func TestFilesystemStoreRecoveryDoesNotReplaceUnreadableCheckpoint(t *testing.T) {
	for _, target := range []string{"successes", "registrations"} {
		t.Run(target, func(t *testing.T) {
			store, err := NewFilesystemStore(t.TempDir(), "source")
			require.NoError(t, err)
			require.NoError(t, boundedwait.Close(t, "store.Close()", store.Close, boundedwait.ForceCloserOf(store)))
			path := store.documentPath()
			if target == "registrations" {
				path = store.registrationsPath()
			}
			restore := testutil.DenyWindowsAccess(t, path, windows.FILE_READ_DATA|windows.WRITE_DAC)
			defer restore()
			restarted, err := NewFilesystemStore(store.directory, "source")
			require.ErrorIs(t, err, fs.ErrPermission)
			require.Nil(t, restarted)
		})
	}
}

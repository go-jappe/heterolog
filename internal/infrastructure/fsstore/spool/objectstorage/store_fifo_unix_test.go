//go:build unix

package objectstoragespool

import (
	"testing"

	"golang.org/x/sys/unix"
)

func createTestFIFO(t *testing.T, path string) {
	t.Helper()
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Skipf("FIFO unsupported: %v", err)
	}
}

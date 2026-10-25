//go:build !unix

package objectstoragespool

import "testing"

func createTestFIFO(t *testing.T, _ string) {
	t.Helper()
	t.Skip("this non-regular file test requires a Unix filesystem FIFO")
}

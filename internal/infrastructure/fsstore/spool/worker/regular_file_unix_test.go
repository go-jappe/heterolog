//go:build unix

package worker

import (
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

func TestProcessingReadsRejectFIFO(t *testing.T) {
	t.Parallel()
	store, path := newProcessingReadFixture(t)
	preserve := createSpoolFIFO(t, path)
	assertProcessingReadRejected(t, store, path)
	preserve()
}

func TestSpoolWorkerSkipsFIFO(t *testing.T) {
	t.Parallel()
	testSpoolWorkerSkipsEntry(t, createSpoolFIFO)
}

func createSpoolFIFO(t *testing.T, path string) func() {
	t.Helper()
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("create FIFO fixture: %v", err)
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return func() {
		t.Helper()
		after, err := os.Lstat(path)
		if err != nil || !os.SameFile(before, after) || after.Mode()&os.ModeNamedPipe == 0 {
			t.Fatalf("FIFO changed: %v, %v", after, err)
		}
	}
}

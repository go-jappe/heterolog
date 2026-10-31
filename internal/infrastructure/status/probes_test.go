package status

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	appstatus "github.com/go-jappe/heterolog/internal/application/status"
)

func TestScanDirectoryCountsRegularFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "2026", "10"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, size := range map[string]int{"a.hlogspool": 10, "2026/10/b.hlogspool": 20} {
		if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	s := scanner{}
	got, err := s.ScanDirectory(context.Background(), dir)
	if want := (appstatus.Usage{Files: 2, Bytes: 30}); err != nil || got != want {
		t.Errorf("ScanDirectory = %+v, %v; want 2 files of 30 bytes", got, err)
	}
	// ないdirectoryは空です。
	var empty appstatus.Usage
	if got, err := s.ScanDirectory(context.Background(), filepath.Join(dir, "missing")); err != nil || got != empty {
		t.Errorf("ScanDirectory(missing) = %+v, %v; want empty", got, err)
	}
	// 親がまだないpathのdiskの空きは、ある親で読みます。
	if free, total, err := s.DiskSpace(filepath.Join(dir, "missing", "deeper")); err != nil || total == 0 || free > total {
		t.Errorf("DiskSpace = %d/%d, %v; want the file system of the parent", free, total, err)
	}
}

func TestCPUTimesReturnTheTimeUsedSoFar(t *testing.T) {
	user, system, err := processProbe{}.CPUTimes()
	if err != nil {
		t.Skipf("CPU times are not available: %v", err)
	}
	if user < 0 || system < 0 || user+system > time.Hour {
		t.Errorf("CPUTimes = %v, %v; want small non-negative durations", user, system)
	}
}

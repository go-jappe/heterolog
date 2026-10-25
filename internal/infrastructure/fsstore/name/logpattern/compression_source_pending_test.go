package logpattern

import "testing"

func TestCompressionSourcePathFromDeletePendingPathRejectsEmptySource(t *testing.T) {
	t.Parallel()

	for _, pendingPath := range []string{"", CompressionSourceDeletePendingSuffix} {
		if sourcePath, ok := CompressionSourcePathFromDeletePendingPath(pendingPath); ok || sourcePath != "" {
			t.Errorf("CompressionSourcePathFromDeletePendingPath(%q) = (%q, %t), want (empty, false)", pendingPath, sourcePath, ok)
		}
	}
}

func TestCompressionSourceDeletePendingPathRoundTrip(t *testing.T) {
	t.Parallel()

	sourcePath := "/var/log/app_20260831_1.log"
	pendingPath := CompressionSourceDeletePendingPath(sourcePath)
	if !IsCompressionSourceDeletePendingPath(pendingPath) {
		t.Fatalf("IsCompressionSourceDeletePendingPath(%q) = false, want true", pendingPath)
	}

	got, ok := CompressionSourcePathFromDeletePendingPath(pendingPath)
	if !ok || got != sourcePath {
		t.Fatalf(
			"CompressionSourcePathFromDeletePendingPath(%q) = (%q, %t), want (%q, true)",
			pendingPath,
			got,
			ok,
			sourcePath,
		)
	}
	if got, ok := CompressionSourcePathFromDeletePendingPath(sourcePath); ok || got != "" {
		t.Fatalf(
			"CompressionSourcePathFromDeletePendingPath(non-pending) = (%q, %t), want (empty, false)",
			got,
			ok,
		)
	}
}

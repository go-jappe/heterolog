package checkpoint

import (
	"path/filepath"
	"testing"

	"github.com/go-jappe/heterolog/internal/infrastructure/fsstore/name/logpattern"
	"github.com/stretchr/testify/require"
)

func TestArchiveCheckpointGenerationKeysUsesSeriesAndGenerationNotCompressionSuffix(t *testing.T) {
	t.Parallel()
	pattern := logpattern.FileNamePattern{Dir: t.TempDir(), Prefix: "error_", Target: "app", Extension: "log"}
	paths := []string{
		filepath.Join(pattern.Dir, "error_app_20260915_2.log.gz"),
		filepath.Join(pattern.Dir, "error_app_20260915_2.log"),
		filepath.Join(pattern.Dir, "error_app_20260915_2.log.zst"),
		filepath.Join(pattern.Dir, "error_app_20260914_1.log"),
		filepath.Join(pattern.Dir, "error_app_20260914_1.log.gz.tmp"),
		logpattern.CompressionSourceDeletePendingPath(filepath.Join(pattern.Dir, "error_app_20260913_3.log")),
		filepath.Join(pattern.Dir, "debug_app_20260915_4.log.gz"),
		filepath.Join(pattern.Dir, "error_app_20260915_5.jsonl.gz"),
		filepath.Join(t.TempDir(), "error_app_20260915_6.log.gz"),
	}
	require.Equal(t, []string{"20260915-2", "20260914-1"}, ArchiveCheckpointGenerationKeys(paths, pattern))
}

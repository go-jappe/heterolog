package lifecycle

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	lifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	"github.com/stretchr/testify/require"
)

func TestTerminalRetentionPreservesPendingProcessingEvidence(t *testing.T) {
	for _, state := range []string{"done", "dead", "quarantine"} {
		for _, mode := range []string{"age", "size"} {
			t.Run(state+"/"+mode, func(t *testing.T) {
				root := t.TempDir()
				quarantineDir := filepath.Join(root, state)
				rel := filepath.Join("2026", "10", "01", "00", "00", "00001_pending.hlogspool")
				processingPath := filepath.Join(root, "processing", rel)
				protected := filepath.Join(quarantineDir, rel)
				unprotected := filepath.Join(quarantineDir, filepath.Dir(rel), "00002_complete.hlogspool")
				for _, path := range []string{processingPath, protected, unprotected} {
					require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
					require.NoError(t, os.WriteFile(path, []byte("keep evidence: "+path), 0o600))
				}
				before, err := os.ReadFile(protected)
				require.NoError(t, err)
				purge := func() lifecyclereport.PurgeReport {
					t.Helper()
					operator := &FileOperator{}
					if mode == "age" {
						report, err := operator.purgeByAgeUsingDayDirs(context.Background(), quarantineDir,
							time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC), 1)
						require.NoError(t, err)
						return report
					}
					report, _, err := operator.purgeBySizeOldestFirst(context.Background(), quarantineDir, 1)
					require.NoError(t, err)
					return report
				}
				report := purge()
				require.Equal(t, 1, report.DeletedFileCount())
				require.NoFileExists(t, unprotected, "ordinary terminal retention must still work")
				actual, err := os.ReadFile(protected)
				require.NoError(t, err)
				require.Equal(t, before, actual)
				require.FileExists(t, processingPath)
				// Once recovery has removed the source, the saved result follows
				// ordinary retention again rather than leaking forever.
				require.NoError(t, os.Remove(processingPath))
				require.Equal(t, 1, purge().DeletedFileCount())
				require.NoFileExists(t, protected)
			})
		}
	}
}

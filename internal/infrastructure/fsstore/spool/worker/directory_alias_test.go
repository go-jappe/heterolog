package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

func TestSpoolWorkerRejectsDestinationBucketAliases(t *testing.T) {
	for _, operation := range []string{"move", "quarantine", "write-records"} {
		t.Run(operation, func(t *testing.T) {
			base, outside := t.TempDir(), t.TempDir()
			root := filepath.Join(base, "test-app", "spool", "opensearch")
			rel := filepath.Join("2026", "09", "13", "12", "00", "00000_preserve"+spooldir.SpoolExt)
			source := filepath.Join(root, "processing", rel)
			require.NoError(t, os.MkdirAll(filepath.Dir(source), 0o755))
			require.NoError(t, os.WriteFile(source, []byte(regularSpoolBody), 0o644))
			state := pkgspool.StateDone
			if operation == "quarantine" {
				state = pkgspool.StateQuarantine
			}
			destination := filepath.Join(root, string(state))
			require.NoError(t, os.MkdirAll(destination, 0o755))
			testutil.Symlink(t, outside, filepath.Join(destination, "2026"))
			store, err := NewStateStore(processingTestConfig{spoolPath: base})
			require.NoError(t, err)

			switch operation {
			case "move":
				_, err = store.moveToState(context.Background(), source, pkgspool.StateProcessing, state)
			case "quarantine":
				err = store.MoveProcessingToQuarantine(context.Background(), source, pkgspool.QuarantineReasonInvalidSpoolFile)
			case "write-records":
				err = writeSpoolFileAtPath(store.RootDir.String(), filepath.Join(destination, rel), []byte(regularSpoolBody))
			}
			require.Error(t, err)
			body, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, regularSpoolBody, string(body))
			entries, err := os.ReadDir(outside)
			require.NoError(t, err)
			require.Empty(t, entries, "destination aliases must not receive files or new bucket directories")
		})
	}
}

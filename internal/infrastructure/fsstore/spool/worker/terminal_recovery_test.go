package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	infrafsstore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestReconcileProcessingTerminalRequiresOneCompleteMatchingOutcome(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		for _, mode := range []string{"complete", "partial", "different payload", "not finalized", "older attempt", "mixed results", "duplicate terminal result", "malformed peer"} {
			t.Run(string(state)+"/"+mode, func(t *testing.T) {
				store, source, destination, original, saved := terminalRecoveryFixture(t, state)
				originalBody := writeRecoveryRecords(t, source, original)
				peer := pkgspool.StateDead
				if state == peer {
					peer = pkgspool.StateDone
				}
				rel, err := filepath.Rel(store.RootDir.ProcessingDir().Path(), source)
				require.NoError(t, err)
				peerPath := filepath.Join(store.RootDir.String(), string(peer), rel)
				switch mode {
				case "partial":
					saved = saved[:1]
				case "different payload":
					saved[1].Payload = json.RawMessage(`{"message":"different"}`)
				case "not finalized":
					saved[1].FinalizedAt = time.Time{}
				case "older attempt":
					saved[1].ResendAttemptCount = 0
				case "mixed results":
					writeRecoveryRecords(t, peerPath, saved[1:])
					saved = saved[:1]
				case "duplicate terminal result":
					writeRecoveryRecords(t, peerPath, saved)
				case "malformed peer":
					require.NoError(t, os.MkdirAll(filepath.Dir(peerPath), 0o700))
					require.NoError(t, os.WriteFile(peerPath, []byte("invalid saved outcome"), 0o600))
				}
				savedBody := writeRecoveryRecords(t, destination, saved)
				recovered, err := store.ReconcileProcessingTerminal(context.Background(), source, original)
				if mode == "complete" {
					require.NoError(t, err)
					require.Equal(t, state, recovered)
					require.NoFileExists(t, source)
				} else {
					require.ErrorIs(t, err, ErrTerminalRecoveryConflict)
					actual, err := os.ReadFile(source)
					require.NoError(t, err)
					require.Equal(t, originalBody, actual)
				}
				actual, err := os.ReadFile(destination)
				require.NoError(t, err)
				require.Equal(t, savedBody, actual)
			})
		}
	}
}

func TestReconcileProcessingTerminalConfirmsSavedDirectoryBeforeRemovingSource(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			store, source, destination, original, saved := terminalRecoveryFixture(t, state)
			originalBody := writeRecoveryRecords(t, source, original)
			savedBody := writeRecoveryRecords(t, destination, saved)
			restore := testutil.DenyDirectorySync(t, filepath.Dir(destination))
			require.ErrorIs(t, infrafsstore.SyncDirectory(filepath.Dir(destination)), pkgfsstore.ErrDurabilityUnconfirmed)
			_, err := store.ReconcileProcessingTerminal(context.Background(), source, original)
			restore()
			require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			actual, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, originalBody, actual)
			actual, err = os.ReadFile(destination)
			require.NoError(t, err)
			require.Equal(t, savedBody, actual)
			// A retry uses the saved decision rather than replacing its contents.
			recovered, err := store.ReconcileProcessingTerminal(context.Background(), source, original)
			require.NoError(t, err)
			require.Equal(t, state, recovered)
			require.NoFileExists(t, source)
			actual, err = os.ReadFile(destination)
			require.NoError(t, err)
			require.Equal(t, savedBody, actual)
		})
	}
}

func terminalRecoveryFixture(t *testing.T, state pkgspool.State) (*StateStore, string, string, []pkgspool.SpoolRecord, []pkgspool.SpoolRecord) {
	t.Helper()
	store, source, _, original, saved := quarantineRecoveryFixture(t)
	rel, err := filepath.Rel(store.RootDir.ProcessingDir().Path(), source)
	require.NoError(t, err)
	return store, source, filepath.Join(store.RootDir.String(), string(state), rel), original, saved
}

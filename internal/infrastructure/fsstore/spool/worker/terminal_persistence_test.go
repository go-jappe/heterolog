package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestTerminalSaveRetriesPublishedFileAfterDirectorySyncFailure(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			store, source, _, original, records := quarantineRecoveryFixture(t)
			originalBody := writeRecoveryRecords(t, source, original)
			rel, err := filepath.Rel(store.RootDir.ProcessingDir().Path(), source)
			require.NoError(t, err)
			destination := filepath.Join(store.RootDir.String(), string(state), rel)
			require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o700))
			restore := testutil.DenyDirectorySync(t, filepath.Dir(destination))
			for range 2 {
				err := store.StoreProcessingRecordsToState(context.Background(), source, state, records)
				require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			}
			restore()
			published, err := os.ReadFile(destination)
			require.NoError(t, err)
			before, err := os.Stat(destination)
			require.NoError(t, err)
			decoded, err := decodeSpoolRecords(published)
			require.NoError(t, err)
			require.Equal(t, records, decoded, "keep the worker's original finalization time")
			// A reconstructed store can retry the same save even if it generates a
			// different finalization time; the first published result is retained.
			retried := append([]pkgspool.SpoolRecord(nil), records...)
			for i := range retried {
				retried[i].FinalizedAt = time.Time{}
			}
			recreated, err := NewStateStore(processingTestConfig{spoolPath: store.Config.SpoolPath()})
			require.NoError(t, err)
			require.NoError(t, recreated.StoreProcessingRecordsToState(context.Background(), source, state, retried))
			after, err := os.Stat(destination)
			require.NoError(t, err)
			require.True(t, os.SameFile(before, after), "reuse the published file without replacing it")
			actual, err := os.ReadFile(destination)
			require.NoError(t, err)
			require.Equal(t, published, actual)
			actual, err = os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, originalBody, actual)
			entries, err := os.ReadDir(filepath.Dir(destination))
			require.NoError(t, err)
			require.Len(t, entries, 1, "retry must leave no temporary copies")
		})
	}
}

func TestTerminalSavePublishesPrivateSpoolFile(t *testing.T) {
	for _, state := range []pkgspool.State{pkgspool.StateDone, pkgspool.StateDead, pkgspool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			store, source, _, original, records := quarantineRecoveryFixture(t)
			writeRecoveryRecords(t, source, original)
			rel, err := filepath.Rel(store.RootDir.ProcessingDir().Path(), source)
			require.NoError(t, err)
			destination := filepath.Join(store.RootDir.String(), string(state), rel)
			require.NoError(t, store.StoreProcessingRecordsToState(context.Background(), source, state, records))
			// Unix requires the owner-only 0600 mode. Windows checks the actual DACL,
			// not the numeric mode: only the current user, SYSTEM and Administrators.
			testutil.RequirePrivatePermissions(t, destination, 0o600)
			published, err := os.ReadFile(destination)
			require.NoError(t, err)
			decoded, err := decodeSpoolRecords(published)
			require.NoError(t, err)
			require.Equal(t, records, decoded)
		})
	}
}

func TestTerminalSavePreservesConflictingPublishedRecords(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func([]pkgspool.SpoolRecord) []pkgspool.SpoolRecord
	}{
		{"different payload", func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord {
			r[0].Payload = json.RawMessage(`{"message":"other"}`)
			return r
		}},
		{"different identity", func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[0].RequestID = "other"; return r }},
		{"different attempt", func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[0].ResendAttemptCount++; return r }},
		{"different outcome", func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[0].LastFailureReason = "other"; return r }},
		{"missing record", func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { return r[:1] }},
		{"reordered records", func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[0], r[1] = r[1], r[0]; return r }},
		{"not finalized", func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[0].FinalizedAt = time.Time{}; return r }},
	} {
		t.Run(change.name, func(t *testing.T) {
			store, source, destination, original, records := quarantineRecoveryFixture(t)
			sourceBody := writeRecoveryRecords(t, source, original)
			existing := change.apply(append([]pkgspool.SpoolRecord(nil), records...))
			storedBody := writeRecoveryRecords(t, destination, existing)
			err := store.StoreProcessingRecordsToQuarantine(context.Background(), source, records)
			require.ErrorIs(t, err, ErrTerminalSpoolConflict)
			for path, expected := range map[string][]byte{source: sourceBody, destination: storedBody} {
				actual, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
		})
	}
}

func TestTerminalSaveRejectsInvalidExistingDestination(t *testing.T) {
	for _, kind := range []string{"malformed", "directory", "symlink", "dangling symlink"} {
		t.Run(kind, func(t *testing.T) {
			store, source, destination, original, records := quarantineRecoveryFixture(t)
			writeRecoveryRecords(t, source, original)
			require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o700))
			switch kind {
			case "malformed":
				require.NoError(t, os.WriteFile(destination, []byte("broken"), 0o600))
			case "directory":
				require.NoError(t, os.Mkdir(destination, 0o700))
			default:
				target := filepath.Join(t.TempDir(), "saved")
				if kind == "symlink" {
					writeRecoveryRecords(t, target, records)
				}
				testutil.Symlink(t, target, destination)
			}
			require.Error(t, store.StoreProcessingRecordsToQuarantine(context.Background(), source, records))
			require.FileExists(t, source)
			_, err := os.Lstat(destination)
			require.NoError(t, err, "leave invalid existing evidence intact")
		})
	}
}

func TestRemoveProcessingFileRetriesDirectorySyncAfterUnlink(t *testing.T) {
	store, source, _, original, _ := quarantineRecoveryFixture(t)
	writeRecoveryRecords(t, source, original)
	directory := filepath.Dir(source)
	restore := testutil.DenyDirectorySync(t, directory)
	for range 2 {
		require.ErrorIs(t, store.RemoveProcessingFile(context.Background(), source), pkgfsstore.ErrDurabilityUnconfirmed)
		require.NoFileExists(t, source)
		_, err := os.Stat(directory)
		require.NoError(t, err, "retain the unsynced parent until retry succeeds")
	}
	restore()
	require.NoError(t, store.RemoveProcessingFile(context.Background(), source))
	// Bucket directories may now be pruned; repeated cleanup still succeeds.
	require.NoError(t, store.RemoveProcessingFile(context.Background(), source))
}

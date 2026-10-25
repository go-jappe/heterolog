package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/testutil"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

func TestReconcileProcessingQuarantineVerifiesWholeStoredRecord(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func([]pkgspool.SpoolRecord) []pkgspool.SpoolRecord
	}{
		{name: "complete match"},
		{name: "partial quarantine", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { return r[:1] }},
		{name: "same ID different payload", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord {
			r[1].Payload = json.RawMessage(`{"message":"unrelated"}`)
			return r
		}},
		{name: "same payload different ID", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[1].RequestID = "other"; return r }},
		{name: "duplicated record replaces another", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[1] = r[0]; return r }},
		{name: "reordered duplicate IDs", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[0], r[1] = r[1], r[0]; return r }},
		{name: "different occurrence", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord {
			r[1].OccurredAt = r[1].OccurredAt.Add(time.Second)
			return r
		}},
		{name: "different original failure", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord {
			r[1].InitialFailureReason = "other failure"
			return r
		}},
		{name: "not finalized", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[1].FinalizedAt = time.Time{}; return r }},
		{name: "older attempt metadata", change: func(r []pkgspool.SpoolRecord) []pkgspool.SpoolRecord { r[1].ResendAttemptCount = 0; return r }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store, source, destination, original, quarantined := quarantineRecoveryFixture(t)
			if tt.change != nil {
				quarantined = tt.change(quarantined)
			}
			originalBody := writeRecoveryRecords(t, source, original)
			quarantineBody := writeRecoveryRecords(t, destination, quarantined)
			found, err := store.ReconcileProcessingQuarantine(context.Background(), source, original)
			require.True(t, found)
			if tt.change == nil {
				require.NoError(t, err)
				require.NoFileExists(t, source)
			} else {
				require.ErrorIs(t, err, ErrQuarantineRecoveryConflict)
				actual, readErr := os.ReadFile(source)
				require.NoError(t, readErr)
				require.Equal(t, originalBody, actual, "conflicting/partial processing records must remain intact")
			}
			actual, readErr := os.ReadFile(destination)
			require.NoError(t, readErr)
			require.Equal(t, quarantineBody, actual, "recovery must preserve saved outcome and payload")
		})
	}
}

func TestReconcileProcessingQuarantinePreservesUnreadableEvidence(t *testing.T) {
	for _, kind := range []string{"malformed", "directory", "symlink", "dangling symlink", "parent symlink", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			store, source, destination, original, quarantined := quarantineRecoveryFixture(t)
			originalBody := writeRecoveryRecords(t, source, original)
			ctx := context.Background()
			switch kind {
			case "malformed":
				require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o700))
				require.NoError(t, os.WriteFile(destination, []byte("broken saved evidence"), 0o600))
			case "directory":
				require.NoError(t, os.MkdirAll(destination, 0o700))
			case "symlink", "dangling symlink":
				require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o700))
				target := filepath.Join(t.TempDir(), "saved")
				if kind == "symlink" {
					writeRecoveryRecords(t, target, quarantined)
				}
				testutil.Symlink(t, target, destination)
			case "parent symlink":
				require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Dir(destination)), 0o700))
				testutil.Symlink(t, t.TempDir(), filepath.Dir(destination))
			case "canceled":
				writeRecoveryRecords(t, destination, quarantined)
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err := store.ReconcileProcessingQuarantine(ctx, source, original)
			require.Error(t, err)
			body, err := os.ReadFile(source)
			require.NoError(t, err)
			require.Equal(t, originalBody, body)
			_, err = os.Lstat(destination)
			if kind != "parent symlink" {
				require.NoError(t, err, "unreadable evidence must not be removed")
			}
		})
	}
}

func TestReconcileProcessingQuarantineLeavesOrdinaryPendingRecord(t *testing.T) {
	store, source, _, original, _ := quarantineRecoveryFixture(t)
	body := writeRecoveryRecords(t, source, original)
	found, err := store.ReconcileProcessingQuarantine(context.Background(), source, original)
	require.NoError(t, err)
	require.False(t, found, "no quarantine evidence must retain ordinary at-least-once processing")
	actual, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, body, actual)
}

func TestReconcileProcessingQuarantineRejectsChangedSource(t *testing.T) {
	store, source, destination, original, quarantined := quarantineRecoveryFixture(t)
	writeRecoveryRecords(t, destination, quarantined)
	changed := append([]pkgspool.SpoolRecord(nil), original...)
	changed[0].Payload = json.RawMessage(`{"message":"replaced source"}`)
	body := writeRecoveryRecords(t, source, changed)
	found, err := store.ReconcileProcessingQuarantine(context.Background(), source, original)
	require.True(t, found)
	require.ErrorIs(t, err, ErrQuarantineRecoveryConflict)
	actual, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, body, actual)
}

// 保存済み結果をdecodeできない競合はErrTerminalRecoveryConflictとして返し、processing file
// 自体の内容不正（ErrInvalidSpoolFile）とは判定させません。ErrInvalidSpoolFileは
// processing fileをquarantineしてよいことを表しますが、競合ではすべてのfileを残します。
func TestReconcileProcessingTerminalSavedDecodeConflictIsNotInvalidProcessingFile(t *testing.T) {
	store, source, destination, original, _ := quarantineRecoveryFixture(t)
	originalBody := writeRecoveryRecords(t, source, original)
	require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o700))
	require.NoError(t, os.WriteFile(destination, []byte("broken saved evidence"), 0o600))

	_, err := store.ReconcileProcessingTerminal(context.Background(), source, original)
	require.ErrorIs(t, err, ErrTerminalRecoveryConflict)
	require.NotErrorIs(t, err, pkgspool.ErrInvalidSpoolFile)
	require.NotErrorIs(t, err, pkgspool.ErrMalformedSpoolJSON)
	body, readErr := os.ReadFile(source)
	require.NoError(t, readErr)
	require.Equal(t, originalBody, body)
}

func quarantineRecoveryFixture(t *testing.T) (*StateStore, string, string, []pkgspool.SpoolRecord, []pkgspool.SpoolRecord) {
	t.Helper()
	store, err := NewStateStore(processingTestConfig{spoolPath: t.TempDir()})
	require.NoError(t, err)
	rel := filepath.Join("2026", "10", "21", "00", "00", "00001_recovery.hlogspool")
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	original := []pkgspool.SpoolRecord{
		{OccurredAt: now, RequestID: "same-id", Payload: json.RawMessage(`{"message":"first"}`), ResendAttemptCount: 2, InitialFailureReason: "original failure"},
		{OccurredAt: now, RequestID: "same-id", Payload: json.RawMessage(`{"message":"second"}`), ResendAttemptCount: 2, InitialFailureReason: "original failure"},
	}
	quarantined := append([]pkgspool.SpoolRecord(nil), original...)
	for i := range quarantined {
		quarantined[i].FinalizedAt = now.Add(time.Hour)
		quarantined[i].LastResendAttemptedAt = now.Add(time.Minute)
		quarantined[i].ResendAttemptCount++
		quarantined[i].LastFailureReason = "response outcome is unknown"
	}
	return store, filepath.Join(store.RootDir.ProcessingDir().Path(), rel), filepath.Join(store.RootDir.QuarantineDir().Path(), rel), original, quarantined
}

func writeRecoveryRecords(t *testing.T, path string, records []pkgspool.SpoolRecord) []byte {
	t.Helper()
	body, err := buildSpoolNDJSON(records)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, body, 0o600))
	return body
}

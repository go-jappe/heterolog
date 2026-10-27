package logspool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/internal/testutil"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/stretchr/testify/require"
)

type terminalDurabilitySender struct {
	calls  int
	status spool.RecordOutcomeStatus
}

func (s *terminalDurabilitySender) Send(_ context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	s.calls++
	report := make(spool.ExportReport, len(records))
	for i := range records {
		report[i] = spool.RecordOutcome{InputIndex: i, Status: s.status, AttemptedAt: time.Now()}
		if s.status != spool.RecordOutcomeStatusSuccess {
			report[i].Err = errors.New("terminal outcome")
		}
	}
	return report, nil
}

func TestTerminalDurabilityFailureRetainsSourceAndRetriesWithoutSend(t *testing.T) {
	for _, tc := range []struct {
		state  spool.State
		status spool.RecordOutcomeStatus
	}{
		{spool.StateDone, spool.RecordOutcomeStatusSuccess},
		{spool.StateDead, spool.RecordOutcomeStatusPermanent},
		{spool.StateQuarantine, spool.RecordOutcomeStatusInvalid},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			for _, denied := range []bool{false, true} {
				name := "normal"
				if denied {
					name = "directory sync failure"
				}
				t.Run(name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						ctx := context.Background()
						now := time.Now()
						cfg := integrationSpoolConfig{spoolPath: resolvedTempDirForTest(t), target: "terminal-durability", dir: "backend"}
						store, err := fsstorestore.NewSpoolStore(fixedSpoolClock{now: now}, cfg, observer.NopTracer{}, observer.NopMeter{})
						require.NoError(t, err)
						record := integrationSpoolRecord(t, now, "record-1", "keep until saved outcome is durable")
						require.NoError(t, store.StoreByState(ctx, spool.StateProcessing, []spool.SpoolRecord{record}))
						stateStore, err := fsworker.NewStateStore(cfg)
						require.NoError(t, err)
						paths, err := stateStore.ListOldestProcessingFilePaths(ctx, 1)
						require.NoError(t, err)
						require.Len(t, paths, 1)
						source := paths[0]
						rel, err := filepath.Rel(stateRoot(cfg, spool.StateProcessing), source)
						require.NoError(t, err)
						destination := filepath.Join(stateRoot(cfg, tc.state), rel)
						require.NoError(t, os.MkdirAll(filepath.Dir(destination), 0o700))
						restore := func() {}
						if denied {
							restore = testutil.DenyDirectorySync(t, filepath.Dir(destination))
						}
						sender := &terminalDurabilitySender{status: tc.status}
						retry, err := worker.NewProcessingFileTransitionRetry(worker.NewProcessingFileTransitionRetryConfigAdapter(
							worker.ProcessingFileTransitionRetryConfig{RetryDelay: time.Second, MaxEntries: 10},
						))
						require.NoError(t, err)
						w, err := worker.New(stateStore, activeSpoolWriter{store: store}, sender, worker.WithProcessingFileTransitionRetry(retry))
						require.NoError(t, err)
						report, err := w.RunOnceSpoolFileProcess(ctx)
						if denied {
							require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
							require.Equal(t, worker.ProcessStatusFailed, report.Status)
							require.FileExists(t, source)
							// 通常のcycleで、分類済みのこのfileを再送してはなりません。
							_, err = w.RunOnceSpoolFileProcess(ctx)
							require.NoError(t, err)
							require.Equal(t, 1, sender.calls)
							restore()
							before, err := os.ReadFile(destination)
							require.NoError(t, err)
							time.Sleep(time.Second)
							retried, handled, err := w.RunOnceProcessingFileTransitionRetry(ctx)
							require.NoError(t, err)
							require.True(t, handled)
							require.Equal(t, worker.ProcessingFileTransitionRetryStatusSucceeded, retried.Status)
							after, err := os.ReadFile(destination)
							require.NoError(t, err)
							require.Equal(t, before, after, "retry must preserve the first saved outcome")
						} else {
							require.NoError(t, err)
							require.Equal(t, worker.ProcessStatusCompleted, report.Status)
						}
						require.NoFileExists(t, source)
						require.Equal(t, 1, sender.calls)
						saved := readIntegrationSpoolRecords(t, stateRoot(cfg, tc.state))
						require.Len(t, saved, 1)
						require.Equal(t, record.Payload, saved[0].Payload)
						require.Equal(t, 1, saved[0].ResendAttemptCount)
						require.False(t, saved[0].FinalizedAt.IsZero())
					})
				})
			}
		})
	}
}

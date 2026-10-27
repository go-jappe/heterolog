package logspool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fslifecycle "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/lifecycle"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	spoollifecycle "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/internal/testutil"
	"github.com/go-jappe/heterolog/internal/testutil/boundedwait"
	pkgfsstore "github.com/go-jappe/heterolog/pkg/fsstore"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/go-jappe/heterolog/pkg/spool/lifecyclemanager/lifecyclefsstore"
	"github.com/stretchr/testify/require"
)

// 中断されたunlinkによって、保存済みのterminal decisionが新たな送信に変わり、
// 同じdone/dead fileとの終わりのない衝突が続いてはなりません。この試験は、実際の
// source削除の失敗、filesystemからの再構築、retentionを先に実行するRunnerを使います。
func TestTerminalCleanupSurvivesRemovalFailureAndRestart(t *testing.T) {
	for _, tc := range []struct {
		state  spool.State
		status spool.RecordOutcomeStatus
	}{
		{spool.StateDone, spool.RecordOutcomeStatusSuccess},
		{spool.StateDead, spool.RecordOutcomeStatusPermanent},
		{spool.StateQuarantine, spool.RecordOutcomeStatusInvalid},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
			cfg := integrationSpoolConfig{spoolPath: t.TempDir(), target: "terminal-recovery", dir: "backend"}
			store, err := fsstorestore.NewSpoolStore(fixedSpoolClock{now: now}, cfg, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			original := []spool.SpoolRecord{
				integrationSpoolRecord(t, now, "same-id", "first record"),
				integrationSpoolRecord(t, now.Add(time.Second), "same-id", "second record"),
			}
			require.NoError(t, store.StoreByState(ctx, spool.StateProcessing, original))
			stateStore, err := fsworker.NewStateStore(cfg)
			require.NoError(t, err)
			paths, err := stateStore.ListOldestProcessingFilePaths(ctx, 1)
			require.NoError(t, err)
			require.Len(t, paths, 1)
			source := paths[0]
			restore := testutil.DenyFileRemoval(t, source)
			if err := os.Remove(source); err == nil {
				restore()
				t.Skip("current user or filesystem bypasses file removal restrictions")
			}
			sender := &terminalRestartSender{status: tc.status, attemptedAt: now.Add(time.Minute)}
			initial, err := worker.New(stateStore, activeSpoolWriter{store: store}, sender)
			require.NoError(t, err)
			_, firstErr := initial.RunOnceSpoolFileProcess(ctx)
			restore()
			require.ErrorIs(t, firstErr, spool.ErrRemoveProcessingSpoolFile)
			require.FileExists(t, source)
			require.Equal(t, 1, sender.calls)
			saved := readIntegrationSpoolRecords(t, stateRoot(cfg, tc.state))
			require.Len(t, saved, len(original))
			require.Equal(t, original, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateProcessing)))

			// workerを作り直して、memory上のretry情報をすべて捨てます。retentionが先に実行され、
			// 期限切れで容量超過の保存済み証拠を保持しなければなりません。
			restartedStore, err := fsworker.NewStateStore(cfg)
			require.NoError(t, err)
			restarted, err := worker.New(restartedStore, activeSpoolWriter{store: store}, sender)
			require.NoError(t, err)
			retentionClock := &fixedSpoolClock{now: now.AddDate(0, 0, 10)}
			retention, err := fslifecycle.NewFileOperator(retentionClock,
				fslifecycle.NewConfigAdapter(lifecyclefsstore.LifeCycleFsstoreConfig{
					SpoolPath: cfg.spoolPath, Target: cfg.target, Dir: cfg.dir,
					EnablePurgeDone: true, DoneMaxFileAge: 1, DoneMaxDirSize: 1,
					EnablePurgeDead: true, DeadMaxFileAge: 1, DeadMaxDirSize: 1,
					EnablePurgeQuarantine: true, QuarantineMaxFileAge: 1, QuarantineMaxDirSize: 1,
					SizePurgeInterval: time.Second,
				}))
			require.NoError(t, err)
			manager, err := spoollifecycle.NewManager(retention, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			runner, err := NewRunner(restarted, manager, runnerConfigStub{}, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			report, err := runner.RunOnce(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, sender.calls, "complete matching terminal evidence needs cleanup, not another send")
			require.NoFileExists(t, source)
			require.Equal(t, saved, readIntegrationSpoolRecords(t, stateRoot(cfg, tc.state)), "recovery must retain the original terminal decision")
			switch tc.state {
			case spool.StateDone:
				require.Equal(t, worker.ProcessStatusCompleted, report.ProcessReport.Status)
				require.Equal(t, len(original), report.ProcessReport.SuccessRecordCount)
			case spool.StateDead:
				require.Equal(t, worker.ProcessStatusCompleted, report.ProcessReport.Status)
				require.Equal(t, len(original), report.ProcessReport.DeadRecordCount)
			case spool.StateQuarantine:
				require.Equal(t, worker.ProcessStatusQuarantined, report.ProcessReport.Status)
				require.Equal(t, len(original), report.ProcessReport.QuarantineRecordCount)
			}
			// cleanupが完了すると、以降のcycleで通常のretentionが再開します。
			retentionClock.now = retentionClock.now.Add(24 * time.Hour)
			_, err = runner.RunOnce(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, sender.calls)
			require.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, tc.state)))
		})
	}
}

type terminalRestartSender struct {
	calls       int
	status      spool.RecordOutcomeStatus
	attemptedAt time.Time
}

func (s *terminalRestartSender) Send(_ context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	s.calls++
	outcomes := make(spool.ExportReport, len(records))
	for i := range records {
		outcomes[i] = spool.RecordOutcome{InputIndex: i, Status: s.status, AttemptedAt: s.attemptedAt}
		if s.status != spool.RecordOutcomeStatusSuccess {
			outcomes[i].Err = errors.New("terminal backend outcome")
		}
	}
	return outcomes, nil
}

func TestTerminalRecoveryRetriesUnlinkSyncBeforeRetentionCanDiscardEvidence(t *testing.T) {
	for _, state := range []spool.State{spool.StateDone, spool.StateDead, spool.StateQuarantine} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
			cfg := integrationSpoolConfig{spoolPath: t.TempDir(), target: "terminal-sync-retry", dir: "backend"}
			store, err := fsstorestore.NewSpoolStore(fixedSpoolClock{now: now}, cfg, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			original := []spool.SpoolRecord{integrationSpoolRecord(t, now, "kept-result", "keep until unlink is durable")}
			require.NoError(t, store.StoreByState(ctx, spool.StateProcessing, original))
			stateStore, err := fsworker.NewStateStore(cfg)
			require.NoError(t, err)
			paths, err := stateStore.ListOldestProcessingFilePaths(ctx, 1)
			require.NoError(t, err)
			require.Len(t, paths, 1)
			source := paths[0]
			// 前のprocessが残した、完全なterminal decisionを用意します。
			require.NoError(t, stateStore.StoreProcessingRecordsToState(ctx, source, state, original))
			before := readIntegrationSpoolRecords(t, stateRoot(cfg, state))
			sender := &terminalRestartSender{status: spool.RecordOutcomeStatusSuccess}
			recoveryStore := &terminalSyncFailureStore{StateStore: stateStore, t: t}
			retry, err := worker.NewProcessingFileTransitionRetry(worker.NewProcessingFileTransitionRetryConfigAdapter(
				worker.ProcessingFileTransitionRetryConfig{RetryDelay: time.Nanosecond, MaxEntries: 10}))
			require.NoError(t, err)
			w, err := worker.New(recoveryStore, activeSpoolWriter{store: store}, sender, worker.WithProcessingFileTransitionRetry(retry))
			require.NoError(t, err)
			_, err = w.RunOnceSpoolFileProcess(ctx)
			require.ErrorIs(t, err, spool.ErrRemoveProcessingSpoolFile)
			require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			require.NoFileExists(t, source, "unlink succeeded; only its directory sync is pending")
			require.True(t, retry.Contains(source))
			require.Zero(t, sender.calls)

			retention, err := fslifecycle.NewFileOperator(fixedSpoolClock{now: now.AddDate(0, 0, 10)},
				fslifecycle.NewConfigAdapter(lifecyclefsstore.LifeCycleFsstoreConfig{
					SpoolPath: cfg.spoolPath, Target: cfg.target, Dir: cfg.dir,
					EnablePurgeDone: true, DoneMaxFileAge: 1, DoneMaxDirSize: 1,
					EnablePurgeDead: true, DeadMaxFileAge: 1, DeadMaxDirSize: 1,
					EnablePurgeQuarantine: true, QuarantineMaxFileAge: 1, QuarantineMaxDirSize: 1,
					SizePurgeInterval: time.Second,
				}))
			require.NoError(t, err)
			manager, err := spoollifecycle.NewManager(retention, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			runner, err := NewRunner(w, manager, runnerConfigStub{}, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			waitProcessingFileTransitionRetryReady(t, retry)
			report, err := runner.RunOnce(ctx)
			require.ErrorIs(t, err, pkgfsstore.ErrDurabilityUnconfirmed)
			require.True(t, report.ProcessingFileTransitionRetryHandled)
			require.Equal(t, worker.ProcessingFileTransitionRetryOpRemoveProcessingFileAfterRedistribution, report.ProcessingFileTransitionRetryReport.Operation)
			require.True(t, retry.Contains(source))
			require.Equal(t, before, readIntegrationSpoolRecords(t, stateRoot(cfg, state)), "retention must not discard evidence before source absence is durable")
			recoveryStore.restore()
			waitProcessingFileTransitionRetryReady(t, retry)
			report, err = runner.RunOnce(ctx)
			require.NoError(t, err)
			require.True(t, report.ProcessingFileTransitionRetryHandled)
			require.Equal(t, worker.ProcessingFileTransitionRetryStatusSucceeded, report.ProcessingFileTransitionRetryReport.Status)
			require.False(t, retry.Contains(source))
			require.Zero(t, sender.calls)
		})
	}
}

// waitProcessingFileTransitionRetryReadyは、retryのentryが再試行可能になるまで待ちます。
// RetryDelayが1nsでも、monotonic clockが失敗時刻より進むまでentryはreadyになりません。
// Windowsのmonotonic clockはtimer割込みの間隔（標準約15.6ms）でしか進まないため、
// 失敗直後のRunOnceでは再試行時刻が未到来のことがあります。
func waitProcessingFileTransitionRetryReady(t *testing.T, retry *worker.ProcessingFileTransitionRetry) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, _, ready := retry.FindReady(time.Now())
		return ready
	}, boundedwait.HangTimeout, time.Millisecond, "processing file transition retry did not become ready")
}

type terminalSyncFailureStore struct {
	*fsworker.StateStore
	t       *testing.T
	restore func()
}

func (s *terminalSyncFailureStore) ReconcileProcessingTerminal(ctx context.Context, path string, records []spool.SpoolRecord) (spool.State, error) {
	// 実際のfilesystem障害をdiscovery/readの後に仕込み、それより前のdirectory scanではなく
	// unlinkのdurabilityを失敗の対象にします。
	s.restore = testutil.DenyDirectorySync(s.t, filepath.Dir(path))
	return s.StateStore.ReconcileProcessingTerminal(ctx, path, records)
}

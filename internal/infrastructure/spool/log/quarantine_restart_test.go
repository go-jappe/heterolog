package logspool

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	fslifecycle "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/lifecycle"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	spoollifecycle "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	spoolsender "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/sender/opensearch"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/opensearch"
	"github.com/go-jappe/heterolog/pkg/spool"
	"github.com/go-jappe/heterolog/pkg/spool/lifecyclemanager/lifecyclefsstore"

	"github.com/stretchr/testify/require"
)

func TestOpenSearchQuarantineSurvivesCleanupFailureAndRestart(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response string
	}{
		{name: "truncated JSON", response: `{"items":`},
		{name: "missing document outcome", response: `{"errors":false,"items":[]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				calls.Add(1)
				// The backend may have accepted the document; an automatic replay
				// must wait for an operator to resolve this uncertain result.
				_, _ = io.WriteString(w, tt.response)
			}))
			defer server.Close()

			ctx := context.Background()
			now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
			clock := fixedSpoolClock{now: now}
			cfg := integrationSpoolConfig{spoolPath: t.TempDir(), target: "response-recovery", dir: "opensearch"}
			store, err := fsstorestore.NewSpoolStore(clock, cfg, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			payload, err := json.Marshal(opensearch.DocumentWriteRequest{
				Index: "logs", ID: "doc-1", Op: opensearch.WriteOpCreate,
				Doc: map[string]any{"message": "keep until outcome is known"},
			})
			require.NoError(t, err)
			require.NoError(t, store.StoreByState(ctx, spool.StateActive, []spool.SpoolRecord{{
				OccurredAt: now, RequestID: "request-1", Payload: payload,
			}}))
			stateStore, err := fsworker.NewStateStore(cfg)
			require.NoError(t, err)
			client, err := opensearch.New(opensearch.Config{
				Endpoint: server.URL, Timeout: time.Second,
				AuthConfig: opensearch.AuthConfig{Type: opensearch.AuthTypeNone},
			})
			require.NoError(t, err)
			sender, err := spoolsender.NewOpensearchSpoolSender(clock, client, 5*1024*1024)
			require.NoError(t, err)
			spoolWorker, err := worker.New(&removeFailureStateStore{StateStore: stateStore}, activeSpoolWriter{store: store}, sender)
			require.NoError(t, err)
			spoolWorker.TargetBackendService = "opensearch"
			runner, err := NewRunner(spoolWorker, newTestLifecycleManager(&spoolLifecycleStoreStub{}),
				runnerConfigStub{}, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)

			first, err := runner.RunOnce(ctx)
			require.ErrorIs(t, err, errInjectedProcessingUnlink)
			require.Equal(t, 1, first.ProcessReport.QuarantineRecordCount)
			require.Zero(t, first.ProcessReport.RetryRecordCount)
			quarantined := readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateQuarantine))
			require.Len(t, quarantined, 1)
			require.JSONEq(t, string(payload), string(quarantined[0].Payload))
			require.Equal(t, "request-1", quarantined[0].RequestID)
			require.Equal(t, 1, quarantined[0].ResendAttemptCount)
			require.NotEmpty(t, quarantined[0].LastFailureReason, "retain the reason for operator inspection")

			// The backend outcome and original payload are durably present in quarantine.
			require.Len(t, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateProcessing)), 1)
			require.EqualValues(t, 1, calls.Load())
			restartedStore, err := fsworker.NewStateStore(cfg)
			require.NoError(t, err)
			restartedWorker, err := worker.New(restartedStore, activeSpoolWriter{store: store}, sender)
			require.NoError(t, err)
			// Retention runs before send on every Runner cycle. Even an expired,
			// over-capacity quarantine must survive until the source is reconciled.
			retention, err := fslifecycle.NewFileOperator(fixedSpoolClock{now: now.AddDate(0, 0, 10)},
				fslifecycle.NewConfigAdapter(lifecyclefsstore.LifeCycleFsstoreConfig{
					SpoolPath: cfg.spoolPath, Target: cfg.target, Dir: cfg.dir,
					EnablePurgeQuarantine: true, QuarantineMaxFileAge: 1,
					QuarantineMaxDirSize: 1, SizePurgeInterval: time.Second,
				}))
			require.NoError(t, err)
			manager, err := spoollifecycle.NewManager(retention, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			restartedRunner, err := NewRunner(restartedWorker, manager,
				runnerConfigStub{}, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			restartReport, restartErr := restartedRunner.RunOnce(ctx)
			require.NoError(t, restartErr)
			require.True(t, restartReport.LifecycleReport.QuarantinePurgeReport.Executed())
			require.Zero(t, restartReport.LifecycleReport.QuarantinePurgeReport.DeletedFileCount())
			recovered := restartReport.ProcessReport
			require.Equal(t, worker.ProcessStatusQuarantined, recovered.Status)
			require.Equal(t, 1, recovered.QuarantineRecordCount)
			require.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateProcessing)))
			require.EqualValues(t, 1, calls.Load(), "restart must not replay a record already durably quarantined")
			require.Equal(t, quarantined, readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateQuarantine)))
			_, err = restartedRunner.RunOnce(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 1, calls.Load(), "cleanup recovery must not leave another replay candidate")
		})
	}
}

func TestQuarantineRestartStopsPartialOrConflictingEvidenceWithoutLosingRecords(t *testing.T) {
	for _, mode := range []string{"partial result", "same ID different payload", "malformed quarantine"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
			cfg := integrationSpoolConfig{spoolPath: resolvedTempDirForTest(t), target: "recovery-conflict", dir: "backend"}
			store, err := fsstorestore.NewSpoolStore(fixedSpoolClock{now: now}, cfg, observer.NopTracer{}, observer.NopMeter{})
			require.NoError(t, err)
			original := []spool.SpoolRecord{
				integrationSpoolRecord(t, now, "uncertain", "keep unknown outcome"),
				integrationSpoolRecord(t, now, "succeeded", "keep successful result"),
			}
			if mode != "partial result" {
				original = original[:1]
			}
			require.NoError(t, store.StoreByState(ctx, spool.StateActive, original))
			stateStore, err := fsworker.NewStateStore(cfg)
			require.NoError(t, err)
			sender := &quarantineRecoverySender{attemptedAt: now.Add(time.Minute)}
			initial, err := worker.New(&removeFailureStateStore{StateStore: stateStore}, activeSpoolWriter{store: store}, sender)
			require.NoError(t, err)
			first, err := initial.RunOnceSpoolFileProcess(ctx)
			require.ErrorIs(t, err, errInjectedProcessingUnlink)
			require.Equal(t, 1, sender.calls)
			rel, err := filepath.Rel(stateRoot(cfg, spool.StateProcessing), first.ProcessingPath)
			require.NoError(t, err)
			quarantinePath := filepath.Join(stateRoot(cfg, spool.StateQuarantine), rel)
			if mode == "same ID different payload" {
				records := readIntegrationSpoolRecords(t, stateRoot(cfg, spool.StateQuarantine))
				records[0].Payload = json.RawMessage(`{"message":"unrelated record with same ID"}`)
				body, err := json.Marshal(records[0])
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(quarantinePath, append(body, '\n'), 0o600))
			} else if mode == "malformed quarantine" {
				require.NoError(t, os.WriteFile(quarantinePath, []byte("truncated quarantine\n"), 0o600))
			}
			before := snapshotRecoveryFiles(t, cfg.spoolPath)
			// Rebuild twice: rejection must depend on retained evidence, not on
			// the first worker's in-memory transition retry or backoff state.
			for restart := 0; restart < 2; restart++ {
				restartedStore, err := fsworker.NewStateStore(cfg)
				require.NoError(t, err)
				restarted, err := worker.New(restartedStore, activeSpoolWriter{store: store}, sender)
				require.NoError(t, err)
				report, err := restarted.RunOnceSpoolFileProcess(ctx)
				require.ErrorIs(t, err, fsworker.ErrQuarantineRecoveryConflict)
				require.Equal(t, worker.ProcessStatusFailed, report.Status)
				require.Equal(t, 1, sender.calls, "ambiguous processing must not be sent again")
				require.Equal(t, before, snapshotRecoveryFiles(t, cfg.spoolPath), "every source and saved result must remain unchanged")
			}
		})
	}
}

func TestQuarantineConflictDoesNotStarveNormalActive(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	cfg := integrationSpoolConfig{spoolPath: t.TempDir(), target: "conflict-progress", dir: "backend"}
	store, err := fsstorestore.NewSpoolStore(fixedSpoolClock{now: now}, cfg, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)
	original := []spool.SpoolRecord{
		integrationSpoolRecord(t, now, "uncertain", "unknown outcome"),
		integrationSpoolRecord(t, now, "succeeded", "successful result"),
	}
	require.NoError(t, store.StoreByState(ctx, spool.StateActive, original))
	stateStore, err := fsworker.NewStateStore(cfg)
	require.NoError(t, err)
	sender := &quarantineRecoverySender{attemptedAt: now.Add(time.Minute)}
	initial, err := worker.New(&removeFailureStateStore{StateStore: stateStore}, activeSpoolWriter{store: store}, sender)
	require.NoError(t, err)
	_, err = initial.RunOnceSpoolFileProcess(ctx)
	require.ErrorIs(t, err, errInjectedProcessingUnlink)
	require.Equal(t, 1, sender.calls)
	preserved := snapshotRecoveryFiles(t, cfg.spoolPath)
	normal := integrationSpoolRecord(t, now.Add(time.Second), "new-normal", "ordinary new record")
	require.NoError(t, store.StoreByState(ctx, spool.StateActive, []spool.SpoolRecord{normal}))
	restartedStore, err := fsworker.NewStateStore(cfg)
	require.NoError(t, err)
	newSender := &recordingSuccessSender{now: now.Add(2 * time.Minute)}
	restarted, err := worker.New(restartedStore, activeSpoolWriter{store: store}, newSender)
	require.NoError(t, err)
	_, err = restarted.RunOnceSpoolFileProcess(ctx)
	require.ErrorIs(t, err, fsworker.ErrQuarantineRecoveryConflict)
	report, err := restarted.RunOnceSpoolFileProcess(ctx)
	require.NoError(t, err)
	require.Equal(t, worker.ProcessStatusCompleted, report.Status)
	require.Equal(t, []spool.SpoolRecord{normal}, newSender.records, "send only the unrelated active record")
	for path, body := range preserved {
		actual, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, body, string(actual), "keep every conflicting source and saved outcome")
	}
}

type quarantineRecoverySender struct {
	calls       int
	attemptedAt time.Time
}

func (s *quarantineRecoverySender) Send(_ context.Context, records []spool.SpoolRecord) (spool.ExportReport, error) {
	s.calls++
	outcomes := make(spool.ExportReport, len(records))
	for i := range records {
		outcomes[i] = spool.RecordOutcome{InputIndex: i, Status: spool.RecordOutcomeStatusSuccess, AttemptedAt: s.attemptedAt}
	}
	outcomes[0].Status = spool.RecordOutcomeStatusInvalid
	outcomes[0].Err = errors.New("backend response outcome is unknown")
	return outcomes, nil
}

func snapshotRecoveryFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[path] = string(body)
		return nil
	})
	require.NoError(t, err)
	return files
}

// errInjectedProcessingUnlinkは、結果を保存した後のprocessing file削除失敗を再現します。
var errInjectedProcessingUnlink = errors.New("injected processing unlink failure")

type removeFailureStateStore struct{ *fsworker.StateStore }

func (s *removeFailureStateStore) RemoveProcessingFile(context.Context, string) error {
	return errInjectedProcessingUnlink
}

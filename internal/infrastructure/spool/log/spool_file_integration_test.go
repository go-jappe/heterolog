package logspool

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/application/observer"
	spooldir "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool"
	fsstorestore "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/store"
	fsworker "github.com/go-jappe/heterolog/internal/infrastructure/fsstore/spool/worker"
	spoolstore "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/store"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	"github.com/go-jappe/heterolog/pkg/logger/durable"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// active spool fileに保存されたrecordをrunnerが読み取り、payloadを解釈・変更せず
// Senderへ渡した後、送信済みrecordをdoneへ確定できることを確認する。
func TestRunnerProcessesActiveSpoolFileToDone(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 10, 12, 0, 0, 123000000, time.UTC)
	cfg := integrationSpoolConfig{
		spoolPath: t.TempDir(),
		target:    "logging-integration",
		dir:       "test-backend",
	}
	clock := fixedSpoolClock{now: now}

	activeStore, err := fsstorestore.NewSpoolStore(clock, cfg, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)

	seedRecords := []pkgspool.SpoolRecord{
		integrationSpoolRecord(t, now, "retry-record-1", "spool resend log 1"),
		integrationSpoolRecord(t, now.Add(time.Second), "retry-record-2", "spool resend log 2"),
	}
	require.NoError(t, activeStore.StoreByState(ctx, pkgspool.StateActive, seedRecords))

	activeBefore := readIntegrationSpoolRecords(t, activeRoot(cfg))
	require.Len(t, activeBefore, 2)
	logSpoolRecords(t, "seed active spool", activeBefore, 2)

	stateStore, err := fsworker.NewStateStore(cfg)
	require.NoError(t, err)

	sender := &recordingSuccessSender{now: now.Add(5 * time.Minute)}
	spoolWorker, err := worker.New(
		stateStore,
		activeSpoolWriter{store: activeStore},
		sender,
	)
	require.NoError(t, err)
	spoolWorker.TargetBackendService = "test-backend"

	runner, err := NewRunner(
		spoolWorker,
		newTestLifecycleManager(&spoolLifecycleStoreStub{}),
		runnerConfigStub{},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, worker.ProcessStatusCompleted, report.ProcessReport.Status)
	assert.Equal(t, 2, report.ProcessReport.RecordCount)
	assert.Equal(t, 2, report.ProcessReport.SuccessRecordCount)

	require.Len(t, sender.records, 2)
	assert.Equal(t, []string{"retry-record-1", "retry-record-2"}, []string{
		sender.records[0].RequestID,
		sender.records[1].RequestID,
	})
	assert.Equal(t, seedRecords[0].Payload, sender.records[0].Payload)
	assert.Equal(t, seedRecords[1].Payload, sender.records[1].Payload)

	assert.Empty(t, readIntegrationSpoolRecords(t, activeRoot(cfg)))
	assert.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateProcessing)))

	doneRecords := readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateDone))
	require.Len(t, doneRecords, 2)
	logSpoolRecords(t, "done spool", doneRecords, 2)

	for _, rec := range doneRecords {
		assert.False(t, rec.FinalizedAt.IsZero())
		assert.False(t, rec.FirstResendAttemptedAt.IsZero())
		assert.Equal(t, sender.now, rec.LastResendAttemptedAt)
		assert.Equal(t, 1, rec.ResendAttemptCount)
	}
}

// Senderが部分成功とerrorを同時に返した場合でも、成功recordはdoneへ確定し、
// 再送対象recordだけをactiveへ戻してprocessing fileを残さないことを実fileで確認する。
func TestRunnerFinalizesPartialResultAndRequeuesOnlyRetryableRecord(t *testing.T) {
	sendErr := errors.New("second clickhouse group result is unknown")
	testRunnerFinalizesPartialResult(
		t,
		"partial-result-integration",
		sendErr,
		sendErr,
	)
}

// Senderが成功とretryableの完全な分類をnil errorとともに返した場合、
// 有効な4分類として、それぞれをdoneとactiveへ実file上で正しく分割する。
func TestRunnerFinalizesCompletePartialResultWithRetryableRecord(t *testing.T) {
	testRunnerFinalizesPartialResult(
		t,
		"partial-result-without-error-integration",
		nil,
		nil,
	)
}

func testRunnerFinalizesPartialResult(
	t *testing.T,
	target string,
	senderErr error,
	wantErr error,
) {
	t.Helper()

	ctx := context.Background()
	now := time.Date(2026, 7, 15, 12, 0, 0, 123000000, time.UTC)
	cfg := integrationSpoolConfig{
		spoolPath: t.TempDir(),
		target:    target,
		dir:       "clickhouse",
	}
	clock := fixedSpoolClock{now: now}

	activeStore, err := fsstorestore.NewSpoolStore(clock, cfg, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)

	successRecord := integrationSpoolRecord(t, now, "success-record", "success log")
	successRecord.RequestID = "success"
	retryableRecord := integrationSpoolRecord(t, now.Add(time.Second), "retryable-record", "retryable log")
	retryableRecord.RequestID = "retryable"
	require.NoError(t, activeStore.StoreByState(
		ctx,
		pkgspool.StateActive,
		[]pkgspool.SpoolRecord{successRecord, retryableRecord},
	))

	stateStore, err := fsworker.NewStateStore(cfg)
	require.NoError(t, err)

	sender := &partialResultSender{
		now: now.Add(5 * time.Minute),
		err: senderErr,
	}
	spoolWorker, err := worker.New(
		stateStore,
		activeSpoolWriter{store: activeStore},
		sender,
	)
	require.NoError(t, err)
	spoolWorker.TargetBackendService = "clickhouse"

	runner, err := NewRunner(
		spoolWorker,
		newTestLifecycleManager(&spoolLifecycleStoreStub{}),
		runnerConfigStub{},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)

	if wantErr == nil {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, wantErr)
	}
	assert.Equal(t, worker.ProcessStatusCompleted, report.ProcessReport.Status)
	if wantErr == nil {
		assert.NoError(t, report.ProcessReport.Err)
	} else {
		assert.ErrorIs(t, report.ProcessReport.Err, wantErr)
	}
	assert.Equal(t, 2, report.ProcessReport.RecordCount)
	assert.Equal(t, 1, report.ProcessReport.SuccessRecordCount)
	assert.Equal(t, 1, report.ProcessReport.RetryRecordCount)
	assert.Equal(t, 1, sender.calls)
	assert.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateProcessing)))

	activeRecords := readIntegrationSpoolRecords(t, activeRoot(cfg))
	require.Len(t, activeRecords, 1)
	assert.Equal(t, "retryable", activeRecords[0].RequestID)
	assert.Zero(t, activeRecords[0].FinalizedAt)
	assert.Equal(t, 1, activeRecords[0].ResendAttemptCount)
	assert.Equal(t, sender.now, activeRecords[0].LastResendAttemptedAt)

	doneRecords := readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateDone))
	require.Len(t, doneRecords, 1)
	assert.Equal(t, "success", doneRecords[0].RequestID)
	assert.False(t, doneRecords[0].FinalizedAt.IsZero())
	assert.Equal(t, 1, doneRecords[0].ResendAttemptCount)
	assert.Equal(t, sender.now, doneRecords[0].LastResendAttemptedAt)
	assert.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateDead)))
	assert.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateQuarantine)))
}

// Senderの分類結果が入力recordを完全に説明できない場合は、部分的な分類を信用せず、
// 元のprocessing file全体を内容を変えずにquarantineへ移すことを実fileで確認する。
func TestRunnerQuarantinesWholeProcessingFileForIncompleteExportReport(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 15, 13, 0, 0, 123000000, time.UTC)
	cfg := integrationSpoolConfig{
		spoolPath: t.TempDir(),
		target:    "incomplete-result-integration",
		dir:       "clickhouse",
	}
	clock := fixedSpoolClock{now: now}

	activeStore, err := fsstorestore.NewSpoolStore(clock, cfg, observer.NopTracer{}, observer.NopMeter{})
	require.NoError(t, err)

	firstRecord := integrationSpoolRecord(t, now, "reported-success-record", "reported success log")
	firstRecord.RequestID = "reported-success"
	missingRecord := integrationSpoolRecord(t, now.Add(time.Second), "missing-record", "missing result log")
	missingRecord.RequestID = "missing-result"
	seedRecords := []pkgspool.SpoolRecord{firstRecord, missingRecord}
	require.NoError(t, activeStore.StoreByState(ctx, pkgspool.StateActive, seedRecords))

	stateStore, err := fsworker.NewStateStore(cfg)
	require.NoError(t, err)

	sender := &incompleteResultSender{now: now.Add(5 * time.Minute)}
	spoolWorker, err := worker.New(
		stateStore,
		activeSpoolWriter{store: activeStore},
		sender,
	)
	require.NoError(t, err)
	spoolWorker.TargetBackendService = "clickhouse"

	runner, err := NewRunner(
		spoolWorker,
		newTestLifecycleManager(&spoolLifecycleStoreStub{}),
		runnerConfigStub{},
		observer.NopTracer{},
		observer.NopMeter{},
	)
	require.NoError(t, err)

	report, err := runner.RunOnce(ctx)

	require.ErrorIs(t, err, pkgspool.ErrInvalidExportReport)
	assert.Equal(t, worker.ProcessStatusQuarantined, report.ProcessReport.Status)
	assert.ErrorIs(t, report.ProcessReport.Err, pkgspool.ErrInvalidExportReport)
	assert.Equal(t, len(seedRecords), report.ProcessReport.RecordCount)
	assert.Zero(t, report.ProcessReport.SuccessRecordCount)
	assert.Equal(t, len(seedRecords), report.ProcessReport.QuarantineRecordCount)
	assert.Equal(t, 1, sender.calls)
	assert.Empty(t, readIntegrationSpoolRecords(t, activeRoot(cfg)))
	assert.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateProcessing)))
	assert.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateDone)))
	assert.Empty(t, readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateDead)))

	quarantineRecords := readIntegrationSpoolRecords(t, stateRoot(cfg, pkgspool.StateQuarantine))
	require.Len(t, quarantineRecords, len(seedRecords))
	assert.Equal(t, seedRecords, quarantineRecords)
	assert.Equal(t, []string{"reported-success", "missing-result"}, []string{
		quarantineRecords[0].RequestID,
		quarantineRecords[1].RequestID,
	})
	for _, record := range quarantineRecords {
		assert.Zero(t, record.ResendAttemptCount)
		assert.True(t, record.FinalizedAt.IsZero())
	}
}

type integrationSpoolConfig struct {
	spoolPath string
	target    string
	dir       string
}

func (c integrationSpoolConfig) SpoolPath() string { return c.spoolPath }
func (c integrationSpoolConfig) Target() string    { return c.target }
func (c integrationSpoolConfig) Dir() string       { return c.dir }
func (c integrationSpoolConfig) FileNameFormat() string {
	return ""
}
func (c integrationSpoolConfig) FileDateFormat() string {
	return "20060102"
}
func (c integrationSpoolConfig) MaxWriteRetries() int {
	return 1
}
func (c integrationSpoolConfig) FileOperationRetryWaitTime() time.Duration {
	return time.Millisecond
}
func (c integrationSpoolConfig) OverloadPolicyConfig() spoolstore.SpoolStoreOverloadPolicyConfigProvider {
	return integrationSpoolOverloadConfig{}
}

type integrationSpoolOverloadConfig struct{}

func (integrationSpoolOverloadConfig) OverloadPolicy() durable.OverloadPolicy {
	return durable.OverloadPolicy("drop")
}
func (integrationSpoolOverloadConfig) SpillDestination() durable.SpillDestination {
	return durable.SpillDestination("stdout")
}
func (integrationSpoolOverloadConfig) MaxCallsSinceScan() uint64 { return 0 }
func (integrationSpoolOverloadConfig) StatsScanInterval() time.Duration {
	return 0
}
func (integrationSpoolOverloadConfig) MaxDirSize() uint64                 { return 0 }
func (integrationSpoolOverloadConfig) ActiveBacklogWarnAfterDays() int    { return 0 }
func (integrationSpoolOverloadConfig) MinAvailableDiskSpace() uint64      { return 0 }
func (integrationSpoolOverloadConfig) UseMemoryPressureForOverload() bool { return false }
func (integrationSpoolOverloadConfig) MemoryCriticalRatio() float64       { return 0 }
func (integrationSpoolOverloadConfig) MinAvailableMemoryBytes() uint64    { return 0 }
func (integrationSpoolOverloadConfig) MemoryCriticalConsecutiveHits() int { return 0 }

type fixedSpoolClock struct {
	now time.Time
}

func (c fixedSpoolClock) Now() time.Time {
	return c.now
}

type activeSpoolWriter struct {
	store *fsstorestore.SpoolStore
}

func (w activeSpoolWriter) StoreActive(ctx context.Context, records []pkgspool.SpoolRecord) error {
	return w.store.StoreByState(ctx, pkgspool.StateActive, records)
}

type recordingSuccessSender struct {
	now     time.Time
	records []pkgspool.SpoolRecord
}

type partialResultSender struct {
	now   time.Time
	err   error
	calls int
}

type incompleteResultSender struct {
	now   time.Time
	calls int
}

func (s *incompleteResultSender) Send(
	ctx context.Context,
	records []pkgspool.SpoolRecord,
) (pkgspool.ExportReport, error) {
	_ = ctx

	s.calls++
	report := make(pkgspool.ExportReport, 0, 1)
	if len(records) == 0 {
		return report, nil
	}

	// index 0だけを成功にして残りを未分類のまま返し、Workerの構造検証で
	// processing file全体がquarantineへ隔離されることを確認する。
	report = append(report, pkgspool.RecordOutcome{
		InputIndex:  0,
		Status:      pkgspool.RecordOutcomeStatusSuccess,
		AttemptedAt: s.now,
	})
	return report, nil
}

func (s *partialResultSender) Send(
	ctx context.Context,
	records []pkgspool.SpoolRecord,
) (pkgspool.ExportReport, error) {
	_ = ctx

	s.calls++
	report := make(pkgspool.ExportReport, 0, len(records))
	if len(records) != 2 {
		return report, errors.New("partial result sender requires exactly two records")
	}

	retryableErr := s.err
	if retryableErr == nil {
		retryableErr = errors.New("record remains retryable")
	}
	report = append(report,
		pkgspool.RecordOutcome{
			InputIndex:  0,
			Status:      pkgspool.RecordOutcomeStatusSuccess,
			AttemptedAt: s.now,
		},
		pkgspool.RecordOutcome{
			InputIndex:  1,
			Status:      pkgspool.RecordOutcomeStatusRetryable,
			AttemptedAt: s.now,
			Err:         retryableErr,
		},
	)
	return report, s.err
}

func (s *recordingSuccessSender) Send(
	ctx context.Context,
	records []pkgspool.SpoolRecord,
) (pkgspool.ExportReport, error) {
	_ = ctx

	report := make(pkgspool.ExportReport, 0, len(records))
	for inputIndex, record := range records {
		report = append(report, pkgspool.RecordOutcome{
			InputIndex:  inputIndex,
			Status:      pkgspool.RecordOutcomeStatusSuccess,
			AttemptedAt: s.now,
		})
		record.Payload = append([]byte(nil), record.Payload...)
		s.records = append(s.records, record)
	}

	return report, nil
}

func integrationSpoolRecord(
	t *testing.T,
	occurredAt time.Time,
	id string,
	message string,
) pkgspool.SpoolRecord {
	t.Helper()

	payload, err := json.Marshal(map[string]string{
		"test_id":      id,
		"test_message": message,
	})
	require.NoError(t, err)

	return pkgspool.SpoolRecord{
		OccurredAt: occurredAt,
		RequestID:  id,
		Payload:    payload,
	}
}

func activeRoot(cfg integrationSpoolConfig) string {
	return stateRoot(cfg, pkgspool.StateActive)
}

func stateRoot(cfg integrationSpoolConfig, state pkgspool.State) string {
	return filepath.Join(cfg.spoolPath, cfg.target, "spool", cfg.dir, string(state))
}

func readIntegrationSpoolRecords(t *testing.T, root string) []pkgspool.SpoolRecord {
	t.Helper()

	var records []pkgspool.SpoolRecord
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != spooldir.SpoolExt {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record pkgspool.SpoolRecord
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return err
			}
			records = append(records, record)
		}
		return scanner.Err()
	})
	require.NoError(t, err)

	return records
}

func logSpoolRecords(t *testing.T, label string, records []pkgspool.SpoolRecord, limit int) {
	t.Helper()

	if limit > len(records) {
		limit = len(records)
	}
	for i := 0; i < limit; i++ {
		t.Logf(
			"%s[%d] occurred_at=%s finalized_at=%s resend_count=%d payload=%s",
			label,
			i,
			records[i].OccurredAt.Format(time.RFC3339Nano),
			records[i].FinalizedAt.Format(time.RFC3339Nano),
			records[i].ResendAttemptCount,
			string(records[i].Payload),
		)
	}
}

var _ worker.ActiveSpoolWriter = activeSpoolWriter{}
var _ worker.Sender = (*recordingSuccessSender)(nil)
var _ worker.Sender = (*partialResultSender)(nil)
var _ worker.Sender = (*incompleteResultSender)(nil)

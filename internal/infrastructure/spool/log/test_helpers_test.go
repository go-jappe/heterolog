package logspool

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle"
	lifecyclereport "github.com/go-jappe/heterolog/internal/infrastructure/spool/log/lifecycle/report"
	"github.com/go-jappe/heterolog/internal/infrastructure/spool/log/worker"
	pkgspool "github.com/go-jappe/heterolog/pkg/spool"
)

// hangDetectionTimeoutは、背景のgoroutineの進行を実時間で待つ試験が、処理が止まったと判断する
// までの上限です。synctestのbubble内では仮想時間の上限になります。
const hangDetectionTimeout = 10 * time.Second

// resolvedTempDirForTestは、symlinkを解決したtemporary directoryを返します。
// macOSの/var→/private/varのようにOSのtemporary directoryがsymlinkを経由しても、
// productionが返す物理pathと試験が組み立てるpathを同じ表記で比較できます。
func resolvedTempDirForTest(t testing.TB) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temp dir: %v", err)
	}
	return dir
}

type runnerConfigStub struct {
	interval    time.Duration
	stopOnError bool
}

func (c runnerConfigStub) Interval() time.Duration {
	return c.interval
}

func (c runnerConfigStub) StopOnError() bool {
	return c.stopOnError
}

type supervisorConfigStub struct {
	restartDelay time.Duration
	maxRestarts  int
}

func (c supervisorConfigStub) RestartDelay() time.Duration {
	return c.restartDelay
}

func (c supervisorConfigStub) MaxRestarts() int {
	return c.maxRestarts
}

type spoolLifecycleStoreStub struct {
	calls []string

	doneReport       lifecyclereport.PurgeReport
	deadReport       lifecyclereport.PurgeReport
	quarantineReport lifecyclereport.PurgeReport

	doneErr       error
	deadErr       error
	quarantineErr error
}

var _ lifecycle.Store = (*spoolLifecycleStoreStub)(nil)

func (s *spoolLifecycleStoreStub) PurgeDoneIfNeed(
	ctx context.Context,
) (lifecyclereport.PurgeReport, error) {
	_ = ctx

	s.calls = append(s.calls, "purge_done")
	return s.doneReport, s.doneErr
}

func (s *spoolLifecycleStoreStub) PurgeDeadIfNeed(
	ctx context.Context,
) (lifecyclereport.PurgeReport, error) {
	_ = ctx

	s.calls = append(s.calls, "purge_dead")
	return s.deadReport, s.deadErr
}

func (s *spoolLifecycleStoreStub) PurgeQuarantineIfNeed(
	ctx context.Context,
) (lifecyclereport.PurgeReport, error) {
	_ = ctx

	s.calls = append(s.calls, "purge_quarantine")
	return s.quarantineReport, s.quarantineErr
}

type spoolWorkerStoreStub struct {
	processingPaths []string
	claimPath       string
	claimOK         bool
	claimErr        error

	readRecords map[string][]pkgspool.SpoolRecord
	readErr     map[string]error

	exists map[string]bool

	moveDoneErr       error
	moveDeadErr       error
	moveQuarantineErr error
	removeErr         error

	storeDoneErr       error
	storeDeadErr       error
	storeQuarantineErr error

	calls            []string
	storedDone       []spoolStoreRecordsCall
	storedDead       []spoolStoreRecordsCall
	storedQuarantine []spoolStoreRecordsCall
}

type spoolStoreRecordsCall struct {
	path    string
	records []pkgspool.SpoolRecord
}

var _ worker.WorkFileStore = (*spoolWorkerStoreStub)(nil)

func (s *spoolWorkerStoreStub) ListProcessingFileCandidatesAfter(
	ctx context.Context,
	afterRelativePath string,
	limit int,
) ([]pkgspool.FileCandidate, error) {
	s.calls = append(s.calls, "list_processing_candidates_after")
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return firstSpoolCandidatesAfter(s.processingPaths, afterRelativePath, limit), nil
}

func (s *spoolWorkerStoreStub) ClaimOldestActiveFile(ctx context.Context) (string, bool, error) {
	_ = ctx

	s.calls = append(s.calls, "claim_active")
	return s.claimPath, s.claimOK, s.claimErr
}

func (s *spoolWorkerStoreStub) ReadProcessingRecords(
	ctx context.Context,
	processingPath string,
) ([]pkgspool.SpoolRecord, error) {
	_ = ctx

	s.calls = append(s.calls, "read:"+processingPath)
	if s.readErr != nil && s.readErr[processingPath] != nil {
		return nil, s.readErr[processingPath]
	}

	return cloneSpoolRecords(s.readRecords[processingPath]), nil
}

func (s *spoolWorkerStoreStub) MoveProcessingToDone(ctx context.Context, processingPath string) error {
	_ = ctx

	s.calls = append(s.calls, "move_done:"+processingPath)
	return s.moveDoneErr
}

func (s *spoolWorkerStoreStub) MoveProcessingToDead(ctx context.Context, processingPath string) error {
	_ = ctx

	s.calls = append(s.calls, "move_dead:"+processingPath)
	return s.moveDeadErr
}

func (s *spoolWorkerStoreStub) MoveProcessingToQuarantine(
	ctx context.Context,
	processingPath string,
	reason pkgspool.QuarantineReason,
) error {
	_ = ctx
	_ = reason

	s.calls = append(s.calls, "move_quarantine:"+processingPath)
	return s.moveQuarantineErr
}

func (s *spoolWorkerStoreStub) ProcessingFileExists(
	ctx context.Context,
	processingPath string,
) (bool, error) {
	_ = ctx

	s.calls = append(s.calls, "exists:"+processingPath)
	if s.exists != nil {
		return s.exists[processingPath], nil
	}

	return true, nil
}

func (s *spoolWorkerStoreStub) RemoveProcessingFile(ctx context.Context, processingPath string) error {
	_ = ctx

	s.calls = append(s.calls, "remove:"+processingPath)
	return s.removeErr
}

func (s *spoolWorkerStoreStub) StoreProcessingRecordsToDone(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) error {
	_ = ctx

	s.calls = append(s.calls, "store_done:"+processingPath)
	if s.storeDoneErr != nil {
		return s.storeDoneErr
	}
	s.storedDone = append(s.storedDone, spoolStoreRecordsCall{
		path:    processingPath,
		records: cloneSpoolRecords(records),
	})

	return nil
}

func (s *spoolWorkerStoreStub) StoreProcessingRecordsToDead(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) error {
	_ = ctx

	s.calls = append(s.calls, "store_dead:"+processingPath)
	if s.storeDeadErr != nil {
		return s.storeDeadErr
	}
	s.storedDead = append(s.storedDead, spoolStoreRecordsCall{
		path:    processingPath,
		records: cloneSpoolRecords(records),
	})

	return nil
}

func (s *spoolWorkerStoreStub) StoreProcessingRecordsToQuarantine(
	ctx context.Context,
	processingPath string,
	records []pkgspool.SpoolRecord,
) error {
	_ = ctx

	s.calls = append(s.calls, "store_quarantine:"+processingPath)
	if s.storeQuarantineErr != nil {
		return s.storeQuarantineErr
	}
	s.storedQuarantine = append(s.storedQuarantine, spoolStoreRecordsCall{
		path:    processingPath,
		records: cloneSpoolRecords(records),
	})

	return nil
}

type spoolActiveStoreStub struct {
	err     error
	records [][]pkgspool.SpoolRecord
}

var _ worker.ActiveSpoolWriter = (*spoolActiveStoreStub)(nil)

func (s *spoolActiveStoreStub) StoreActive(
	ctx context.Context,
	records []pkgspool.SpoolRecord,
) error {
	_ = ctx

	if s.err != nil {
		return s.err
	}
	s.records = append(s.records, cloneSpoolRecords(records))

	return nil
}

type spoolSenderStub struct {
	report pkgspool.ExportReport
	err    error

	calls   int
	records []pkgspool.SpoolRecord
}

func newSpoolExportReport(
	status pkgspool.RecordOutcomeStatus,
	recordCount int,
	attemptedAt time.Time,
) pkgspool.ExportReport {
	report := make(pkgspool.ExportReport, 0, recordCount)
	for inputIndex := 0; inputIndex < recordCount; inputIndex++ {
		var outcomeErr error
		if status != pkgspool.RecordOutcomeStatusSuccess {
			outcomeErr = errors.New("test spool outcome failure")
		}
		report = append(report, pkgspool.RecordOutcome{
			InputIndex:  inputIndex,
			Status:      status,
			AttemptedAt: attemptedAt,
			Err:         outcomeErr,
		})
	}
	return report
}

var _ worker.Sender = (*spoolSenderStub)(nil)

func (s *spoolSenderStub) Send(
	ctx context.Context,
	records []pkgspool.SpoolRecord,
) (pkgspool.ExportReport, error) {
	_ = ctx

	s.calls++
	s.records = cloneSpoolRecords(records)
	return s.report, s.err
}

type recordingRunnerReporter struct {
	calls []runnerReportCall
}

type runnerReportCall struct {
	report RunReport
	err    error
}

var _ RunnerReporter = (*recordingRunnerReporter)(nil)

func (r *recordingRunnerReporter) ReportSpoolRunnerRun(
	ctx context.Context,
	report RunReport,
	err error,
) {
	_ = ctx

	r.calls = append(r.calls, runnerReportCall{
		report: report,
		err:    err,
	})
}

func newTestLifecycleManager(store *spoolLifecycleStoreStub) *lifecycle.Manager {
	return &lifecycle.Manager{Store: store}
}

func newTestRunner(
	t testing.TB,
	spoolWorker *worker.Worker,
	manager *lifecycle.Manager,
	cfg RunnerConfig,
) *Runner {
	t.Helper()

	runner, err := NewRunner(spoolWorker, manager, cfg, nil, nil)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}
	return runner
}

func newTestWorker(
	store *spoolWorkerStoreStub,
	sender *spoolSenderStub,
) *worker.Worker {
	return &worker.Worker{
		Store:             store,
		ActiveSpoolWriter: &spoolActiveStoreStub{},
		Sender:            sender,
	}
}

func firstSpoolCandidatesAfter(paths []string, afterRelativePath string, limit int) []pkgspool.FileCandidate {
	if limit <= 0 {
		return nil
	}
	candidates := make([]pkgspool.FileCandidate, 0, len(paths))
	for _, path := range paths {
		relativePath := strings.TrimPrefix(path, "processing/")
		if relativePath > afterRelativePath {
			candidates = append(candidates, pkgspool.FileCandidate{Path: path, RelativePath: relativePath})
		}
	}
	slices.SortFunc(candidates, func(a, b pkgspool.FileCandidate) int {
		return strings.Compare(a.RelativePath, b.RelativePath)
	})
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}

func cloneSpoolRecords(records []pkgspool.SpoolRecord) []pkgspool.SpoolRecord {
	if len(records) == 0 {
		return nil
	}

	out := make([]pkgspool.SpoolRecord, len(records))
	copy(out, records)
	return out
}

func spoolRecord(id string) pkgspool.SpoolRecord {
	return pkgspool.SpoolRecord{
		RequestID: id,
		Payload:   []byte(`{"id":"` + id + `"}`),
	}
}

func lifecycleReportWithDeletedFiles(count int) lifecyclereport.PurgeReport {
	return lifecyclereport.PurgeReport{
		ByAge: lifecyclereport.PurgeStepReport{
			Executed:         true,
			DeletedFileCount: count,
		},
	}
}

func panicWorker(t testing.TB) *worker.Worker {
	t.Helper()
	retry, err := worker.NewProcessingFileTransitionRetry(worker.NewProcessingFileTransitionRetryConfigAdapter(worker.ProcessingFileTransitionRetryConfig{
		RetryDelay: time.Nanosecond,
		MaxEntries: 1,
	}))
	if err != nil {
		t.Fatalf("NewProcessingFileTransitionRetry() error = %v", err)
	}
	retry.Upsert(
		"processing/panic",
		worker.ProcessingFileTransitionRetryOpMoveProcessingToDone,
		errors.New("seed"),
		time.Now().Add(-time.Second),
	)

	return &worker.Worker{
		ProcessingFileTransitionRetry: retry,
	}
}
